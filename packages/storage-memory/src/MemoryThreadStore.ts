import { digestCanonicalBatch, EMPTY_TAIL_DIGEST } from "@yielded/agent/digest";
import { ThreadId } from "@yielded/agent/identifiers";
import {
  type ProducerEpoch,
  type RecordId,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  ObservationOffset,
  type BatchId,
  type Digest,
} from "@yielded/agent/records";
import {
  runIdForSubmission,
  subagentLineageRecordId,
  workerOriginRecordId,
} from "@yielded/agent/run-journal";
import {
  type ThreadCheckpoint,
  ThreadPeerCountRequest,
  ThreadIdentity,
  ThreadIdentityRequest,
  AppendConflict,
  AppendResult,
  CheckpointRejected,
  ThreadExportRequest,
  ThreadExport,
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadObservation,
  ThreadReadRequest,
  ThreadStore,
  ThreadReader,
  type ThreadCheckpoints,
  ThreadStoreError,
  ThreadTail,
  ThreadTailRequest,
  FenceRejected,
  FencedAppendRequest,
  LoadCheckpointRequest,
  SaveCheckpointRequest,
  SaveRecoveryCheckpointRequest,
  type ThreadRecoveryCheckpoints,
  MAX_THREAD_EXPORT_RECORDS,
} from "@yielded/agent/thread-store";
import {
  Context,
  Crypto,
  Effect,
  Layer,
  Option,
  PubSub,
  Ref,
  Schema,
  Semaphore,
  Stream,
} from "effect";
import { Base64 } from "effect/encoding";

import {
  MemoryThreadStoreKernel,
  type PreparedMemoryAppend,
} from "./internal/MemoryThreadStoreKernel.ts";

const MAX_THREADS = 256;
const MAX_RECORDS_PER_THREAD = MAX_THREAD_EXPORT_RECORDS;
const MAX_CHECKPOINTS_PER_THREAD = 1_024;

const ThreadCapacity = Context.Reference<number>(
  "@effect-agent/storage-memory/MemoryThreadStore/ThreadCapacity",
  { defaultValue: () => MAX_THREADS },
);

interface StoredBatch {
  readonly digest: Digest;
  readonly result: AppendResult;
}

interface StoredThread {
  readonly peerCount: number;
  readonly workerRecords: ReadonlyMap<string, ReadonlyArray<CanonicalRecordEnvelope>>;
  readonly byId: ReadonlyMap<string, CanonicalRecordEnvelope>;
  readonly runInputs: ReadonlyMap<string, CanonicalRecordEnvelope | null>;
  readonly producerEpoch: ProducerEpoch;
  readonly tailSequence: CanonicalSequence;
  readonly tailDigest: Digest;
  readonly records: ReadonlyArray<CanonicalRecordEnvelope>;
  readonly recordIds: ReadonlySet<RecordId>;
  readonly batches: ReadonlyMap<BatchId, StoredBatch>;
  readonly tailDigests: ReadonlyMap<CanonicalSequence, Digest>;
  readonly checkpoints: ReadonlyMap<CanonicalSequence, ThreadCheckpoint>;
  readonly recoveryCheckpoint?: ThreadCheckpoint;
}

interface MemoryState {
  readonly threads: ReadonlyMap<ThreadId, StoredThread>;
}

type AppendDecision =
  | {
      readonly _tag: "failure";
      readonly error: ThreadStoreError | ThreadNotMaterialized | AppendConflict | FenceRejected;
    }
  | {
      readonly _tag: "success";
      readonly result: AppendResult;
      readonly records: ReadonlyArray<CanonicalRecordEnvelope>;
    };

type MaterializeDecision =
  | { readonly _tag: "failure"; readonly error: ThreadStoreError | FenceRejected }
  | { readonly _tag: "success" };

type CheckpointDecision =
  | {
      readonly _tag: "failure";
      readonly error: ThreadNotMaterialized | ThreadStoreError | CheckpointRejected;
    }
  | { readonly _tag: "success" };

const storeError = (operation: string, message: string, cause?: unknown): ThreadStoreError =>
  cause === undefined
    ? ThreadStoreError.make({ operation, message })
    : ThreadStoreError.make({ operation, message, cause });

const validate = Effect.fn("MemoryThreadStore.validate")(
  <A, I>(
    schema: Schema.Codec<A, I>,
    operation: string,
    value: unknown,
  ): Effect.Effect<A, ThreadStoreError> =>
    Schema.encodeUnknownEffect(schema)(value).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(schema)),
      Effect.mapError((error) => storeError(operation, `Invalid ${operation} request`, error)),
    ),
);

const decodeCanonicalSequence = Schema.decodeSync(CanonicalSequence);
const ZERO_CANONICAL_SEQUENCE = decodeCanonicalSequence(0);

const offsetSequence = Effect.fn("MemoryThreadStore.offsetSequence")((
  threadId: ThreadId,
  offset: ObservationOffset | undefined,
): Effect.Effect<CanonicalSequence, ThreadStoreError> => {
  if (offset === undefined) return Effect.succeed(ZERO_CANONICAL_SEQUENCE);
  const prefix = `memory:v1:${Base64.encode(threadId)}:`;
  const encodedSequence = offset.startsWith(prefix) ? offset.slice(prefix.length) : "";

  if (!/^\d+$/.test(encodedSequence)) {
    return Effect.fail(storeError("observe", "Malformed observation offset"));
  }
  const sequence = Number(encodedSequence);

  return Number.isSafeInteger(sequence)
    ? Schema.decodeEffect(CanonicalSequence)(sequence).pipe(
        Effect.mapError(() => storeError("observe", "Malformed observation offset")),
      )
    : Effect.fail(storeError("observe", "Malformed observation offset"));
});

const observationOffset = (threadId: ThreadId, sequence: CanonicalSequence): ObservationOffset =>
  Schema.decodeSync(ObservationOffset)(`memory:v1:${Base64.encode(threadId)}:${sequence}`);

const findThread = Effect.fn("MemoryThreadStore.findThread")((
  state: MemoryState,
  threadId: ThreadId,
): Effect.Effect<StoredThread, ThreadNotMaterialized> => {
  const thread = state.threads.get(threadId);

  return thread === undefined
    ? Effect.fail(ThreadNotMaterialized.make({ threadId }))
    : Effect.succeed(thread);
});

const CheckpointVersionEnvelope = Schema.Struct({
  checkpoint: Schema.Struct({
    threadId: ThreadId,
    schemaVersion: Schema.Natural,
  }),
});

const validateCheckpointVersion = Effect.fn("MemoryThreadStore.validateCheckpointVersion")(
  function* (value: unknown): Effect.fn.Return<void, ThreadStoreError | CheckpointRejected> {
    const envelope = yield* Schema.decodeUnknownEffect(CheckpointVersionEnvelope)(value).pipe(
      Effect.mapError(() => storeError("saveCheckpoint", "Invalid saveCheckpoint request")),
    );

    if (envelope.checkpoint.schemaVersion !== 1) {
      return yield* CheckpointRejected.make({
        threadId: envelope.checkpoint.threadId,
        reason: "unsupported-version",
      });
    }
  },
);

const makeThreadStore = Effect.gen(function* () {
  const maxThreads = yield* ThreadCapacity;
  const crypto = yield* Crypto.Crypto;
  const state = yield* Ref.make<MemoryState>({ threads: new Map() });
  const gate = yield* Semaphore.make(1);
  const withMutation = gate.withPermits(1);
  const updates = yield* PubSub.sliding<void>(1);

  yield* Effect.addFinalizer(() => PubSub.shutdown(updates));

  const materialize: ThreadStore["Service"]["materialize"] = Effect.fn(
    "MemoryThreadStore.materialize",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(ThreadMaterialization, "materialize", unvalidated);

      const decision = yield* Ref.modify(
        state,
        (current): readonly [MaterializeDecision, MemoryState] => {
          const existing = current.threads.get(request.threadId);

          if (existing !== undefined) {
            if (request.producerEpoch < existing.producerEpoch) {
              return [
                {
                  _tag: "failure",
                  error: FenceRejected.make({
                    threadId: request.threadId,
                    actualEpoch: existing.producerEpoch,
                    attemptedEpoch: request.producerEpoch,
                  }),
                },
                current,
              ];
            }
            if (request.producerEpoch === existing.producerEpoch) {
              return [{ _tag: "success" }, current];
            }
            const threads = new Map(current.threads);

            threads.set(request.threadId, {
              ...existing,
              producerEpoch: request.producerEpoch,
            });

            return [{ _tag: "success" }, { threads }];
          }
          if (current.threads.size >= maxThreads) {
            return [
              {
                _tag: "failure",
                error: storeError("materialize", `In-memory thread limit ${maxThreads} exceeded`),
              },
              current,
            ];
          }
          const threads = new Map(current.threads);

          threads.set(request.threadId, {
            producerEpoch: request.producerEpoch,
            tailSequence: ZERO_CANONICAL_SEQUENCE,
            tailDigest: EMPTY_TAIL_DIGEST,
            byId: new Map(),
            workerRecords: new Map(),
            peerCount: 0,
            runInputs: new Map(),
            records: [],
            recordIds: new Set(),
            batches: new Map(),
            tailDigests: new Map([[ZERO_CANONICAL_SEQUENCE, EMPTY_TAIL_DIGEST]]),
            checkpoints: new Map(),
          });

          return [{ _tag: "success" }, { threads }];
        },
      );

      if (decision._tag === "failure") return yield* decision.error;
    }),
  );

  const prepareAppend = Effect.fnUntraced(function* (unvalidated: FencedAppendRequest) {
    const codec = Schema.fromJsonString(FencedAppendRequest);

    const request = yield* Schema.encodeUnknownEffect(codec)(unvalidated).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(codec)),
      Effect.mapError((error) => storeError("append", "Invalid append request", error)),
    );

    const digest = yield* digestCanonicalBatch(request.expectedTailDigest, request.batch).pipe(
      Effect.provideService(Crypto.Crypto, crypto),
      Effect.mapError((error) => storeError("append", error.message, error)),
    );

    return { request, digest };
  });

  const appendPrepared = Effect.fnUntraced(function* ({ request, digest }: PreparedMemoryAppend) {
    const decision = yield* Effect.uninterruptible(
      Ref.modify(state, (current): readonly [AppendDecision, MemoryState] => {
        const thread = current.threads.get(request.threadId);

        if (thread === undefined) {
          return [
            {
              _tag: "failure",
              error: ThreadNotMaterialized.make({
                threadId: request.threadId,
              }),
            },
            current,
          ];
        }
        if (request.producerEpoch !== thread.producerEpoch) {
          return [
            {
              _tag: "failure",
              error: FenceRejected.make({
                threadId: request.threadId,
                actualEpoch: thread.producerEpoch,
                attemptedEpoch: request.producerEpoch,
              }),
            },
            current,
          ];
        }
        const previous = thread.batches.get(request.batch.batchId);

        if (previous !== undefined) {
          if (previous.digest !== digest) {
            return [
              {
                _tag: "failure",
                error: AppendConflict.make({
                  threadId: request.threadId,
                  batchId: request.batch.batchId,
                  reason: "batch-digest",
                }),
              },
              current,
            ];
          }

          return [
            {
              _tag: "success",
              result: AppendResult.make({
                firstSequence: previous.result.firstSequence,
                lastSequence: previous.result.lastSequence,
                tailDigest: previous.result.tailDigest,
                replayed: true,
              }),
              records: [],
            },
            current,
          ];
        }
        if (
          request.expectedTailSequence !== thread.tailSequence ||
          request.expectedTailDigest !== thread.tailDigest
        ) {
          return [
            {
              _tag: "failure",
              error: AppendConflict.make({
                threadId: request.threadId,
                batchId: request.batch.batchId,
                reason: "tail",
                actualTailSequence: thread.tailSequence,
                actualTailDigest: thread.tailDigest,
              }),
            },
            current,
          ];
        }
        if (thread.records.length + request.batch.records.length > MAX_RECORDS_PER_THREAD) {
          return [
            {
              _tag: "failure",
              error: storeError(
                "append",
                `In-memory record limit ${MAX_RECORDS_PER_THREAD} exceeded`,
              ),
            },
            current,
          ];
        }

        const batchRecordIds = new Set<RecordId>();

        for (const record of request.batch.records) {
          if (thread.recordIds.has(record.recordId) || batchRecordIds.has(record.recordId)) {
            return [
              {
                _tag: "failure",
                error: AppendConflict.make({
                  threadId: request.threadId,
                  batchId: request.batch.batchId,
                  reason: "record-identity",
                }),
              },
              current,
            ];
          }
          batchRecordIds.add(record.recordId);
        }

        const records = request.batch.records.map((record, index) => {
          const sequence = decodeCanonicalSequence(thread.tailSequence + index + 1);

          return CanonicalRecordEnvelope.make({
            threadId: request.threadId,
            batchId: request.batch.batchId,
            sequence,
            offset: observationOffset(request.threadId, sequence),
            record,
          });
        });

        const lastSequence = decodeCanonicalSequence(thread.tailSequence + records.length);

        const result = AppendResult.make({
          firstSequence: decodeCanonicalSequence(thread.tailSequence + 1),
          lastSequence,
          tailDigest: digest,
          replayed: false,
        });

        const batches = new Map(thread.batches);

        batches.set(request.batch.batchId, { digest, result });
        const recordIds = new Set(thread.recordIds);

        for (const recordId of batchRecordIds) recordIds.add(recordId);
        const tailDigests = new Map(thread.tailDigests);

        tailDigests.set(lastSequence, digest);
        const threads = new Map(current.threads);

        let peerCount = thread.peerCount;
        const workerRecords = new Map(thread.workerRecords);
        const byId = new Map(thread.byId);
        const runInputs = new Map(thread.runInputs);

        for (const entry of records) {
          byId.set(entry.record.recordId, entry);
          const payload = entry.record.payload;

          if (payload._tag === "PeerMessagePrepared") peerCount++;
          if (
            (payload._tag === "UserInputRecorded" || payload._tag === "RunStarted") &&
            payload.runId !== undefined
          )
            workerRecords.set(`execution:${payload._tag}`, [entry]);

          const workerKey =
            payload._tag === "SubtreeBudgetReserved"
              ? `subtree:${payload.sourceSubmissionId ?? ""}`
              : payload._tag === "SubagentJoined"
                ? `joined:${payload.runId}`
                : [
                      "ThreadCreated",
                      "WorkerOriginRecorded",
                      "SubagentLineageRecorded",
                      "WorkerInputRequested",
                      "WorkerInputCompleted",
                      "WorkerStopRequested",
                    ].includes(payload._tag)
                  ? "worker"
                  : undefined;

          if (workerKey !== undefined)
            workerRecords.set(workerKey, [...(workerRecords.get(workerKey) ?? []), entry]);

          if (
            payload._tag === "UserInputRecorded" &&
            payload.kind === "user" &&
            payload.runId !== undefined
          )
            runInputs.set(payload.runId, runInputs.has(payload.runId) ? null : entry);
        }
        threads.set(request.threadId, {
          ...thread,
          byId,
          workerRecords,
          peerCount,
          runInputs,
          tailSequence: lastSequence,
          tailDigest: digest,
          records: [...thread.records, ...records],
          recordIds,
          batches,
          tailDigests,
        });

        return [{ _tag: "success", result, records }, { threads }];
      }).pipe(
        Effect.tap((decision) =>
          decision._tag === "success" && decision.records.length > 0
            ? PubSub.publish(updates, undefined)
            : Effect.void,
        ),
      ),
    );

    if (decision._tag === "failure") return yield* decision.error;

    return decision.result;
  });

  const append: ThreadStore["Service"]["append"] = Effect.fn("MemoryThreadStore.append")(
    (request) =>
      prepareAppend(request).pipe(
        Effect.flatMap((prepared) => withMutation(appendPrepared(prepared))),
      ),
  );

  const countPeerMessages: NonNullable<ThreadStore["Service"]["countPeerMessages"]> =
    Effect.fnUntraced(function* (request) {
      yield* validate(ThreadPeerCountRequest, "countPeerMessages", request);
      const thread = yield* findThread(yield* Ref.get(state), request.threadId);

      return Math.min(thread.peerCount, request.limit);
    });

  const readSnapshot = Effect.fn("MemoryThreadStore.readSnapshot")(
    (threadId: ThreadId, afterSequence: CanonicalSequence | undefined, limit: number) =>
      Ref.get(state).pipe(
        Effect.flatMap((current) => findThread(current, threadId)),
        Effect.map((thread) => {
          // Append assigns gap-free sequences starting at 1, so the exclusive cursor is an index.
          const start = afterSequence ?? ZERO_CANONICAL_SEQUENCE;

          return thread.records.slice(start, start + limit);
        }),
      ),
  );

  const read: ThreadStore["Service"]["read"] = (unvalidated) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const request = yield* validate(ThreadReadRequest, "read", unvalidated);

        if ("selection" in request) {
          const thread = yield* findThread(yield* Ref.get(state), request.threadId);
          const selection = request.selection;

          if (
            "expectedTailSequence" in selection &&
            (selection.expectedTailSequence !== thread.tailSequence ||
              selection.expectedTailDigest !== thread.tailDigest)
          )
            return yield* storeError("selected read", "Canonical tail changed");
          let records: ReadonlyArray<CanonicalRecordEnvelope>;

          switch (selection._tag) {
            case "RecordId": {
              const record = thread.byId.get(selection.recordId);

              records = record === undefined ? [] : [record];
              break;
            }
            case "RunInput": {
              const input = thread.runInputs.get(selection.runId);

              if (input === null)
                return yield* storeError("selected read", "Ambiguous original Run input");
              records = input === undefined ? [] : [input];
              break;
            }
            case "WorkerExecution":
              records = ["UserInputRecorded", "RunStarted"].flatMap(
                (tag) => thread.workerRecords.get(`execution:${tag}`) ?? [],
              );
              break;
            case "WorkerState":
              records = [
                ...(thread.workerRecords.get("worker") ?? []),
                ...(thread.workerRecords.get(`subtree:${selection.sourceSubmissionId ?? ""}`) ??
                  []),
                ...(selection.sourceSubmissionId === undefined
                  ? []
                  : (thread.workerRecords.get(
                      `joined:${runIdForSubmission(selection.sourceSubmissionId)}`,
                    ) ?? [])),
              ];
              break;
          }

          return Stream.fromIterable(
            records
              .filter((entry) => entry.sequence > (request.page.afterSequence ?? 0))
              .sort((a, b) => a.sequence - b.sequence)
              .slice(0, request.page.limit),
          );
        }
        const records = yield* readSnapshot(request.threadId, request.afterSequence, request.limit);

        return Stream.fromIterable(records);
      }),
    );

  const observe: ThreadStore["Service"]["observe"] = (unvalidated) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const request = yield* validate(ThreadObservation, "observe", unvalidated);
        const afterSequence = yield* offsetSequence(request.threadId, request.afterOffset);

        return Stream.unwrap(
          Effect.gen(function* () {
            const subscription = yield* PubSub.subscribe(updates);

            const initial = yield* readSnapshot(
              request.threadId,
              afterSequence,
              MAX_RECORDS_PER_THREAD,
            );

            const highWater =
              initial.length === 0 ? afterSequence : (initial.at(-1)?.sequence ?? afterSequence);

            const live = Stream.fromEffectRepeat(PubSub.take(subscription)).pipe(
              Stream.mapAccumEffect(
                () => highWater,
                (lastSequence) =>
                  readSnapshot(request.threadId, lastSequence, MAX_RECORDS_PER_THREAD).pipe(
                    Effect.map(
                      (records) => [records.at(-1)?.sequence ?? lastSequence, records] as const,
                    ),
                  ),
              ),
            );

            return Stream.fromIterable(initial).pipe(Stream.concat(live));
          }),
        );
      }),
    );

  const exportThread: ThreadStore["Service"]["export"] = Effect.fn("MemoryThreadStore.export")(
    (unvalidated) =>
      Effect.gen(function* () {
        const request = yield* validate(ThreadExportRequest, "export", unvalidated);

        const thread = yield* Ref.get(state).pipe(
          Effect.flatMap((current) => findThread(current, request.threadId)),
        );

        return ThreadExport.make({
          format: "effect-agent/thread@1",
          threadId: request.threadId,
          tailSequence: thread.tailSequence,
          tailDigest: thread.tailDigest,
          records: thread.records,
        });
      }),
  );

  const inspectTail: ThreadStore["Service"]["inspectTail"] = Effect.fn(
    "MemoryThreadStore.inspectTail",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(ThreadTailRequest, "inspectTail", unvalidated);

      const thread = yield* Ref.get(state).pipe(
        Effect.flatMap((current) => findThread(current, request.threadId)),
      );

      return ThreadTail.make({
        threadId: request.threadId,
        tailSequence: thread.tailSequence,
        tailDigest: thread.tailDigest,
        producerEpoch: thread.producerEpoch,
      });
    }),
  );

  const readIdentity: ThreadStore["Service"]["readIdentity"] = Effect.fn(
    "MemoryThreadStore.readIdentity",
  )(function* (unvalidated) {
    const request = yield* validate(ThreadIdentityRequest, "readIdentity", unvalidated);

    const thread = yield* Ref.get(state).pipe(
      Effect.flatMap((current) => findThread(current, request.threadId)),
    );

    const selected = [
      thread.records[0],
      thread.byId.get(workerOriginRecordId(request.threadId)),
      thread.byId.get(subagentLineageRecordId(request.threadId)),
    ].filter((entry) => entry !== undefined);

    return yield* ThreadIdentity.makeEffect({
      threadId: request.threadId,
      tailSequence: thread.tailSequence,
      tailDigest: thread.tailDigest,
      producerEpoch: thread.producerEpoch,
      records: selected.filter(
        (entry, index) =>
          selected.findIndex((other) => other.record.recordId === entry.record.recordId) === index,
      ),
    }).pipe(
      Effect.mapError((cause) => storeError("readIdentity", "Invalid canonical identity", cause)),
    );
  });

  const saveCheckpoint: ThreadCheckpoints["save"] = Effect.fn("MemoryThreadStore.saveCheckpoint")(
    (unvalidated) =>
      Effect.gen(function* () {
        yield* validateCheckpointVersion(unvalidated);
        const request = yield* validate(SaveCheckpointRequest, "saveCheckpoint", unvalidated);

        const decision = yield* Ref.modify(
          state,
          (current): readonly [CheckpointDecision, MemoryState] => {
            const checkpoint = request.checkpoint;
            const thread = current.threads.get(checkpoint.threadId);

            if (thread === undefined) {
              return [
                {
                  _tag: "failure",
                  error: ThreadNotMaterialized.make({
                    threadId: checkpoint.threadId,
                  }),
                },
                current,
              ];
            }
            if (checkpoint.throughSequence > thread.tailSequence) {
              return [
                {
                  _tag: "failure",
                  error: CheckpointRejected.make({
                    threadId: checkpoint.threadId,
                    reason: "ahead-of-tail",
                  }),
                },
                current,
              ];
            }
            if (thread.tailDigests.get(checkpoint.throughSequence) !== checkpoint.tailDigest) {
              return [
                {
                  _tag: "failure",
                  error: CheckpointRejected.make({
                    threadId: checkpoint.threadId,
                    reason: "digest-mismatch",
                  }),
                },
                current,
              ];
            }
            if (
              !thread.checkpoints.has(checkpoint.throughSequence) &&
              thread.checkpoints.size >= MAX_CHECKPOINTS_PER_THREAD
            ) {
              return [
                {
                  _tag: "failure",
                  error: storeError(
                    "saveCheckpoint",
                    `In-memory checkpoint limit ${MAX_CHECKPOINTS_PER_THREAD} exceeded`,
                  ),
                },
                current,
              ];
            }
            const checkpoints = new Map(thread.checkpoints);

            checkpoints.set(checkpoint.throughSequence, checkpoint);
            const threads = new Map(current.threads);

            threads.set(checkpoint.threadId, { ...thread, checkpoints });

            return [{ _tag: "success" }, { threads }];
          },
        );

        if (decision._tag === "failure") return yield* decision.error;
      }),
  );

  const loadCheckpoint: ThreadCheckpoints["load"] = Effect.fn("MemoryThreadStore.loadCheckpoint")(
    (unvalidated) =>
      Effect.gen(function* () {
        const request = yield* validate(LoadCheckpointRequest, "loadCheckpoint", unvalidated);

        const thread = yield* Ref.get(state).pipe(
          Effect.flatMap((current) => findThread(current, request.threadId)),
        );

        const maximum = request.atOrBeforeSequence ?? thread.tailSequence;
        let selected: ThreadCheckpoint | undefined;

        for (const [sequence, checkpoint] of thread.checkpoints) {
          if (
            sequence <= maximum &&
            (selected === undefined || sequence > selected.throughSequence)
          ) {
            selected = checkpoint;
          }
        }
        if (
          selected !== undefined &&
          thread.tailDigests.get(selected.throughSequence) !== selected.tailDigest
        ) {
          return yield* CheckpointRejected.make({
            threadId: request.threadId,
            reason: "digest-mismatch",
          });
        }

        return Option.fromNullishOr(selected);
      }),
  );

  const saveRecoveryCheckpoint: ThreadRecoveryCheckpoints["save"] = Effect.fn(
    "MemoryThreadStore.saveRecoveryCheckpoint",
  )(function* (unvalidated) {
    const request = yield* validate(
      SaveRecoveryCheckpointRequest,
      "saveRecoveryCheckpoint",
      unvalidated,
    );

    const decision = yield* Ref.modify(
      state,
      (
        current,
      ): readonly [
        CheckpointDecision | { readonly _tag: "failure"; readonly error: FenceRejected },
        MemoryState,
      ] => {
        const checkpoint = request.checkpoint;
        const thread = current.threads.get(checkpoint.threadId);

        if (thread === undefined)
          return [
            {
              _tag: "failure",
              error: ThreadNotMaterialized.make({ threadId: checkpoint.threadId }),
            },
            current,
          ];
        if (thread.producerEpoch !== request.producerEpoch)
          return [
            {
              _tag: "failure",
              error: FenceRejected.make({
                threadId: checkpoint.threadId,
                actualEpoch: thread.producerEpoch,
                attemptedEpoch: request.producerEpoch,
              }),
            },
            current,
          ];
        if (checkpoint.throughSequence > thread.tailSequence)
          return [
            {
              _tag: "failure",
              error: CheckpointRejected.make({
                threadId: checkpoint.threadId,
                reason: "ahead-of-tail",
              }),
            },
            current,
          ];
        if (thread.tailDigests.get(checkpoint.throughSequence) !== checkpoint.tailDigest)
          return [
            {
              _tag: "failure",
              error: CheckpointRejected.make({
                threadId: checkpoint.threadId,
                reason: "digest-mismatch",
              }),
            },
            current,
          ];
        if ((thread.recoveryCheckpoint?.throughSequence ?? -1) > checkpoint.throughSequence)
          return [{ _tag: "success" }, current];
        const threads = new Map(current.threads);

        threads.set(checkpoint.threadId, { ...thread, recoveryCheckpoint: checkpoint });

        return [{ _tag: "success" }, { threads }];
      },
    );

    if (decision._tag === "failure") return yield* decision.error;
  });

  const loadRecoveryCheckpoint: ThreadRecoveryCheckpoints["load"] = Effect.fn(
    "MemoryThreadStore.loadRecoveryCheckpoint",
  )(function* (unvalidated) {
    const request = yield* validate(LoadCheckpointRequest, "loadRecoveryCheckpoint", unvalidated);

    const thread = yield* Ref.get(state).pipe(
      Effect.flatMap((current) => findThread(current, request.threadId)),
    );

    const checkpoint = thread.recoveryCheckpoint;

    if (
      checkpoint === undefined ||
      checkpoint.throughSequence > (request.atOrBeforeSequence ?? thread.tailSequence)
    )
      return Option.none();
    if (thread.tailDigests.get(checkpoint.throughSequence) !== checkpoint.tailDigest)
      return yield* CheckpointRejected.make({
        threadId: request.threadId,
        reason: "digest-mismatch",
      });

    return Option.some(checkpoint);
  });

  const threadStore = ThreadStore.of({
    readIdentity,
    countPeerMessages,
    materialize: (request) => withMutation(materialize(request)),
    append,
    read,
    observe,
    export: exportThread,
    inspectTail,
    checkpoints: { save: (request) => withMutation(saveCheckpoint(request)), load: loadCheckpoint },
    recoveryCheckpoints: {
      save: (request) => withMutation(saveRecoveryCheckpoint(request)),
      load: loadRecoveryCheckpoint,
    },
  });

  return Context.make(ThreadStore, threadStore).pipe(
    Context.add(MemoryThreadStoreKernel, {
      withMutation,
      prepareAppend,
      appendPrepared,
      record: (threadId, recordId) =>
        Ref.get(state).pipe(
          Effect.map((current) => current.threads.get(threadId)?.byId.get(recordId)?.record),
        ),
      tail: (threadId) => inspectTail(ThreadTailRequest.make({ threadId })),
    }),
  );
});

/**
 * In-memory canonical Thread persistence. Durable accepted work is served by the separate
 * SubmissionLedger port; this Layer provides ThreadStore and its ThreadReader.
 */
export const MemoryThreadStoreLive = ThreadReader.layer().pipe(
  Layer.provideMerge(Layer.effectContext(makeThreadStore)),
);

/** Configure a finite retained Thread capacity. Invalid construction options throw immediately. */
export const memoryThreadStoreLayer = (options: { readonly maxThreads?: number } = {}) =>
  MemoryThreadStoreLive.pipe(
    Layer.provide(
      Layer.succeed(ThreadCapacity)(
        Schema.decodeSync(
          Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(65_536)),
        )(options.maxThreads ?? MAX_THREADS),
      ),
    ),
  );
