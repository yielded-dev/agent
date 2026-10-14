import { utf8ByteLength, digestCanonicalJson } from "@yielded/agent/digest";
import { ThreadId } from "@yielded/agent/identifiers";
import { decodeExportRecordJson } from "@yielded/agent/record-format";
import {
  CanonicalRecordEnvelope,
  CanonicalSequence,
  ObservationOffset,
  RecordEnvelope,
} from "@yielded/agent/records";
import { isOperationalRecord } from "@yielded/agent/run-continuation";
import {
  ThreadArchive,
  makeThreadImportProgress,
  prepareImportPage,
  finishThreadImport,
} from "@yielded/agent/thread-import";
import {
  AppendConflict,
  AppendResult,
  CheckpointRejected,
  ThreadCheckpoint,
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadObservation,
  ThreadReadRequest,
  ThreadStore,
  type ThreadCheckpoints,
  ThreadStoreError,
  ThreadExport,
  ThreadTail,
  ThreadTailRequest,
  type AppendRequest,
  PreparedAppend,
  LoadCheckpointRequest,
  SaveCheckpointRequest,
  canonicalBatchFitsTransfer,
} from "@yielded/agent/thread-store";
import { Clock, Crypto, Effect, Option, Ref, Schema, Stream } from "effect";
import { SqlClient } from "effect/sql/SqlClient";

import { capturePreparedRecordStorage } from "./SqlCanonicalStorage.ts";
import {
  type RawAppendRequest,
  RawCheckpoint,
  RawReadRequest,
  type SqlJournal,
} from "./SqlJournal.ts";
import { makeSqlQuery, SqlInteger } from "./SqlStorage.ts";
import type { Diagnostic, SqlStorageErrors, SqlStorageFailpoint } from "./SqlStorage.ts";
import type { SqlStorageFailpointLocation } from "./SqlStorageFailpoint.ts";
import { canonicalBatchHeaderJson, makeSqlThreadArchiveRange } from "./SqlThreadArchiveRange.ts";
import { makeSqlThreadImport } from "./SqlThreadImport.ts";
import {
  canonicalRecordMetadata,
  makeSelectedReads,
  makeExecutionRead,
} from "./SqlThreadNativeReads.ts";

export interface SqlThreadStoreOptions<
  S extends Diagnostic,
  C extends Diagnostic,
  F extends Diagnostic,
> {
  readonly namespace?: string;
  readonly errors: SqlStorageErrors<S, C>;
  readonly hitFailpoint: SqlStorageFailpoint<F>;
  readonly observationPollInterval: number;
  readonly verifyOnOpen: boolean;
  readonly offsetPrefix: string;
}

const encodeOperationalFacts = Schema.encodeSync(
  Schema.fromJsonString(Schema.Array(RecordEnvelope)),
);

/** Stable retry intent; derived progress and the caller's newly observed tail are excluded. */
export const sqlCommitIntentDigest = (batchJson: string, facts: ReadonlyArray<RecordEnvelope>) =>
  digestCanonicalJson(JSON.stringify([batchJson, encodeOperationalFacts(facts)]));

/** Capture exact wire and active retry intent before acquiring the writer transaction. */
export const prepareSqlAppend = Effect.fnUntraced(function* (
  request: AppendRequest,
  offsetPrefix: string,
) {
  const invalid = (operation: string) => (cause: { readonly message: string }) =>
    ThreadStoreError.make({ operation, message: cause.message, cause });

  const captured = yield* PreparedAppend.capture(request);

  if (
    captured.records.some(
      ({ canonical }) =>
        (isOperationalRecord(canonical) && canonical.payload._tag !== "RunCompleted") ||
        canonical.payload._tag === "RunContinuation",
    )
  )
    return yield* ThreadStoreError.make({
      operation: "prepare durable commit",
      message: "Execution records must be committed as active execution state",
    });

  const offsetBytes =
    utf8ByteLength(JSON.stringify(offsetPrefix)) + 3 * utf8ByteLength(captured.threadId) + 25;

  if (
    !canonicalBatchFitsTransfer(captured.threadId, captured.batch, captured.batchBytes, offsetBytes)
  )
    return yield* ThreadStoreError.make({
      operation: "prepare canonical append",
      message: "Canonical batch exceeds its bounded transfer representation",
    });

  const records = (yield* capturePreparedRecordStorage(captured)).map((record) =>
    Object.freeze({ ...record, readMetadata: canonicalRecordMetadata(record) }),
  );

  const executionWrites = captured.executionWrites;

  const factsJson = `[${executionWrites
    .map(({ factsJson }) => factsJson.slice(1, -1))
    .filter((json) => json.length > 0)
    .join(",")}]`;

  const commitDigest = yield* digestCanonicalJson(
    JSON.stringify([captured.batchJson, factsJson]),
  ).pipe(Effect.mapError(invalid("digest execution commit")));

  return {
    threadId: captured.threadId,
    batchId: captured.batch.batchId,
    batchJson: captured.batchJson,
    batchHeaderJson: canonicalBatchHeaderJson(captured.batch),
    producerIdJson: JSON.stringify(captured.batch.producerId),
    batchBytes: captured.batchBytes,
    expectedTailSequence: captured.expectedTailSequence,
    records,
    progress: captured.progress,
    executionWrites,
    commitDigest,
  } satisfies RawAppendRequest;
});

/** Canonical ThreadStore behavior over an adapter-initialized SQL journal. */
export const makeSqlThreadStoreKernel = Effect.fnUntraced(function* <
  S extends Diagnostic,
  C extends Diagnostic,
  W extends Diagnostic,
  F extends Diagnostic,
>(journal: SqlJournal<S, C, W, F>, options: SqlThreadStoreOptions<S, C, F>) {
  const OffsetText = Schema.String.check(Schema.isMaxLength(4 * 1024));
  const OFFSET_PREFIX = options.offsetPrefix;
  const ZERO_CANONICAL_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);
  const isAppendConflict = Schema.is(AppendConflict);
  const isCheckpointRejected = Schema.is(CheckpointRejected);

  const decodeThreadId = Schema.decodeEffect(CanonicalRecordEnvelope.fields.threadId);
  const decodeBatchId = Schema.decodeEffect(CanonicalRecordEnvelope.fields.batchId);
  const decodeSequence = Schema.decodeEffect(CanonicalSequence);
  const decodeObservationOffset = Schema.decodeEffect(ObservationOffset);
  const decodeReadRequest = Schema.decodeEffect(Schema.toType(ThreadReadRequest));
  const decodeTailRequest = Schema.decodeEffect(Schema.toType(ThreadTailRequest));
  const decodeAppendResult = Schema.decodeEffect(AppendResult);

  const storeError = (operation: string, error: { readonly message: string }) =>
    ThreadStoreError.make({
      cause: error,
      operation,
      message: error.message,
    });

  const schemaStoreError = (operation: string, error: { readonly message: string }) =>
    ThreadStoreError.make({
      cause: error,
      operation,
      message: error.message,
    });

  const parseOffset = Effect.fnUntraced(function* (
    threadId: ThreadMaterialization["threadId"],
    offset: ObservationOffset | undefined,
  ): Effect.fn.Return<CanonicalSequence, ThreadStoreError> {
    if (offset === undefined) return ZERO_CANONICAL_SEQUENCE;

    const text = yield* Schema.decodeEffect(OffsetText)(offset).pipe(
      Effect.mapError((error) => schemaStoreError("decode observation offset", error)),
    );

    const threadPrefix = `${OFFSET_PREFIX}${encodeURIComponent(threadId)}:`;

    if (!text.startsWith(threadPrefix)) {
      return yield* ThreadStoreError.make({
        operation: "decode observation offset",
        message:
          "The observation offset belongs to a different adapter, storage version, or Thread.",
      });
    }
    const sequenceText = text.slice(threadPrefix.length);

    if (!/^(0|[1-9][0-9]*)$/.test(sequenceText)) {
      return yield* ThreadStoreError.make({
        operation: "decode observation offset",
        message: "The observation offset is malformed.",
      });
    }

    return yield* Schema.decodeEffect(CanonicalSequence)(Number(sequenceText)).pipe(
      Effect.mapError((error) => schemaStoreError("decode observation offset", error)),
    );
  });

  const encodeCheckpoint = (
    checkpoint: ThreadCheckpoint,
  ): Effect.Effect<string, ThreadStoreError> =>
    Effect.suspend(() =>
      Schema.encodeEffect(Schema.fromJsonString(ThreadCheckpoint))(checkpoint).pipe(
        Effect.mapError((error) => schemaStoreError("encode checkpoint", error)),
      ),
    );

  // Scalar decoders resolve immediately; eager error mapping preserves that fast path between
  // the full canonical JSON decode and envelope construction, within the enclosing read span.
  const decodeEnvelope = Effect.fnUntraced(function* (row: {
    readonly batch_id: string;
    readonly thread_id: string;
    readonly record_json: string;
    readonly sequence: CanonicalSequence;
  }) {
    const record = yield* decodeExportRecordJson(row.record_json).pipe(
      Effect.mapErrorEager((error) =>
        ThreadStoreError.make({
          operation: "decode canonical record",
          message: error.message,
        }),
      ),
    );

    const threadId = yield* decodeThreadId(row.thread_id).pipe(
      Effect.mapErrorEager((error) => schemaStoreError("decode thread identity", error)),
    );

    const sequence = yield* decodeSequence(row.sequence).pipe(
      Effect.mapErrorEager((error) => schemaStoreError("encode observation offset", error)),
    );

    const offset = yield* decodeObservationOffset(
      `${OFFSET_PREFIX}${encodeURIComponent(threadId)}:${sequence}`,
    ).pipe(Effect.mapErrorEager((error) => schemaStoreError("encode observation offset", error)));

    const batchId = yield* decodeBatchId(row.batch_id).pipe(
      Effect.mapErrorEager((error) => schemaStoreError("decode batch identity", error)),
    );

    return CanonicalRecordEnvelope.make(
      { threadId, batchId, sequence, offset, record },
      { disableChecks: true },
    );
  });

  const decodeCheckpoint = (
    checkpointJson: string,
  ): Effect.Effect<ThreadCheckpoint, ThreadStoreError> =>
    Effect.suspend(() =>
      Schema.decodeEffect(Schema.fromJsonString(ThreadCheckpoint))(checkpointJson).pipe(
        Effect.mapError((error) => schemaStoreError("decode checkpoint", error)),
      ),
    );

  const requireThread = Effect.fnUntraced(function* (
    journal: SqlJournal<S, C, W, F>,
    threadId: ThreadMaterialization["threadId"],
  ) {
    const rows = yield* journal
      .getThread(threadId)
      .pipe(Effect.mapError((error) => storeError("read thread", error)));

    if (rows.length === 0) {
      return yield* ThreadNotMaterialized.make({ threadId });
    }

    return rows[0];
  });

  const requireBatchBoundary = Effect.fnUntraced(function* (
    journal: SqlJournal<S, C, W, F>,
    threadId: ThreadMaterialization["threadId"],
    sequence: CanonicalSequence,
  ) {
    const valid = yield* journal
      .isBatchBoundary(threadId, sequence)
      .pipe(Effect.mapError((error) => storeError("read checkpoint boundary", error)));

    if (!valid) {
      return yield* CheckpointRejected.make({
        threadId,
        reason: "not-batch-boundary",
      });
    }
  });

  const config = options;
  const failpoint = { hit: options.hitFailpoint };
  const crypto = yield* Crypto.Crypto;

  const hitFailpoint = (
    location: SqlStorageFailpointLocation,
  ): Effect.Effect<void, ThreadStoreError> =>
    Effect.suspend(() =>
      failpoint
        .hit(location)
        .pipe(Effect.mapError((error) => storeError(`storage failpoint ${location}`, error))),
    );

  const materialize: ThreadStore["Service"]["materialize"] = Effect.fnUntraced(function* (
    request: ThreadMaterialization,
  ) {
    const validated = yield* Schema.decodeEffect(Schema.toType(ThreadMaterialization))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate materialization", error)));

    const now = yield* Clock.currentTimeMillis;

    yield* hitFailpoint("materialize:before");
    yield* journal
      .materialize(validated.threadId, new Date(now).toISOString())
      .pipe(Effect.mapError((error) => storeError("materialize thread", error)));
    yield* hitFailpoint("materialize:after");
  });

  const makeAppend = (commit: SqlJournal<S, C, W, F>["append"], requireMaterialized: boolean) =>
    Effect.fn("SqlThreadStore.append")(function* (request: AppendRequest) {
      const rawRequest = yield* prepareSqlAppend(request, options.offsetPrefix).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
      );

      if (requireMaterialized) yield* requireThread(journal, rawRequest.threadId);

      yield* hitFailpoint("append:before");

      const result = yield* journal.owner
        .withProgress(rawRequest.threadId, commit(rawRequest))
        .pipe(
          Effect.mapError((error) => {
            if (isAppendConflict(error)) return error;

            return storeError("append canonical batch", error);
          }),
          Effect.flatMap((result) =>
            decodeAppendResult(result).pipe(
              Effect.mapError((error) => schemaStoreError("decode append result", error)),
            ),
          ),
        );

      yield* hitFailpoint("append:after");

      return result;
    });

  const loadRecords = Effect.fnUntraced(function* (request: RawReadRequest) {
    const rows = yield* journal
      .read(request)
      .pipe(Effect.mapError((error) => storeError("read canonical records", error)));

    return yield* Effect.forEach(rows, decodeEnvelope);
  });

  const readEffect = Effect.fnUntraced(function* (request: ThreadReadRequest) {
    const validated = yield* decodeReadRequest(request).pipe(
      Effect.mapError((error) => schemaStoreError("validate thread read", error)),
    );

    if ("selection" in validated) return Stream.fromIterable(yield* selectedReads.read(validated));
    yield* requireThread(journal, validated.threadId);

    const records = yield* loadRecords(
      RawReadRequest.make({
        threadId: validated.threadId,
        fromSequenceExclusive: validated.afterSequence ?? ZERO_CANONICAL_SEQUENCE,
        limit: validated.limit,
      }),
    );

    return Stream.fromIterable(records);
  });

  const read: ThreadStore["Service"]["read"] = (request) => Stream.unwrap(readEffect(request));

  const observeEffect = Effect.fnUntraced(function* (request: ThreadObservation) {
    const validated = yield* Schema.decodeEffect(Schema.toType(ThreadObservation))(request).pipe(
      Effect.mapError((error) => schemaStoreError("validate thread observation", error)),
    );

    yield* requireThread(journal, validated.threadId);
    const initialSequence = yield* parseOffset(validated.threadId, validated.afterOffset);
    const cursor = yield* Ref.make(initialSequence);

    const poll = Effect.fnUntraced(function* () {
      const fromSequenceExclusive = yield* Ref.get(cursor);

      const records = yield* loadRecords(
        RawReadRequest.make({
          threadId: validated.threadId,
          fromSequenceExclusive,
          limit: 1_024,
        }),
      );

      if (records.length === 0) {
        yield* Effect.sleep(config.observationPollInterval);

        return [];
      }
      yield* Ref.set(cursor, records[records.length - 1].sequence);

      return records;
    });

    return Stream.fromIterableEffectRepeat(poll());
  });

  const observe: ThreadStore["Service"]["observe"] = (request) =>
    Stream.unwrap(observeEffect(request));

  const transfer = yield* makeSqlThreadImport<S | W | F>({
    owner: journal.owner,
    namespace: options.namespace,
    offsetPrefix: options.offsetPrefix,
    read: (body) =>
      journal
        .withReadTransaction("export transaction")(body)
        .pipe(
          Effect.mapError((e) =>
            journal.isTransactionFailure(e) ? storeError("export transaction", e) : e,
          ),
        ),
    write: (body) =>
      journal
        .withWriteTransaction("import transaction")(body)
        .pipe(
          Effect.mapError((e) =>
            journal.isTransactionFailure(e) ? storeError("import transaction", e) : e,
          ),
        ),
    afterThreadRead: options.hitFailpoint("export:after-thread-read"),
  });

  // Opt-in offline audit holds one read snapshot and hydrates one bounded batch at a time.
  // No database-wide arrays or lifetime directories.
  if (config.verifyOnOpen) {
    const sql = yield* SqlClient;
    const { table, execute } = yield* makeSqlQuery(options.namespace);

    const audit = Effect.gen(function* () {
      // The enclosing audit already owns its read snapshot. Public transfer and archive
      // ports open their own transactions, so use private read-only views of this connection.
      const transactions = {
        read: <A, E, R>(body: Effect.Effect<A, E, R>) => body,
        write: () => Effect.die("A startup audit cannot mutate storage"),
      };

      const snapshotTransfer = yield* makeSqlThreadImport({
        owner: journal.owner,
        namespace: options.namespace,
        offsetPrefix: options.offsetPrefix,
        ...transactions,
      });

      const snapshotArchives =
        journal.archives === undefined
          ? undefined
          : yield* makeSqlThreadArchiveRange(transactions, options.namespace);

      const query = <A extends object>(statement: ReturnType<typeof sql<A>>) =>
        execute(statement).pipe(
          Effect.mapError((e) =>
            options.errors.corruption({
              table: "effect_agent_threads",
              rowKey: "startup_scan",
              message: e.message,
            }),
          ),
        );

      const decode = <A, I>(schema: Schema.Codec<A, I>, value: unknown) =>
        Schema.decodeUnknownEffect(schema)(value).pipe(
          Effect.mapError((e) =>
            options.errors.corruption({
              table: "effect_agent_threads",
              rowKey: "startup_scan",
              message: e.message,
            }),
          ),
        );

      for (const name of [
        "effect_agent_canonical_records",
        "effect_agent_canonical_batches",
        "effect_agent_checkpoints",
      ])
        if (
          (yield* query(sql`SELECT thread_id FROM ${table(name)} orphan WHERE NOT EXISTS
          (SELECT 1 FROM ${table("effect_agent_threads")} t WHERE t.thread_id=orphan.thread_id) LIMIT 1`))
            .length > 0
        )
          return yield* options.errors.corruption({
            table: name,
            rowKey: "startup_scan",
            message: "Canonical rows exist without their Thread",
          });
      let afterThread: string | undefined;

      while (true) {
        const threads = yield* decode(
          Schema.Array(Schema.Struct({ thread_id: ThreadId })),
          yield* query(
            sql`SELECT thread_id FROM ${table("effect_agent_threads")} WHERE created_at IS NOT NULL ${afterThread === undefined ? sql`` : sql`AND thread_id>${afterThread}`} ORDER BY thread_id LIMIT 1`,
          ),
        );

        const thread = threads[0];

        if (thread === undefined) break;
        const state = makeThreadImportProgress();
        let cursor: string | undefined;

        do {
          const page = yield* snapshotTransfer
            .verificationPage({
              threadId: thread.thread_id,
              ...(cursor === undefined ? {} : { cursor }),
            })
            .pipe(
              Effect.mapError((e) =>
                options.errors.corruption({
                  table: "effect_agent_threads",
                  rowKey: thread.thread_id,
                  message: e.message,
                }),
              ),
            );

          const prepared = yield* Schema.encodeEffect(ThreadExport)(page).pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(ThreadArchive)),
            Effect.flatMap((page) => prepareImportPage(state, page, { verification: true })),
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.mapError((e) =>
              options.errors.corruption({
                table: "effect_agent_canonical_batches",
                rowKey: thread.thread_id,
                message: e.message,
              }),
            ),
          );

          for (const batch of prepared.batches) {
            const stored = yield* decode(
              Schema.Array(
                Schema.Struct({ first_sequence: SqlInteger, last_sequence: SqlInteger }),
              ),
              yield* query(
                sql`SELECT first_sequence, last_sequence FROM ${table("effect_agent_canonical_batches")} WHERE thread_id=${thread.thread_id} AND batch_id=${JSON.stringify(batch.batch.batchId)}`,
              ),
            );

            if (
              stored.length !== 1 ||
              stored[0]?.first_sequence !== batch.firstSequence ||
              stored[0]?.last_sequence !== batch.lastSequence
            )
              return yield* options.errors.corruption({
                table: "effect_agent_canonical_batches",
                rowKey: `${thread.thread_id}/${batch.batch.batchId}`,
                message: "Stored batch span differs from its exported records",
              });
          }
          cursor = page.cursor;
        } while (cursor !== undefined);
        yield* finishThreadImport(state).pipe(
          Effect.mapError((e) =>
            options.errors.corruption({
              table: "effect_agent_threads",
              rowKey: thread.thread_id,
              message: e.message,
            }),
          ),
        );
        yield* snapshotTransfer.verifyPointers(thread.thread_id).pipe(
          Effect.mapError((cause) =>
            options.errors.corruption({
              table: "effect_agent_threads",
              rowKey: thread.thread_id,
              message: cause.message,
            }),
          ),
        );
        let afterCheckpoint = -1;

        while (true) {
          const checkpoints = yield* decode(
            Schema.Array(
              Schema.Struct({
                through_sequence: SqlInteger,
                checkpoint_json: Schema.String,
              }),
            ),
            yield* query(
              sql`SELECT through_sequence, checkpoint_json FROM ${table("effect_agent_checkpoints")} WHERE thread_id=${thread.thread_id}
              AND through_sequence>${afterCheckpoint} ORDER BY through_sequence LIMIT 1`,
            ),
          );

          const row = checkpoints[0];

          if (row === undefined) break;

          const checkpoint = yield* Schema.decodeEffect(Schema.fromJsonString(ThreadCheckpoint))(
            row.checkpoint_json,
          ).pipe(
            Effect.mapError((e) =>
              options.errors.corruption({
                table: "effect_agent_checkpoints",
                rowKey: `${thread.thread_id}/${row.through_sequence}`,
                message: e.message,
              }),
            ),
          );

          const bound = yield* journal.isBatchBoundary(
            thread.thread_id,
            CanonicalSequence.make(row.through_sequence),
          );

          if (
            checkpoint.threadId !== thread.thread_id ||
            checkpoint.throughSequence !== row.through_sequence ||
            !bound
          )
            return yield* options.errors.corruption({
              table: "effect_agent_checkpoints",
              rowKey: `${thread.thread_id}/${row.through_sequence}`,
              message: "Checkpoint is not bound to a canonical batch tail",
            });
          afterCheckpoint = row.through_sequence;
        }
        if (snapshotArchives !== undefined) {
          let afterSequence: CanonicalSequence | undefined;

          do {
            const page = yield* snapshotArchives.storage.page({
              threadId: thread.thread_id,
              limit: 32,
              ...(afterSequence === undefined ? {} : { afterSequence }),
            });

            for (const range of page.ranges)
              yield* snapshotArchives.storage.verify({
                threadId: thread.thread_id,
                firstSequence: range.firstSequence,
              });
            afterSequence = page.afterSequence;
          } while (afterSequence !== undefined);
        }
        afterThread = thread.thread_id;
      }
    });

    yield* journal.withReadTransaction("bounded startup audit")(
      audit.pipe(
        Effect.mapError((e) =>
          options.errors.corruption({
            table: "effect_agent_threads",
            rowKey: "startup_scan",
            message: e.message,
          }),
        ),
      ),
    );
  }

  const inspectTail: ThreadStore["Service"]["inspectTail"] = Effect.fnUntraced(function* (
    request: ThreadTailRequest,
  ) {
    const validated = yield* decodeTailRequest(request).pipe(
      Effect.mapError((error) => schemaStoreError("validate tail inspection", error)),
    );

    const thread = yield* requireThread(journal, validated.threadId);

    return ThreadTail.make({
      threadId: validated.threadId,
      tailSequence: thread.tail_sequence,
    });
  });

  const saveCheckpoint: ThreadCheckpoints["save"] = Effect.fnUntraced(function* (
    request: SaveCheckpointRequest,
  ) {
    const validated = yield* Schema.decodeEffect(Schema.toType(SaveCheckpointRequest))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate checkpoint", error)));

    const thread = yield* requireThread(journal, validated.checkpoint.threadId);

    if (validated.checkpoint.throughSequence > thread.tail_sequence) {
      return yield* CheckpointRejected.make({
        threadId: validated.checkpoint.threadId,
        reason: "ahead-of-tail",
      });
    }

    yield* requireBatchBoundary(
      journal,
      validated.checkpoint.threadId,
      validated.checkpoint.throughSequence,
    );
    const checkpointJson = yield* encodeCheckpoint(validated.checkpoint);

    const raw = RawCheckpoint.make({
      threadId: validated.checkpoint.threadId,
      throughSequence: validated.checkpoint.throughSequence,
      checkpointJson,
    });

    yield* hitFailpoint("save-checkpoint:before");
    yield* journal
      .saveCheckpoint(raw)
      .pipe(
        Effect.mapError((error) =>
          isCheckpointRejected(error) ? error : storeError("save checkpoint", error),
        ),
      );
    yield* hitFailpoint("save-checkpoint:after");
  });

  const loadCheckpoint: ThreadCheckpoints["load"] = Effect.fnUntraced(function* (
    request: LoadCheckpointRequest,
  ) {
    const validated = yield* Schema.decodeEffect(Schema.toType(LoadCheckpointRequest))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate checkpoint lookup", error)));

    const thread = yield* requireThread(journal, validated.threadId);

    const rows = yield* journal
      .loadCheckpoint(validated.threadId, validated.atOrBeforeSequence ?? thread.tail_sequence)
      .pipe(Effect.mapError((error) => storeError("load checkpoint", error)));

    if (rows.length === 0) return Option.none();
    if (rows.length !== 1) {
      return yield* ThreadStoreError.make({
        operation: "load checkpoint",
        message: `Expected at most one checkpoint row but found ${rows.length}.`,
      });
    }
    const row = rows[0];
    const checkpoint = yield* decodeCheckpoint(row.checkpoint_json);

    if (
      row.thread_id !== validated.threadId ||
      checkpoint.threadId !== row.thread_id ||
      checkpoint.throughSequence !== row.through_sequence
    ) {
      return yield* ThreadStoreError.make({
        operation: "load checkpoint",
        message: "Stored checkpoint metadata does not match its canonical row.",
      });
    }

    yield* requireBatchBoundary(journal, checkpoint.threadId, checkpoint.throughSequence);

    return Option.some(checkpoint);
  });

  const selectedReads = yield* makeSelectedReads(decodeEnvelope, options.namespace);
  const readExecution = yield* makeExecutionRead(options.namespace);

  const store = ThreadStore.of({
    work: journal.work.storage,
    archives: journal.archives,
    ...(journal.lifecycle === undefined
      ? {}
      : { lifecyclePublications: journal.lifecycle.storage }),
    countPeerMessages: selectedReads.countPeerMessages,
    readWorkerCapacity: selectedReads.readWorkerCapacity,
    readIdentity: selectedReads.readIdentity,
    readExecution: (request) =>
      requireThread(journal, request.threadId).pipe(Effect.andThen(readExecution(request))),
    append: makeAppend(journal.append, true),
    export: transfer.export,
    verification: transfer.verification,
    inspectTail,
    materialize,
    observe,
    read,
    readPrompt: (request) => Stream.fromIterableEffect(selectedReads.readPrompt(request)),
    checkpoints: { save: saveCheckpoint, load: loadCheckpoint },
  });

  return {
    store,
    importer: transfer.importer,
    /** Bind the writer once when constructing a claimed Run session. */
    makeOwnedAppend: (commit: SqlJournal<S, C, W, F>["append"]) => makeAppend(commit, false),
  };
});

export const makeSqlThreadStore = <
  S extends Diagnostic,
  C extends Diagnostic,
  W extends Diagnostic,
  F extends Diagnostic,
>(
  journal: SqlJournal<S, C, W, F>,
  options: SqlThreadStoreOptions<S, C, F>,
) => Effect.map(makeSqlThreadStoreKernel(journal, options), (kernel) => kernel.store);
