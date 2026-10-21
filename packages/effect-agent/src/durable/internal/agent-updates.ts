import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { Update, UpdateError } from "../../core/AgentUpdates.ts";
import type { RunId, SubmissionId, ThreadId } from "../../core/Identifiers.ts";
import { nativeUpdateId } from "../../core/internal/agent-updates.ts";
import { utf8ByteLength } from "../../core/internal/utf8.ts";
import { IdempotencyKey } from "../../core/Receipt.ts";
import { digestJson } from "../Digest.ts";
import { DurableRuntimeFailpoint } from "../DurableFailpoint.ts";
import { MessageDeliveryStore } from "../MessageDelivery.ts";
import {
  AgentUpdateEmitted,
  BatchId,
  CanonicalBatch,
  DefinitionDigests,
  type CanonicalRecordEnvelope,
  type DeclaredToolCall,
  type DeploymentId,
  PersistedJson,
  type ProducerId,
  RecordEnvelope,
  RecordId,
} from "../Records.ts";
import { CurrentRunWriter, readRunEvidenceSnapshot } from "../RunContinuation.ts";
import { runIdForSubmission } from "../RunJournal.ts";
import { LedgerError, type SubmissionSnapshot } from "../SubmissionLedger.ts";
import { getRecord, ThreadReader, ThreadStore, ThreadTailRequest } from "../ThreadStore.ts";
import { deliveryPredecessor } from "../ThreadWork.ts";
import { WakeScheduler } from "../WakeScheduler.ts";
import { WorkerRuntime } from "./worker-runtime.ts";

type Delivery = NonNullable<AgentUpdateEmitted["delivery"]>;

const storage = () =>
  LedgerError.make({ operation: "agent-update", message: "Durable update persistence failed" });

const rejected = (reason: UpdateError["reason"]) => UpdateError.make({ reason });

const publicationError = (error: { readonly _tag: string; readonly deliveryReason?: string }) =>
  error.deliveryReason === "capacity" || error.deliveryReason === "conflict"
    ? rejected(error.deliveryReason)
    : storage();

const updateDigest = (threadId: ThreadId, runId: RunId, updateId: string) =>
  digestJson([threadId, runId, updateId]);

const nativeParameters = Schema.Struct({ value: Schema.Json });
const isNativeParameters = Schema.is(nativeParameters);
const definitionDigestsEqual = Schema.toEquivalence(DefinitionDigests);
const updateValuesEqual = Schema.toEquivalence(Schema.Json);

/** Canonical native acceptance proves only this exact update call, never an external effect.
 * Its frozen delivery remains independently recoverable even when its Tool acknowledgement is lost.
 */
export const nativeUpdateAccepted = (
  submission: SubmissionSnapshot,
  call: DeclaredToolCall,
  records: ReadonlyArray<CanonicalRecordEnvelope>,
): boolean => {
  if (call.toolName !== "emit_update" || !isNativeParameters(call.parameters)) return false;
  const value = call.parameters.value;

  return records.some(
    ({ record: { payload } }) =>
      payload._tag === "AgentUpdateEmitted" &&
      payload.update.threadId === submission.threadId &&
      payload.update.agentId === submission.agentId &&
      payload.update.runId === call.runId &&
      payload.update.updateId === nativeUpdateId(call.runId, call.toolCallId) &&
      definitionDigestsEqual(payload.definitions, submission.agentDigests) &&
      updateValuesEqual(payload.update.value, value),
  );
};

/** The emitting Attempt fences acceptance; native storage publishes its optional outbox row atomically. */
export const makeAgentUpdateRuntime = Effect.fnUntraced(function* (options: {
  readonly deploymentId: DeploymentId;
  readonly producerId: ProducerId;
}) {
  const store = yield* ThreadStore;
  const workers = yield* WorkerRuntime;
  const wake = yield* WakeScheduler;
  const deliveries = yield* Effect.serviceOption(MessageDeliveryStore);
  const crypto = yield* Crypto.Crypto;
  const failpoint = yield* DurableRuntimeFailpoint;

  const reader = ThreadReader.fromStore(store);

  // Terminal operation evidence excludes updates; fetch the original acceptance by its
  // stable identity instead of expanding every operation's recovery history.
  const accepted = Effect.fnUntraced(function* (
    submission: SubmissionSnapshot,
    call: DeclaredToolCall,
  ) {
    if (call.toolName !== "emit_update") return false;

    const digest = yield* updateDigest(
      submission.threadId,
      call.runId,
      nativeUpdateId(call.runId, call.toolCallId),
    ).pipe(Effect.provideService(Crypto.Crypto, crypto), Effect.mapError(storage));

    const record = yield* getRecord({
      threadId: submission.threadId,
      recordId: RecordId.make(`agent-update:${digest}`),
    }).pipe(Effect.provideService(ThreadReader, reader), Effect.mapError(storage));

    return Option.isSome(record) && nativeUpdateAccepted(submission, call, [record.value]);
  });

  const readRun = Effect.fnUntraced(function* (threadId: ThreadId, submissionId: SubmissionId) {
    const tail = yield* store
      .inspectTail(ThreadTailRequest.make({ threadId }))
      .pipe(Effect.mapError(storage));

    const records = yield* readRunEvidenceSnapshot(threadId, submissionId, tail.tailSequence).pipe(
      Effect.provideService(ThreadReader, reader),
      Effect.provideService(Crypto.Crypto, crypto),
      Effect.mapError(storage),
    );

    const latest = yield* Stream.runCollect(
      reader.read({
        threadId,
        selection: { _tag: "LastAgentUpdate", throughSequence: tail.tailSequence },
        page: { limit: 1 },
      }),
    ).pipe(Effect.mapError(storage));

    if (
      latest.length > 1 ||
      latest.some(
        (entry) =>
          entry.threadId !== threadId ||
          entry.sequence > tail.tailSequence ||
          entry.record.payload._tag !== "AgentUpdateEmitted",
      )
    )
      return yield* storage();
    const payload = latest[0]?.record.payload;

    return {
      ...tail,
      records,
      execution: yield* store
        .readExecution({ threadId, runId: runIdForSubmission(submissionId) })
        .pipe(Effect.mapError(storage)),
      updateSequence: payload?._tag === "AgentUpdateEmitted" ? payload.update.sequence : 0,
    };
  });

  const emit = Effect.fnUntraced(
    function* (request: {
      readonly submission: SubmissionSnapshot;
      readonly runId: RunId;
      readonly definitions: DefinitionDigests;
      readonly updateId: IdempotencyKey;
      readonly value: Schema.Json;
      readonly maxCount?: number;
      readonly maxBytes?: number;
    }) {
      const value = yield* Schema.encodeEffect(Schema.fromJsonString(PersistedJson))(
        request.value,
      ).pipe(
        Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(PersistedJson))),
        Effect.mapError(() => rejected("validation")),
      );

      const updateId = yield* Schema.decodeEffect(IdempotencyKey)(request.updateId).pipe(
        Effect.mapError(() => rejected("validation")),
      );

      const digest = yield* updateDigest(request.submission.threadId, request.runId, updateId).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.mapError(storage),
      );

      const id = `agent-update:${digest}`;
      const messageId = Schema.decodeSync(IdempotencyKey)(`worker-update:${digest}`);
      const maxCount = request.maxCount ?? 32;
      const maxBytes = request.maxBytes ?? 16_384;

      if (
        !Number.isSafeInteger(maxCount) ||
        maxCount < 1 ||
        !Number.isSafeInteger(maxBytes) ||
        maxBytes < 1
      )
        return yield* rejected("validation");

      const writer = yield* CurrentRunWriter;

      for (let attempt = 0; attempt < 32; attempt++) {
        const history = yield* readRun(
          request.submission.threadId,
          request.submission.submissionId,
        );

        const updates = history.records.flatMap(({ record }) =>
          record.payload._tag === "AgentUpdateEmitted" ? [record.payload] : [],
        );

        const existing = updates.find(
          (entry) => entry.update.runId === request.runId && entry.update.updateId === updateId,
        );

        if (existing !== undefined) {
          if (
            existing.update.agentId !== request.submission.agentId ||
            !Schema.toEquivalence(PersistedJson)(existing.update.value, value) ||
            !Schema.toEquivalence(DefinitionDigests)(existing.definitions, request.definitions)
          )
            return yield* rejected("conflict");

          return existing.update;
        }
        const own = updates.filter((entry) => entry.update.runId === request.runId);

        if (
          own.length >= maxCount ||
          own.reduce(
            (total, entry) => total + utf8ByteLength(JSON.stringify(entry.update.value)),
            0,
          ) +
            utf8ByteLength(JSON.stringify(value)) >
            maxBytes
        )
          return yield* rejected("capacity");
        if (
          history.records.some(
            ({ record }) =>
              record.payload._tag === "SubmissionSettled" && record.payload.runId === request.runId,
          ) ||
          Option.isNone(history.execution) ||
          !history.execution.value.operational.some(
            ({ payload }) => payload._tag === "RunStarted" && payload.runId === request.runId,
          )
        )
          return yield* rejected("identity");

        const update = Update.make({
          schemaVersion: 1,
          agentId: request.submission.agentId,
          threadId: request.submission.threadId,
          runId: request.runId,
          updateId,
          sequence: history.updateSequence + 1,
          value,
        });

        const prepared = yield* workers.prepareUpdate(update, request.submission, messageId);
        let delivery: Delivery | undefined;

        if (prepared !== undefined) {
          if (Option.isNone(deliveries)) return yield* rejected("unavailable");

          const predecessor = yield* deliveryPredecessor(
            request.submission.threadId,
            history.tailSequence,
          ).pipe(Effect.provideService(ThreadReader, reader), Effect.mapError(storage));

          delivery = { ...prepared, ...(predecessor === undefined ? {} : { predecessor }) };
        }

        const payload = AgentUpdateEmitted.make({
          update,
          definitions: request.definitions,
          ...(delivery === undefined ? {} : { delivery }),
        });

        const tail = yield* store
          .inspectTail(ThreadTailRequest.make({ threadId: request.submission.threadId }))
          .pipe(Effect.mapError(storage));

        // Reenter the progress guard to recheck this Attempt's generation after preflight.
        yield* writer.withProgress(Effect.void).pipe(Effect.mapError(storage));
        if (tail.tailSequence !== history.tailSequence) continue;
        yield* failpoint.hit("update:before-canonical-append");

        const appended = yield* writer
          .append(
            CanonicalBatch.make({
              batchId: Schema.decodeSync(BatchId)(id),
              producerId: options.producerId,
              records: [
                RecordEnvelope.make({
                  recordId: Schema.decodeSync(RecordId)(id),
                  family: "thread",
                  schemaVersion: 1,
                  createdAt: DateTime.makeUnsafe(yield* Clock.currentTimeMillis),
                  deploymentId: options.deploymentId,
                  payload,
                }),
              ],
            }),
          )
          .pipe(
            Effect.as(true),
            Effect.catchTag("AppendConflict", () => Effect.succeed(false)),
            Effect.mapError(publicationError),
          );

        if (!appended) continue;
        yield* failpoint.hit("update:after-canonical-append");
        // Canonical progress is observable even when no parent report is selected.
        yield* wake.notify(request.submission.threadId);

        return update;
      }

      return yield* storage();
    },
    (effect) =>
      Effect.flatMap(CurrentRunWriter, (writer) =>
        writer.withProgress(effect).pipe(
          Effect.catchTags({
            OwnershipLost: () => storage(),
            ThreadStoreError: publicationError,
            ThreadNotMaterialized: () => storage(),
            AppendConflict: () => storage(),
          }),
        ),
      ),
  );

  return { emit, accepted };
});
