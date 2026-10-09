import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { Update, UpdateError } from "../../core/AgentUpdates.ts";
import type { RunId, SubmissionId, ThreadId } from "../../core/Identifiers.ts";
import { utf8ByteLength } from "../../core/internal/utf8.ts";
import { IdempotencyKey } from "../../core/Receipt.ts";
import { digestJson } from "../Digest.ts";
import { DurableRuntimeFailpoint } from "../DurableFailpoint.ts";
import {
  MessageDeliveryStore,
  MessageDeliveryRecord,
  prepareMessageDelivery,
} from "../MessageDelivery.ts";
import {
  AgentUpdateEmitted,
  BatchId,
  CanonicalBatch,
  DefinitionDigests,
  type DeploymentId,
  PersistedJson,
  type ProducerEpoch,
  type ProducerId,
  RecordEnvelope,
  RecordId,
} from "../Records.ts";
import { readRunEvidenceSnapshot } from "../RunContinuation.ts";
import { LedgerError, type SubmissionSnapshot } from "../SubmissionLedger.ts";
import { PreparedInput } from "../Subscription.ts";
import {
  FencedAppendRequest,
  ThreadReader,
  ThreadStore,
  ThreadTailRequest,
} from "../ThreadStore.ts";
import {
  deliveryPredecessor,
  decodeWorkCursor,
  encodeWorkCursor,
  MAX_RECOVERY_PAGES,
  type ThreadWorkPage,
} from "../ThreadWork.ts";
import { WakeScheduler } from "../WakeScheduler.ts";
import { WorkerRuntime } from "./worker-runtime.ts";

type Delivery = NonNullable<AgentUpdateEmitted["delivery"]>;

const storage = () =>
  LedgerError.make({ operation: "agent-update", message: "Durable update persistence failed" });

const rejected = (reason: UpdateError["reason"]) => UpdateError.make({ reason });

/** Acceptance is fenced by the actual emitting Attempt; recovery only recreates frozen outbox rows. */
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
      updateSequence: payload?._tag === "AgentUpdateEmitted" ? payload.update.sequence : 0,
    };
  });

  const pendingUpdates = Effect.fnUntraced(function* (threadId: ThreadId) {
    if (store.work === undefined) return yield* rejected("unavailable");

    const initial = yield* decodeWorkCursor({ threadId, limit: 128 }).pipe(
      Effect.provideService(Crypto.Crypto, crypto),
      Effect.mapError(storage),
    );

    let cursor: string | undefined = encodeWorkCursor({ ...initial, source: "canonical" });

    let count = 0;

    for (let pages = 0; pages < MAX_RECOVERY_PAGES; pages++) {
      const page: ThreadWorkPage = yield* store.work
        .page({ threadId, limit: 128, cursor })
        .pipe(Effect.mapError(() => rejected("unavailable")));

      for (const entry of page.entries) {
        if (entry.owner._tag === "Handoff" && entry.owner.kind === "update") count++;
        if (entry.owner._tag === "Delivery") {
          if (entry.partition === undefined) return yield* rejected("unavailable");
          if (entry.partition === "update") count++;
        }
      }
      cursor = page.cursor;
      if (cursor === undefined) return count;
    }

    return yield* rejected("capacity");
  });

  const prepareDelivery = Effect.fnUntraced(function* (update: Update, delivery: Delivery) {
    const envelope = yield* Schema.decodeUnknownEffect(PreparedInput)(delivery.envelope).pipe(
      Effect.mapError(storage),
    );

    return yield* prepareMessageDelivery({
      key: { ownerThreadId: update.threadId, messageId: delivery.messageId },
      envelope,
      createdAtMillis: delivery.createdAtMillis,
      deadlineAtMillis: delivery.deadlineAtMillis,
      ...(delivery.predecessor === undefined ? {} : { predecessor: delivery.predecessor }),
    }).pipe(Effect.provideService(Crypto.Crypto, crypto), Effect.mapError(storage));
  });

  const insert = Effect.fnUntraced(function* (record: AgentUpdateEmitted) {
    const delivery = record.delivery;

    if (delivery === undefined) return;
    if (Option.isNone(deliveries)) return yield* storage();
    const prepared = yield* prepareDelivery(record.update, delivery);

    yield* failpoint.hit("update:before-delivery-insert");
    yield* deliveries.value.insert(prepared).pipe(Effect.mapError(storage));
    yield* failpoint.hit("update:after-delivery-insert");
  });

  const repairRun = Effect.fnUntraced(function* (threadId: ThreadId, submissionId: SubmissionId) {
    const history = yield* readRun(threadId, submissionId);

    for (const { record } of history.records) {
      if (record.payload._tag === "AgentUpdateEmitted" && record.payload.delivery !== undefined)
        yield* insert(record.payload);
    }
  });

  const emit = Effect.fnUntraced(function* (request: {
    readonly submission: SubmissionSnapshot;
    readonly runId: RunId;
    readonly producerEpoch: ProducerEpoch;
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

    const digest = yield* digestJson([request.submission.threadId, request.runId, updateId]).pipe(
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

    for (let attempt = 0; attempt < 32; attempt++) {
      const history = yield* readRun(request.submission.threadId, request.submission.submissionId);

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
        yield* insert(existing);

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
        !history.records.some(
          ({ record }) =>
            record.payload._tag === "RunStarted" && record.payload.runId === request.runId,
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
        if (
          utf8ByteLength(JSON.stringify(prepared.envelope)) >
          deliveries.value.limits.maxEnvelopeBytes
        )
          return yield* rejected("capacity");
        if (
          (yield* pendingUpdates(request.submission.threadId)) >=
          (deliveries.value.limits.maxPendingUpdatesPerOwner ?? 32)
        )
          return yield* rejected("capacity");

        const predecessor = yield* deliveryPredecessor(
          request.submission.threadId,
          history.tailSequence,
        ).pipe(Effect.provideService(ThreadReader, reader), Effect.mapError(storage));

        delivery = { ...prepared, ...(predecessor === undefined ? {} : { predecessor }) };
        const deliveryRecord = yield* prepareDelivery(update, delivery);

        const stored = yield* Schema.encodeEffect(Schema.fromJsonString(MessageDeliveryRecord))(
          deliveryRecord,
        ).pipe(Effect.mapError(storage));

        if (utf8ByteLength(stored) > deliveries.value.maxStoredValueBytes)
          return yield* rejected("capacity");
      }

      const payload = AgentUpdateEmitted.make({
        update,
        definitions: request.definitions,
        ...(delivery === undefined ? {} : { delivery }),
      });

      const tail = yield* store
        .inspectTail(ThreadTailRequest.make({ threadId: request.submission.threadId }))
        .pipe(Effect.mapError(storage));

      if (tail.producerEpoch !== request.producerEpoch) return yield* storage();
      if (tail.tailSequence !== history.tailSequence || tail.tailDigest !== history.tailDigest)
        continue;
      yield* failpoint.hit("update:before-canonical-append");

      const appended = yield* store
        .append(
          FencedAppendRequest.make({
            threadId: request.submission.threadId,
            producerEpoch: request.producerEpoch,
            expectedTailSequence: history.tailSequence,
            expectedTailDigest: history.tailDigest,
            batch: CanonicalBatch.make({
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
          }),
        )
        .pipe(
          Effect.as(true),
          Effect.catchTag("AppendConflict", () => Effect.succeed(false)),
          Effect.mapError(storage),
        );

      if (!appended) continue;
      yield* failpoint.hit("update:after-canonical-append");
      // Canonical progress is observable even when no parent report is selected.
      yield* wake.notify(request.submission.threadId);
      yield* insert(payload);

      return update;
    }

    return yield* storage();
  });

  return { emit, repair: insert, repairRun };
});
