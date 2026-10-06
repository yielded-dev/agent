import { ThreadId } from "@yielded/agent/identifiers";
import {
  applyMessageDeliveryChange,
  defaultMessageDeliveryStoreLimits,
  MessageDeliveryChange,
  MessageDeliveryError,
  MessageDeliveryFailpoint,
  MessageDeliveryKey,
  MessageDeliveryPageRequest,
  MessageDeliveryRecord,
  MessageDeliveryStore,
  MessageDeliveryStoreLimits,
  messageDeliveryDeadline,
  messageDeliveryKeyString,
  messageDeliveryUsesCapacity,
  isWorkerUpdateDelivery,
  messageDeliveryCapacity,
  sameMessageDeliveryIdentity,
  validateMessageDelivery,
} from "@yielded/agent/message-delivery";
import { ScheduleInstant } from "@yielded/agent/schedule";
import { ThreadStoreError } from "@yielded/agent/thread-store";
import { workId, type ThreadWorkEntry } from "@yielded/agent/thread-work";
import { Effect, Layer, MutableRef, Ref, Schema } from "effect";

import { MemoryThreadStoreKernel } from "./internal/MemoryThreadStoreKernel.ts";
import { boundedWorkSelection } from "./internal/WorkSelection.ts";

const codec = Schema.fromJsonString(MessageDeliveryRecord);

const decode = (text: string) =>
  Schema.decodeEffect(codec)(text).pipe(
    Effect.mapError(() => MessageDeliveryError.make({ reason: "corrupt", operation: "decode" })),
  );

const Scan = Schema.Struct({
  nowMillis: ScheduleInstant,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)),
  ownerThreadId: Schema.optionalKey(ThreadId),
});

export interface MemoryMessageDeliveryStoreOptions {
  /** UTF-8 bound on the complete stored record, including a processed Settlement. */
  readonly maxStoredValueBytes?: number;
}

/**
 * Pair with MemoryThreadStoreLive so export/import sees every retained delivery obligation.
 * All retained rows are bounded; completed deduplication evidence is never evicted.
 */
export const memoryMessageDeliveryStoreLayer = (
  limits: MessageDeliveryStoreLimits = defaultMessageDeliveryStoreLimits,
  options: MemoryMessageDeliveryStoreOptions = {},
): Layer.Layer<MessageDeliveryStore, MessageDeliveryError, MemoryThreadStoreKernel> =>
  Layer.effect(
    MessageDeliveryStore,
    Effect.gen(function* () {
      const journal = yield* MemoryThreadStoreKernel;
      const config = yield* validateMessageDelivery(MessageDeliveryStoreLimits, limits, "limits");

      const maxStoredValueBytes = yield* validateMessageDelivery(
        Schema.Int.check(Schema.isGreaterThan(0)),
        options.maxStoredValueBytes ?? 16 * 1_024 * 1_024,
        "stored-value-limit",
      );

      const state = yield* Ref.make({
        records: new Map<string, string>(),
        work: new Map<string, ThreadWorkEntry>(),
        owners: new Set<ThreadId>(),
        pending: new Map<ThreadId, ReadonlySet<MessageDeliveryKey["messageId"]>>(),
        workers: new Map<string, ReadonlySet<MessageDeliveryKey["messageId"]>>(),
      });

      yield* journal.registerMessageDeliveryStore((threadId) =>
        Ref.get(state).pipe(Effect.map((current) => current.owners.has(threadId))),
      );

      yield* journal.registerDeliveryLookup((threadId, messageId) =>
        MutableRef.get(state.ref).records.has(
          messageDeliveryKeyString({ ownerThreadId: threadId, messageId }),
        ),
      );
      yield* journal.registerWorkOwner("deliveries", {
        threads: (request) =>
          Ref.get(state).pipe(
            Effect.map((current) => {
              const candidates = boundedWorkSelection<ThreadId>(
                request.limit,
                (id) => id,
                request.afterThreadId,
              );

              for (const threadId of current.pending.keys()) candidates.add(threadId);
              const threadIds = candidates.values;

              const afterThreadId =
                threadIds.length === request.limit ? threadIds.at(-1) : undefined;

              return {
                threadIds,
                ...(afterThreadId === undefined ? {} : { afterThreadId }),
              };
            }),
          ),
        page: (threadId, after, limit) =>
          Effect.gen(function* () {
            const current = yield* Ref.get(state);

            const candidates = boundedWorkSelection<MessageDeliveryKey["messageId"]>(
              limit + 1,
              (id) => id,
              after,
            );

            for (const id of current.pending.get(threadId) ?? []) candidates.add(id);
            const ids = candidates.values;

            const entries: Array<ThreadWorkEntry> = [];

            for (const messageId of ids.slice(0, limit)) {
              const key = messageDeliveryKeyString({ ownerThreadId: threadId, messageId });
              const entry = current.work.get(key);

              if (entry === undefined || !current.records.has(key))
                return yield* ThreadStoreError.make({
                  operation: "delivery work",
                  message: "Pending delivery index is incomplete or corrupt",
                });
              entries.push(entry);
            }
            const next = ids.length > limit ? ids[limit - 1] : undefined;

            return { entries, ...(next === undefined ? {} : { after: next }) };
          }),
      });

      const commit = (record: MessageDeliveryRecord, encoded: string) =>
        Ref.update(state, (current) => {
          const key = messageDeliveryKeyString(record.key);
          const keys = new Set(current.pending.get(record.key.ownerThreadId));

          if (messageDeliveryUsesCapacity(record)) keys.add(record.key.messageId);
          else keys.delete(record.key.messageId);
          const pending = new Map(current.pending);

          if (keys.size === 0) pending.delete(record.key.ownerThreadId);
          else pending.set(record.key.ownerThreadId, keys);

          const workers = new Map(current.workers);
          const admission = record.envelope.workerAdmission;

          if (admission !== undefined) {
            const worker = admission.origin.worker;

            const startKey = JSON.stringify([
              record.key.ownerThreadId,
              worker.delegationId,
              worker.targetAgentId,
            ]);

            if (record.key.messageId === admission.origin.firstMessageId)
              workers.set(
                startKey,
                new Set([...(workers.get(startKey) ?? []), record.key.messageId]),
              );
            const pendingKey = JSON.stringify([record.key.ownerThreadId, worker.threadId]);
            const inputs = new Set(workers.get(pendingKey));

            if (
              (record.status === "pending" || record.status === "parked") &&
              record.receipt === null
            )
              inputs.add(record.key.messageId);
            else inputs.delete(record.key.messageId);
            workers.set(pendingKey, inputs);
          }

          const work = new Map(current.work);
          const notBeforeMillis = messageDeliveryDeadline(record);

          if (messageDeliveryUsesCapacity(record))
            work.set(key, {
              id: workId("delivery", record.key.messageId),
              owner: { _tag: "Delivery", messageId: record.key.messageId },
              partition: isWorkerUpdateDelivery(record) ? "update" : "ordinary",
              stateReference: {
                _tag: "Delivery",
                messageId: record.key.messageId,
                version: record.version,
              },
              state: record.status === "pending" ? "ready" : "waiting",
              ...(record.status === "parked"
                ? { wait: "parked" as const }
                : record.receipt !== null
                  ? { wait: "destination" as const }
                  : {}),
              ...(notBeforeMillis === null ? {} : { notBeforeMillis }),
              ...(record.receipt === null
                ? {}
                : {
                    receiptId: record.receipt.receiptId,
                    queueSequence: record.receipt.queueSequence,
                  }),
            });
          else work.delete(key);
          // Source work and authoritative delivery membership publish in one synchronous turn.
          journal.initializeWork(record.key.ownerThreadId);
          journal.retainDelivery(record.key.ownerThreadId, record.key.messageId);

          return {
            work,
            records: new Map(current.records).set(key, encoded),
            owners: current.owners.has(record.key.ownerThreadId)
              ? current.owners
              : new Set(current.owners).add(record.key.ownerThreadId),
            pending,
            workers,
          };
        });

      const failpoint = yield* MessageDeliveryFailpoint;

      const encode = Effect.fnUntraced(function* (record: MessageDeliveryRecord) {
        const text = yield* Schema.encodeEffect(codec)(record).pipe(
          Effect.mapError(() =>
            MessageDeliveryError.make({ reason: "corrupt", operation: "encode" }),
          ),
        );

        if (new TextEncoder().encode(text).byteLength > maxStoredValueBytes) {
          return yield* MessageDeliveryError.make({
            reason: "capacity",
            operation: "stored-value-bytes",
          });
        }

        return text;
      });

      const all = Effect.fnUntraced(function* () {
        return yield* Effect.forEach((yield* Ref.get(state)).records.values(), decode);
      });

      const insert: MessageDeliveryStore["Service"]["insert"] = Effect.fnUntraced(
        function* (record) {
          const encoded = yield* encode(record);
          const input = yield* decode(encoded);

          if (
            input.version !== 1 ||
            input.status !== "pending" ||
            input.leaseUntilMillis !== null ||
            input.retry.attempts !== 0 ||
            input.retry.generation !== 0 ||
            input.retry.automaticAttempts !== 0 ||
            input.retry.nextAttemptAtMillis !== input.createdAtMillis ||
            input.retry.lastAttemptAtMillis !== null ||
            input.retry.lastFailure !== null ||
            input.deadlineAtMillis !== input.initialDeadlineAtMillis
          ) {
            return yield* MessageDeliveryError.make({
              reason: "validation",
              operation: "insert-state",
            });
          }
          if (
            new TextEncoder().encode(JSON.stringify(input.envelope)).byteLength >
            config.maxEnvelopeBytes
          ) {
            return yield* MessageDeliveryError.make({
              reason: "capacity",
              operation: "envelope-bytes",
            });
          }
          yield* failpoint.hit("message-delivery:insert:before");

          const inserted = yield* journal.withMutation(
            Effect.uninterruptible(
              Effect.gen(function* () {
                const { records } = yield* Ref.get(state);
                const key = messageDeliveryKeyString(input.key);
                const existingText = records.get(key);

                if (existingText !== undefined) {
                  const existing = yield* decode(existingText);

                  if (!sameMessageDeliveryIdentity(existing, input))
                    return yield* MessageDeliveryError.make({
                      reason: "conflict",
                      operation: "insert",
                    });

                  journal.retainDelivery(existing.key.ownerThreadId, existing.key.messageId);

                  return existing;
                }

                const owned = (yield* all()).filter(
                  (record) =>
                    record.key.ownerThreadId === input.key.ownerThreadId &&
                    isWorkerUpdateDelivery(record) === isWorkerUpdateDelivery(input),
                );

                const capacity = messageDeliveryCapacity(config, isWorkerUpdateDelivery(input));

                if (
                  owned.length >= capacity.retained ||
                  owned.filter(messageDeliveryUsesCapacity).length >= capacity.pending
                ) {
                  return yield* MessageDeliveryError.make({
                    reason: "capacity",
                    operation: "insert",
                  });
                }
                yield* commit(input, encoded);

                return input;
              }),
            ),
          );

          yield* failpoint.hit("message-delivery:insert:after");

          return yield* decode(yield* encode(inserted));
        },
      );

      const get: MessageDeliveryStore["Service"]["get"] = Effect.fnUntraced(function* (key) {
        const input = yield* validateMessageDelivery(MessageDeliveryKey, key, "get");
        const text = (yield* Ref.get(state)).records.get(messageDeliveryKeyString(input));

        return text === undefined ? null : yield* decode(text);
      });

      const change: MessageDeliveryStore["Service"]["change"] = Effect.fnUntraced(
        function* (key, change) {
          const input = yield* validateMessageDelivery(MessageDeliveryChange, change, "change");
          const decodedKey = yield* validateMessageDelivery(MessageDeliveryKey, key, "change");
          const point = `message-delivery:${input._tag.toLowerCase()}`;

          yield* failpoint.hit(`${point}:before`);

          const changed = yield* journal.withMutation(
            Effect.uninterruptible(
              Effect.gen(function* () {
                const existing = yield* get(decodedKey);

                if (existing === null)
                  return yield* MessageDeliveryError.make({
                    reason: "not-found",
                    operation: "change",
                  });
                const next = yield* Effect.fromResult(applyMessageDeliveryChange(existing, input));
                const encoded = yield* encode(next);

                yield* commit(next, encoded);

                return next;
              }),
            ),
          );

          yield* failpoint.hit(`${point}:after`);

          return yield* decode(yield* encode(changed));
        },
      );

      return MessageDeliveryStore.of({
        limits: config,
        maxStoredValueBytes,
        insert,
        get,
        change,
        list: Effect.fnUntraced(function* (request) {
          const input = yield* validateMessageDelivery(MessageDeliveryPageRequest, request, "list");

          const current = yield* Ref.get(state);

          const workerKey =
            input.workerStarts !== undefined
              ? JSON.stringify([
                  input.ownerThreadId,
                  input.workerStarts.delegationId,
                  input.workerStarts.targetAgentId,
                ])
              : input.pendingWorker !== undefined
                ? JSON.stringify([input.ownerThreadId, input.pendingWorker])
                : undefined;

          const retained =
            workerKey !== undefined || input.pendingOnly
              ? yield* Effect.forEach(
                  [
                    ...((workerKey === undefined
                      ? current.pending.get(input.ownerThreadId)
                      : current.workers.get(workerKey)) ?? []),
                  ]
                    .filter((messageId) => input.after === undefined || messageId > input.after)
                    .sort()
                    .slice(0, input.limit + 1),
                  (messageId) =>
                    decode(
                      current.records.get(
                        messageDeliveryKeyString({ ownerThreadId: input.ownerThreadId, messageId }),
                      ) ?? "",
                    ),
                )
              : yield* all();

          const records = retained
            .filter(
              (record) =>
                record.key.ownerThreadId === input.ownerThreadId &&
                (input.after === undefined || record.key.messageId > input.after),
            )
            .sort((left, right) =>
              left.key.messageId < right.key.messageId
                ? -1
                : left.key.messageId > right.key.messageId
                  ? 1
                  : 0,
            );

          const items = records.slice(0, input.limit);

          return {
            items,
            next: records.length > input.limit ? (items.at(-1)?.key.messageId ?? null) : null,
          };
        }),
        due: Effect.fnUntraced(function* (nowMillis, limit, ownerThreadId) {
          const input = yield* validateMessageDelivery(
            Scan,
            { nowMillis, limit, ...(ownerThreadId === undefined ? {} : { ownerThreadId }) },
            "due",
          );

          return (yield* all())
            .flatMap((record) => {
              const deadline = messageDeliveryDeadline(record);

              return deadline !== null &&
                deadline <= input.nowMillis &&
                (input.ownerThreadId === undefined ||
                  input.ownerThreadId === record.key.ownerThreadId)
                ? [{ key: record.key, deadline }]
                : [];
            })
            .sort(
              (left, right) =>
                left.deadline - right.deadline ||
                (messageDeliveryKeyString(left.key) < messageDeliveryKeyString(right.key) ? -1 : 1),
            )
            .slice(0, input.limit)
            .map((record) => record.key);
        }),
        nextDeadline: Effect.fnUntraced(function* (ownerThreadId) {
          if (ownerThreadId !== undefined)
            yield* validateMessageDelivery(ThreadId, ownerThreadId, "nextDeadline");
          let next: number | null = null;

          for (const record of yield* all()) {
            if (ownerThreadId !== undefined && ownerThreadId !== record.key.ownerThreadId) continue;
            const deadline = messageDeliveryDeadline(record);

            if (deadline !== null && (next === null || deadline < next)) next = deadline;
          }

          return next;
        }),
      });
    }),
  );

export const MemoryMessageDeliveryStoreLive = memoryMessageDeliveryStoreLayer();
