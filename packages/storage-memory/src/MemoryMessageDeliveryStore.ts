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
import { validateCanonicalSettlement } from "@yielded/agent/settlement-publisher";
import {
  Settlement,
  settlementFailureFromRecord,
  submissionSettlementRecordId,
  submissionSettlementId,
} from "@yielded/agent/submission-ledger";
import { PreparedInput } from "@yielded/agent/subscription";
import { ThreadImportRejected, invalidThreadArchive } from "@yielded/agent/thread-import";
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

const position = (keys: ReadonlyArray<string>, key: string) => {
  let low = 0;
  let high = keys.length;

  while (low < high) {
    const mid = Math.floor((low + high) / 2);

    if (keys[mid] < key) low = mid + 1;
    else high = mid;
  }

  return low;
};

const retainKey = (keys: Array<string>, key: string) => {
  const at = position(keys, key);

  if (keys[at] !== key) keys.splice(at, 0, key);
};

const removeKey = (keys: Array<string>, key: string) => {
  const at = position(keys, key);

  if (keys[at] === key) keys.splice(at, 1);
};

const backend = () => ({
  records: new Map<string, string>(),
  work: new Map<string, ThreadWorkEntry>(),
  ordered: new Map<string, Array<string>>(),
  pending: new Map<ThreadId, Set<MessageDeliveryKey["messageId"]>>(),
  counts: new Map<
    ThreadId,
    { retained: number; ordinary: number; update: number; peer: number; revision: number }
  >(),
  metadata: new Map<
    string,
    {
      update: boolean;
      pending: boolean;
      peer: boolean;
      deadlineKey?: string;
      importVisit?: "visiting" | "verified";
    }
  >(),
  deadlines: new Map<string, { readonly key: MessageDeliveryKey; readonly deadline: number }>(),
  deadlineKeys: new Array<string>(),
  ownerDeadlineKeys: new Map<ThreadId, Array<string>>(),
});

const ownerKey = (id: ThreadId) => JSON.stringify([id]);

const keysFor = (current: ReturnType<typeof backend>, key: string) => {
  let keys = current.ordered.get(key);

  if (keys === undefined) current.ordered.set(key, (keys = []));

  return keys;
};

export interface MemoryMessageDeliveryStoreOptions {
  /** UTF-8 bound on the complete stored record, including a processed Settlement. */
  readonly maxStoredValueBytes?: number;
}

/**
 * Pair with MemoryThreadStoreLive so export/import sees every retained delivery obligation.
 * Each retained value and live partition is bounded; completed deduplication evidence is never evicted.
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

      const state = yield* Ref.make(backend());

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

      const commitInto = (
        current: ReturnType<typeof backend>,
        record: MessageDeliveryRecord,
        encoded: string,
        publish: boolean,
      ) => {
        const key = messageDeliveryKeyString(record.key);
        const owner = record.key.ownerThreadId;
        const old = current.metadata.get(key);
        const update = isWorkerUpdateDelivery(record);
        const pending = messageDeliveryUsesCapacity(record);

        const peer =
          record.envelope.messageAdmission !== undefined &&
          "peerName" in record.envelope.messageAdmission;

        let counts = current.counts.get(owner);

        if (counts === undefined)
          current.counts.set(
            owner,
            (counts = { retained: 0, ordinary: 0, update: 0, peer: 0, revision: 0 }),
          );
        counts.revision++;
        if (old === undefined) counts.retained++;
        if (old?.pending) {
          if (old.update) counts.update--;
          else counts.ordinary--;
          if (old.peer) counts.peer--;
        }
        if (pending) {
          if (update) counts.update++;
          else counts.ordinary++;
          if (peer) counts.peer++;
        }
        let owned = current.pending.get(owner);

        if (owned === undefined) current.pending.set(owner, (owned = new Set()));
        if (pending) owned.add(record.key.messageId);
        else owned.delete(record.key.messageId);
        if (owned.size === 0) current.pending.delete(owner);
        retainKey(keysFor(current, ownerKey(owner)), record.key.messageId);
        const admission = record.envelope.workerAdmission;

        if (admission !== undefined) {
          const worker = admission.origin.worker;

          if (record.key.messageId === admission.origin.firstMessageId)
            retainKey(
              keysFor(current, JSON.stringify([owner, worker.delegationId, worker.targetAgentId])),
              record.key.messageId,
            );
          const inputs = keysFor(current, JSON.stringify([owner, worker.threadId]));

          if (
            (record.status === "pending" || record.status === "parked") &&
            record.receipt === null
          )
            retainKey(inputs, record.key.messageId);
          else removeKey(inputs, record.key.messageId);
        }
        const deadline = messageDeliveryDeadline(record);
        let ownerDeadlines = current.ownerDeadlineKeys.get(owner);

        if (ownerDeadlines === undefined)
          current.ownerDeadlineKeys.set(owner, (ownerDeadlines = []));
        if (old?.deadlineKey !== undefined) {
          current.deadlines.delete(old.deadlineKey);
          removeKey(current.deadlineKeys, old.deadlineKey);
          removeKey(ownerDeadlines, old.deadlineKey);
        }

        const deadlineKey =
          deadline === null ? undefined : `${String(deadline).padStart(16, "0")}:${key}`;

        if (deadlineKey !== undefined && deadline !== null) {
          current.deadlines.set(deadlineKey, { key: record.key, deadline });
          retainKey(current.deadlineKeys, deadlineKey);
          retainKey(ownerDeadlines, deadlineKey);
        }
        current.metadata.set(key, {
          update,
          pending,
          peer,
          ...(deadlineKey === undefined ? {} : { deadlineKey }),
        });
        if (pending)
          current.work.set(key, {
            id: workId("delivery", record.key.messageId),
            owner: { _tag: "Delivery", messageId: record.key.messageId },
            partition: update ? "update" : "ordinary",
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
            ...(deadline === null ? {} : { notBeforeMillis: deadline }),
            ...(record.receipt === null
              ? {}
              : {
                  receiptId: record.receipt.receiptId,
                  queueSequence: record.receipt.queueSequence,
                }),
          });
        else current.work.delete(key);
        current.records.set(key, encoded);
        if (publish) {
          journal.initializeWork(owner);
          journal.retainDelivery(owner, record.key.messageId);
        }
      };

      const commit = (record: MessageDeliveryRecord, encoded: string) =>
        Effect.sync(() => commitInto(MutableRef.get(state.ref), record, encoded, true));

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
                yield* journal.checkThreadCapacity(input.key.ownerThreadId).pipe(
                  Effect.mapError(() =>
                    MessageDeliveryError.make({
                      reason: "capacity",
                      operation: "Thread capacity",
                    }),
                  ),
                );
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

                const update = isWorkerUpdateDelivery(input);
                const counts = MutableRef.get(state.ref).counts.get(input.key.ownerThreadId);
                const capacity = messageDeliveryCapacity(config, update);

                if ((update ? (counts?.update ?? 0) : (counts?.ordinary ?? 0)) >= capacity.pending)
                  return yield* MessageDeliveryError.make({
                    reason: "capacity",
                    operation: "insert",
                  });
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

      const transferDecode = (text: string) =>
        decode(text).pipe(
          Effect.mapError(() =>
            ThreadStoreError.make({
              operation: "delivery transfer",
              message: "Invalid retained delivery wire",
            }),
          ),
        );

      yield* journal.registerDeliveryTransfer({
        record: (threadId, messageId) => {
          const text = MutableRef.get(state.ref).records.get(
            messageDeliveryKeyString({ ownerThreadId: threadId, messageId }),
          );

          return text === undefined ? Effect.succeed(undefined) : transferDecode(text);
        },
        snapshot: (threadId) => {
          const counts = MutableRef.get(state.ref).counts.get(threadId);

          return { revision: counts?.revision ?? 0, deliveries: counts?.retained ?? 0 };
        },
        pendingPeerCount: (threadId, limit) =>
          Math.min(MutableRef.get(state.ref).counts.get(threadId)?.peer ?? 0, limit),
        facts: Effect.fnUntraced(function* (threadId, after) {
          const current = MutableRef.get(state.ref);
          const keys = current.ordered.get(ownerKey(threadId)) ?? [];

          const at =
            after === undefined
              ? 0
              : position(keys, after) + Number(keys[position(keys, after)] === after);

          const messageId = keys[at];

          const record =
            messageId === undefined
              ? undefined
              : yield* transferDecode(
                  current.records.get(
                    messageDeliveryKeyString({
                      ownerThreadId: threadId,
                      messageId: Schema.decodeSync(MessageDeliveryKey.fields.messageId)(messageId),
                    }),
                  ) ?? "",
                );

          return {
            facts: {
              admissions: [],
              commands: { aborts: [], approvals: [], resolutions: [] },
              deliveries:
                record === undefined
                  ? []
                  : [MessageDeliveryRecord.make({ ...record, leaseUntilMillis: null })],
            },
            after: messageId ?? after ?? "",
          };
        }),
        startImport: Effect.fnUntraced(function* (threadId) {
          const current = MutableRef.get(state.ref);

          if ((current.counts.get(threadId)?.retained ?? 0) > 0)
            return yield* ThreadImportRejected.make({
              threadId,
              reason: "target-not-empty",
              message: "Destination Thread owns retained deliveries",
            });
          const staged = backend();

          return {
            record: (messageId) => {
              const text = staged.records.get(
                messageDeliveryKeyString({ ownerThreadId: threadId, messageId }),
              );

              return text === undefined ? Effect.succeed(undefined) : transferDecode(text);
            },
            has: (messageId) =>
              staged.records.has(messageDeliveryKeyString({ ownerThreadId: threadId, messageId })),
            stage: Effect.fnUntraced(function* (page) {
              for (const record of page.archive.deliveries) {
                const key = messageDeliveryKeyString(record.key);

                if (staged.records.has(key) || current.records.has(key))
                  return yield* invalidThreadArchive("Duplicate retained delivery", threadId);

                const text = yield* encode(record).pipe(
                  Effect.mapError(() =>
                    ThreadImportRejected.make({
                      threadId,
                      reason: "unsupported-capacity",
                      message: "Delivery wire exceeds destination stored-value bound",
                    }),
                  ),
                );

                if (
                  new TextEncoder().encode(
                    JSON.stringify(
                      yield* Schema.encodeEffect(PreparedInput)(record.envelope).pipe(
                        Effect.mapError(() =>
                          invalidThreadArchive("Invalid delivery envelope", threadId),
                        ),
                      ),
                    ),
                  ).byteLength > config.maxEnvelopeBytes
                )
                  return yield* ThreadImportRejected.make({
                    threadId,
                    reason: "unsupported-capacity",
                    message: "Delivery envelope exceeds destination bound",
                  });
                const counts = staged.counts.get(threadId);

                if (
                  messageDeliveryUsesCapacity(record) &&
                  (isWorkerUpdateDelivery(record)
                    ? (counts?.update ?? 0)
                    : (counts?.ordinary ?? 0)) >=
                    messageDeliveryCapacity(config, isWorkerUpdateDelivery(record)).pending
                )
                  return yield* ThreadImportRejected.make({
                    threadId,
                    reason: "unsupported-capacity",
                    message: "Pending delivery capacity exceeded",
                  });
                commitInto(staged, record, text, false);
              }
            }),
            prepareCommit: Effect.fnUntraced(function* (readAdmission, readRecord) {
              const read = (messageId: MessageDeliveryKey["messageId"]) => {
                const text = staged.records.get(
                  messageDeliveryKeyString({ ownerThreadId: threadId, messageId }),
                );

                return text === undefined
                  ? Effect.fail(invalidThreadArchive("Missing delivery predecessor", threadId))
                  : transferDecode(text);
              };

              const visit = (messageId: MessageDeliveryKey["messageId"]) => {
                const metadata = staged.metadata.get(
                  messageDeliveryKeyString({ ownerThreadId: threadId, messageId }),
                );

                return metadata === undefined
                  ? Effect.fail(
                      invalidThreadArchive("Missing delivery predecessor metadata", threadId),
                    )
                  : Effect.succeed(metadata);
              };

              const verifyPredecessors = Effect.fnUntraced(function* (
                start: MessageDeliveryKey["messageId"],
              ) {
                let cursor: MessageDeliveryKey["messageId"] | undefined = start;

                while (cursor !== undefined) {
                  const metadata = yield* visit(cursor);

                  if (metadata.importVisit === "verified") break;
                  if (metadata.importVisit === "visiting")
                    return yield* invalidThreadArchive("Delivery predecessor cycle", threadId);
                  metadata.importVisit = "visiting";
                  const record: MessageDeliveryRecord = yield* read(cursor);

                  if (
                    record.predecessor !== undefined &&
                    (yield* read(record.predecessor)).createdAtMillis > record.createdAtMillis
                  )
                    return yield* invalidThreadArchive(
                      "Delivery predecessor has a later boundary",
                      threadId,
                    );
                  cursor = record.predecessor;
                }
                cursor = start;
                while (cursor !== undefined) {
                  const metadata = yield* visit(cursor);

                  if (metadata.importVisit === "verified") break;
                  metadata.importVisit = "verified";
                  cursor = (yield* read(cursor)).predecessor;
                }
              });

              for (const text of staged.records.values()) {
                const d = yield* transferDecode(text);

                yield* verifyPredecessors(d.key.messageId);
                if (d.receipt !== null) {
                  if (
                    d.settlement !== null &&
                    d.settlement.settlementId !== submissionSettlementId(d.receipt.submissionId)
                  )
                    return yield* invalidThreadArchive(
                      "Delivery settlement identity is not canonical",
                      threadId,
                    );
                  const foreign = d.receipt.threadId !== threadId;

                  if (foreign && d.status !== "processed")
                    return yield* ThreadImportRejected.make({
                      threadId,
                      reason: "unsupported-obligations",
                      message: "Live foreign delivery receipt cannot be restored",
                    });

                  const destination = yield* readAdmission(
                    d.receipt.threadId,
                    d.receipt.submissionId,
                  );

                  // A closed foreign delivery retains frozen evidence, not the receiver's execution authority.
                  if (destination === undefined) {
                    if (foreign && d.status === "processed") continue;

                    return yield* invalidThreadArchive(
                      "Destination delivery receipt evidence is missing",
                      threadId,
                    );
                  }

                  const expected = PreparedInput.make({
                    schemaVersion: 1,
                    authorization: d.envelope.authorization,
                    threadId: destination.threadId,
                    deliveryPrincipal: destination.principal,
                    admissionKey: destination.idempotencyKey,
                    agentId: destination.agentId,
                    definitions: destination.agentDigests,
                    input: destination.inputPayload,
                    inputDigest: destination.inputDigest,
                    ...(destination.admissionGroup === undefined
                      ? {}
                      : { admissionGroup: destination.admissionGroup }),
                    ...(destination.admissionFence === undefined
                      ? {}
                      : { admissionFence: destination.admissionFence }),
                    ...(destination.workerAdmission === undefined
                      ? {}
                      : { workerAdmission: destination.workerAdmission }),
                    ...(destination.messageAdmission === undefined
                      ? {}
                      : { messageAdmission: destination.messageAdmission }),
                  });

                  if (
                    d.receipt.receiptId !== destination.receiptId ||
                    d.receipt.queueSequence !== destination.queueSequence ||
                    !Schema.toEquivalence(PreparedInput)(d.envelope, expected)
                  )
                    return yield* invalidThreadArchive(
                      "Delivery envelope differs from destination admission",
                      threadId,
                    );
                  if (d.settlement !== null) {
                    const record = yield* readRecord(
                      destination.threadId,
                      submissionSettlementRecordId(destination.submissionId),
                    );

                    if (record === undefined) {
                      if (foreign) continue;

                      return yield* invalidThreadArchive(
                        "Processed delivery has no destination settlement",
                        threadId,
                      );
                    }

                    const settled = yield* validateCanonicalSettlement(record, destination).pipe(
                      Effect.mapError(() =>
                        invalidThreadArchive("Invalid destination settlement", threadId),
                      ),
                    );

                    const failure = settlementFailureFromRecord(record);

                    const expected = Settlement.make({
                      submissionId: settled.submissionId,
                      receiptId: settled.receiptId,
                      settlementId: settled.settlementId,
                      outcome: settled.outcome,
                      settledAt: d.settlement.settledAt,
                      ...(failure === undefined ? {} : { failure }),
                      ...(d.settlement.runDisposition === undefined ||
                      settled.runDisposition === undefined
                        ? {}
                        : { runDisposition: settled.runDisposition }),
                      ...(d.settlement.usageSummary === undefined ||
                      settled.usageSummary === undefined
                        ? {}
                        : { usageSummary: settled.usageSummary }),
                    });

                    if (!Schema.toEquivalence(Settlement)(d.settlement, expected))
                      return yield* invalidThreadArchive(
                        "Delivery settlement differs from destination evidence",
                        threadId,
                      );
                  }
                }
              }

              return () => {
                for (const text of staged.records.values())
                  commitInto(current, Schema.decodeSync(codec)(text), text, true);
              };
            }),
          };
        }),
      });

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

          const keys =
            workerKey === undefined
              ? input.pendingOnly
                ? Array.from(current.pending.get(input.ownerThreadId) ?? []).sort()
                : (current.ordered.get(ownerKey(input.ownerThreadId)) ?? [])
              : (current.ordered.get(workerKey) ?? []);

          const start =
            input.after === undefined
              ? 0
              : position(keys, input.after) +
                Number(keys[position(keys, input.after)] === input.after);

          const items: Array<MessageDeliveryRecord> = [];
          let bytes = 0;
          let at = start;

          while (at < keys.length && items.length < input.limit) {
            const text = current.records.get(
              messageDeliveryKeyString({
                ownerThreadId: input.ownerThreadId,
                messageId: Schema.decodeSync(MessageDeliveryKey.fields.messageId)(keys[at]),
              }),
            );

            if (text === undefined)
              return yield* MessageDeliveryError.make({
                reason: "corrupt",
                operation: "list locator",
              });
            const size = new TextEncoder().encode(text).byteLength;

            if (size > 32 * 1024 * 1024)
              return yield* MessageDeliveryError.make({
                reason: "capacity",
                operation: "list byte bound",
              });
            if (bytes + size > 32 * 1024 * 1024) break;
            items.push(yield* decode(text));
            bytes += size;
            at++;
          }

          return {
            items,
            next: at < keys.length ? (items.at(-1)?.key.messageId ?? null) : null,
          };
        }),
        due: Effect.fnUntraced(function* (nowMillis, limit, ownerThreadId) {
          const input = yield* validateMessageDelivery(
            Scan,
            { nowMillis, limit, ...(ownerThreadId === undefined ? {} : { ownerThreadId }) },
            "due",
          );

          const current = MutableRef.get(state.ref);

          const keys =
            input.ownerThreadId === undefined
              ? current.deadlineKeys
              : (current.ownerDeadlineKeys.get(input.ownerThreadId) ?? []);

          const selected: Array<MessageDeliveryKey> = [];

          for (const key of keys) {
            const row = current.deadlines.get(key);

            if (row === undefined)
              return yield* MessageDeliveryError.make({
                reason: "corrupt",
                operation: "deadline index",
              });
            if (row.deadline > input.nowMillis || selected.length === input.limit) break;
            selected.push(row.key);
          }

          return selected;
        }),
        nextDeadline: Effect.fnUntraced(function* (ownerThreadId) {
          if (ownerThreadId !== undefined)
            yield* validateMessageDelivery(ThreadId, ownerThreadId, "nextDeadline");
          const current = MutableRef.get(state.ref);

          const first = (
            ownerThreadId === undefined
              ? current.deadlineKeys
              : current.ownerDeadlineKeys.get(ownerThreadId)
          )?.[0];

          return first === undefined ? null : (current.deadlines.get(first)?.deadline ?? null);
        }),
      });
    }),
  );

export const MemoryMessageDeliveryStoreLive = memoryMessageDeliveryStoreLayer();
