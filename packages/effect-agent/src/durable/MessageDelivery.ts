/**
 * Source-Thread-owned frozen input delivery, with separate destination acceptance and settlement
 * tracking. The host drives bounded retries and keeps parked obligations visible.
 *
 * @since 0.1.0
 */

import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import * as FailureDiagnostic from "../core/FailureDiagnostic.ts";
import { AgentId, DelegationId, ThreadId } from "../core/Identifiers.ts";
import { Receipt } from "../core/Receipt.ts";
import { WorkerUpdate } from "../core/Worker.ts";
import { digestJson } from "./Digest.ts";
import type { LifecyclePublicationStorage } from "./LifecyclePublication.ts";
import {
  admitPreparedInput,
  PreparedInputAdmission,
  PreparedInputRetryReason,
  PreparedInput,
} from "./PreparedInputAdmission.ts";
import { Digest, type RecordEnvelope } from "./Records.ts";
import { ScheduleInstant, ScheduleRetry } from "./Schedule.ts";
import { IdempotencyKey, Settlement } from "./SubmissionLedger.ts";

const Positive = Schema.Int.check(Schema.isGreaterThan(0));
const BoundedName = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));
const receiptsEqual = Schema.toEquivalence(Receipt);
const settlementsEqual = Schema.toEquivalence(Settlement);

/**
 * An owner is always a source Thread. Destination admission has its own frozen key.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MessageDeliveryKey = Schema.Struct({
  ownerThreadId: ThreadId,
  messageId: IdempotencyKey,
});

/**
 * Decoded value of {@link MessageDeliveryKey}.
 *
 * @category models
 * @since 0.1.0
 */
export type MessageDeliveryKey = typeof MessageDeliveryKey.Type;

/**
 * Automatic-attempt, timeout, retry-delay, and settlement-poll bounds; all durations are
 * milliseconds.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MessageDeliveryPolicy = Schema.Struct({
  maxAutomaticAttempts: Positive.check(Schema.isLessThanOrEqualTo(100)),
  attemptTimeoutMillis: Positive.check(Schema.isLessThanOrEqualTo(300_000)),
  retryBaseMillis: Positive,
  retryMaxMillis: Positive,
  settlementPollMillis: Positive,
}).check(Schema.makeFilter((value) => value.retryBaseMillis <= value.retryMaxMillis));

/**
 * Decoded value of {@link MessageDeliveryPolicy}.
 *
 * @category models
 * @since 0.1.0
 */
export type MessageDeliveryPolicy = typeof MessageDeliveryPolicy.Type;

/**
 * Eight automatic attempts, a 30-second attempt timeout, retries from 1 to 60 seconds, and 5-second
 * settlement polling.
 *
 * @category defaults
 * @since 0.1.0
 */
export const defaultMessageDeliveryPolicy: MessageDeliveryPolicy = {
  maxAutomaticAttempts: 8,
  attemptTimeoutMillis: 30_000,
  retryBaseMillis: 1_000,
  retryMaxMillis: 60_000,
  settlementPollMillis: 5_000,
};

/**
 * Per-source-Thread pending capacities and the maximum encoded envelope size in bytes, with a
 * separate worker-update allowance.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MessageDeliveryStoreLimits = Schema.Struct({
  maxPendingPerOwner: Positive,
  maxEnvelopeBytes: Positive,
  /** Separate update capacity preserves the full ordinary allowance for terminal reports. */
  maxPendingUpdatesPerOwner: Schema.optionalKey(Positive),
});

/**
 * Decoded value of {@link MessageDeliveryStoreLimits}.
 *
 * @category models
 * @since 0.1.0
 */
export type MessageDeliveryStoreLimits = typeof MessageDeliveryStoreLimits.Type;

/**
 * Allows 100 ordinary pending deliveries, 32 pending worker updates, and 256 KiB per envelope.
 *
 * @category defaults
 * @since 0.1.0
 */
export const defaultMessageDeliveryStoreLimits: MessageDeliveryStoreLimits = {
  maxPendingPerOwner: 100,
  maxEnvelopeBytes: 262_144,
  maxPendingUpdatesPerOwner: 32,
};

const ParkReason = Schema.Literals([
  "exhausted",
  "deadline",
  "status-unavailable",
  "awaiting-settlement",
]);

/**
 * Source-owned frozen admission envelope and its retry, acceptance, settlement, refusal, or parked
 * state.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MessageDeliveryRecord = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  key: MessageDeliveryKey,
  envelope: PreparedInput,
  envelopeDigest: Digest,
  /** Same-owner predecessor whose admission must be known before this message is submitted. */
  predecessor: Schema.optionalKey(IdempotencyKey),
  createdAtMillis: ScheduleInstant,
  /** Immutable creation identity; explicit recovery only renews deadlineAtMillis. */
  initialDeadlineAtMillis: ScheduleInstant,
  deadlineAtMillis: ScheduleInstant,
  policy: MessageDeliveryPolicy,
  version: Positive,
  status: Schema.Literals(["pending", "accepted", "processed", "refused", "parked"]),
  receipt: Schema.NullOr(Receipt),
  settlement: Schema.NullOr(Settlement),
  refusal: Schema.NullOr(BoundedName),
  parkReason: Schema.NullOr(ParkReason),
  leaseUntilMillis: Schema.NullOr(ScheduleInstant),
  retry: ScheduleRetry,
  /** Last failed admission/status check, retained across retries and successful recovery. */
  lastFailureDiagnostic: Schema.optionalKey(FailureDiagnostic.Diagnostic),
}).check(
  Schema.makeFilter(
    (record) =>
      record.deadlineAtMillis > record.createdAtMillis &&
      record.initialDeadlineAtMillis > record.createdAtMillis &&
      record.predecessor !== record.key.messageId &&
      (record.status === "processed") === (record.settlement !== null) &&
      (record.status === "refused") === (record.refusal !== null) &&
      (record.status === "parked") === (record.parkReason !== null) &&
      (record.status === "parked") === record.retry.parked &&
      (record.status === "accepted" || record.status === "processed"
        ? record.receipt !== null
        : true) &&
      (record.status === "pending" || record.status === "refused"
        ? record.receipt === null
        : true) &&
      (record.receipt === null || record.receipt.threadId === record.envelope.threadId) &&
      (record.settlement === null ||
        (record.settlement.receiptId === record.receipt?.receiptId &&
          record.settlement.submissionId === record.receipt.submissionId)) &&
      (record.status === "processed" || record.status === "refused" || record.status === "parked"
        ? record.leaseUntilMillis === null
        : true),
  ),
);

/**
 * Decoded value of {@link MessageDeliveryRecord}.
 *
 * @category models
 * @since 0.1.0
 */
export type MessageDeliveryRecord = typeof MessageDeliveryRecord.Type;

/**
 * A delivery operation failed validation, conflicted, exceeded capacity, or encountered missing,
 * unavailable, or corrupt storage.
 *
 * @category errors
 * @since 0.1.0
 */
export class MessageDeliveryError extends Schema.TaggedError<MessageDeliveryError>()(
  "MessageDeliveryError",
  {
    cause: Schema.optionalKey(FailureDiagnostic.Value),
    stack: Schema.optionalKey(Schema.String),
    reason: Schema.Literals([
      "validation",
      "conflict",
      "capacity",
      "not-found",
      "storage",
      "corrupt",
    ]),
    operation: Schema.String,
  },
) {}

/**
 * An injected failure at a message-delivery mutation boundary.
 *
 * @category errors
 * @since 0.1.0
 */
export class MessageDeliveryFailpointError extends Schema.TaggedError<MessageDeliveryFailpointError>()(
  "MessageDeliveryFailpointError",
  {
    point: Schema.String,
  },
) {}

/**
 * Expected delivery storage failures and injected mutation failures.
 *
 * @category models
 * @since 0.1.0
 */
export type MessageDeliveryFailure = MessageDeliveryError | MessageDeliveryFailpointError;

/**
 * Optional message-delivery fault injection; the default handler does nothing.
 *
 * @category services
 * @since 0.1.0
 */
export const MessageDeliveryFailpoint = Context.Reference<{
  readonly hit: (point: string) => Effect.Effect<void, MessageDeliveryFailpointError>;
}>("@effect-agent/thread/MessageDeliveryFailpoint", {
  defaultValue: () => ({ hit: () => Effect.void }),
});

const fence = { expectedVersion: Positive, nowMillis: ScheduleInstant };

/**
 * Terminal destination evidence, independent of an expiring source transport claim.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MessageDeliveryCompletion = Schema.Struct({
  receipt: Receipt,
  settlement: Settlement,
  admissionKey: IdempotencyKey,
  inputDigest: Digest,
});

/**
 * Atomic delivery lifecycle commands, fenced by the current version except for terminal destination
 * evidence.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MessageDeliveryChange = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("Complete"),
    nowMillis: ScheduleInstant,
    ...MessageDeliveryCompletion.fields,
  }),
  Schema.Struct({ _tag: Schema.Literal("Claim"), ...fence }),
  Schema.Struct({ _tag: Schema.Literal("Accept"), ...fence, receipt: Receipt }),
  Schema.Struct({ _tag: Schema.Literal("Process"), ...fence, settlement: Settlement }),
  Schema.Struct({ _tag: Schema.Literal("ObservePending"), ...fence }),
  Schema.Struct({
    _tag: Schema.Literal("Refuse"),
    ...fence,
    code: BoundedName,
    diagnostic: Schema.optionalKey(FailureDiagnostic.Diagnostic),
  }),
  Schema.Struct({
    _tag: Schema.Literal("Retry"),
    ...fence,
    reason: PreparedInputRetryReason,
    diagnostic: Schema.optionalKey(FailureDiagnostic.Diagnostic),
  }),
  Schema.Struct({
    _tag: Schema.Literal("Park"),
    ...fence,
    reason: ParkReason,
    diagnostic: Schema.optionalKey(FailureDiagnostic.Diagnostic),
  }),
  Schema.Struct({ _tag: Schema.Literal("Recover"), ...fence, deadlineAtMillis: ScheduleInstant }),
  /** Waiting for earlier admission never spends an automatic transport attempt. */
  Schema.Struct({ _tag: Schema.Literal("Defer"), ...fence, untilMillis: ScheduleInstant }),
]);

/**
 * Decoded value of {@link MessageDeliveryChange}.
 *
 * @category models
 * @since 0.1.0
 */
export type MessageDeliveryChange = typeof MessageDeliveryChange.Type;

/**
 * Select one native index per page; index filters cannot be combined.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MessageDeliveryPageRequest = Schema.Struct({
  ownerThreadId: ThreadId,
  limit: Positive.check(Schema.isLessThanOrEqualTo(4096)),
  after: Schema.optionalKey(IdempotencyKey),
  /** Native pending index, including accepted, future-due and parked obligations. */
  pendingOnly: Schema.optionalKey(Schema.Boolean),
  /** First retained envelopes, including starts not admitted at the destination. */
  workerStarts: Schema.optionalKey(
    Schema.Struct({ delegationId: DelegationId, targetAgentId: AgentId }),
  ),
  /** Retained inputs not yet acknowledged by one destination worker. */
  pendingWorker: Schema.optionalKey(ThreadId),
}).check(
  Schema.makeFilter(
    (request) =>
      Number(request.pendingOnly === true) +
        Number(request.workerStarts !== undefined) +
        Number(request.pendingWorker !== undefined) <=
      1,
  ),
);

/**
 * Decoded value of {@link MessageDeliveryPageRequest}.
 *
 * @category models
 * @since 0.1.0
 */
export type MessageDeliveryPageRequest = typeof MessageDeliveryPageRequest.Type;

/**
 * A bounded list of retained deliveries and the last-message cursor for the next page, or null when
 * complete.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MessageDeliveryPage = Schema.Struct({
  items: Schema.Array(MessageDeliveryRecord),
  next: Schema.NullOr(IdempotencyKey),
});

/**
 * Decoded value of {@link MessageDeliveryPage}.
 *
 * @category models
 * @since 0.1.0
 */
export type MessageDeliveryPage = typeof MessageDeliveryPage.Type;

/**
 * Trusted host port, independent of either Thread's active Submission. List/get require an owner.
 * Inserts and changes are atomic; identical inserts return current state even after completion.
 * Completed rows retain deduplication evidence without consuming pending capacity. The host owns
 * total retention quotas; rows are never deleted automatically.
 * `due`/`nextDeadline` are bounded host recovery seams, never model-facing management operations.
 *
 * @category services
 * @since 0.1.0
 */
export class MessageDeliveryStore extends Context.Service<
  MessageDeliveryStore,
  {
    readonly lifecyclePublications?: LifecyclePublicationStorage;
    readonly limits: MessageDeliveryStoreLimits;
    readonly maxStoredValueBytes: number;
    readonly insert: (
      record: MessageDeliveryRecord,
    ) => Effect.Effect<MessageDeliveryRecord, MessageDeliveryFailure>;
    readonly get: (
      key: MessageDeliveryKey,
    ) => Effect.Effect<MessageDeliveryRecord | null, MessageDeliveryError>;
    readonly list: (
      request: MessageDeliveryPageRequest,
    ) => Effect.Effect<MessageDeliveryPage, MessageDeliveryError>;
    readonly change: (
      key: MessageDeliveryKey,
      change: MessageDeliveryChange,
    ) => Effect.Effect<MessageDeliveryRecord, MessageDeliveryFailure>;
    readonly due: (
      nowMillis: number,
      limit: number,
      ownerThreadId?: ThreadId,
    ) => Effect.Effect<ReadonlyArray<MessageDeliveryKey>, MessageDeliveryError>;
    readonly nextDeadline: (
      ownerThreadId?: ThreadId,
    ) => Effect.Effect<number | null, MessageDeliveryError>;
  }
>()("@effect-agent/thread/MessageDeliveryStore") {}

/**
 * Read current source-owned obligations without acquiring a runtime or scanning completed history.
 * A retained receipt is native accepted-admission evidence, not proof of destination materialization.
 * Shipped worker admission reserves the source input before returning that receipt. Policy can
 * therefore leave accepted worker discovery to canonical outstanding inputs; a no-receipt action
 * delivery remains uncertain, including when parked or due in the future.
 *
 * @category operations
 * @since 0.1.0
 */
export const readPending = Effect.fnUntraced(function* (
  request: Pick<MessageDeliveryPageRequest, "ownerThreadId" | "limit">,
) {
  const store = yield* MessageDeliveryStore;

  const input = yield* validateMessageDelivery(
    MessageDeliveryPageRequest,
    {
      ownerThreadId: request.ownerThreadId,
      limit: request.limit,
      pendingOnly: true,
    },
    "read-pending",
  );

  const page = yield* validateMessageDelivery(
    MessageDeliveryPage,
    yield* store.list(input),
    "read-pending result",
  );

  if (page.next !== null || page.items.length > input.limit)
    return yield* MessageDeliveryError.make({ reason: "capacity", operation: "read-pending" });
  const records = page.items;

  if (
    records.some(
      (record) =>
        record.key.ownerThreadId !== input.ownerThreadId || !messageDeliveryUsesCapacity(record),
    ) ||
    new Set(records.map((record) => record.key.messageId)).size !== records.length
  )
    return yield* MessageDeliveryError.make({
      reason: "corrupt",
      operation: "read-pending result",
    });

  return records;
});

/**
 * Encodes source Thread and message identity as an unambiguous JSON tuple for storage lookup.
 *
 * @category operations
 * @since 0.1.0
 */
export const messageDeliveryKeyString = (key: MessageDeliveryKey): string =>
  JSON.stringify([key.ownerThreadId, key.messageId]);

/**
 * Returns whether a delivery still reserves pending capacity; parked and accepted deliveries remain
 * counted.
 *
 * @category operations
 * @since 0.1.0
 */
export const messageDeliveryUsesCapacity = (record: MessageDeliveryRecord): boolean =>
  record.status !== "processed" && record.status !== "refused";

/**
 * Terminal peer sends retain capacity through the last granted delivery window, never for life.
 *
 * @category operations
 * @since 0.1.0
 */
export const peerMessageRetentionDeadline = (record: MessageDeliveryRecord): number | null =>
  !messageDeliveryUsesCapacity(record) &&
  record.envelope.messageAdmission !== undefined &&
  "peerName" in record.envelope.messageAdmission
    ? record.deadlineAtMillis
    : null;

/**
 * Checks whether the frozen envelope contains a typed worker update admission.
 *
 * @category guards
 * @since 0.1.0
 */
export const isWorkerUpdateDelivery = (record: MessageDeliveryRecord): boolean =>
  Schema.is(WorkerUpdate)(record.envelope.messageAdmission);

/**
 * Selects the ordinary or worker-update pending allowance; omitted update capacity defaults to 32.
 *
 * @category operations
 * @since 0.1.0
 */
export const messageDeliveryCapacity = (limits: MessageDeliveryStoreLimits, update: boolean) => ({
  pending: update ? (limits.maxPendingUpdatesPerOwner ?? 32) : limits.maxPendingPerOwner,
});

/**
 * Returns the next epoch-millisecond check for pending or accepted work, or null for inactive
 * delivery states.
 *
 * @category operations
 * @since 0.1.0
 */
export const messageDeliveryDeadline = (record: MessageDeliveryRecord): number | null =>
  record.status === "pending" || record.status === "accepted"
    ? Math.min(record.deadlineAtMillis, record.leaseUntilMillis ?? record.retry.nextAttemptAtMillis)
    : null;

/**
 * Creation identity deliberately excludes operational counters and renewed recovery deadlines.
 *
 * @category operations
 * @since 0.1.0
 */
export const sameMessageDeliveryIdentity = (
  left: MessageDeliveryRecord,
  right: MessageDeliveryRecord,
): boolean =>
  messageDeliveryKeyString(left.key) === messageDeliveryKeyString(right.key) &&
  left.envelopeDigest === right.envelopeDigest &&
  left.predecessor === right.predecessor &&
  Schema.toEquivalence(PreparedInput)(left.envelope, right.envelope) &&
  Schema.toEquivalence(MessageDeliveryPolicy)(left.policy, right.policy) &&
  left.createdAtMillis === right.createdAtMillis &&
  left.initialDeadlineAtMillis === right.initialDeadlineAtMillis;

const error = (reason: MessageDeliveryError["reason"], operation: string, cause?: unknown) =>
  MessageDeliveryError.make({ reason, operation, ...(cause === undefined ? {} : { cause }) });

/**
 * Validates a value against a schema's decoded shape and reports a delivery validation error
 * labeled with the operation.
 *
 * @category operations
 * @since 0.1.0
 */
export const validateMessageDelivery = <A, I>(
  schema: Schema.Codec<A, I>,
  value: unknown,
  operation: string,
) =>
  Schema.decodeUnknownEffect(Schema.toType(schema))(value).pipe(
    Effect.mapError(() => error("validation", operation)),
  );

const MessageDeliveryPreparation = Schema.Struct({
  key: MessageDeliveryKey,
  envelope: PreparedInput,
  createdAtMillis: ScheduleInstant,
  deadlineAtMillis: ScheduleInstant,
  policy: MessageDeliveryPolicy,
  predecessor: Schema.optionalKey(IdempotencyKey),
});

/**
 * Freeze and digest the complete admission envelope before publishing the obligation.
 *
 * @category operations
 * @since 0.1.0
 */
export const prepareMessageDelivery = Effect.fnUntraced(function* (options: {
  readonly key: MessageDeliveryKey;
  readonly envelope: PreparedInput;
  readonly createdAtMillis: number;
  readonly deadlineAtMillis: number;
  readonly policy?: MessageDeliveryPolicy;
  readonly predecessor?: IdempotencyKey;
}): Effect.fn.Return<MessageDeliveryRecord, MessageDeliveryError, Crypto.Crypto> {
  // JSON detaches every caller-owned value before Crypto can suspend preparation.
  const snapshot = yield* Schema.encodeEffect(Schema.fromJsonString(MessageDeliveryPreparation))({
    ...options,
    policy: options.policy ?? defaultMessageDeliveryPolicy,
  }).pipe(
    Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(MessageDeliveryPreparation))),
    Effect.mapError((cause) => error("validation", "prepare", cause)),
  );

  const envelope = snapshot.envelope;

  const encoded = yield* Schema.encodeEffect(PreparedInput)(envelope).pipe(
    Effect.mapError((cause) => error("validation", "prepare", cause)),
  );

  const inputDigest = yield* digestJson(encoded.input).pipe(
    Effect.mapError((cause) => error("storage", "digest", cause)),
  );

  if (inputDigest !== envelope.inputDigest) return yield* error("corrupt", "input-digest");

  const envelopeDigest = yield* digestJson(encoded).pipe(
    Effect.mapError((cause) => error("storage", "digest", cause)),
  );

  return yield* validateMessageDelivery(
    MessageDeliveryRecord,
    {
      schemaVersion: 1,
      key: snapshot.key,
      envelope,
      envelopeDigest,
      ...(snapshot.predecessor === undefined ? {} : { predecessor: snapshot.predecessor }),
      createdAtMillis: snapshot.createdAtMillis,
      initialDeadlineAtMillis: snapshot.deadlineAtMillis,
      deadlineAtMillis: snapshot.deadlineAtMillis,
      policy: snapshot.policy,
      version: 1,
      status: "pending",
      receipt: null,
      settlement: null,
      refusal: null,
      parkReason: null,
      leaseUntilMillis: null,
      retry: {
        attempts: 0,
        generation: 0,
        automaticAttempts: 0,
        parked: false,
        nextAttemptAtMillis: snapshot.createdAtMillis,
        lastAttemptAtMillis: null,
        lastFailure: null,
      },
    },
    "prepare",
  );
});

/**
 * Derive source-owned outbox rows from privately captured canonical records. Native adapters
 * publish these rows, their due indexes and the source records in one transaction. The v1
 * record contract uses the default delivery policy; replay retains the existing row's progress.
 *
 * @category operations
 * @since 0.1.0
 */
export const prepareCanonicalMessageDeliveries = Effect.fnUntraced(function* (
  threadId: ThreadId,
  records: ReadonlyArray<RecordEnvelope>,
): Effect.fn.Return<ReadonlyArray<MessageDeliveryRecord>, MessageDeliveryError, Crypto.Crypto> {
  const deliveries: Array<MessageDeliveryRecord> = [];

  for (const record of records) {
    const payload = record.payload;

    const frozen =
      payload._tag === "PeerMessagePrepared"
        ? {
            messageId: payload.messageId,
            envelope: payload.encodedEnvelope,
            createdAtMillis: DateTime.toEpochMillis(record.createdAt),
            deadlineAtMillis: payload.deadlineAtMillis,
          }
        : payload._tag === "WorkerReportPrepared"
          ? payload
          : payload._tag === "AgentUpdateEmitted"
            ? payload.delivery
            : undefined;

    if (frozen === undefined) continue;
    if (
      (payload._tag === "PeerMessagePrepared" && payload.source.threadId !== threadId) ||
      (payload._tag === "AgentUpdateEmitted" && payload.update.threadId !== threadId)
    )
      return yield* error("validation", "canonical delivery owner");

    const envelope = yield* Schema.decodeUnknownEffect(PreparedInput)(frozen.envelope).pipe(
      Effect.mapError((cause) => error("validation", "canonical delivery envelope", cause)),
    );

    deliveries.push(
      yield* prepareMessageDelivery({
        key: { ownerThreadId: threadId, messageId: frozen.messageId },
        envelope,
        createdAtMillis: frozen.createdAtMillis,
        deadlineAtMillis: frozen.deadlineAtMillis,
        ...("predecessor" in frozen && frozen.predecessor !== undefined
          ? { predecessor: frozen.predecessor }
          : {}),
      }),
    );
  }

  return deliveries;
});

/**
 * Pure reducer used under the adapter's atomic compare-and-swap boundary.
 *
 * @category operations
 * @since 0.1.0
 */
export const applyMessageDeliveryChange = (
  record: MessageDeliveryRecord,
  change: MessageDeliveryChange,
): Result.Result<MessageDeliveryRecord, MessageDeliveryError> => {
  const conflict = () => Result.fail(error("conflict", change._tag));

  if (change._tag === "Complete") {
    if (
      change.admissionKey !== record.envelope.admissionKey ||
      change.inputDigest !== record.envelope.inputDigest ||
      change.receipt.threadId !== record.envelope.threadId ||
      change.settlement.submissionId !== change.receipt.submissionId ||
      change.settlement.receiptId !== change.receipt.receiptId ||
      (record.receipt !== null && !receiptsEqual(record.receipt, change.receipt))
    )
      return Result.fail(error("corrupt", "completion-identity"));
    if (record.status === "refused") return conflict();
    if (record.status === "processed")
      return record.settlement !== null && settlementsEqual(record.settlement, change.settlement)
        ? Result.succeed(record)
        : Result.fail(error("corrupt", "completion-identity"));

    return Result.succeed({
      ...record,
      version: record.version + 1,
      status: "processed",
      receipt: change.receipt,
      settlement: change.settlement,
      leaseUntilMillis: null,
      parkReason: null,
      retry: { ...record.retry, parked: false },
    });
  }

  if (record.version !== change.expectedVersion) return conflict();
  if (record.status === "processed" || record.status === "refused") return conflict();
  const now = change.nowMillis;

  const base = {
    ...record,
    version: record.version + 1,
    ...("diagnostic" in change && change.diagnostic !== undefined
      ? { lastFailureDiagnostic: change.diagnostic }
      : {}),
  };

  const parked = (reason: typeof ParkReason.Type): MessageDeliveryRecord => ({
    ...base,
    status: "parked",
    parkReason: reason,
    leaseUntilMillis: null,
    retry: { ...record.retry, parked: true },
  });

  if (change._tag === "Defer") {
    if (
      record.status !== "pending" ||
      (record.leaseUntilMillis !== null && record.leaseUntilMillis > now)
    )
      return conflict();
    if (now >= record.deadlineAtMillis) return Result.succeed(parked("deadline"));
    if (change.untilMillis <= now) return conflict();

    return Result.succeed({
      ...base,
      leaseUntilMillis: null,
      retry: {
        ...record.retry,
        nextAttemptAtMillis: Math.min(change.untilMillis, record.deadlineAtMillis),
      },
    });
  }

  if (change._tag === "Recover") {
    if (record.status !== "parked" || change.deadlineAtMillis <= now) return conflict();

    return Result.succeed({
      ...base,
      deadlineAtMillis: change.deadlineAtMillis,
      status: record.receipt === null ? "pending" : "accepted",
      parkReason: null,
      retry: {
        ...record.retry,
        generation: record.retry.generation + 1,
        automaticAttempts: 0,
        parked: false,
        nextAttemptAtMillis: now,
      },
    });
  }
  if (record.status === "parked") return conflict();
  if (change._tag === "Park") return Result.succeed(parked(change.reason));
  if (change._tag === "Claim") {
    const due = messageDeliveryDeadline(record);

    if (due === null || due > now) return conflict();
    if (now >= record.deadlineAtMillis) return Result.succeed(parked("deadline"));
    if (record.retry.automaticAttempts >= record.policy.maxAutomaticAttempts)
      return Result.succeed(parked("exhausted"));

    return Result.succeed({
      ...base,
      leaseUntilMillis: Math.min(now + record.policy.attemptTimeoutMillis, record.deadlineAtMillis),
      retry: {
        ...record.retry,
        attempts: record.retry.attempts + 1,
        automaticAttempts: record.retry.automaticAttempts + 1,
        lastAttemptAtMillis: now,
      },
    });
  }
  if (record.leaseUntilMillis === null || now > record.leaseUntilMillis) return conflict();
  if (change._tag === "Accept") {
    if (record.status !== "pending") return conflict();
    if (change.receipt.threadId !== record.envelope.threadId)
      return Result.fail(error("corrupt", "receipt-identity"));

    return Result.succeed({
      ...base,
      status: "accepted",
      receipt: change.receipt,
      leaseUntilMillis: null,
      retry: {
        ...record.retry,
        automaticAttempts: 0,
        nextAttemptAtMillis: Math.min(
          now + record.policy.settlementPollMillis,
          record.deadlineAtMillis,
        ),
        lastFailure: null,
      },
    });
  }
  if (change._tag === "Process") {
    if (record.status !== "accepted") return conflict();
    if (
      change.settlement.submissionId !== record.receipt?.submissionId ||
      change.settlement.receiptId !== record.receipt.receiptId
    )
      return Result.fail(error("corrupt", "settlement-identity"));

    return Result.succeed({
      ...base,
      status: "processed",
      settlement: change.settlement,
      leaseUntilMillis: null,
    });
  }
  if (change._tag === "ObservePending") {
    if (record.status !== "accepted") return conflict();

    return Result.succeed(parked("awaiting-settlement"));
  }
  if (change._tag === "Refuse") {
    if (record.status !== "pending") return conflict();

    return Result.succeed({
      ...base,
      status: "refused",
      refusal: change.code,
      leaseUntilMillis: null,
    });
  }
  if (now >= record.deadlineAtMillis) return Result.succeed(parked("deadline"));
  if (record.retry.automaticAttempts >= record.policy.maxAutomaticAttempts)
    return Result.succeed(parked("exhausted"));

  const delay = Math.min(
    record.policy.retryBaseMillis * 2 ** Math.min(record.retry.automaticAttempts - 1, 52),
    record.policy.retryMaxMillis,
  );

  return Result.succeed({
    ...base,
    leaseUntilMillis: null,
    retry: {
      ...record.retry,
      nextAttemptAtMillis: Math.min(
        now + (record.status === "accepted" ? record.policy.settlementPollMillis : delay),
        record.deadlineAtMillis,
      ),
      lastFailure: change.reason,
    },
  });
};

/**
 * Bounds one delivery wave to 1–100 records and 1–32 concurrent operations.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MessageDeliveryDriverLimits = Schema.Struct({
  batchSize: Positive.check(Schema.isLessThanOrEqualTo(100)),
  concurrency: Positive.check(Schema.isLessThanOrEqualTo(32)),
});

/**
 * Decoded value of {@link MessageDeliveryDriverLimits}.
 *
 * @category models
 * @since 0.1.0
 */
export type MessageDeliveryDriverLimits = typeof MessageDeliveryDriverLimits.Type;

/**
 * Finite, structured host pump. The host owns its Scope and wakes it from nextDeadline even
 * after source/receiver settlement. Failed or interrupted calls retain their expiring claim;
 * no ordinary tool is replayed here, only idempotent PreparedInput admission/status lookup.
 * Spans expose operation names; envelopes, principals, and message contents are never logged.
 *
 * @category services
 * @since 0.1.0
 */
export class MessageDeliveryDriver extends Context.Service<
  MessageDeliveryDriver,
  {
    readonly process: (
      key: MessageDeliveryKey,
    ) => Effect.Effect<MessageDeliveryRecord, MessageDeliveryFailure>;
    readonly runDue: (
      ownerThreadId?: ThreadId,
    ) => Effect.Effect<ReadonlyArray<MessageDeliveryRecord>, MessageDeliveryFailure>;
    readonly retry: (
      key: MessageDeliveryKey,
      expectedVersion: number,
      deadlineAtMillis: number,
    ) => Effect.Effect<MessageDeliveryRecord, MessageDeliveryFailure>;
  }
>()("@effect-agent/thread/MessageDeliveryDriver") {
  static layer(
    limits: MessageDeliveryDriverLimits = { batchSize: 100, concurrency: 4 },
  ): Layer.Layer<
    MessageDeliveryDriver,
    MessageDeliveryError,
    MessageDeliveryStore | PreparedInputAdmission | Crypto.Crypto
  > {
    return Layer.effect(
      MessageDeliveryDriver,
      Effect.gen(function* () {
        const config = yield* validateMessageDelivery(
          MessageDeliveryDriverLimits,
          limits,
          "driver-limits",
        );

        const store = yield* MessageDeliveryStore;
        const admission = yield* PreparedInputAdmission;
        const crypto = yield* Crypto.Crypto;
        const failpoint = yield* MessageDeliveryFailpoint;
        const semaphore = yield* Semaphore.make(config.concurrency);

        const process = (key: MessageDeliveryKey) =>
          semaphore
            .withPermit(
              Effect.gen(function* () {
                const current = yield* store.get(key);

                if (current === null) return yield* error("not-found", "process");
                const nowMillis = yield* Clock.currentTimeMillis;
                const due = messageDeliveryDeadline(current);

                if (due === null || due > nowMillis) return current;

                if (current.status === "pending" && current.predecessor !== undefined) {
                  const previous = yield* store.get({
                    ownerThreadId: key.ownerThreadId,
                    messageId: current.predecessor,
                  });

                  if (
                    previous === null ||
                    (previous.receipt === null && previous.status !== "refused")
                  ) {
                    return yield* store.change(key, {
                      _tag: "Defer",
                      expectedVersion: current.version,
                      nowMillis,
                      untilMillis: nowMillis + current.policy.settlementPollMillis,
                    });
                  }
                }

                const claimed = yield* store.change(key, {
                  _tag: "Claim",
                  expectedVersion: current.version,
                  nowMillis,
                });

                if (claimed.status === "parked") return claimed;

                const envelope = yield* Schema.encodeEffect(PreparedInput)(claimed.envelope).pipe(
                  Effect.mapError((cause) => error("corrupt", "envelope", cause)),
                );

                const digest = yield* digestJson(envelope).pipe(
                  Effect.provideService(Crypto.Crypto, crypto),
                  Effect.mapError((cause) => error("storage", "digest", cause)),
                );

                if (digest !== claimed.envelopeDigest)
                  return yield* error("corrupt", "envelope-digest");

                const timeout = Math.min(
                  claimed.policy.attemptTimeoutMillis,
                  (claimed.leaseUntilMillis ?? claimed.deadlineAtMillis) -
                    (yield* Clock.currentTimeMillis),
                );

                // Digest computation cannot extend this claim's admission authority.
                if (timeout <= 0) return claimed;

                type WithoutFence<T> =
                  T extends Exclude<MessageDeliveryChange, { _tag: "Complete" }>
                    ? Omit<T, "expectedVersion" | "nowMillis">
                    : never;

                const commit = (change: WithoutFence<MessageDeliveryChange>) =>
                  Clock.currentTimeMillis.pipe(
                    Effect.flatMap((time) =>
                      store.change(key, {
                        ...change,
                        expectedVersion: claimed.version,
                        nowMillis: time,
                      }),
                    ),
                  );

                if (claimed.status === "pending") {
                  const outcome = yield* admitPreparedInput(
                    admission.submit(claimed.envelope),
                    timeout,
                  ).pipe(Effect.mapError((cause) => error("corrupt", "admission", cause)));

                  yield* failpoint.hit("message-delivery:admission:after");
                  if (outcome._tag === "Receipt") {
                    const accepted = yield* commit({ _tag: "Accept", receipt: outcome.receipt });

                    return yield* store.change(key, {
                      _tag: "Park",
                      reason: "awaiting-settlement",
                      expectedVersion: accepted.version,
                      nowMillis: yield* Clock.currentTimeMillis,
                    });
                  }
                  if (outcome._tag === "Refused")
                    return yield* commit({
                      _tag: "Refuse",
                      code: outcome.error.code,
                      diagnostic: FailureDiagnostic.capture(outcome.error),
                    });

                  return yield* commit({
                    _tag: "Retry",
                    reason: outcome.reason,
                    diagnostic: outcome.diagnostic,
                  });
                }
                if (claimed.receipt === null) return yield* error("corrupt", "receipt");
                if (admission.submissionStatus === undefined)
                  return yield* commit({ _tag: "Park", reason: "status-unavailable" });

                const status = yield* admission.submissionStatus(claimed.receipt).pipe(
                  Effect.timeout(timeout),
                  Effect.map((status) => ({ _tag: "Status" as const, status })),
                  Effect.catchTag("PreparedInputRetryable", (failure) =>
                    Effect.succeed({
                      _tag: "Retry" as const,
                      reason: failure.reason,
                      diagnostic: FailureDiagnostic.capture(failure),
                    }),
                  ),
                  Effect.catchTag("PreparedInputRefused", (failure) =>
                    Effect.succeed({
                      _tag: "Unavailable" as const,
                      diagnostic: FailureDiagnostic.capture(failure),
                    }),
                  ),
                  Effect.catchTag("TimeoutError", (failure) =>
                    Effect.succeed({
                      _tag: "Retry" as const,
                      reason: "timeout" as const,
                      diagnostic: FailureDiagnostic.capture(failure),
                    }),
                  ),
                  Effect.catchTag("ScheduleStorageError", (failure) =>
                    failure.reason === "unavailable"
                      ? Effect.succeed({
                          _tag: "Retry" as const,
                          reason: "storage" as const,
                          diagnostic: FailureDiagnostic.capture(failure),
                        })
                      : Effect.fail(error("corrupt", "status", failure)),
                  ),
                );

                if (status._tag === "Unavailable")
                  return yield* commit({
                    _tag: "Park",
                    reason: "status-unavailable",
                    diagnostic: status.diagnostic,
                  });
                if (status._tag === "Status" && status.status._tag === "settled")
                  return yield* commit({ _tag: "Process", settlement: status.status.settlement });
                if (status._tag === "Status") return yield* commit({ _tag: "ObservePending" });

                return yield* commit({
                  _tag: "Retry",
                  reason: status.reason,
                  diagnostic: status.diagnostic,
                });
              }),
            )
            .pipe(
              Effect.catchTag("MessageDeliveryError", (failure) =>
                failure.reason === "conflict"
                  ? store.get(key).pipe(
                      Effect.filterOrFail(
                        (record) => record !== null,
                        () => failure,
                      ),
                    )
                  : Effect.fail(failure),
              ),
            );

        return MessageDeliveryDriver.of({
          process,
          runDue: Effect.fnUntraced(function* (ownerThreadId) {
            const keys = yield* store.due(
              yield* Clock.currentTimeMillis,
              config.batchSize,
              ownerThreadId,
            );

            return yield* Effect.forEach(keys, process, { concurrency: config.concurrency });
          }),
          retry: Effect.fnUntraced(function* (key, expectedVersion, deadlineAtMillis) {
            return yield* store.change(key, {
              _tag: "Recover",
              expectedVersion,
              deadlineAtMillis,
              nowMillis: yield* Clock.currentTimeMillis,
            });
          }),
        });
      }),
    );
  }
}
