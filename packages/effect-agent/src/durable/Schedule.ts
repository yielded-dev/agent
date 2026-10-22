/**
 * Schemas and host ports for owner-scoped timed input delivery, authorization, storage, and wake
 * hints. Each prepared occurrence retains its admission identity until receipt or conclusive
 * refusal.
 *
 * @since 0.1.0
 */

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";

import { AgentId, ThreadId } from "../core/Identifiers.ts";
import { Receipt } from "../core/Receipt.ts";
import type { PreparedInputAuthorizationDecision } from "./PreparedInputAdmission.ts";
import { PreparedInput, PreparedInputRetryReason } from "./PreparedInputAdmission.ts";
import { DefinitionDigests, Digest, PersistedJson } from "./Records.ts";
import { AdmissionFence, AdmissionGroup, Principal } from "./SubmissionLedger.ts";

const Name = Schema.NonEmptyString.check(
  Schema.isMaxLength(128),
  Schema.isPattern(/^[^\p{Surrogate}]*$/u),
);

/**
 * Milliseconds since the Unix epoch, within DateTime's supported range.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleInstant = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(8_640_000_000_000_000),
);

const Positive = Schema.Int.check(Schema.isGreaterThan(0));

/**
 * Owner-local schedule name, bounded to 128 characters and branded separately from other
 * identifiers.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleId = Name.pipe(Schema.brand("@effect-agent/thread/ScheduleId"));
/**
 * Decoded value of {@link ScheduleId}.
 *
 * @category schemas
 * @since 0.1.0
 */
export type ScheduleId = typeof ScheduleId.Type;
/**
 * Tenant-qualified application owner for schedule management and listing.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleOwner = Schema.Struct({ tenantId: Name, ownerId: Name });
/**
 * Decoded value of {@link ScheduleOwner}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleOwner = typeof ScheduleOwner.Type;
/**
 * Addresses one schedule within its tenant-qualified owner.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleKey = Schema.Struct({ owner: ScheduleOwner, scheduleId: ScheduleId });
/**
 * Decoded value of {@link ScheduleKey}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleKey = typeof ScheduleKey.Type;
/**
 * Pairs the managed schedule owner with the authenticated principal used for authorization.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleScope = Schema.Struct({ owner: ScheduleOwner, principal: Principal });
/**
 * Decoded value of {@link ScheduleScope}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleScope = typeof ScheduleScope.Type;

const At = Schema.Struct({ _tag: Schema.Literal("At"), atMillis: ScheduleInstant });
const After = Schema.Struct({ _tag: Schema.Literal("After"), delayMillis: Positive });

const Interval = Schema.Struct({
  _tag: Schema.Literal("Interval"),
  everyMillis: Positive,
  anchorMillis: ScheduleInstant,
});

const Cron = Schema.Struct({
  _tag: Schema.Literal("Cron"),
  expression: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  timeZone: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
});

/**
 * A relative delay is a request value, never a stored unresolved deadline.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleTimingRequest = Schema.Union([
  At,
  After,
  Schema.Struct({
    _tag: Schema.Literal("Interval"),
    everyMillis: Positive,
    anchorMillis: Schema.optionalKey(ScheduleInstant),
  }),
  Schema.Struct({
    _tag: Schema.Literal("Cron"),
    expression: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
    timeZone: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(128))),
  }),
]);

/**
 * Decoded value of {@link ScheduleTimingRequest}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleTimingRequest = typeof ScheduleTimingRequest.Type;
/**
 * Resolved one-time, anchored interval, or time-zone-aware cron timing retained by a schedule.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleTiming = Schema.Union([At, Interval, Cron]);
/**
 * Decoded value of {@link ScheduleTiming}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleTiming = typeof ScheduleTiming.Type;

/**
 * Delivers occurrences to an existing Thread or a fresh Thread for each occurrence.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleDestination = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("ExistingThread"), threadId: ThreadId }),
  Schema.Struct({ _tag: Schema.Literal("FreshThread") }),
]);

/**
 * Decoded value of {@link ScheduleDestination}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleDestination = typeof ScheduleDestination.Type;

/**
 * Versioned schedule behavior, including resolved timing, destination, delivery authority, and
 * encoded Agent input.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleConfiguration = Schema.Struct({
  timing: ScheduleTiming,
  destination: ScheduleDestination,
  deliveryPrincipal: Principal,
  admissionGroup: Schema.optionalKey(AdmissionGroup),
  admissionFence: Schema.optionalKey(AdmissionFence),
  agentId: AgentId,
  definitions: DefinitionDigests,
  input: PersistedJson,
  inputDigest: Digest,
});

/**
 * Decoded value of {@link ScheduleConfiguration}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleConfiguration = typeof ScheduleConfiguration.Type;

/**
 * Immutable admission data for one prepared occurrence, pinned to its configuration revision and
 * intended time.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduledEnvelope = Schema.Struct({
  schemaVersion: PreparedInput.fields.schemaVersion,
  owner: ScheduleOwner,
  scheduleId: ScheduleId,
  configurationRevision: Positive,
  intendedAtMillis: ScheduleInstant,
  preparedAtMillis: ScheduleInstant,
  occurrenceId: Digest,
  ...Struct.omit(PreparedInput.fields, ["schemaVersion", "workerAdmission", "messageAdmission"]),
});

/**
 * Decoded value of {@link ScheduledEnvelope}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduledEnvelope = typeof ScheduledEnvelope.Type;

/**
 * Retained attempt counters, recovery generation, parked state, and epoch-millisecond retry timing.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleRetry = Schema.Struct({
  attempts: Schema.Natural,
  generation: Schema.Natural,
  automaticAttempts: Schema.Natural,
  parked: Schema.Boolean,
  nextAttemptAtMillis: ScheduleInstant,
  lastAttemptAtMillis: Schema.NullOr(ScheduleInstant),
  lastFailure: Schema.NullOr(PreparedInputRetryReason),
});

/**
 * Decoded value of {@link ScheduleRetry}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleRetry = typeof ScheduleRetry.Type;
/**
 * A prepared occurrence whose exact envelope remains owed until receipt or conclusive refusal.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SchedulePending = Schema.Struct({ envelope: ScheduledEnvelope, retry: ScheduleRetry });
/**
 * Decoded value of {@link SchedulePending}.
 *
 * @category models
 * @since 0.1.0
 */
export type SchedulePending = typeof SchedulePending.Type;

/**
 * Conclusive preparation or admission refusal for an occurrence, with bounded code and
 * epoch-millisecond timing.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleRefusal = Schema.Struct({
  atMillis: ScheduleInstant,
  intendedAtMillis: ScheduleInstant,
  occurrenceId: Schema.NullOr(Digest),
  phase: Schema.Literals(["preparation", "admission"]),
  code: Name,
});

/**
 * Decoded value of {@link ScheduleRefusal}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleRefusal = typeof ScheduleRefusal.Type;

/**
 * Half-open interval [fromMillis, toMillis); the upper bound is not skipped.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleSkippedRange = Schema.Struct({
  fromMillis: ScheduleInstant,
  toMillis: ScheduleInstant,
});

/**
 * Decoded value of {@link ScheduleSkippedRange}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleSkippedRange = typeof ScheduleSkippedRange.Type;

/**
 * Stored schedule configuration, revision, next occurrence, pending admission obligation, and last
 * delivery outcomes.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleRecord = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  owner: ScheduleOwner,
  scheduleId: ScheduleId,
  creationFingerprint: Digest,
  createdBy: Principal,
  createdAtMillis: ScheduleInstant,
  updatedAtMillis: ScheduleInstant,
  configurationRevision: Positive,
  version: Positive,
  configuration: ScheduleConfiguration,
  state: Schema.Literals(["active", "paused", "cancelled"]),
  nextAtMillis: Schema.NullOr(ScheduleInstant),
  pending: Schema.NullOr(SchedulePending),
  lastReceipt: Schema.NullOr(
    Schema.Struct({
      atMillis: ScheduleInstant,
      intendedAtMillis: ScheduleInstant,
      occurrenceId: Digest,
      receipt: Receipt,
    }),
  ),
  lastRefusal: Schema.NullOr(ScheduleRefusal),
  lastSkippedRange: Schema.NullOr(ScheduleSkippedRange),
});

/**
 * Decoded value of {@link ScheduleRecord}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleRecord = typeof ScheduleRecord.Type;

/**
 * Management view of a schedule and its delivery progress without the encoded input or definition
 * digests.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleSnapshot = Schema.Struct({
  owner: ScheduleOwner,
  scheduleId: ScheduleId,
  createdAtMillis: ScheduleInstant,
  updatedAtMillis: ScheduleInstant,
  configurationRevision: Positive,
  configuration: Schema.Struct({
    timing: ScheduleTiming,
    destination: ScheduleDestination,
    deliveryPrincipal: Principal,
    admissionGroup: Schema.optionalKey(AdmissionGroup),
    admissionFence: Schema.optionalKey(AdmissionFence),
    agentId: AgentId,
  }),
  state: Schema.Literals(["active", "paused", "cancelled"]),
  nextAtMillis: Schema.NullOr(ScheduleInstant),
  pending: Schema.NullOr(
    Schema.Struct({
      intendedAtMillis: ScheduleInstant,
      preparedAtMillis: ScheduleInstant,
      configurationRevision: Schema.optionalKey(Positive),
      occurrenceId: Digest,
      retry: ScheduleRetry,
    }),
  ),
  lastReceipt: Schema.NullOr(
    Schema.Struct({
      atMillis: ScheduleInstant,
      intendedAtMillis: ScheduleInstant,
      occurrenceId: Digest,
      receipt: Receipt,
    }),
  ),
  lastRefusal: Schema.NullOr(ScheduleRefusal),
  lastSkippedRange: Schema.NullOr(ScheduleSkippedRange),
  observedAtMillis: ScheduleInstant,
  pendingAgeMillis: Schema.NullOr(Schema.Natural),
  latenessMillis: Schema.Natural,
});

/**
 * Decoded value of {@link ScheduleSnapshot}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleSnapshot = typeof ScheduleSnapshot.Type;

/**
 * Up to 100 schedule management views with an owner-local continuation cursor.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleSnapshotPage = Schema.Struct({
  items: Schema.Array(ScheduleSnapshot).check(Schema.isMaxLength(100)),
  next: Schema.NullOr(ScheduleId),
});

/**
 * Decoded value of {@link ScheduleSnapshotPage}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleSnapshotPage = typeof ScheduleSnapshotPage.Type;

/**
 * A schedule request, timing rule, input, or configured bound is invalid.
 *
 * @category errors
 * @since 0.1.0
 */
export class ScheduleValidationError extends Schema.TaggedError<ScheduleValidationError>()(
  "ScheduleValidationError",
  { message: Schema.String },
) {}

/**
 * A schedule's creation identity, expected revision, or cancelled state conflicts with the
 * requested mutation.
 *
 * @category errors
 * @since 0.1.0
 */
export class ScheduleConflict extends Schema.TaggedError<ScheduleConflict>()("ScheduleConflict", {
  reason: Schema.Literals(["creation", "revision", "cancelled"]),
  key: ScheduleKey,
}) {}

/**
 * No retained schedule exists at the requested owner-qualified key.
 *
 * @category errors
 * @since 0.1.0
 */
export class ScheduleNotFound extends Schema.TaggedError<ScheduleNotFound>()("ScheduleNotFound", {
  key: ScheduleKey,
}) {}

/**
 * Host policy denied schedule management or preparation of an occurrence.
 *
 * @category errors
 * @since 0.1.0
 */
export class ScheduleAuthorizationError extends Schema.TaggedError<ScheduleAuthorizationError>()(
  "ScheduleAuthorizationError",
  { code: Name },
) {}

/**
 * Creating or reopening a schedule would exceed the owner's active-work allowance.
 *
 * @category errors
 * @since 0.1.0
 */
export class ScheduleCapacityError extends Schema.TaggedError<ScheduleCapacityError>()(
  "ScheduleCapacityError",
  { limit: Positive },
) {}

/**
 * Schedule storage is unavailable or contains corrupt state.
 *
 * @category errors
 * @since 0.1.0
 */
export class ScheduleStorageError extends Schema.TaggedError<ScheduleStorageError>()(
  "ScheduleStorageError",
  {
    cause: Schema.optionalKey(Schema.Defect()),
    operation: Schema.String,
    reason: Schema.Literals(["unavailable", "corrupt"]),
  },
) {}

/**
 * Schedule management actions checked by the host authorizer.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleManagementOperation =
  | "create"
  | "get"
  | "list"
  | "update"
  | "pause"
  | "resume"
  | "cancel"
  | "recover";

/**
 * Host authorization request for management of a schedule or owner-scoped listing.
 *
 * @category models
 * @since 0.1.0
 */
export interface ScheduleManagementAuthorization {
  readonly operation: ScheduleManagementOperation;
  readonly scope: ScheduleScope;
  readonly scheduleId?: ScheduleId;
  readonly configuration?: ScheduleConfiguration;
}

/**
 * Host authorization request for one exact configuration revision and intended occurrence.
 *
 * @category models
 * @since 0.1.0
 */
export interface ScheduleOccurrenceAuthorization {
  readonly key: ScheduleKey;
  readonly configurationRevision: number;
  readonly configuration: ScheduleConfiguration;
  readonly intendedAtMillis: number;
  readonly occurrenceId: Digest;
}

/**
 * Hosts must supply an explicit authorizer. There is no possession/default allow Layer.
 *
 * @category services
 * @since 0.1.0
 */
export class ScheduleAuthorizer extends Context.Service<
  ScheduleAuthorizer,
  {
    readonly manage: (
      request: ScheduleManagementAuthorization,
    ) => Effect.Effect<void, ScheduleAuthorizationError | ScheduleStorageError>;
    readonly prepare: (
      request: ScheduleOccurrenceAuthorization,
    ) => Effect.Effect<
      PreparedInputAuthorizationDecision,
      ScheduleAuthorizationError | ScheduleStorageError
    >;
  }
>()("@effect-agent/thread/ScheduleAuthorizer") {}

/**
 * Owner capacity, input bytes, timing, retry, and concurrency bounds for management and delivery;
 * durations are milliseconds.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SchedulingLimits = Schema.Struct({
  maxSchedulesPerOwner: Positive.check(Schema.isLessThanOrEqualTo(100_000)),
  minIntervalMillis: Positive,
  maxInputBytes: Positive.check(Schema.isLessThanOrEqualTo(65_536)),
  dueBatchSize: Positive.check(Schema.isLessThanOrEqualTo(1_024)),
  admissionConcurrency: Positive.check(Schema.isLessThanOrEqualTo(64)),
  retryBaseMillis: Positive,
  retryMaxMillis: Positive,
  admissionTimeoutMillis: Positive,
  recoveryPollMillis: Positive,
  maxAutomaticAttempts: Schema.optionalKey(Positive),
});

/**
 * Decoded value of {@link SchedulingLimits}.
 *
 * @category models
 * @since 0.1.0
 */
export type SchedulingLimits = typeof SchedulingLimits.Type;

/**
 * Default schedule policy: 1,000 per owner, a one-minute minimum interval, 64 KiB input, and eight
 * concurrent admissions with eight automatic attempts.
 *
 * @category defaults
 * @since 0.1.0
 */
export const defaultSchedulingLimits: SchedulingLimits = {
  maxSchedulesPerOwner: 1_000,
  minIntervalMillis: 60_000,
  maxInputBytes: 65_536,
  dueBatchSize: 64,
  admissionConcurrency: 8,
  retryBaseMillis: 1_000,
  retryMaxMillis: 300_000,
  admissionTimeoutMillis: 30_000,
  recoveryPollMillis: 30_000,
  maxAutomaticAttempts: 8,
};

/**
 * Owner-scoped keyset page request, returning at most 100 stored schedules after the supplied
 * schedule identity.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SchedulePageRequest = Schema.Struct({
  owner: ScheduleOwner,
  after: Schema.optionalKey(ScheduleId),
  limit: Positive.check(Schema.isLessThanOrEqualTo(100)),
});

/**
 * Decoded value of {@link SchedulePageRequest}.
 *
 * @category models
 * @since 0.1.0
 */
export type SchedulePageRequest = typeof SchedulePageRequest.Type;

/**
 * Stored schedules and the owner-local schedule cursor for continuing the listing.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SchedulePage = Schema.Struct({
  items: Schema.Array(ScheduleRecord).check(Schema.isMaxLength(100)),
  next: Schema.NullOr(ScheduleId),
});

/**
 * Decoded value of {@link SchedulePage}.
 *
 * @category models
 * @since 0.1.0
 */
export type SchedulePage = typeof SchedulePage.Type;

/**
 * Driver pagination uses the index only; a corrupt record cannot poison an entire page.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleDueCursor = Schema.Struct({
  owner: ScheduleOwner,
  scheduleId: ScheduleId,
  deadlineAtMillis: ScheduleInstant,
});

/**
 * Decoded value of {@link ScheduleDueCursor}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleDueCursor = typeof ScheduleDueCursor.Type;

/**
 * Local transaction commands. Admission is deliberately absent from this union.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScheduleChange = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("Recover"),
    expectedGeneration: Schema.Natural,
    expectedRevision: Positive,
    occurrenceId: Digest,
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Update"),
    expectedRevision: Positive,
    configuration: ScheduleConfiguration,
    nextAtMillis: Schema.NullOr(ScheduleInstant),
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Control"),
    expectedRevision: Positive,
    expectedVersion: Positive,
    action: Schema.Literals(["pause", "resume", "cancel"]),
    nextAtMillis: Schema.NullOr(ScheduleInstant),
    nowMillis: ScheduleInstant,
    skippedRange: Schema.NullOr(ScheduleSkippedRange),
  }),
  Schema.Struct({
    _tag: Schema.Literal("Prepare"),
    expectedRevision: Positive,
    expectedCursor: ScheduleInstant,
    envelope: ScheduledEnvelope,
    nextAtMillis: Schema.NullOr(ScheduleInstant),
    skippedRange: Schema.NullOr(ScheduleSkippedRange),
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({
    _tag: Schema.Literal("DenyPreparation"),
    expectedRevision: Positive,
    expectedCursor: ScheduleInstant,
    refusal: ScheduleRefusal,
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Complete"),
    occurrenceId: Digest,
    receipt: Receipt,
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Retry"),
    occurrenceId: Digest,
    retry: ScheduleRetry,
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Refuse"),
    occurrenceId: Digest,
    refusal: ScheduleRefusal,
    nowMillis: ScheduleInstant,
  }),
]);

/**
 * Decoded value of {@link ScheduleChange}.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleChange = typeof ScheduleChange.Type;

/**
 * Expected schedule storage, conflict, missing-record, capacity, or injected mutation failures.
 *
 * @category models
 * @since 0.1.0
 */
export type ScheduleStoreFailure =
  | ScheduleStorageError
  | ScheduleConflict
  | ScheduleNotFound
  | ScheduleCapacityError
  | ScheduleFailpointError;

/**
 * Each insert/change is one atomic local transaction, including platform wake changes.
 *
 * @category services
 * @since 0.1.0
 */
export class ScheduleStore extends Context.Service<
  ScheduleStore,
  {
    readonly insert: (
      record: ScheduleRecord,
      ownerLimit: number,
    ) => Effect.Effect<ScheduleRecord, ScheduleStoreFailure>;
    readonly get: (key: ScheduleKey) => Effect.Effect<ScheduleRecord | null, ScheduleStorageError>;
    readonly list: (
      request: SchedulePageRequest,
    ) => Effect.Effect<SchedulePage, ScheduleStorageError>;
    readonly change: (
      key: ScheduleKey,
      change: ScheduleChange,
      ownerLimit?: number,
    ) => Effect.Effect<ScheduleRecord, ScheduleStoreFailure>;
    /** Internal driver query, not a public management listing. Pending deadlines take precedence. */
    readonly due: (
      nowMillis: number,
      limit: number,
      owner?: ScheduleOwner,
      after?: ScheduleDueCursor,
    ) => Effect.Effect<ReadonlyArray<ScheduleDueCursor>, ScheduleStorageError>;
    readonly nextDeadline: (
      owner?: ScheduleOwner,
    ) => Effect.Effect<number | null, ScheduleStorageError>;
  }
>()("@effect-agent/thread/ScheduleStore") {}

/**
 * A dropped hint is repaired by the driver's indexed recovery query.
 *
 * @category services
 * @since 0.1.0
 */
export class ScheduleWake extends Context.Service<
  ScheduleWake,
  {
    readonly notify: Effect.Effect<void>;
    readonly await: Effect.Effect<void>;
  }
>()("@effect-agent/thread/ScheduleWake") {}

/**
 * An injected failure at a schedule storage mutation boundary.
 *
 * @category errors
 * @since 0.1.0
 */
export class ScheduleFailpointError extends Schema.TaggedError<ScheduleFailpointError>()(
  "ScheduleFailpointError",
  { point: Schema.String },
) {}

/**
 * Optional schedule-storage fault injection; its default handler does nothing.
 *
 * @category services
 * @since 0.1.0
 */
export const ScheduleFailpoint = Context.Reference<{
  readonly hit: (point: string) => Effect.Effect<void, ScheduleFailpointError>;
}>("@effect-agent/thread/ScheduleFailpoint", {
  defaultValue: () => ({ hit: () => Effect.void }),
});
