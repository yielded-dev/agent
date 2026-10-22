/**
 * Versioned source-partition registrations, accepted events, selected deliveries, and their host
 * authorization and storage contracts. Selection pins the configuration needed to finish delivery
 * after later edits.
 *
 * @since 0.1.0
 */

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { AgentId, ThreadId } from "../core/Identifiers.ts";
import { Receipt } from "../core/Receipt.ts";
import type { PreparedInputAuthorizationDecision } from "./PreparedInputAdmission.ts";
import { PreparedInput } from "./PreparedInputAdmission.ts";
import { DefinitionDigests, Digest, PersistedJson } from "./Records.ts";
import { ScheduleDestination, ScheduleInstant, ScheduleRetry } from "./Schedule.ts";
import { AdmissionFence, AdmissionGroup, IdempotencyKey, Principal } from "./SubmissionLedger.ts";

/**
 * Nonempty subscription-domain name, bounded to 256 characters and excluding unpaired UTF-16
 * surrogates.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubscriptionName = Schema.NonEmptyString.check(
  Schema.isMaxLength(256),
  Schema.isPattern(/^[^\p{Surrogate}]*$/u),
);

const Positive = Schema.Int.check(Schema.isGreaterThan(0));

/**
 * Stable storage identity. Deployment and payload values must never change its routing.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SourcePartition = Schema.Struct({
  tenantId: SubscriptionName,
  address: SubscriptionName,
});

/**
 * Decoded value of {@link SourcePartition}.
 *
 * @category models
 * @since 0.1.0
 */
export type SourcePartition = typeof SourcePartition.Type;

/**
 * Stable source name and semantic version selecting normalization and matching behavior.
 *
 * @category schemas
 * @since 0.1.0
 */
export const EventSourceVersion = Schema.Struct({
  name: SubscriptionName,
  version: SubscriptionName,
});

/**
 * Decoded value of {@link EventSourceVersion}.
 *
 * @category models
 * @since 0.1.0
 */
export type EventSourceVersion = typeof EventSourceVersion.Type;

/**
 * Addresses one owner's subscription within its tenant-qualified source partition.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubscriptionKey = Schema.Struct({
  partition: SourcePartition,
  ownerId: SubscriptionName,
  subscriptionId: SubscriptionName,
});

/**
 * Decoded value of {@link SubscriptionKey}.
 *
 * @category models
 * @since 0.1.0
 */
export type SubscriptionKey = typeof SubscriptionKey.Type;

/**
 * Source partition and owner being managed, paired with the caller's principal for host
 * authorization.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubscriptionScope = Schema.Struct({
  partition: SourcePartition,
  ownerId: SubscriptionName,
  principal: Principal,
});

/**
 * Decoded value of {@link SubscriptionScope}.
 *
 * @category models
 * @since 0.1.0
 */
export type SubscriptionScope = typeof SubscriptionScope.Type;

/**
 * Versioned event matching, lifetime, destination, delivery authority, and input-preparation
 * context pinned by a registration.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubscriptionConfiguration = Schema.Struct({
  source: EventSourceVersion,
  matchingKey: SubscriptionName,
  parameters: PersistedJson,
  context: PersistedJson,
  mode: Schema.Literals(["once", "continuous"]),
  /** Null disables time-based expiry; once selection and cancellation still apply. */
  expiresAtMillis: Schema.NullOr(ScheduleInstant),
  destination: ScheduleDestination,
  deliveryPrincipal: Principal,
  admissionGroup: Schema.optionalKey(AdmissionGroup),
  admissionFence: Schema.optionalKey(AdmissionFence),
  agentId: AgentId,
  definitions: DefinitionDigests,
});

/**
 * Decoded value of {@link SubscriptionConfiguration}.
 *
 * @category models
 * @since 0.1.0
 */
export type SubscriptionConfiguration = typeof SubscriptionConfiguration.Type;

/**
 * Retained source-reconciliation attempts and next epoch-millisecond retry; null retry time records
 * a conclusive failure.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SourceRecovery = Schema.Struct({
  attempts: Schema.Natural,
  /** Null retains a conclusive source failure without continuing provider polling. */
  nextAttemptAtMillis: Schema.NullOr(ScheduleInstant),
  lastFailure: Schema.NullOr(SubscriptionName),
}).check(
  Schema.makeFilter((value) => value.nextAttemptAtMillis !== null || value.lastFailure !== null),
);

/**
 * Retained registration identity, current configuration revision, selection order, lifecycle state,
 * and source recovery progress.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubscriptionRecord = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  key: SubscriptionKey,
  creationFingerprint: Digest,
  configurationRevision: Positive,
  configurationFingerprint: Digest,
  creationConfiguration: SubscriptionConfiguration,
  createdBy: Principal,
  createdAtMillis: ScheduleInstant,
  /** Assigned atomically in the same order as event eligibility cutoffs. */
  ordinal: Schema.Natural,
  configuration: SubscriptionConfiguration,
  state: Schema.Literals(["active", "paused", "consumed", "cancelled"]),
  recovery: Schema.NullOr(SourceRecovery),
}).check(
  Schema.makeFilter(
    (value) =>
      (value.state !== "consumed" || value.configuration.mode === "once") &&
      (value.recovery === null ||
        (value.configuration.mode === "once" &&
          (value.state === "active" || value.state === "paused"))),
  ),
);

/**
 * Decoded value of {@link SubscriptionRecord}.
 *
 * @category models
 * @since 0.1.0
 */
export type SubscriptionRecord = typeof SubscriptionRecord.Type;

/**
 * Retained normalized event identity, payload digest, eligibility cutoff, and routing progress
 * within one source partition.
 *
 * @category schemas
 * @since 0.1.0
 */
export const AcceptedEvent = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  partition: SourcePartition,
  eventId: SubscriptionName,
  source: EventSourceVersion,
  matchingKey: SubscriptionName,
  payload: PersistedJson,
  payloadDigest: Digest,
  acceptedAtMillis: ScheduleInstant,
  occurredAtMillis: Schema.optionalKey(ScheduleInstant),
  /** Deduplication evidence after completed payload/delivery retention ends. */
  tombstone: Schema.optionalKey(Schema.Boolean),
  cutoff: Schema.Natural,
  cursor: Schema.Natural,
  routingComplete: Schema.Boolean,
  routingFailure: Schema.NullOr(SubscriptionName),
  nextAttemptAtMillis: ScheduleInstant,
}).check(
  Schema.makeFilter(
    (value) =>
      value.cursor <= value.cutoff &&
      (value.tombstone !== true || (value.payload === null && value.routingComplete)),
  ),
);

/**
 * Decoded value of {@link AcceptedEvent}.
 *
 * @category models
 * @since 0.1.0
 */
export type AcceptedEvent = typeof AcceptedEvent.Type;

/**
 * Acknowledges retained event routing work; it is not a destination Submission receipt.
 *
 * @category schemas
 * @since 0.1.0
 */
export const EventAcknowledgement = Schema.Struct({
  partition: SourcePartition,
  eventId: SubscriptionName,
  acceptedAtMillis: ScheduleInstant,
});

/**
 * Decoded value of {@link EventAcknowledgement}.
 *
 * @category models
 * @since 0.1.0
 */
export type EventAcknowledgement = typeof EventAcknowledgement.Type;

/**
 * Identifies one selected event delivery for a particular subscription.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubscriptionDeliveryKey = Schema.Struct({
  subscription: SubscriptionKey,
  eventId: SubscriptionName,
});

/**
 * Decoded value of {@link SubscriptionDeliveryKey}.
 *
 * @category models
 * @since 0.1.0
 */
export type SubscriptionDeliveryKey = typeof SubscriptionDeliveryKey.Type;

/**
 * Bounded reason for conclusive input-preparation or destination-admission refusal.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubscriptionRefusal = Schema.Struct({
  phase: Schema.Literals(["preparation", "admission"]),
  code: SubscriptionName,
});

/**
 * Frozen subscription/event selection and its prepared envelope, retries, destination receipt, and
 * completion evidence.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubscriptionDelivery = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  key: SubscriptionDeliveryKey,
  deliveryId: Digest,
  subscriptionFingerprint: Digest,
  configurationRevision: Positive,
  configuration: SubscriptionConfiguration,
  eventDigest: Digest,
  source: EventSourceVersion,
  threadId: ThreadId,
  admissionKey: IdempotencyKey,
  selectedAtMillis: ScheduleInstant,
  observeSettlement: Schema.optionalKey(Schema.Boolean),
  settledAtMillis: Schema.optionalKey(ScheduleInstant),
  completedAtMillis: Schema.optionalKey(ScheduleInstant),
  state: Schema.Literals(["selected", "prepared", "delivered", "refused"]),
  envelope: Schema.NullOr(PreparedInput),
  envelopeDigest: Schema.NullOr(Digest),
  retry: ScheduleRetry,
  receipt: Schema.NullOr(Receipt),
  refusal: Schema.NullOr(SubscriptionRefusal),
}).check(
  Schema.makeFilter((value) => {
    const hasEnvelope = value.envelope !== null && value.envelopeDigest !== null;
    const noEnvelope = value.envelope === null && value.envelopeDigest === null;

    switch (value.state) {
      case "selected":
        return noEnvelope && value.receipt === null && value.refusal === null;
      case "prepared":
        return hasEnvelope && value.receipt === null && value.refusal === null;
      case "delivered":
        return hasEnvelope && value.receipt !== null && value.refusal === null;
      case "refused":
        return (
          value.receipt === null &&
          value.refusal !== null &&
          (value.refusal.phase === "preparation" ? noEnvelope : hasEnvelope)
        );
    }
  }),
);

/**
 * Decoded value of {@link SubscriptionDelivery}.
 *
 * @category models
 * @since 0.1.0
 */
export type SubscriptionDelivery = typeof SubscriptionDelivery.Type;

/**
 * Management projections intentionally exclude parameters, context, payload, and input.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubscriptionSnapshot = Schema.Struct({
  key: SubscriptionKey,
  source: EventSourceVersion,
  mode: Schema.Literals(["once", "continuous"]),
  state: Schema.Literals(["active", "paused", "consumed", "cancelled", "expired"]),
  configurationRevision: Positive,
  configurationFingerprint: Digest,
  admissionGroup: SubscriptionConfiguration.fields.admissionGroup,
  admissionFence: SubscriptionConfiguration.fields.admissionFence,
  createdAtMillis: ScheduleInstant,
  /** Null disables time-based expiry; once selection and cancellation still apply. */
  expiresAtMillis: Schema.NullOr(ScheduleInstant),
  recovery: Schema.NullOr(SourceRecovery),
});

/**
 * Decoded value of {@link SubscriptionSnapshot}.
 *
 * @category models
 * @since 0.1.0
 */
export type SubscriptionSnapshot = typeof SubscriptionSnapshot.Type;

/**
 * Management view of a selected delivery's state, retries, receipt, and refusal without private
 * envelope contents.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubscriptionDeliverySnapshot = Schema.Struct({
  key: SubscriptionDeliveryKey,
  state: SubscriptionDelivery.fields.state,
  configurationRevision: Positive,
  observeSettlement: Schema.optionalKey(Schema.Boolean),
  settledAtMillis: Schema.optionalKey(ScheduleInstant),
  retry: ScheduleRetry,
  receipt: Schema.NullOr(Receipt),
  refusal: Schema.NullOr(SubscriptionRefusal),
});

/**
 * Typed validation, conflict, capacity, authorization, source/binding, or storage failure in
 * subscription processing.
 *
 * @category errors
 * @since 0.1.0
 */
export class SubscriptionError extends Schema.TaggedError<SubscriptionError>()(
  "SubscriptionError",
  {
    reason: Schema.Literals([
      "validation",
      "conflict",
      "not-found",
      "capacity",
      "unauthorized",
      "unsupported-source",
      "unsupported-binding",
      "storage",
      "corrupt",
    ]),
    code: SubscriptionName,
    cause: Schema.optionalKey(Schema.Defect()),
    currentRevision: Schema.optionalKey(Positive),
    currentState: Schema.optionalKey(
      Schema.Literals(["active", "paused", "consumed", "cancelled"]),
    ),
  },
) {}

/**
 * Host source adapters classify expected failures; raw provider diagnostics are never stored.
 *
 * @category errors
 * @since 0.1.0
 */
export class SubscriptionSourceError extends Schema.TaggedError<SubscriptionSourceError>()(
  "SubscriptionSourceError",
  {
    code: SubscriptionName,
    retryable: Schema.Boolean,
  },
) {}

/**
 * An injected failure at a subscription storage mutation boundary.
 *
 * @category errors
 * @since 0.1.0
 */
export class SubscriptionFailpointError extends Schema.TaggedError<SubscriptionFailpointError>()(
  "SubscriptionFailpointError",
  { point: Schema.String },
) {}

/**
 * Expected subscription storage failures and injected mutation failures.
 *
 * @category models
 * @since 0.1.0
 */
export type SubscriptionStoreFailure = SubscriptionError | SubscriptionFailpointError;

/**
 * Optional subscription-storage fault injection; the default handler does nothing.
 *
 * @category services
 * @since 0.1.0
 */
export const SubscriptionFailpoint = Context.Reference<{
  readonly hit: (point: string) => Effect.Effect<void, SubscriptionFailpointError>;
}>("@effect-agent/thread/SubscriptionFailpoint", {
  defaultValue: () => ({ hit: () => Effect.void }),
});

/**
 * Replay horizon and completed-evidence lifetime in milliseconds, with a bounded
 * deduplication-tombstone allowance.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubscriptionRetentionPolicy = Schema.Struct({
  replayHorizonMillis: Positive,
  completedRetentionMillis: Schema.Natural,
  maxTombstones: Positive.check(Schema.isLessThanOrEqualTo(100_000)),
});

/**
 * Decoded value of {@link SubscriptionRetentionPolicy}.
 *
 * @category models
 * @since 0.1.0
 */
export type SubscriptionRetentionPolicy = typeof SubscriptionRetentionPolicy.Type;

/**
 * Partition and owner capacities, payload/context byte limits, lifetime, retry, timeout, and
 * concurrency bounds.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubscriptionLimits = Schema.Struct({
  retention: Schema.optionalKey(SubscriptionRetentionPolicy),
  /** Partition-wide active/paused, unexpired registration capacity. */
  maxRegistrations: Positive.check(Schema.isLessThanOrEqualTo(100_000)),
  /** Active and paused registrations whose expiry has not passed. */
  maxRegistrationsPerOwner: Positive.check(Schema.isLessThanOrEqualTo(10_000)),
  /** Partition-wide unfinished routing capacity; selected deliveries have their own limits. */
  maxEvents: Positive.check(Schema.isLessThanOrEqualTo(100_000)),
  /** Partition-wide selected/prepared work and observed unsettled admission capacity. */
  maxDeliveries: Positive.check(Schema.isLessThanOrEqualTo(100_000)),
  /** Selected/prepared work, including parked retries, and observed unsettled admissions. */
  maxDeliveriesPerOwner: Positive.check(Schema.isLessThanOrEqualTo(10_000)),
  maxPayloadBytes: Positive.check(Schema.isLessThanOrEqualTo(65_536)),
  maxContextBytes: Positive.check(Schema.isLessThanOrEqualTo(16_384)),
  maxLifetimeMillis: Positive.check(Schema.isLessThanOrEqualTo(8_640_000_000_000_000)),
  batchSize: Positive.check(Schema.isLessThanOrEqualTo(100)),
  concurrency: Positive.check(Schema.isLessThanOrEqualTo(16)),
  retryMillis: Positive.check(Schema.isLessThanOrEqualTo(86_400_000)),
  maxAutomaticAttempts: Schema.optionalKey(Positive),
  operationTimeoutMillis: Positive.check(Schema.isLessThanOrEqualTo(300_000)),
});

/**
 * Decoded value of {@link SubscriptionLimits}.
 *
 * @category models
 * @since 0.1.0
 */
export type SubscriptionLimits = typeof SubscriptionLimits.Type;

/**
 * Default limits allow 10,000 registrations, events, and deliveries per partition, 1,000 per owner,
 * four concurrent operations, and eight automatic attempts.
 *
 * @category defaults
 * @since 0.1.0
 */
export const defaultSubscriptionLimits: SubscriptionLimits = {
  maxRegistrations: 10_000,
  maxRegistrationsPerOwner: 1_000,
  maxEvents: 10_000,
  maxDeliveries: 10_000,
  maxDeliveriesPerOwner: 1_000,
  maxPayloadBytes: 65_536,
  maxContextBytes: 16_384,
  maxLifetimeMillis: 30 * 86_400_000,
  batchSize: 16,
  concurrency: 4,
  retryMillis: 30_000,
  operationTimeoutMillis: 30_000,
  maxAutomaticAttempts: 8,
};

/**
 * Atomic revision-checked registration changes for recovery, configuration replacement, pause, and
 * resume.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubscriptionChange = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Recover"), nowMillis: ScheduleInstant }),
  Schema.Struct({
    _tag: Schema.Literal("Update"),
    configuration: SubscriptionConfiguration,
    configurationFingerprint: Digest,
    recovery: Schema.NullOr(SourceRecovery),
  }),
  Schema.Struct({ _tag: Schema.Literals(["Pause", "Resume"]) }),
]);

/**
 * Decoded value of {@link SubscriptionChange}.
 *
 * @category models
 * @since 0.1.0
 */
export type SubscriptionChange = typeof SubscriptionChange.Type;

/**
 * Atomic selected-delivery changes for preparation, admission, refusal, retry, recovery, and
 * settlement observation.
 *
 * @category schemas
 * @since 0.1.0
 */
export const DeliveryChange = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("ObserveSettlement"),
    receipt: Receipt,
    settled: Schema.Boolean,
    nowMillis: ScheduleInstant,
    nextAttemptAtMillis: ScheduleInstant,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Recover"),
    expectedGeneration: Schema.Natural,
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Prepare"),
    envelope: PreparedInput,
    envelopeDigest: Digest,
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({ _tag: Schema.Literal("Complete"), receipt: Receipt, nowMillis: ScheduleInstant }),
  Schema.Struct({
    _tag: Schema.Literal("Refuse"),
    refusal: SubscriptionRefusal,
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Retry"),
    retry: ScheduleRetry,
    nowMillis: ScheduleInstant,
  }),
]);

/**
 * Decoded value of {@link DeliveryChange}.
 *
 * @category models
 * @since 0.1.0
 */
export type DeliveryChange = typeof DeliveryChange.Type;

/**
 * Progress of bounded recovery sweeps, independent of individual work success.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubscriptionScanCursors = Schema.Struct({
  events: Schema.String,
  deliveries: Schema.String,
  recovery: Schema.Natural,
});

/**
 * Decoded value of {@link SubscriptionScanCursors}.
 *
 * @category models
 * @since 0.1.0
 */
export type SubscriptionScanCursors = typeof SubscriptionScanCursors.Type;

/**
 * One store instance owns exactly one tenant/source partition. Every mutation is local and
 * atomic, including required Cloudflare wakes. Node has one process owner per database.
 * Without retention, records remain for the partition lifetime. Partition-wide and per-owner
 * quotas bound live work, never completed replay history. Retention controls evidence reclamation.
 * No pruning may discard creation replay, deduplication, or unfinished-work evidence.
 *
 * @category services
 * @since 0.1.0
 */
export class SubscriptionStore extends Context.Service<
  SubscriptionStore,
  {
    readonly partition: SourcePartition;
    readonly compact: (
      nowMillis: number,
      policy: SubscriptionRetentionPolicy,
      limit: number,
    ) => Effect.Effect<number, SubscriptionStoreFailure>;
    readonly readScanCursors: Effect.Effect<SubscriptionScanCursors, SubscriptionError>;
    readonly advanceScanCursors: (
      cursors: SubscriptionScanCursors,
    ) => Effect.Effect<void, SubscriptionStoreFailure>;
    readonly register: (
      record: SubscriptionRecord,
      limits: SubscriptionLimits,
    ) => Effect.Effect<SubscriptionRecord, SubscriptionStoreFailure>;
    readonly get: (
      key: SubscriptionKey,
    ) => Effect.Effect<SubscriptionRecord | null, SubscriptionError>;
    readonly list: (
      ownerId: string,
      after: number,
      limit: number,
    ) => Effect.Effect<ReadonlyArray<SubscriptionRecord>, SubscriptionError>;
    readonly change: (
      key: SubscriptionKey,
      expectedRevision: number,
      change: SubscriptionChange,
      /** Reopening expired work reserves capacity; defaults to defaultSubscriptionLimits. */
      limits?: SubscriptionLimits,
    ) => Effect.Effect<SubscriptionRecord, SubscriptionStoreFailure>;
    readonly cancel: (
      key: SubscriptionKey,
      expectedRevision?: number,
    ) => Effect.Effect<SubscriptionRecord, SubscriptionStoreFailure>;
    /** Duplicate identity returns the original cutoff and routing progress; payload/version conflicts fail. */
    readonly accept: (
      event: AcceptedEvent,
      limits: SubscriptionLimits,
    ) => Effect.Effect<AcceptedEvent, SubscriptionStoreFailure>;
    readonly event: (eventId: string) => Effect.Effect<AcceptedEvent | null, SubscriptionError>;
    readonly pendingEvents: (
      nowMillis: number,
      after: string,
      limit: number,
    ) => Effect.Effect<ReadonlyArray<string>, SubscriptionError>;
    /** Indexed by partition, source/version, matchingKey, ordinal; never scan arbitrary parameters. */
    readonly candidates: (
      event: AcceptedEvent,
      limit: number,
    ) => Effect.Effect<ReadonlyArray<SubscriptionRecord>, SubscriptionError>;
    /** Distinct delivery keys only. Recheck eligibility and consume once + insert obligations + cursor atomically. */
    readonly select: (
      event: AcceptedEvent,
      deliveries: ReadonlyArray<SubscriptionDelivery>,
      cursor: number,
      complete: boolean,
      nowMillis: number,
      limits: SubscriptionLimits,
    ) => Effect.Effect<void, SubscriptionStoreFailure>;
    /** Only the explicitly reconciled watch may bypass cutoff, never reopen ordinary routing. */
    readonly catchUp: (
      event: AcceptedEvent,
      delivery: SubscriptionDelivery,
      nowMillis: number,
      limits: SubscriptionLimits,
    ) => Effect.Effect<void, SubscriptionStoreFailure>;
    readonly deferEvent: (
      eventId: string,
      nextAttemptAtMillis: number,
      code?: string,
    ) => Effect.Effect<void, SubscriptionStoreFailure>;
    readonly delivery: (
      key: SubscriptionDeliveryKey,
    ) => Effect.Effect<SubscriptionDelivery | null, SubscriptionError>;
    readonly pendingDeliveries: (
      nowMillis: number,
      after: string,
      limit: number,
    ) => Effect.Effect<ReadonlyArray<SubscriptionDeliveryKey>, SubscriptionError>;
    readonly listDeliveries: (
      key: SubscriptionKey,
      after: string,
      limit: number,
    ) => Effect.Effect<ReadonlyArray<SubscriptionDelivery>, SubscriptionError>;
    /**
     * A matching deliveryId fences stale outcomes. Prepare rechecks cancellation and expiry atomically.
     * Retry advances only with a higher attempt count and a nondecreasing next-attempt time.
     * Starting settlement observation reserves capacity using the supplied or default limits.
     */
    readonly changeDelivery: (
      key: SubscriptionDeliveryKey,
      deliveryId: Digest,
      change: DeliveryChange,
      limits?: SubscriptionLimits,
    ) => Effect.Effect<SubscriptionDelivery, SubscriptionStoreFailure>;
    readonly recovering: (
      nowMillis: number,
      after: number,
      limit: number,
    ) => Effect.Effect<
      ReadonlyArray<{ readonly key: SubscriptionKey; readonly ordinal: number }>,
      SubscriptionError
    >;
    readonly deferRecovery: (
      key: SubscriptionKey,
      expectedRevision: number,
      recovery: typeof SourceRecovery.Type | null,
    ) => Effect.Effect<void, SubscriptionStoreFailure>;
    readonly nextDeadline: Effect.Effect<number | null, SubscriptionError>;
  }
>()("@effect-agent/thread/SubscriptionStore") {}

/**
 * Explicit host policy. Management scope is not inferred from handle possession.
 *
 * @category services
 * @since 0.1.0
 */
export class SubscriptionAuthorizer extends Context.Service<
  SubscriptionAuthorizer,
  {
    readonly manage: (
      operation:
        | "get"
        | "subscribe"
        | "update"
        | "pause"
        | "resume"
        | "recover"
        | "list"
        | "cancel"
        | "deliveries",
      scope: SubscriptionScope,
      configuration?: SubscriptionConfiguration,
    ) => Effect.Effect<void, SubscriptionError>;
    readonly intake: (
      partition: SourcePartition,
      source: EventSourceVersion,
      principal: Principal,
    ) => Effect.Effect<void, SubscriptionError>;
    /** Host recovery authority, independent of the principal who registered the watch. */
    readonly reconcile: (
      subscription: SubscriptionRecord,
    ) => Effect.Effect<void, SubscriptionError>;
    readonly prepare: (
      subscription: SubscriptionRecord,
      event: AcceptedEvent,
    ) => Effect.Effect<typeof PreparedInputAuthorizationDecision.Type, SubscriptionError>;
  }
>()("@effect-agent/thread/SubscriptionAuthorizer") {}

/**
 * Encodes the full partition, owner, and subscription identity as an unambiguous JSON tuple.
 *
 * @category operations
 * @since 0.1.0
 */
export const subscriptionKeyString = (key: SubscriptionKey): string =>
  JSON.stringify([key.partition.tenantId, key.partition.address, key.ownerId, key.subscriptionId]);

/**
 * Encodes owner, subscription, and event identity for lookup within an already selected source
 * partition.
 *
 * @category operations
 * @since 0.1.0
 */
export const subscriptionDeliveryKeyString = (key: SubscriptionDeliveryKey): string =>
  JSON.stringify([key.subscription.ownerId, key.subscription.subscriptionId, key.eventId]);
