/**
 * Durable cursor and prepared-output storage for application-owned committed-activity processors.
 * Scoped local sessions pin output before external application and reject late mutations after
 * closure.
 *
 * @since 0.1.0
 */

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

import { ThreadId } from "../core/Identifiers.ts";
import { CanonicalSequence, Digest, PersistedJson, RecordId } from "./Records.ts";

const Identity = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const Timestamp = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));

/**
 * Application-owned processor identity. Changing the version starts independent progress.
 *
 * @category models
 * @since 0.1.0
 */
export class ActivityProcessorKey extends Schema.Class<ActivityProcessorKey>(
  "@effect-agent/thread/ActivityProcessorKey",
)({
  processorId: Identity,
  processorVersion: Identity,
  threadId: ThreadId.check(Schema.isMaxLength(1_024)),
}) {}

/**
 * Durable extraction output, pinned before any external application of that output.
 *
 * @category models
 * @since 0.1.0
 */
export class PreparedActivity extends Schema.Class<PreparedActivity>(
  "@effect-agent/thread/PreparedActivity",
)({
  version: Schema.Literal(1),
  key: ActivityProcessorKey,
  sequence: CanonicalSequence.check(Schema.isGreaterThan(0)),
  workId: Digest,
  recordId: RecordId,
  output: PersistedJson,
}) {}

/**
 * Durable cursor and exact pending output; local authority is never stored here.
 *
 * @category models
 * @since 0.1.0
 */
export class ActivityProgress extends Schema.Class<ActivityProgress>(
  "@effect-agent/thread/ActivityProgress",
)({
  version: Schema.Literal(2),
  key: ActivityProcessorKey,
  throughSequence: CanonicalSequence,
  pending: Schema.NullOr(PreparedActivity),
  /** Time of the last cursor advance, not the activity time or a global freshness clock. */
  advancedAt: Schema.NullOr(Timestamp),
}) {}

/**
 * An activity progress operation rejected its input or encountered unavailable, corrupt, or
 * incompatible storage.
 *
 * @category errors
 * @since 0.1.0
 */
export class ActivityStoreError extends Schema.TaggedError<ActivityStoreError>()(
  "ActivityStoreError",
  {
    operation: Schema.NonEmptyString,
    reason: Schema.Literals(["invalid-input", "unavailable", "corrupt", "incompatible"]),
  },
) {}

/**
 * Another local session already owns this processor, version, and Thread.
 *
 * @category errors
 * @since 0.1.0
 */
export class ActivityBusy extends Schema.TaggedError<ActivityBusy>()("ActivityBusy", {
  key: ActivityProcessorKey,
}) {}

/**
 * The scoped activity session has closed and can no longer mutate progress.
 *
 * @category errors
 * @since 0.1.0
 */
export class ActivityOwnershipLost extends Schema.TaggedError<ActivityOwnershipLost>()(
  "ActivityOwnershipLost",
  { key: ActivityProcessorKey },
) {}

/**
 * Prepared work or a cursor advance does not match the stored processor progress or its pinned
 * output.
 *
 * @category errors
 * @since 0.1.0
 */
export class ActivityWorkConflict extends Schema.TaggedError<ActivityWorkConflict>()(
  "ActivityWorkConflict",
  { key: ActivityProcessorKey, workId: Digest },
) {}

/**
 * Named storage boundaries at which activity progress mutations can be fault-injected.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ActivityMutationPoint = Schema.Literals([
  "activity:initialize:before",
  "activity:initialize:after",
  "activity:prepare:before",
  "activity:prepare:after-state",
  "activity:prepare:after",
  "activity:advance:before",
  "activity:advance:after-state",
  "activity:advance:after",
]);

/**
 * Decoded value of {@link ActivityMutationPoint}.
 *
 * @category models
 * @since 0.1.0
 */
export type ActivityMutationPoint = typeof ActivityMutationPoint.Type;

/**
 * An injected failure at an activity progress mutation boundary.
 *
 * @category errors
 * @since 0.1.0
 */
export class ActivityMutationFailure extends Schema.TaggedError<ActivityMutationFailure>()(
  "ActivityMutationFailure",
  { point: ActivityMutationPoint },
) {}

/**
 * Fault-injection service for activity progress storage; its default layer does nothing.
 *
 * @category services
 * @since 0.1.0
 */
export class ActivityMutationFailpoint extends Context.Service<
  ActivityMutationFailpoint,
  { readonly hit: (point: ActivityMutationPoint) => Effect.Effect<void, ActivityMutationFailure> }
>()("@effect-agent/thread/ActivityMutationFailpoint") {
  static readonly layer = Layer.succeed(this, { hit: () => Effect.void });
}

/**
 * Expected storage, contention, ownership, work-conflict, and injected failures of activity
 * progress operations.
 *
 * @category models
 * @since 0.1.0
 */
export type ActivityStoreFailure =
  | ActivityStoreError
  | ActivityBusy
  | ActivityOwnershipLost
  | ActivityWorkConflict
  | ActivityMutationFailure;

/**
 * A local consumer capability. It cannot be encoded or restored from durable progress.
 *
 * @category models
 * @since 0.1.0
 */
export interface ActivitySession {
  /** Progress captured under the owner's gate at acquisition. */
  readonly progress: ActivityProgress;
  readonly checkActive: Effect.Effect<void, ActivityOwnershipLost>;
  readonly prepare: (
    work: PreparedActivity,
  ) => Effect.Effect<PreparedActivity, ActivityStoreFailure>;
  readonly advance: (workId: Digest) => Effect.Effect<ActivityProgress, ActivityStoreFailure>;
}

/**
 * Optional durable progress for finite committed-activity passes. One process owns each
 * processor/version/Thread key; all live facets over that storage share a local owner.
 * acquire is scoped and read-only: elapsed time never grants another session. Closing the
 * caller or store Scope rejects its late mutations without writing a release record.
 *
 * prepare pins one exact next output; advance requires that output's workId and atomically
 * advances the cursor while clearing pending. Neither operation grants canonical Thread
 * ownership. External application remains conditional/idempotent across lost acknowledgements.
 *
 * @category services
 * @since 0.1.0
 */
export class ActivityProcessorStore extends Context.Service<
  ActivityProcessorStore,
  {
    readonly inspect: (
      key: ActivityProcessorKey,
    ) => Effect.Effect<ActivityProgress | null, ActivityStoreError>;
    readonly acquire: (
      key: ActivityProcessorKey,
    ) => Effect.Effect<ActivitySession, ActivityStoreFailure, Scope.Scope>;
  }
>()("@effect-agent/thread/ActivityProcessorStore") {}
