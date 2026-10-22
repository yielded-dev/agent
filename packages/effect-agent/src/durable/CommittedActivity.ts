/**
 * Finite processing of a captured committed Thread prefix into application-owned outputs. Durable
 * progress pins each output before application; destination effects require their own idempotency.
 *
 * @since 0.1.0
 */

import * as Clock from "effect/Clock";
import type * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import {
  ActivityProgress,
  ActivityProcessorKey,
  ActivityProcessorStore,
  PreparedActivity,
  type ActivityStoreFailure,
} from "./ActivityStore.ts";
import { digestJson, type DigestError } from "./Digest.ts";
import { CanonicalRecordEnvelope, CanonicalSequence, PersistedJson } from "./Records.ts";
import {
  ThreadRead,
  ThreadStore,
  ThreadTail,
  ThreadTailRequest,
  type ThreadNotMaterialized,
  type ThreadStoreError,
} from "./ThreadStore.ts";

/**
 * Bounds for one finite activity pass: 1–4,096 records, pages of 1–128 records, and a 1–300,000
 * millisecond timeout.
 *
 * @category models
 * @since 0.1.0
 */
export class ActivityPassLimits extends Schema.Class<ActivityPassLimits>(
  "@effect-agent/thread/ActivityPassLimits",
)({
  maxRecords: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 4_096 })),
  pageSize: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 128 })),
  timeoutMillis: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 300_000 })),
}) {}

/**
 * Invalid processor input, noncontiguous committed evidence, or a timed-out activity pass.
 *
 * @category errors
 * @since 0.1.0
 */
export class ActivityProcessingError extends Schema.TaggedError<ActivityProcessingError>()(
  "ActivityProcessingError",
  {
    reason: Schema.Literals(["invalid-input", "noncontiguous", "timeout"]),
    message: Schema.String.check(Schema.isMaxLength(1_024)),
  },
) {}

/**
 * Progress through the captured Thread tail, with processed and remaining record counts and
 * epoch-millisecond start and finish times.
 *
 * @category models
 * @since 0.1.0
 */
export class ActivityPassResult extends Schema.Class<ActivityPassResult>(
  "@effect-agent/thread/ActivityPassResult",
)({
  key: ActivityProcessorKey,
  capturedTail: CanonicalSequence,
  throughSequence: CanonicalSequence,
  processed: Schema.Natural,
  pendingRecords: Schema.Natural,
  startedAt: Schema.Finite,
  finishedAt: Schema.Finite,
}) {}

/**
 * Application-owned extraction and idempotent application of committed records, with a versioned
 * progress identity and finite pass limits.
 *
 * @category models
 * @since 0.1.0
 */
export interface CommittedActivityProcessor<E = never, R = never, EApply = never, RApply = never> {
  readonly key: ActivityProcessorKey;
  readonly limits: ActivityPassLimits;
  /** Select eligibility and encode an application Schema; null can represent no output. */
  readonly extract: (record: CanonicalRecordEnvelope) => Effect.Effect<PersistedJson, E, R>;
  /**
   * Must durably reconcile workId before returning, using atomic idempotency and conditional
   * writes. This may run again after any lost acknowledgement. A stale invocation can finish
   * only the already pinned output; it must not overwrite a later correction or withdrawal.
   * Retain reconciliation evidence for the lifetime of pending work, including any supported
   * backup/restore window. Returning before durable application would permit skipped output.
   */
  readonly apply: (work: PreparedActivity) => Effect.Effect<void, EApply, RApply>;
}

const invalid = (message: string) =>
  ActivityProcessingError.make({ reason: "invalid-input", message });

const contiguous = () =>
  ActivityProcessingError.make({
    reason: "noncontiguous",
    message: "Committed activity does not match its captured Thread prefix or pinned record",
  });

/**
 * Stable work identity independent of local sessions, retry count, extraction, and clocks.
 *
 * @category operations
 * @since 0.1.0
 */
export const activityWorkId = (key: ActivityProcessorKey, sequence: CanonicalSequence) =>
  digestJson([
    "effect-agent/activity@1",
    key.processorId,
    key.processorVersion,
    key.threadId,
    sequence,
  ]);

/**
 * Process one bounded committed prefix of one application-selected Thread. Nothing runs until
 * invoked, and no daemon, global Thread directory, admission ledger, or engine checkpoint is
 * involved. PersistentHistory makes a successful Run's batch visible together; durable Runs
 * expose incremental committed records. The application decides what each record means.
 *
 * A durable pending output wins over a new extraction after restart. Apply finishes before
 * progress advances, so separate output stores require durable idempotency. This does not
 * promise exactly-once external side effects. Each callback owns a Scope; the finite pass has
 * a timeout. Its local session closes with the pass Scope and rejects late mutations;
 * recovery reuses durable progress immediately under the same single-owner routing contract.
 *
 * @category operations
 * @since 0.1.0
 */
export const processCommittedActivity = Effect.fnUntraced(function* <
  E = never,
  R = never,
  EApply = never,
  RApply = never,
>(
  processor: CommittedActivityProcessor<E, R, EApply, RApply>,
): Effect.fn.Return<
  ActivityPassResult,
  | E
  | EApply
  | ActivityStoreFailure
  | ActivityProcessingError
  | DigestError
  | ThreadStoreError
  | ThreadNotMaterialized,
  ActivityProcessorStore | ThreadStore | Crypto.Crypto | Exclude<R | RApply, Scope.Scope>
> {
  const key = yield* Schema.decodeEffect(ActivityProcessorKey)(processor.key).pipe(
    Effect.mapError(() => invalid("Invalid activity processor key")),
  );

  const limits = yield* Schema.decodeEffect(ActivityPassLimits)(processor.limits).pipe(
    Effect.mapError(() => invalid("Invalid activity pass limits")),
  );

  const progress = yield* ActivityProcessorStore;
  const threads = yield* ThreadStore;

  return yield* Effect.scoped(
    Effect.gen(function* () {
      const session = yield* progress.acquire(key);

      let current = yield* Schema.decodeEffect(Schema.toType(ActivityProgress))(
        session.progress,
      ).pipe(Effect.mapError(() => invalid("Malformed activity progress")));

      if (!Schema.toEquivalence(ActivityProcessorKey)(current.key, key))
        return yield* invalid("Activity progress identity does not match its request");
      yield* session.checkActive;
      const startedAt = yield* Clock.currentTimeMillis;

      const tail = yield* threads
        .inspectTail(ThreadTailRequest.make({ threadId: key.threadId }))
        .pipe(
          Effect.flatMap((value) =>
            Schema.decodeEffect(ThreadTail)(value).pipe(Effect.mapError(contiguous)),
          ),
        );

      if (
        tail.threadId !== key.threadId ||
        tail.tailSequence < current.throughSequence ||
        (current.pending !== null && tail.tailSequence < current.pending.sequence)
      ) {
        return yield* contiguous();
      }
      const through = Math.min(tail.tailSequence, current.throughSequence + limits.maxRecords);
      let processed = 0;

      while (current.throughSequence < through) {
        const limit = Math.min(limits.pageSize, through - current.throughSequence);

        const page = yield* threads
          .read(
            ThreadRead.make({
              threadId: key.threadId,
              afterSequence: current.throughSequence,
              limit,
            }),
          )
          .pipe(Stream.take(limit + 1), Stream.runCollect);

        if (page.length !== limit) return yield* contiguous();
        for (const rawRecord of page) {
          const record = yield* Schema.decodeEffect(Schema.toType(CanonicalRecordEnvelope))(
            rawRecord,
          ).pipe(Effect.mapError(contiguous));

          if (record.threadId !== key.threadId || record.sequence !== current.throughSequence + 1) {
            return yield* contiguous();
          }
          yield* session.checkActive;
          const workId = yield* activityWorkId(key, record.sequence);

          let work = current.pending;

          if (work === null) {
            const output = yield* Effect.scoped(processor.extract(record)).pipe(
              Effect.flatMap((value) =>
                Schema.decodeEffect(PersistedJson)(value).pipe(
                  Effect.mapError(() => invalid("Malformed activity output")),
                ),
              ),
            );

            work = yield* session.prepare(
              PreparedActivity.make({
                version: 1,
                key,
                sequence: record.sequence,
                workId,
                recordId: record.record.recordId,
                output,
              }),
            );
          }

          const prepared = yield* Schema.decodeEffect(PreparedActivity)(work).pipe(
            Effect.mapError(() => invalid("Malformed prepared activity")),
          );

          if (
            !Schema.toEquivalence(ActivityProcessorKey)(prepared.key, key) ||
            prepared.sequence !== record.sequence ||
            prepared.workId !== workId ||
            prepared.recordId !== record.record.recordId
          )
            return yield* contiguous();
          yield* session.checkActive;
          yield* Effect.scoped(processor.apply(prepared));

          const next = yield* session
            .advance(workId)
            .pipe(
              Effect.flatMap((value) =>
                Schema.decodeEffect(Schema.toType(ActivityProgress))(value).pipe(
                  Effect.mapError(() => invalid("Malformed activity progress")),
                ),
              ),
            );

          if (
            !Schema.toEquivalence(ActivityProcessorKey)(next.key, key) ||
            next.throughSequence !== record.sequence ||
            next.pending !== null
          )
            return yield* invalid("Activity progress advanced outside the prepared record");
          current = next;
          processed += 1;
        }
      }

      return ActivityPassResult.make({
        key,
        capturedTail: tail.tailSequence,
        throughSequence: current.throughSequence,
        processed,
        pendingRecords: tail.tailSequence - current.throughSequence,
        startedAt,
        finishedAt: yield* Clock.currentTimeMillis,
      });
    }),
  ).pipe(
    Effect.timeoutOrElse({
      duration: limits.timeoutMillis,
      orElse: () =>
        Effect.fail(
          ActivityProcessingError.make({ reason: "timeout", message: "Activity pass timed out" }),
        ),
    }),
  );
});
