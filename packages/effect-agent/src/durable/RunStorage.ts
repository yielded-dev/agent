import type { Scope } from "effect";
import { Clock, Context, Duration, Effect, Layer, Option, Schema, Semaphore } from "effect";

import type { SubmissionId, ThreadId } from "../core/Identifiers.ts";
import type { CanonicalBatch, CanonicalSequence, Digest, ProducerEpoch } from "./Records.ts";
import {
  SettlementPublication,
  type SettlementPublicationFailure,
  type SettlementPublicationResult,
  SettlementPublisher,
} from "./SettlementPublisher.ts";
import {
  AttachChildToReservationRequest,
  ChildBudgetReservationRequest,
  ClaimJoiningRequest,
  type Claim,
  type ClaimRequest,
  type InputAppliedMarker,
  LedgerError,
  MarkInputAppliedRequest,
  MarkJoinedRequest,
  OwnershipLost,
  type OwnershipToken,
  ReleaseOwnershipRequest,
  RenewOwnershipRequest,
  RevertJoiningRequest,
  SubmissionLedger,
  SuspendRequest,
  type SuspensionReason,
} from "./SubmissionLedger.ts";
import {
  type AppendResult,
  FenceRejected,
  FencedAppendRequest,
  ThreadMaterialization,
  type ThreadNotMaterialized,
  ThreadStore,
  ThreadStoreError,
  type ThreadStoreFailure,
  ThreadTailRequest,
} from "./ThreadStore.ts";

export const RunChildBudgetReservation = Schema.Struct({
  reservationId: ChildBudgetReservationRequest.fields.reservationId,
  parentToolCallId: ChildBudgetReservationRequest.fields.parentToolCallId,
  allocation: ChildBudgetReservationRequest.fields.allocation,
  allocationDigest: ChildBudgetReservationRequest.fields.allocationDigest,
});

export type RunChildBudgetReservation = typeof RunChildBudgetReservation.Type;

export const RunChildAttachment = Schema.Struct({
  reservationId: AttachChildToReservationRequest.fields.reservationId,
  childSubmissionId: AttachChildToReservationRequest.fields.childSubmissionId,
});

export type RunChildAttachment = typeof RunChildAttachment.Type;

/** Commands bound to one ownership period. Callers never reconstruct its rotating token. */
export interface RunOwnership {
  readonly release: Effect.Effect<void, OwnershipLost | LedgerError>;
  readonly markInputApplied: (
    marker: InputAppliedMarker,
  ) => ReturnType<SubmissionLedger["Service"]["markInputApplied"]>;
  readonly claimJoining: (
    maxCount: number,
  ) => ReturnType<SubmissionLedger["Service"]["claimJoining"]>;
  readonly markJoined: (
    submissionId: SubmissionId,
    marker: InputAppliedMarker,
  ) => ReturnType<SubmissionLedger["Service"]["markJoined"]>;
  readonly revertJoining: (
    submissionId: SubmissionId,
  ) => ReturnType<SubmissionLedger["Service"]["revertJoining"]>;
  readonly suspend: (
    reason: SuspensionReason,
  ) => ReturnType<SubmissionLedger["Service"]["suspend"]>;
  readonly reserveChildBudget: (
    request: RunChildBudgetReservation,
  ) => ReturnType<SubmissionLedger["Service"]["reserveChildBudget"]>;
  readonly attachChildToReservation: (
    request: RunChildAttachment,
  ) => ReturnType<SubmissionLedger["Service"]["attachChildToReservation"]>;
}

export interface RunWriter {
  readonly threadId: ThreadId;
  readonly producerEpoch: ProducerEpoch;
  readonly tail: Effect.Effect<{ readonly sequence: CanonicalSequence; readonly digest: Digest }>;
  readonly append: (batch: CanonicalBatch) => Effect.Effect<AppendResult, ThreadStoreFailure>;
  /** Recheck the writer fence after effectful preflight, before dispatch; append no record. */
  readonly checkFence: Effect.Effect<
    void,
    ThreadStoreError | ThreadNotMaterialized | FenceRejected
  >;
}

/** A claim-scoped owner; a committed suspension ends it, while resume-immediately does not. */
export interface RunStorageSession extends RunWriter, RunOwnership {
  readonly publishSettlement: (
    batch: CanonicalBatch,
  ) => Effect.Effect<SettlementPublicationResult, SettlementPublicationFailure>;
  /** Initial grant for binding identity. Its token is not an authority for subsequent commands. */
  readonly claim: Claim;
  /** Rebind once after initialization's administrative canonical writes. */
  readonly refresh: Effect.Effect<void, ThreadStoreError | ThreadNotMaterialized | FenceRejected>;
  readonly renew: Effect.Effect<void, OwnershipLost | LedgerError>;
  readonly maintain: (
    renewalInterval: Duration.Duration,
  ) => Effect.Effect<never, OwnershipLost | LedgerError>;
}

export class RunStorage extends Context.Service<
  RunStorage,
  {
    readonly publishSettlement: SettlementPublisher["Service"]["publish"];
    readonly claim: (
      request: ClaimRequest,
    ) => Effect.Effect<
      Option.Option<RunStorageSession>,
      LedgerError | ThreadStoreFailure,
      Scope.Scope
    >;
  }
>()("@effect-agent/thread/RunStorage") {}

/** Administrative recovery binds its separately acquired authority once, outside active Runs. */
export const bindRunOwnership = (
  ledger: SubmissionLedger["Service"],
  threadId: ThreadId,
  submissionId: SubmissionId,
  token: Effect.Effect<OwnershipToken>,
): RunOwnership => ({
  release: Effect.flatMap(token, (ownershipToken) =>
    ledger.releaseOwnership(ReleaseOwnershipRequest.make({ submissionId, ownershipToken })),
  ),
  markInputApplied: (marker) =>
    Effect.flatMap(token, (ownershipToken) =>
      ledger.markInputApplied(
        MarkInputAppliedRequest.make({ ...marker, submissionId, ownershipToken }),
      ),
    ),
  claimJoining: (maxCount) =>
    Effect.flatMap(token, (ownershipToken) =>
      ledger.claimJoining(
        ClaimJoiningRequest.make({
          threadId,
          hostSubmissionId: submissionId,
          ownershipToken,
          maxCount,
        }),
      ),
    ),
  markJoined: (joinedSubmissionId, marker) =>
    Effect.flatMap(token, (ownershipToken) =>
      ledger.markJoined(
        MarkJoinedRequest.make({ ...marker, submissionId: joinedSubmissionId, ownershipToken }),
      ),
    ),
  revertJoining: (joinedSubmissionId) =>
    Effect.flatMap(token, (ownershipToken) =>
      ledger.revertJoining(
        RevertJoiningRequest.make({
          submissionId: joinedSubmissionId,
          guard: { hostSubmissionId: submissionId, ownershipToken },
        }),
      ),
    ),
  suspend: (reason) =>
    Effect.flatMap(token, (ownershipToken) =>
      ledger.suspend(SuspendRequest.make({ submissionId, ownershipToken, reason })),
    ),
  reserveChildBudget: (request) =>
    Effect.flatMap(token, (ownershipToken) =>
      ledger.reserveChildBudget(
        ChildBudgetReservationRequest.make({
          ...request,
          parentSubmissionId: submissionId,
          ownershipToken,
        }),
      ),
    ),
  attachChildToReservation: (request) =>
    Effect.flatMap(token, (ownershipToken) =>
      ledger.attachChildToReservation(
        AttachChildToReservationRequest.make({ ...request, ownershipToken }),
      ),
    ),
});

/** Fenced canonical writer shared by generic Run sessions and administrative repair owners. */
export const makeRunWriter = Effect.fnUntraced(function* (
  store: ThreadStore["Service"],
  threadId: ThreadId,
  producerEpoch: ProducerEpoch,
) {
  const initial = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));
  const gate = yield* Semaphore.make(1);
  let tail = { sequence: initial.tailSequence, digest: initial.tailDigest };

  const refresh = gate.withPermits(1)(
    Effect.gen(function* () {
      const current = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

      if (current.producerEpoch !== producerEpoch)
        return yield* FenceRejected.make({
          threadId,
          attemptedEpoch: producerEpoch,
          actualEpoch: current.producerEpoch,
        });
      tail = { sequence: current.tailSequence, digest: current.tailDigest };
    }),
  );

  const append = (batch: CanonicalBatch) =>
    gate.withPermits(1)(
      Effect.gen(function* () {
        for (let retries = 0; ; retries++) {
          const result = yield* store
            .append(
              FencedAppendRequest.make({
                threadId,
                producerEpoch,
                batch,
                expectedTailSequence: tail.sequence,
                expectedTailDigest: tail.digest,
              }),
            )
            .pipe(
              Effect.catchTag("AppendConflict", (conflict) => {
                // Same-epoch administrative repair may advance the tail. Only a failed tail CAS
                // permits retry; epoch, batch-digest, and record-identity failures still propagate.
                if (
                  conflict.reason !== "tail" ||
                  conflict.actualTailSequence === undefined ||
                  conflict.actualTailDigest === undefined ||
                  retries >= 8
                )
                  return Effect.fail(conflict);
                tail = { sequence: conflict.actualTailSequence, digest: conflict.actualTailDigest };

                return Effect.succeed(undefined);
              }),
            );

          if (result === undefined) continue;
          tail = { sequence: result.lastSequence, digest: result.tailDigest };

          return result;
        }
      }),
    );

  return {
    threadId,
    producerEpoch,
    tail: Effect.sync(() => tail),
    append,
    refresh,
    checkFence: refresh,
  };
});

/** Explicit assembly for adapters whose own transactions validate every mutation. */
export const make = Effect.gen(function* () {
  const ledger = yield* SubmissionLedger;
  const store = yield* ThreadStore;
  const publisher = yield* SettlementPublisher;

  const claim = Effect.fn("RunStorage.claim")(function* (request: ClaimRequest) {
    const acquiredAt = yield* Clock.currentTimeMillis;
    const granted = yield* ledger.claim(request);

    if (Option.isNone(granted)) return Option.none();
    const claimed = granted.value;
    let token = claimed.ownershipToken;
    let renewedAt = acquiredAt;
    let closed = false;
    let released = false;
    const gate = yield* Semaphore.make(1);

    const ownership = bindRunOwnership(
      ledger,
      request.threadId,
      claimed.submissionId,
      Effect.sync(() => token),
    );

    const release = gate.withPermits(1)(
      Effect.gen(function* () {
        if (released) return;
        // Stop writes before releasing database authority; a failed acknowledgement still
        // allows the Scope finalizer to retry the release with the latest token.
        closed = true;
        yield* ownership.release;
        released = true;
      }).pipe(Effect.uninterruptible),
    );

    yield* Effect.addFinalizer(() =>
      release.pipe(
        Effect.catchTag("OwnershipLost", () => Effect.void),
        Effect.catchTag("LedgerError", () =>
          Effect.logWarning(
            "Attempt ownership release failed; lease recovery remains required",
          ).pipe(Effect.annotateLogs({ submissionId: claimed.submissionId })),
        ),
        Effect.ensuring(
          Effect.sync(() => {
            closed = true;
          }),
        ),
      ),
    );
    if (request.handoff !== undefined && request.handoff.submissionId !== claimed.submissionId)
      return yield* LedgerError.make({
        operation: "claim handoff",
        message: "The submission adapter did not honor the requested handoff",
      });
    yield* store.materialize(
      ThreadMaterialization.make({
        threadId: request.threadId,
        producerEpoch: claimed.producerEpoch,
      }),
    );
    const writer = yield* makeRunWriter(store, request.threadId, claimed.producerEpoch);

    const renew = gate.withPermits(1)(
      Effect.gen(function* () {
        if (closed)
          return yield* OwnershipLost.make({
            submissionId: claimed.submissionId,
            actualEpoch: claimed.producerEpoch,
          });
        const at = yield* Clock.currentTimeMillis;

        const renewal = yield* ledger.renewOwnership(
          RenewOwnershipRequest.make({ submissionId: claimed.submissionId, ownershipToken: token }),
        );

        token = renewal.ownershipToken;
        renewedAt = at;
      }).pipe(Effect.uninterruptible),
    );

    const owned = <A, E>(effect: Effect.Effect<A, E>) =>
      gate.withPermits(1)(
        Effect.suspend((): Effect.Effect<A, E | LedgerError> =>
          closed
            ? Effect.fail(
                LedgerError.make({
                  operation: "run ownership",
                  message: "Run storage session is closed",
                }),
              )
            : effect,
        ),
      );

    const canonical = <A, E>(effect: Effect.Effect<A, E>) =>
      gate.withPermits(1)(
        Effect.suspend((): Effect.Effect<A, E | ThreadStoreError> =>
          closed
            ? Effect.fail(
                ThreadStoreError.make({
                  operation: "run append",
                  message: "Run storage session is closed",
                }),
              )
            : effect,
        ),
      );

    const session: RunStorageSession = {
      ...writer,
      claim: claimed,
      release,
      renew,
      append: (batch) => canonical(writer.append(batch)),
      checkFence: canonical(writer.checkFence),
      refresh: canonical(writer.refresh),
      maintain: (interval) =>
        Effect.forever(
          Effect.gen(function* () {
            const now = yield* Clock.currentTimeMillis;

            yield* Effect.sleep(Math.max(0, renewedAt + Duration.toMillis(interval) - now));
            yield* renew;
          }),
        ),
      markInputApplied: (marker) => owned(ownership.markInputApplied(marker)),
      claimJoining: (maxCount) => owned(ownership.claimJoining(maxCount)),
      markJoined: (id, marker) => owned(ownership.markJoined(id, marker)),
      revertJoining: (id) => owned(ownership.revertJoining(id)),
      suspend: (reason) =>
        owned(
          Effect.gen(function* () {
            const outcome = yield* ownership.suspend(reason);

            if (outcome === "suspended") {
              closed = true;
              released = true;
            }

            return outcome;
          }).pipe(
            Effect.onError(() =>
              Effect.sync(() => {
                closed = true;
              }),
            ),
            Effect.uninterruptible,
          ),
        ),
      publishSettlement: (batch) =>
        owned<SettlementPublicationResult, SettlementPublicationFailure>(
          Effect.gen(function* () {
            let tail = yield* writer.tail;

            for (let retries = 0; ; retries++) {
              const result = yield* publisher
                .publish(
                  SettlementPublication.make({
                    submissionId: claimed.submissionId,
                    authority: { _tag: "Owned", ownershipToken: token },
                    append: FencedAppendRequest.make({
                      threadId: request.threadId,
                      producerEpoch: claimed.producerEpoch,
                      expectedTailSequence: tail.sequence,
                      expectedTailDigest: tail.digest,
                      batch,
                    }),
                  }),
                )
                .pipe(
                  Effect.catchTag("AppendConflict", (conflict) => {
                    if (
                      conflict.reason !== "tail" ||
                      conflict.actualTailSequence === undefined ||
                      conflict.actualTailDigest === undefined ||
                      retries >= 8
                    )
                      return Effect.fail(conflict);
                    tail = {
                      sequence: conflict.actualTailSequence,
                      digest: conflict.actualTailDigest,
                    };

                    return Effect.succeed(undefined);
                  }),
                );

              if (result === undefined) continue;
              yield* writer.refresh;

              return result;
            }
          }).pipe(
            Effect.onError(() =>
              Effect.sync(() => {
                closed = true;
              }),
            ),
          ),
        ),
      reserveChildBudget: (value) => owned(ownership.reserveChildBudget(value)),
      attachChildToReservation: (value) => owned(ownership.attachChildToReservation(value)),
    };

    return Option.some(session);
  }, Effect.uninterruptible);

  return RunStorage.of({ claim, publishSettlement: publisher.publish });
});

/** Bind one assembly to its selected storage ports; separate assemblies never share a captured owner. */
export const layer = () => Layer.effect(RunStorage, make);
