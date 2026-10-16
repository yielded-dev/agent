import type { Scope } from "effect";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { SubmissionId, ThreadId } from "../core/Identifiers.ts";
import { StorageOwner } from "./internal/StorageOwner.ts";
import type { CanonicalBatch, CanonicalSequence } from "./Records.ts";
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
  type RecoverySnapshot,
  RecoverySnapshotRequest,
  LedgerError,
  MarkInputAppliedRequest,
  MarkJoinedRequest,
  OwnershipLost,
  RevertJoiningRequest,
  SettlementConflict,
  SubmissionLedger,
  SuspendRequest,
  type SuspensionReason,
} from "./SubmissionLedger.ts";
import {
  type AppendResult,
  AppendConflict,
  AppendRequest,
  type CommitBatch,
  type ExecutionUpdate,
  PreparedAppend,
  FreshRunRead,
  type ThreadIdentity,
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadStore,
  ThreadStoreError,
  type ThreadStoreFailure,
  ThreadTailRequest,
} from "./ThreadStore.ts";

export {
  StorageOwner,
  makeStorageOwner,
  type MutationGate,
  type OwnerGeneration,
  type StorageOwnerService,
} from "./internal/StorageOwner.ts";

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

/** Commands bound to one local scoped owner. No authority is transported in their values. */
export interface RunOwnership {
  readonly release: Effect.Effect<void>;
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
  ) => Effect.Effect<void, LedgerError | OwnershipLost>;
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
  /** Opaque cache identity for this local writer lifetime; never portable authority. */
  readonly generation: object;
  readonly refresh: Effect.Effect<void, ThreadStoreFailure>;
  readonly withProgress: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ThreadStoreFailure, R>;
  readonly tail: Effect.Effect<{ readonly sequence: CanonicalSequence }>;
  /** Uncertain commit failures close this session before another command may enter. */
  readonly append: (
    batch: CommitBatch,
    executions?: ReadonlyArray<ExecutionUpdate>,
  ) => Effect.Effect<AppendResult, ThreadStoreFailure>;
  /** Recheck the captured local capability after preflight, immediately before effect dispatch. */
  readonly checkActive: Effect.Effect<void, ThreadStoreFailure>;
}

export interface RunStorageSession extends RunWriter, RunOwnership {
  readonly claim: Claim;
  /** Atomic original input/context plus the ledger marker, when the adapter supports it. */
  readonly appendInput?: RunWriter["append"];
  /** Read current controls, root identity and empty-Run evidence under this owner.
   * Its tail is adopted by the writer; append still checks the tail and revision. */
  readonly prepareStart?: Effect.Effect<
    { readonly snapshot: RecoverySnapshot; readonly identity: ThreadIdentity } | undefined,
    LedgerError | ThreadStoreFailure
  >;
  readonly refresh: Effect.Effect<void, ThreadStoreFailure>;
  readonly publishSettlement: (
    batch: CanonicalBatch,
    executions?: ReadonlyArray<ExecutionUpdate>,
  ) => Effect.Effect<SettlementPublicationResult, SettlementPublicationFailure>;
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

/** One writer's scoped capability, selected at claim or administrative construction. */
class WriterOwnership extends Context.Service<
  WriterOwnership,
  {
    readonly identity: object;
    readonly checkActive: Effect.Effect<void, ThreadStoreFailure>;
    readonly release: Effect.Effect<void>;
  }
>()("@effect-agent/durable/internal/WriterOwnership") {}

const isDefiniteWriteRefusal = Schema.is(
  Schema.Union([AppendConflict, OwnershipLost, SettlementConflict, ThreadNotMaterialized]),
);

const isDefiniteWriteFailure = (cause: Cause.Cause<unknown>) =>
  cause.reasons.length > 0 &&
  cause.reasons.every(
    (reason) => Cause.isFailReason(reason) && isDefiniteWriteRefusal(reason.error),
  );

const makeWriter = Effect.fnUntraced(function* (threadId: ThreadId) {
  const store = yield* ThreadStore;
  const owner = yield* StorageOwner;
  const ownership = yield* WriterOwnership;
  const checkActive = ownership.checkActive;
  const initial = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));
  let tail = { sequence: initial.tailSequence };

  const refresh = owner.withMutation(
    Effect.gen(function* () {
      yield* checkActive;
      const current = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

      tail = { sequence: current.tailSequence };
    }),
  );

  const makeAppend = Effect.gen(function* () {
    const publisher = yield* ThreadStore;

    const append: RunWriter["append"] = Effect.fnUntraced(function* (batch, executions) {
      const captured = yield* PreparedAppend.capture({
        threadId,
        expectedTailSequence: tail.sequence,
        batch,
        ...(executions === undefined ? {} : { executions }),
      }).pipe(
        Effect.mapError((cause) =>
          ThreadStoreError.make({
            operation: "capture append",
            message: cause.message,
            cause,
          }),
        ),
      );

      return yield* owner.withProgress(
        threadId,
        owner.withMutation(
          Effect.gen(function* () {
            yield* checkActive;
            for (let retries = 0; ; retries++) {
              const result = yield* publisher
                .append(
                  captured.expectedTailSequence === tail.sequence
                    ? captured
                    : AppendRequest.make({
                        ...captured,
                        expectedTailSequence: tail.sequence,
                      }),
                )
                .pipe(
                  Effect.catchTag("AppendConflict", (conflict) => {
                    if (
                      conflict.reason !== "tail" ||
                      conflict.actualTailSequence === undefined ||
                      retries >= 8
                    )
                      return Effect.fail(conflict);
                    tail = { sequence: conflict.actualTailSequence };
                    if (captured.executions !== undefined) return Effect.fail(conflict);

                    return Effect.succeed(undefined);
                  }),
                );

              if (result === undefined) continue;
              // A historical replay returns its original coordinates, not today's canonical tail.
              if (result.replayed) yield* refresh;
              else tail = { sequence: result.lastSequence };

              return result;
            }
          }).pipe(
            // Revoke uncertain writes before releasing the writer's mutation gate.
            Effect.onError((cause) =>
              isDefiniteWriteFailure(cause) ? Effect.void : ownership.release,
            ),
          ),
        ),
      );
    });

    return append;
  });

  const writer = {
    threadId,
    generation: ownership.identity,
    withProgress: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      owner.withProgress(threadId, Effect.andThen(checkActive, effect)),
    tail: Effect.sync(() => tail),
    append: yield* makeAppend,
    checkActive: owner.withMutation(checkActive),
    refresh,
  };

  return {
    writer,
    makeAppend,
    adoptTail: (sequence: CanonicalSequence) => {
      tail = { sequence };
    },
  };
});

/** Trusted administrative writer sharing the physical owner's gate and lifetime. */
export const makeRunWriter = Effect.fnUntraced(function* (threadId: ThreadId) {
  const owner = yield* StorageOwner;

  return (yield* makeWriter(threadId).pipe(
    Effect.provideService(WriterOwnership, {
      identity: owner.identity,
      checkActive: Effect.suspend(() =>
        owner.isClosed()
          ? Effect.fail(
              ThreadStoreError.make({
                operation: "write Thread",
                message: "Storage owner is closed",
              }),
            )
          : Effect.void,
      ),
      release: Effect.void,
    }),
  )).writer;
});

/** The same scoped single-owner protocol is used by every adapter. */
export const make = Effect.gen(function* () {
  const ledger = yield* SubmissionLedger;
  const store = yield* ThreadStore;
  const publisher = yield* SettlementPublisher;
  const owner = yield* StorageOwner;

  const claim: RunStorage["Service"]["claim"] = (request) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const granted = yield* owner.acquire(request.threadId, ledger.selectHead(request));

        if (Option.isNone(granted)) return Option.none();
        const generation = granted.value;
        const claimed = generation.claim;

        return yield* restore(
          Effect.gen(function* () {
            if (
              request.handoff !== undefined &&
              request.handoff.submissionId !== claimed.submissionId
            )
              return yield* LedgerError.make({
                operation: "claim handoff",
                message: "The submission adapter did not honor the requested handoff",
              });
            yield* store.materialize(ThreadMaterialization.make({ threadId: request.threadId }));

            const { writer, makeAppend, adoptTail } = yield* makeWriter(request.threadId).pipe(
              Effect.provideService(WriterOwnership, generation),
              Effect.provideService(ThreadStore, store),
              Effect.provideService(StorageOwner, owner),
            );

            const owned = <A, E>(effect: Effect.Effect<A, E>) => generation.use(effect);

            const inputPublisher = ledger.appendInput;

            const appendInput =
              inputPublisher === undefined
                ? undefined
                : yield* makeAppend.pipe(
                    Effect.provideService(ThreadStore, {
                      ...store,
                      append: (request) => inputPublisher(claimed.submissionId, request),
                    }),
                  );

            const session: RunStorageSession = {
              ...writer,
              claim: claimed,
              ...(appendInput === undefined ? {} : { appendInput }),
              ...(store.readFreshRun === undefined || appendInput === undefined
                ? {}
                : {
                    prepareStart: owner.withProgress(
                      request.threadId,
                      generation.use(
                        Effect.gen(function* () {
                          const snapshot = yield* ledger.loadRecoverySnapshot(
                            RecoverySnapshotRequest.make({ submissionId: claimed.submissionId }),
                          );

                          const submission = snapshot.submission;

                          if (
                            submission.threadId !== request.threadId ||
                            submission.state !== "ready" ||
                            submission.workerAdmission !== undefined ||
                            submission.messageAdmission !== undefined ||
                            submission.parentLinkage !== undefined ||
                            snapshot.inputApplied !== undefined ||
                            snapshot.abortIntent !== undefined ||
                            snapshot.approvalDecisions.length !== 0
                          )
                            return undefined;

                          const identity = yield* store.readFreshRun!(
                            FreshRunRead.make({
                              threadId: request.threadId,
                              submissionId: claimed.submissionId,
                            }),
                          );

                          if (Option.isNone(identity)) return undefined;
                          adoptTail(identity.value.tailSequence);

                          return { snapshot, identity: identity.value };
                        }),
                      ),
                    ),
                  }),
              release: generation.release,
              refresh: writer.refresh,
              markInputApplied: (marker) =>
                owned(
                  ledger.markInputApplied(
                    MarkInputAppliedRequest.make({
                      ...marker,
                      submissionId: claimed.submissionId,
                    }),
                  ),
                ),
              claimJoining: (maxCount) =>
                owned(
                  ledger.claimJoining(
                    ClaimJoiningRequest.make({
                      threadId: request.threadId,
                      hostSubmissionId: claimed.submissionId,
                      maxCount,
                    }),
                  ),
                ),
              markJoined: (submissionId, marker) =>
                owned(
                  ledger.markJoined(
                    MarkJoinedRequest.make({
                      ...marker,
                      submissionId,
                    }),
                  ),
                ),
              revertJoining: (submissionId) =>
                owned(
                  ledger.revertJoining(
                    RevertJoiningRequest.make({
                      submissionId,
                      guard: { hostSubmissionId: claimed.submissionId },
                    }),
                  ),
                ),
              suspend: (reason) =>
                owned(
                  ledger
                    .suspend(
                      SuspendRequest.make({
                        submissionId: claimed.submissionId,
                        reason,
                      }),
                    )
                    .pipe(
                      Effect.tap((outcome) =>
                        outcome === "suspended" ? generation.release : Effect.void,
                      ),
                    ),
                ),
              publishSettlement: Effect.fnUntraced(function* (batch, executions) {
                const captured = yield* PreparedAppend.capture(
                  AppendRequest.make({
                    threadId: request.threadId,
                    expectedTailSequence: (yield* writer.tail).sequence,
                    batch,
                    ...(executions === undefined ? {} : { executions }),
                  }),
                ).pipe(
                  Effect.mapError((cause) =>
                    LedgerError.make({
                      operation: "capture settlement",
                      message: cause.message,
                      cause,
                    }),
                  ),
                );

                return yield* owner.withProgress(
                  request.threadId,
                  owned(
                    Effect.gen(function* () {
                      for (let retries = 0; ; retries++) {
                        const result = yield* publisher
                          .publish(
                            SettlementPublication.make({
                              submissionId: claimed.submissionId,
                              authority: { _tag: "Owned" },
                              append: AppendRequest.make({
                                ...captured,
                                expectedTailSequence: (yield* writer.tail).sequence,
                              }),
                            }),
                          )
                          .pipe(
                            Effect.catchTag("AppendConflict", (conflict) => {
                              if (
                                conflict.reason !== "tail" ||
                                conflict.actualTailSequence === undefined ||
                                retries >= 8
                              )
                                return Effect.fail(conflict);

                              return writer.refresh.pipe(
                                Effect.andThen(
                                  captured.executions === undefined
                                    ? Effect.succeed(undefined)
                                    : Effect.fail(conflict),
                                ),
                              );
                            }),
                          );

                        if (result === undefined) continue;
                        adoptTail(result.tailSequence);

                        return result;
                      }
                    }).pipe(
                      Effect.onError((cause) =>
                        isDefiniteWriteFailure(cause) ? Effect.void : generation.release,
                      ),
                    ),
                  ),
                );
              }),
              reserveChildBudget: (value) =>
                owned(
                  ledger.reserveChildBudget(
                    ChildBudgetReservationRequest.make({
                      ...value,
                      parentSubmissionId: claimed.submissionId,
                    }),
                  ),
                ),
              attachChildToReservation: (value) =>
                owned(ledger.attachChildToReservation(AttachChildToReservationRequest.make(value))),
            };

            return Option.some(session);
          }).pipe(Effect.onError(() => generation.release)),
        );
      }),
    );

  return RunStorage.of({ claim, publishSettlement: publisher.publish });
});

export const layer = () => Layer.effect(RunStorage, make);
