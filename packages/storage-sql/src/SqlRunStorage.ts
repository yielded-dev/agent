import {
  Channel,
  Clock,
  Context,
  Crypto,
  DateTime,
  Duration,
  Effect,
  Exit,
  Option,
  Schema,
  Scope,
  Semaphore,
  Stream,
} from "effect";
import { Digest } from "effect-agent/records";
import { runIdForSubmission } from "effect-agent/run-journal";
import { bindRunOwnership, RunStorage, type RunStorageSession } from "effect-agent/run-storage";
import { SettlementPublication, SettlementPublisher } from "effect-agent/settlement-publisher";
import {
  type SubmissionLedger,
  LedgerError,
  MarkInputAppliedRequest,
  OwnershipLost,
  ReleaseOwnershipRequest,
  RenewOwnershipRequest,
  submissionInputBatchId,
  submissionInputRecordId,
  type ClaimRequest,
  type OwnershipToken,
} from "effect-agent/submission-ledger";
import type { ThreadStore } from "effect-agent/thread-store";
import {
  FenceRejected,
  FencedAppendRequest,
  ThreadReader,
  ThreadStoreError,
} from "effect-agent/thread-store";
import { SqlClient } from "effect/sql/SqlClient";
import { CurrentTransformer } from "effect/sql/Statement";

import type { makeSqlJournalKernel } from "./SqlJournal.ts";
import type { Diagnostic } from "./SqlStorage.ts";
import {
  makeSqlSubmissionLedgerKernel,
  type SqlRunAuthority,
  type SqlSubmissionLedgerOptions,
} from "./SqlSubmissionLedger.ts";
import { makeSqlThreadStoreKernel, type SqlThreadStoreOptions } from "./SqlThreadStore.ts";

/**
 * Required storage owner for a process-exclusive database. Construction and every operation
 * pin private storage services while preserving caller diagnostics and Clock. Only claim-scoped
 * state is retained; canonical facts, queued followers, aborts, approvals and reservations remain
 * database-authoritative.
 */
export const makeSqlRunStorage = Effect.fn("SqlRunStorage.make")(function* <
  S extends Diagnostic,
  C extends Diagnostic,
  W extends Diagnostic,
  F extends Diagnostic,
>(
  journalKernel: Effect.Success<ReturnType<typeof makeSqlJournalKernel<S, C, W, F>>>,
  storeOptions: SqlThreadStoreOptions<S, C, F>,
  ledgerOptions: SqlSubmissionLedgerOptions<S, C, F>,
) {
  const context = Context.pick(
    SqlClient,
    Crypto.Crypto,
    Scope.Scope,
  )(yield* Effect.context<SqlClient | Crypto.Crypto | Scope.Scope>());

  const gate = yield* Semaphore.make(1);
  const storeKernel = yield* makeSqlThreadStoreKernel(journalKernel.journal, storeOptions);
  const ledgerKernel = yield* makeSqlSubmissionLedgerKernel(journalKernel.journal, ledgerOptions);
  const rawStore = storeKernel.store;
  const rawLedger = ledgerKernel.ledger;
  const sql = Context.get(context, SqlClient);

  const storageContext = (live: Context.Context<never>, scope?: Scope.Scope) =>
    Context.merge(
      Context.omit(CurrentTransformer, sql.transactionService)(live),
      scope === undefined ? context : Context.add(context, Scope.Scope, scope),
    );

  const bind = <A, E>(effect: Effect.Effect<A, E>) =>
    Effect.contextWith((live: Context.Context<never>) =>
      Effect.setContext(effect, storageContext(live)),
    );

  const bindStream = <A, E>(stream: Stream.Stream<A, E>) =>
    Stream.fromChannel(
      Channel.fromTransform((upstream, scope) => {
        const scoped = <A, E>(effect: Effect.Effect<A, E>) =>
          Effect.contextWith((live: Context.Context<never>) =>
            Effect.setContext(effect, storageContext(live, scope)),
          );

        return Effect.map(
          scoped(Channel.toTransform(Stream.toChannel(stream))(upstream, scope)),
          scoped,
        );
      }),
    );

  interface Active {
    readonly authority: SqlRunAuthority;
    readonly epoch: SqlRunAuthority["thread"]["producer_epoch"];
    digest: Digest;
    token: OwnershipToken;
    closed: boolean;
    releaseNeeded: boolean;
  }
  const active = new Set<Active>();
  const decodeDigest = Schema.decodeEffect(Digest);

  const close = (state: Active) => {
    state.closed = true;
    state.authority.owned = false;
  };

  const refresh = Effect.fnUntraced(function* (state: Active) {
    state.releaseNeeded = yield* ledgerKernel.refreshAuthority(state.authority);
    state.digest = yield* decodeDigest(state.authority.thread.tail_digest).pipe(
      Effect.mapError((cause) =>
        LedgerError.make({ operation: "refresh Run tail", message: cause.message, cause }),
      ),
    );
    if (
      state.authority.thread.producer_epoch !== state.epoch ||
      (!state.authority.owned && state.authority.submission.state !== "settled")
    )
      close(state);
  });

  // Administrative transitions are uncommon during a live claim. Re-read their committed
  // poststate, including lost acknowledgement cases, rather than trusting request aliases.
  const admin = <A, E>(
    effect: Effect.Effect<A, E>,
    affected?: { readonly threadId?: string; readonly submissionId?: string },
  ) =>
    bind(
      gate.withPermits(1)(
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const exit = yield* restore(effect).pipe(Effect.exit);

            const affectedRuns =
              affected === undefined
                ? []
                : Array.from(active).filter(
                    (state) =>
                      state.authority.submission.thread_id === affected.threadId ||
                      state.authority.submissionId === affected.submissionId,
                  );

            const refreshed = yield* Effect.forEach(affectedRuns, refresh, { discard: true }).pipe(
              Effect.exit,
            );

            if (Exit.isFailure(refreshed)) {
              for (const state of affectedRuns) close(state);
              if (Exit.isSuccess(exit)) return yield* Effect.failCause(refreshed.cause);
            }

            return yield* exit;
          }),
        ),
      ),
    );

  const stopWorker = rawLedger.stopWorker;
  const inspectWorker = rawLedger.inspectWorker;

  const ledger: SubmissionLedger["Service"] = {
    ...rawLedger,
    capabilities: bind(rawLedger.capabilities),
    admit: (request) => admin(rawLedger.admit(request)),
    markReady: (request) =>
      admin(rawLedger.markReady(request), { submissionId: request.submissionId }),
    claim: (request) => admin(rawLedger.claim(request), { threadId: request.threadId }),
    renewOwnership: (request) =>
      admin(rawLedger.renewOwnership(request), { submissionId: request.submissionId }),
    releaseOwnership: (request) =>
      admin(rawLedger.releaseOwnership(request), { submissionId: request.submissionId }),
    markInputApplied: (request) =>
      admin(rawLedger.markInputApplied(request), { submissionId: request.submissionId }),
    finalizeSettlement: (request) =>
      admin(rawLedger.finalizeSettlement(request), { submissionId: request.submissionId }),
    requestAbort: (request) => admin(rawLedger.requestAbort(request)),
    ...(stopWorker === undefined
      ? {}
      : { stopWorker: (request: Parameters<typeof stopWorker>[0]) => admin(stopWorker(request)) }),
    claimJoining: (request) =>
      admin(rawLedger.claimJoining(request), { threadId: request.threadId }),
    markJoined: (request) =>
      admin(rawLedger.markJoined(request), { submissionId: request.submissionId }),
    revertJoining: (request) =>
      admin(rawLedger.revertJoining(request), { submissionId: request.submissionId }),
    suspend: (request) => admin(rawLedger.suspend(request), { submissionId: request.submissionId }),
    recordApprovalDecision: (request) =>
      admin(rawLedger.recordApprovalDecision(request), { submissionId: request.submissionId }),
    markUnknown: (request) =>
      admin(rawLedger.markUnknown(request), { submissionId: request.submissionId }),
    recordUnknownResolution: (request) =>
      admin(rawLedger.recordUnknownResolution(request), { submissionId: request.submissionId }),
    recordChildSettled: (request) =>
      admin(rawLedger.recordChildSettled(request), { submissionId: request.parentSubmissionId }),
    reserveChildBudget: (request) => admin(rawLedger.reserveChildBudget(request)),
    attachChildToReservation: (request) => admin(rawLedger.attachChildToReservation(request)),
    beginChildBudgetRelease: (request) => admin(rawLedger.beginChildBudgetRelease(request)),
    releaseChildBudget: (request) => admin(rawLedger.releaseChildBudget(request)),
    lookup: (request) => bind(rawLedger.lookup(request)),
    resolveAdmission: (request) => bind(rawLedger.resolveAdmission(request)),
    ...(inspectWorker === undefined
      ? {}
      : {
          inspectWorker: (request: Parameters<typeof inspectWorker>[0]) =>
            bind(inspectWorker(request)),
        }),
    loadRecoverySnapshot: (request) => bind(rawLedger.loadRecoverySnapshot(request)),
    readAbortIntent: (request) => bind(rawLedger.readAbortIntent(request)),
    scanNonterminal: bindStream(rawLedger.scanNonterminal),
  };

  const publisher = SettlementPublisher.of({
    publish: (request) =>
      admin(ledgerKernel.publisher.publish(request), { threadId: request.append.threadId }),
  });

  const checkpoints = rawStore.checkpoints;
  const recoveryCheckpoints = rawStore.recoveryCheckpoints;
  const countPeerMessages = rawStore.countPeerMessages;

  const store: ThreadStore["Service"] = {
    ...rawStore,
    materialize: (request) =>
      admin(rawStore.materialize(request), { threadId: request.threadId }).pipe(
        Effect.mapError((cause) =>
          cause._tag === "LedgerError"
            ? ThreadStoreError.make({
                operation: "materialize thread",
                message: cause.message,
                cause,
              })
            : cause,
        ),
      ),
    append: (request) =>
      admin(rawStore.append(request), { threadId: request.threadId }).pipe(
        Effect.mapError((cause) =>
          cause._tag === "LedgerError"
            ? ThreadStoreError.make({
                operation: "append canonical batch",
                message: cause.message,
                cause,
              })
            : cause,
        ),
      ),
    read: (request) => bindStream(rawStore.read(request)),
    observe: (request) => bindStream(rawStore.observe(request)),
    export: (request) => bind(rawStore.export(request)),
    inspectTail: (request) => bind(rawStore.inspectTail(request)),
    readIdentity: (request) => bind(rawStore.readIdentity(request)),
    countPeerMessages:
      countPeerMessages === undefined ? undefined : (request) => bind(countPeerMessages(request)),
    checkpoints:
      checkpoints === undefined
        ? undefined
        : {
            save: (request) => bind(checkpoints.save(request)),
            load: (request) => bind(checkpoints.load(request)),
          },
    recoveryCheckpoints:
      recoveryCheckpoints === undefined
        ? undefined
        : {
            save: (request) => bind(recoveryCheckpoints.save(request)),
            load: (request) => bind(recoveryCheckpoints.load(request)),
          },
  };

  const claimImpl = Effect.fn("SqlRunStorage.claim")(function* (
    request: ClaimRequest,
    finalizer: { release: Effect.Effect<void> },
  ) {
    const claimedThreadId = request.threadId;
    const acquiredAt = yield* Clock.currentTimeMillis;

    const granted = yield* rawLedger.claim(request).pipe(
      Effect.catchCause((cause) => {
        // A claim may have committed its epoch before an acknowledgement hook failed.
        for (const previous of active)
          if (previous.authority.submission.thread_id === claimedThreadId) close(previous);

        return Effect.failCause(cause);
      }),
    );

    if (Option.isNone(granted)) return Option.none<RunStorageSession>();
    const claimed = granted.value;
    const { submissionId, ownershipToken: initialToken } = claimed;

    // A new grant supersedes any writer retained after settlement on this Thread.
    for (const previous of active) {
      if (previous.authority.submission.thread_id === claimedThreadId) {
        close(previous);
        previous.releaseNeeded = false;
      }
    }
    let state: Active | undefined;
    let releaseOwned: SubmissionLedger["Service"]["releaseOwnership"] | undefined;

    const release = bind(
      gate.withPermits(1)(
        Effect.gen(function* () {
          if (state !== undefined && !state.releaseNeeded) {
            close(state);

            return;
          }

          const releaseCommand =
            state?.authority.owned === true && releaseOwned !== undefined
              ? releaseOwned
              : rawLedger.releaseOwnership;

          const released = yield* releaseCommand(
            ReleaseOwnershipRequest.make({
              submissionId: submissionId,
              ownershipToken: state?.token ?? initialToken,
            }),
          ).pipe(Effect.exit);

          if (state !== undefined) {
            close(state);
            if (Exit.isSuccess(released)) state.releaseNeeded = false;
          }

          return yield* released;
        }).pipe(Effect.uninterruptible),
      ),
    );

    finalizer.release = release.pipe(
      Effect.catchTag("OwnershipLost", () => Effect.void),
      Effect.catchTag("LedgerError", () =>
        Effect.logWarning("Attempt ownership release failed; lease recovery remains required"),
      ),
      Effect.ensuring(
        Effect.sync(() => {
          if (state !== undefined) {
            close(state);
            active.delete(state);
          }
        }),
      ),
    );

    const authority = yield* ledgerKernel.loadAuthority(claimed).pipe(
      Effect.catchTag("OwnershipLost", (cause) =>
        LedgerError.make({
          operation: "bind claimed Run storage",
          message: cause.message,
          cause,
        }),
      ),
    );

    const initialDigest = yield* decodeDigest(authority.thread.tail_digest).pipe(
      Effect.mapError((cause) =>
        ThreadStoreError.make({ operation: "bind Run tail", message: cause.message, cause }),
      ),
    );

    const owned: Active = {
      authority,
      epoch: authority.thread.producer_epoch,
      digest: initialDigest,
      token: initialToken,
      closed: false,
      releaseNeeded: true,
    };

    state = owned;
    active.add(owned);
    let renewedAt = acquiredAt;
    const commands = ledgerKernel.bindAuthority(authority);

    releaseOwned = commands.releaseOwnership;

    const ownership = bindRunOwnership(
      { ...rawLedger, ...commands },
      claimedThreadId,
      submissionId,
      Effect.sync(() => owned.token),
    );

    const command = <A, E>(
      effect: Effect.Effect<A, E>,
      publish: (value: A) => void = () => {},
      requiresOwnership = true,
    ) =>
      bind(
        gate.withPermits(1)(
          Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              if (owned.closed || (requiresOwnership && !authority.owned))
                return yield* LedgerError.make({
                  operation: "Run ownership",
                  message: "Run storage session is closed",
                });
              const exit = yield* restore(effect).pipe(Effect.exit);

              if (Exit.isFailure(exit)) {
                close(owned);

                return yield* exit;
              }
              publish(exit.value);

              return exit.value;
            }),
          ),
        ),
      );

    const renew = bind(
      gate.withPermits(1)(
        Effect.gen(function* () {
          if (owned.closed || !authority.owned)
            return yield* OwnershipLost.make({
              submissionId: submissionId,
              actualEpoch: authority.thread.producer_epoch,
            });
          const at = yield* Clock.currentTimeMillis;

          const result = yield* commands
            .renewOwnership(
              RenewOwnershipRequest.make({
                submissionId: submissionId,
                ownershipToken: owned.token,
              }),
            )
            .pipe(Effect.exit);

          if (Exit.isFailure(result)) {
            close(owned);

            return yield* result;
          }
          owned.token = result.value.ownershipToken;
          authority.ownership = Object.freeze({
            ...authority.ownership,
            ownership_token: result.value.ownershipToken,
            lease_expires_at: DateTime.formatIso(result.value.leaseExpiresAt),
          });
          renewedAt = at;
        }).pipe(Effect.uninterruptible),
      ),
    );

    const append: RunStorageSession["append"] = (batch) =>
      bind(
        gate.withPermits(1)(
          Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              if (owned.closed)
                return yield* ThreadStoreError.make({
                  operation: "Run append",
                  message: "Run storage session is closed",
                });

              const request = FencedAppendRequest.make({
                threadId: claimedThreadId,
                producerEpoch: owned.epoch,
                expectedTailSequence: authority.thread.tail_sequence,
                expectedTailDigest: owned.digest,
                batch,
              });

              let inputPoststate: SqlRunAuthority["submission"] | undefined;

              const result = yield* restore(
                storeKernel.appendOwned(request, (raw) =>
                  Effect.gen(function* () {
                    const record = raw.records[0]?.canonical;

                    if (
                      !authority.owned ||
                      authority.submission.input_applied_record_id !== null ||
                      raw.records.length !== 1 ||
                      raw.batchId !== submissionInputBatchId(submissionId) ||
                      record?.recordId !== submissionInputRecordId(submissionId) ||
                      record.payload._tag !== "UserInputRecorded" ||
                      record.payload.kind !== "user" ||
                      record.payload.submissionId !== submissionId ||
                      record.payload.runId !== runIdForSubmission(submissionId)
                    )
                      return yield* journalKernel.appendWithThread(raw, authority.thread);

                    const committed = yield* journalKernel.journal.withWriteTransaction(
                      "append applied input transaction",
                    )(
                      Effect.gen(function* () {
                        yield* ledgerOptions.hitFailpoint("ledger:mark-input-applied:before");

                        const appended = yield* journalKernel.appendWithThreadInTransaction(
                          raw,
                          authority.thread,
                        );

                        const submission = yield* commands
                          .markInputAppliedInTransaction(
                            MarkInputAppliedRequest.make({
                              submissionId,
                              ownershipToken: owned.token,
                              recordId: record.recordId,
                              sequence: appended.firstSequence,
                            }),
                          )
                          .pipe(
                            Effect.mapError((cause) =>
                              storeOptions.errors.storage({
                                operation: "append applied input",
                                message: cause.message,
                                cause,
                              }),
                            ),
                          );

                        return { appended, submission };
                      }),
                    );

                    yield* ledgerOptions.hitFailpoint("ledger:mark-input-applied:after");
                    inputPoststate = committed.submission;

                    return committed.appended;
                  }),
                ),
              ).pipe(Effect.exit);

              if (Exit.isFailure(result)) {
                close(owned);

                return yield* result;
              }
              if (inputPoststate !== undefined)
                authority.submission = Object.freeze(inputPoststate);
              if (!result.value.replayed) {
                authority.thread = Object.freeze({
                  ...authority.thread,
                  tail_sequence: result.value.lastSequence,
                  tail_digest: result.value.tailDigest,
                });
                owned.digest = result.value.tailDigest;
              }

              return result.value;
            }),
          ),
        ),
      );

    const checkFence = bind(
      gate.withPermits(1)(
        Effect.suspend(() =>
          owned.closed
            ? Effect.fail(
                FenceRejected.make({
                  threadId: claimedThreadId,
                  attemptedEpoch: owned.epoch,
                  actualEpoch: authority.thread.producer_epoch,
                }),
              )
            : Effect.void,
        ),
      ),
    );

    const session: RunStorageSession = {
      claim: claimed,
      threadId: claimedThreadId,
      producerEpoch: owned.epoch,
      tail: bind(
        Effect.sync(() => ({ sequence: authority.thread.tail_sequence, digest: owned.digest })),
      ),
      append,
      release,
      renew,
      checkFence,
      refresh: checkFence,
      maintain: (interval: Duration.Duration) =>
        bind(
          Effect.forever(
            Effect.gen(function* () {
              const now = yield* Clock.currentTimeMillis;

              yield* Effect.sleep(Math.max(0, renewedAt + Duration.toMillis(interval) - now));
              yield* renew;
            }),
          ),
        ),
      markInputApplied: (marker) => {
        const { recordId, sequence } = marker;

        return command(ownership.markInputApplied({ recordId, sequence }), () => {
          authority.submission = Object.freeze({
            ...authority.submission,
            input_applied_record_id: recordId,
            input_applied_sequence: sequence,
            state: ["admitted", "ready", "running"].includes(authority.submission.state)
              ? "input-applied"
              : authority.submission.state,
          });
        });
      },
      claimJoining: (maxCount) => command(ownership.claimJoining(maxCount)),
      markJoined: (id, marker) => command(ownership.markJoined(id, marker)),
      revertJoining: (id) => command(ownership.revertJoining(id)),
      suspend: (reason) =>
        command(ownership.suspend(reason), (outcome) => {
          if (outcome === "suspended") {
            close(owned);
            owned.releaseNeeded = false;
          }
        }),
      publishSettlement: (batch) =>
        command(
          Effect.suspend(() =>
            ledgerKernel.publishWithState(
              SettlementPublication.make({
                submissionId,
                authority: { _tag: "Owned", ownershipToken: owned.token },
                append: FencedAppendRequest.make({
                  threadId: claimedThreadId,
                  producerEpoch: owned.epoch,
                  expectedTailSequence: authority.thread.tail_sequence,
                  expectedTailDigest: owned.digest,
                  batch,
                }),
              }),
            ),
          ),
          (result) => {
            authority.submission = Object.freeze(result.submission);
            if (result.submission.state === "settled") {
              authority.owned = false;
              owned.releaseNeeded = false;
            }
            authority.thread = Object.freeze({
              ...authority.thread,
              tail_sequence: result.publication.tailSequence,
              tail_digest: result.publication.tailDigest,
            });
            owned.digest = result.publication.tailDigest;
          },
          // The publisher checks live authority even on replay. Finalization releases
          // the token; only joined follow-up appends may keep this same-epoch writer.
          false,
        ).pipe(Effect.map((result) => result.publication)),
      reserveChildBudget: (value) => command(ownership.reserveChildBudget(value)),
      attachChildToReservation: (value) => command(ownership.attachChildToReservation(value)),
    };

    return Option.some(session);
  });

  const runStorage = RunStorage.of({
    publishSettlement: publisher.publish,
    claim: (request) =>
      Effect.flatMap(Effect.scope, (scope) =>
        Effect.contextWith((live: Context.Context<never>) =>
          Effect.setContext(
            Effect.gen(function* () {
              const finalizer: { release: Effect.Effect<void> } = { release: Effect.void };

              const result = yield* gate
                .withPermits(1)(claimImpl(request, finalizer))
                .pipe(Effect.exit);

              // A closed Scope runs its finalizer immediately: register only after leaving the gate.
              yield* Effect.addFinalizer(() => finalizer.release);

              return yield* result;
            }).pipe(Effect.uninterruptible),
            storageContext(live, scope),
          ),
        ),
      ),
  });

  return { store, ledger, publisher, runStorage, reader: ThreadReader.fromStore(store) };
});
