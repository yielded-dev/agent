import { Digest } from "@yielded/agent/records";
import { runIdForSubmission } from "@yielded/agent/run-journal";
import { bindRunOwnership, RunStorage, type RunStorageSession } from "@yielded/agent/run-storage";
import { SettlementPublication, SettlementPublisher } from "@yielded/agent/settlement-publisher";
import {
  SubmissionLedger,
  LedgerError,
  MarkInputAppliedRequest,
  OwnershipLost,
  ReleaseOwnershipRequest,
  RenewOwnershipRequest,
  submissionInputBatchId,
  submissionInputRecordId,
  type ClaimRequest,
  type OwnershipToken,
} from "@yielded/agent/submission-ledger";
import {
  ThreadImport,
  captureImportSource,
  type ThreadArchive,
} from "@yielded/agent/thread-import";
import type { ThreadStore } from "@yielded/agent/thread-store";
import {
  AppendConflict,
  FenceRejected,
  FencedAppendRequest,
  ThreadReader,
  ThreadStoreError,
  ThreadTailRequest,
} from "@yielded/agent/thread-store";
import {
  Channel,
  Cause,
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
import { SqlClient } from "effect/sql/SqlClient";
import { CurrentTransformer } from "effect/sql/Statement";

import type { makeSqlJournalKernel, RawAppendRequest } from "./SqlJournal.ts";
import type { Diagnostic } from "./SqlStorage.ts";
import {
  makeSqlSubmissionLedgerKernel,
  type SqlRunAuthority,
  type SqlSubmissionLedgerOptions,
} from "./SqlSubmissionLedger.ts";
import { makeSqlThreadStoreKernel, type SqlThreadStoreOptions } from "./SqlThreadStore.ts";

/**
 * Required storage owner for a process-exclusive database. Construction and every operation
 * pin private storage services while preserving caller Scope, diagnostics and Clock. Only claim-scoped
 * state is retained; canonical facts, queued followers, aborts, approvals and reservations remain
 * database-authoritative.
 */
export const makeSqlRunStorage = Effect.fnUntraced(function* <
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
  )(yield* Effect.context<SqlClient | Crypto.Crypto>());

  const gate = yield* Semaphore.make(1);
  const storeKernel = yield* makeSqlThreadStoreKernel(journalKernel.journal, storeOptions);
  const ledgerKernel = yield* makeSqlSubmissionLedgerKernel(journalKernel.journal, ledgerOptions);
  const rawStore = storeKernel.store;
  const rawLedger = ledgerKernel.ledger;
  const sql = Context.get(context, SqlClient);

  const storageContext = <R>(live: Context.Context<R>) =>
    Context.merge(Context.omit(CurrentTransformer, sql.transactionService)(live), context);

  const bind = <A, E>(effect: Effect.Effect<A, E>) =>
    Effect.contextWith((live: Context.Context<never>) =>
      Effect.setContext(effect, storageContext(live)),
    );

  const bindStream = <A, E>(stream: Stream.Stream<A, E>) =>
    Stream.fromChannel(
      Channel.fromTransform((upstream, scope) => {
        const scoped = <A, E>(effect: Effect.Effect<A, E>) =>
          Effect.contextWith((live: Context.Context<never>) =>
            Effect.setContext(effect, Context.add(storageContext(live), Scope.Scope, scope)),
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
  const resolveFundingOwner = rawLedger.resolveFundingOwner;

  const ledger: SubmissionLedger["Service"] = {
    ...rawLedger,
    ...(resolveFundingOwner === undefined
      ? {}
      : {
          resolveFundingOwner: (request: Parameters<typeof resolveFundingOwner>[0]) =>
            bind(resolveFundingOwner(request)),
        }),
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
  const countPeerMessages = rawStore.countPeerMessages;
  const work = rawStore.work;
  const archives = rawStore.archives;
  const verification = rawStore.verification;
  const readWorkerCapacity = rawStore.readWorkerCapacity;

  const archiveMutation = <A, E>(effect: Effect.Effect<A, E>, threadId: string) =>
    admin(effect, { threadId }).pipe(
      Effect.mapError((cause) =>
        Schema.is(LedgerError)(cause)
          ? ThreadStoreError.make({ operation: "archive Thread", message: cause.message, cause })
          : cause,
      ),
    );

  const store: ThreadStore["Service"] = {
    ...rawStore,
    archives:
      archives === undefined
        ? undefined
        : {
            page: (request) => bind(archives.page(request)),
            verify: (request) => bind(archives.verify(request)),
            seal: (request) => archiveMutation(archives.seal(request), request.threadId),
            archive: (request) => archiveMutation(archives.archive(request), request.threadId),
          },
    verification:
      verification === undefined
        ? undefined
        : { verify: (request) => bind(verification.verify(request)) },
    readWorkerCapacity:
      readWorkerCapacity === undefined ? undefined : (request) => bind(readWorkerCapacity(request)),
    work:
      work === undefined
        ? undefined
        : {
            page: (request) => bind(work.page(request)),
            threads: (request) => bind(work.threads(request)),
            rebuild: (request) =>
              admin(work.rebuild(request), { threadId: request.threadId }).pipe(
                Effect.mapError((cause) =>
                  cause._tag === "LedgerError"
                    ? ThreadStoreError.make({
                        operation: "rebuild work",
                        message: cause.message,
                        cause,
                      })
                    : cause,
                ),
              ),
          },
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
    readPrompt: (request) => bindStream(rawStore.readPrompt(request)),
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
  };

  const claimImpl = Effect.fn("SqlRunStorage.claim")(function* (
    request: ClaimRequest,
    finalizer: { release: Effect.Effect<void> },
  ) {
    const claimedThreadId = request.threadId;
    const acquiredAt = yield* Clock.currentTimeMillis;

    const granted = yield* rawLedger.claim(request).pipe(
      Effect.catchCause((cause) =>
        Effect.gen(function* () {
          // Refresh committed authority: failure may precede the claim or lose its acknowledgement.
          for (const previous of active) {
            if (previous.authority.submission.thread_id !== claimedThreadId) continue;
            yield* refresh(previous).pipe(
              Effect.catchCause(() => Effect.sync(() => close(previous))),
            );
          }

          return yield* Effect.failCause(cause);
        }),
      ),
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
    // Authority refresh may observe administrative commits after progress preparation.
    // Keep the writer's observed CAS frontier separate so stale progress returns to reprepare.
    let writerTail = { sequence: authority.thread.tail_sequence, digest: initialDigest };

    const acceptTailConflict = (cause: Cause.Cause<unknown>) => {
      const error = Cause.squash(cause);

      if (
        !Schema.is(AppendConflict)(error) ||
        error.reason !== "tail" ||
        error.actualTailSequence === undefined ||
        error.actualTailDigest === undefined ||
        authority.thread.producer_epoch !== owned.epoch
      )
        return false;
      writerTail = { sequence: error.actualTailSequence, digest: error.actualTailDigest };
      authority.thread = Object.freeze({
        ...authority.thread,
        tail_sequence: writerTail.sequence,
        tail_digest: writerTail.digest,
      });
      owned.digest = writerTail.digest;

      return true;
    };

    let renewedAt = acquiredAt;
    const commands = ledgerKernel.bindAuthority(authority);

    releaseOwned = commands.releaseOwnership;

    const ownership = yield* bindRunOwnership(
      claimedThreadId,
      submissionId,
      Effect.sync(() => owned.token),
    ).pipe(Effect.provideService(SubmissionLedger, { ...rawLedger, ...commands }));

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
                if (!acceptTailConflict(exit.cause)) close(owned);

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

    const appendOwned = storeKernel.makeOwnedAppend(
      Effect.fnUntraced(function* (raw: RawAppendRequest) {
        const record = raw.records[0]?.canonical;

        // Original input and its own progress share the existing applied-input transaction.
        // The journal validates sidecar revision, frontier and charges before either write.
        if (
          !authority.owned ||
          authority.submission.input_applied_record_id !== null ||
          raw.records
            .slice(1)
            .some(
              ({ canonical: sidecar }) =>
                sidecar.payload._tag !== "RunContinuation" ||
                sidecar.payload.runId !== runIdForSubmission(submissionId) ||
                sidecar.payload.submissionId !== submissionId ||
                sidecar.payload.lastFact.recordId !== record?.recordId,
            ) ||
          raw.batchId !== submissionInputBatchId(submissionId) ||
          record?.recordId !== submissionInputRecordId(submissionId) ||
          record.payload._tag !== "UserInputRecorded" ||
          record.payload.kind !== "user" ||
          record.payload.submissionId !== submissionId ||
          record.payload.runId !== runIdForSubmission(submissionId)
        )
          return yield* journalKernel.journal.append(raw);

        const committed = yield* journalKernel.journal.withWriteTransaction(
          "append applied input transaction",
        )(
          Effect.gen(function* () {
            yield* ledgerOptions.hitFailpoint("ledger:mark-input-applied:before");

            const appended = yield* journalKernel.journal.appendInTransaction(raw);

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
        authority.submission = Object.freeze(committed.submission);

        return committed.appended;
      }),
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

              for (let retries = 0; ; retries++) {
                const request = FencedAppendRequest.make({
                  threadId: claimedThreadId,
                  producerEpoch: owned.epoch,
                  expectedTailSequence: writerTail.sequence,
                  expectedTailDigest: writerTail.digest,
                  batch,
                });

                const result = yield* restore(appendOwned(request)).pipe(Effect.exit);

                if (Exit.isFailure(result)) {
                  if (!acceptTailConflict(result.cause)) {
                    close(owned);

                    return yield* result;
                  }
                  if (
                    retries >= 8 ||
                    batch.records.some(({ payload }) => payload._tag === "RunContinuation")
                  )
                    return yield* result;
                  continue;
                }
                if (!result.value.replayed) {
                  authority.thread = Object.freeze({
                    ...authority.thread,
                    tail_sequence: result.value.lastSequence,
                    tail_digest: result.value.tailDigest,
                  });
                  owned.digest = result.value.tailDigest;
                }
                writerTail = { sequence: authority.thread.tail_sequence, digest: owned.digest };

                return result.value;
              }
            }),
          ),
        ),
      );

    const checkFence = bind(
      gate.withPermits(1)(
        Effect.gen(function* () {
          if (owned.closed)
            return yield* FenceRejected.make({
              threadId: claimedThreadId,
              attemptedEpoch: owned.epoch,
              actualEpoch: authority.thread.producer_epoch,
            });

          const current = yield* rawStore.inspectTail(
            ThreadTailRequest.make({ threadId: claimedThreadId }),
          );

          authority.thread = Object.freeze({
            ...authority.thread,
            producer_epoch: current.producerEpoch,
            tail_sequence: current.tailSequence,
            tail_digest: current.tailDigest,
          });
          owned.digest = current.tailDigest;
          if (current.producerEpoch !== owned.epoch)
            return yield* FenceRejected.make({
              threadId: claimedThreadId,
              attemptedEpoch: owned.epoch,
              actualEpoch: current.producerEpoch,
            });
        }).pipe(Effect.onError(() => Effect.sync(() => close(owned)))),
      ),
    );

    const session: RunStorageSession = {
      claim: claimed,
      threadId: claimedThreadId,
      producerEpoch: owned.epoch,
      tail: bind(Effect.sync(() => ({ ...writerTail }))),
      append,
      release,
      renew,
      checkFence,
      refresh: checkFence.pipe(
        Effect.andThen(
          Effect.sync(() => {
            writerTail = { sequence: authority.thread.tail_sequence, digest: owned.digest };
          }),
        ),
      ),
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
                  expectedTailSequence: writerTail.sequence,
                  expectedTailDigest: writerTail.digest,
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
            writerTail = {
              sequence: result.publication.tailSequence,
              digest: result.publication.tailDigest,
            };
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
            Context.add(storageContext(live), Scope.Scope, scope),
          ),
        ),
      ),
  });

  const importer = ThreadImport.of({
    // Empty-target validation makes a successful import disjoint from every live claim.
    import: <E, R>(request: Stream.Stream<ThreadArchive, E, R>) =>
      Effect.contextWith((live: Context.Context<R>) =>
        bind(
          Effect.scoped(
            Effect.gen(function* () {
              const captured = yield* captureImportSource(Stream.provideContext(request, live));

              return yield* gate.withPermits(1)(storeKernel.importer.import(captured));
            }),
          ),
        ),
      ),
  });

  return { store, importer, ledger, publisher, runStorage, reader: ThreadReader.fromStore(store) };
});
