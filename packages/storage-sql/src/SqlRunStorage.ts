import { runIdForSubmission } from "@yielded/agent/run-journal";
import { make as makeRunStorage, StorageOwner } from "@yielded/agent/run-storage";
import { SettlementPublisher } from "@yielded/agent/settlement-publisher";
import {
  SubmissionLedger,
  MarkInputAppliedRequest,
  submissionInputBatchId,
  submissionInputRecordId,
} from "@yielded/agent/submission-ledger";
import {
  ThreadImport,
  captureImportSource,
  type ThreadArchive,
} from "@yielded/agent/thread-import";
import { ThreadStore, ThreadReader } from "@yielded/agent/thread-store";
import { Channel, Context, Crypto, Effect, Scope, Stream } from "effect";
import { SqlClient } from "effect/sql/SqlClient";
import { CurrentTransformer } from "effect/sql/Statement";

import type { makeSqlJournalKernel, RawAppendRequest } from "./SqlJournal.ts";
import type { Diagnostic } from "./SqlStorage.ts";
import {
  makeSqlSubmissionLedgerKernel,
  type SqlSubmissionLedgerOptions,
} from "./SqlSubmissionLedger.ts";
import { makeSqlThreadStoreKernel, type SqlThreadStoreOptions } from "./SqlThreadStore.ts";

/** Bind all ports to the physical client's root, retaining caller Scope, Clock and diagnostics. */
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

  const owner = journalKernel.journal.owner;
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

  const bindScoped = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>) =>
    Effect.contextWith((live: Context.Context<Scope.Scope>) =>
      Effect.setContext(
        effect,
        Context.add(storageContext(live), Scope.Scope, Context.get(live, Scope.Scope)),
      ),
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

  const admin = <A, E>(
    effect: Effect.Effect<A, E>,
    _affected?: { readonly threadId?: string; readonly submissionId?: string },
  ) => bind(owner.withMutation(effect));

  const appendNative = storeKernel.makeOwnedAppend(
    Effect.fnUntraced(function* (raw: RawAppendRequest) {
      const record = raw.records[0]?.canonical;

      if (
        raw.records.length !== 1 ||
        record?.payload._tag !== "UserInputRecorded" ||
        record.payload.kind !== "user" ||
        record.payload.submissionId === undefined
      )
        return yield* journalKernel.journal.append(raw);
      const submissionId = record.payload.submissionId;

      if (
        !owner.hasOwner(raw.threadId, submissionId) ||
        raw.batchId !== submissionInputBatchId(submissionId) ||
        record.recordId !== submissionInputRecordId(submissionId) ||
        record.payload.runId !== runIdForSubmission(submissionId)
      )
        return yield* journalKernel.journal.append(raw);

      // Original input, active state and applied marker are one native transaction.
      const appended = yield* journalKernel.journal.withWriteTransaction(
        "append applied input transaction",
      )(
        Effect.gen(function* () {
          yield* ledgerOptions.hitFailpoint("ledger:mark-input-applied:before");
          const result = yield* journalKernel.journal.appendInTransaction(raw);

          yield* ledgerKernel
            .markInputAppliedInTransaction(
              MarkInputAppliedRequest.make({
                submissionId,
                recordId: record.recordId,
                sequence: result.firstSequence,
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

          return result;
        }),
      );

      yield* ledgerOptions.hitFailpoint("ledger:mark-input-applied:after");

      return appended;
    }),
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
    selectHead: (request) => admin(rawLedger.selectHead(request)),
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
      bind(owner.withProgress(request.append.threadId, ledgerKernel.publisher.publish(request))),
  });

  const checkpoints = rawStore.checkpoints;
  const countPeerMessages = rawStore.countPeerMessages;
  const work = rawStore.work;
  const archives = rawStore.archives;
  const verification = rawStore.verification;
  const readWorkerCapacity = rawStore.readWorkerCapacity;

  const archiveMutation = <A, E>(effect: Effect.Effect<A, E>, threadId: string) =>
    admin(effect, { threadId });

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
            rebuild: (request) => admin(work.rebuild(request), { threadId: request.threadId }),
          },
    materialize: (request) => admin(rawStore.materialize(request), { threadId: request.threadId }),
    append: (request) => bind(appendNative(request)),
    read: (request) => bindStream(rawStore.read(request)),
    readPrompt: (request) => bindStream(rawStore.readPrompt(request)),
    observe: (request) => bindStream(rawStore.observe(request)),
    export: (request) => bind(rawStore.export(request)),
    inspectTail: (request) => bind(rawStore.inspectTail(request)),
    readIdentity: (request) => bind(rawStore.readIdentity(request)),
    readExecution: (request) => bind(rawStore.readExecution(request)),
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

  const shared = yield* makeRunStorage.pipe(
    Effect.provideService(ThreadStore, store),
    Effect.provideService(SubmissionLedger, ledger),
    Effect.provideService(SettlementPublisher, publisher),
    Effect.provideService(StorageOwner, owner),
  );

  const runStorage = {
    ...shared,
    claim: (request: Parameters<typeof shared.claim>[0]) => bindScoped(shared.claim(request)),
  };

  const importer = ThreadImport.of({
    import: <E, R>(request: Stream.Stream<ThreadArchive, E, R>) =>
      Effect.contextWith((live: Context.Context<R>) =>
        bind(
          Effect.scoped(
            Effect.gen(function* () {
              const captured = yield* captureImportSource(Stream.provideContext(request, live));

              return yield* owner.withMutation(storeKernel.importer.import(captured));
            }),
          ),
        ),
      ),
  });

  return {
    store,
    importer,
    ledger,
    publisher,
    runStorage,
    reader: ThreadReader.fromStore(store),
    owner,
  };
});
