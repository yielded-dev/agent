import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import * as Agent from "@yielded/agent/agent";
import { AgentPolicy } from "@yielded/agent/agent-policy";
import {
  DelegationId,
  ReceiptId,
  RunId,
  SubmissionId,
  ThreadId,
  ToolCallId,
} from "@yielded/agent/identifiers";
import { IdempotencyKey, QueueSequence, Receipt } from "@yielded/agent/receipt";
import * as Subagent from "@yielded/agent/subagent";
import {
  SubagentDelegationCaps,
  SubagentGrant,
  SubagentParentLink,
  SubagentReservationAmounts,
} from "@yielded/agent/subagent-contract";
import {
  SubagentHost,
  WorkerReportPreparationFailure,
  type StartWorkerRequest,
  type WorkerReporting,
} from "@yielded/agent/subagent-host";
import { ToolResultBounds } from "@yielded/agent/tool-result";
import { WorkerCompletion, WorkerError, WorkerUpdate } from "@yielded/agent/worker";
import {
  Clock,
  Crypto,
  DateTime,
  Deferred,
  Duration,
  Effect,
  Fiber,
  Option,
  Schema,
  Stream,
} from "effect";
import { Toolkit } from "effect/ai";
import { TestClock } from "effect/testing";

import { automaticReporting } from "../../src/capabilities/internal/subagent-reporting.ts";
import {
  DurableRuntimeFailpoint,
  DurableRuntimeFailpointError,
  type DurableRuntimeFailpointLocation,
} from "../../src/durable/DurableFailpoint.ts";
import { makeAgentUpdateRuntime } from "../../src/durable/internal/agent-updates.ts";
import { makeWorkerRuntime, WorkerInputControl } from "../../src/durable/internal/worker-host.ts";
import { WorkerRuntime } from "../../src/durable/internal/worker-runtime.ts";
import {
  MessageDeliveryStore,
  defaultMessageDeliveryStoreLimits,
  applyMessageDeliveryChange,
  type MessageDeliveryRecord,
} from "../../src/durable/MessageDelivery.ts";
import {
  BatchId,
  CanonicalBatch,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ObservationOffset,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RunStartedRecord,
  SubmissionSettled,
  SubmissionSettledRecord,
  SubagentLineageRecorded,
  ThreadCreated,
  UserInputRecorded,
  type CanonicalRecordPayload,
} from "../../src/durable/Records.ts";
import {
  CurrentRunWriter,
  canonicalRunIds,
  makeProgressWriter,
} from "../../src/durable/RunContinuation.ts";
import { subagentLineageRecordId, workerOriginRecordId } from "../../src/durable/RunJournal.ts";
import {
  AbortIntent,
  AdmissionPolicyError,
  LedgerError,
  AdmissionNotAdmitted,
  AdmissionAdmitted,
  FundingOwner,
  Principal,
  Settlement,
  SubmissionSnapshot,
  SubmissionLedger,
  submissionSettlementId,
} from "../../src/durable/SubmissionLedger.ts";
import { PendingSubmission, SettledSubmission } from "../../src/durable/SubmissionStatus.ts";
import {
  AppendConflict,
  AppendResult,
  FenceRejected,
  ThreadIdentity,
  ThreadNotMaterialized,
  ThreadTail,
  ThreadWorkerCapacity,
  ThreadStore,
  ThreadReader,
  ThreadStoreError,
  type ThreadReadRequest,
} from "../../src/durable/ThreadStore.ts";
import { ThreadWorkEntry, workId } from "../../src/durable/ThreadWork.ts";
import { WakeScheduler } from "../../src/durable/WakeScheduler.ts";
import {
  WorkerBudgetAuthorizer,
  WorkerConcurrencyResolver,
  WorkerHostAuthorizer,
  WorkerHostConfig,
  WorkerPolicyResolver,
} from "../../src/durable/WorkerHost.ts";

const sourceId = Schema.decodeSync(ThreadId)("source");
const principal = Schema.decodeSync(Principal)("owner");
const digest = Schema.decodeSync(Digest)("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

const policy = AgentPolicy.make({
  maxTurns: 2,
  maxToolCalls: 2,
  maxDuration: "1 second",
  toolConcurrency: 2,
  toolResultBounds: ToolResultBounds.make({ maxBytes: 1_024 }),
});

const target = Agent.make("worker-target", {
  input: Schema.Struct({ text: Schema.String }),
  output: Schema.String,
  instructions: "Answer",
  toolkit: Toolkit.empty,
  policy,
});

const sourceAgent = Agent.make("source-agent", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Delegate",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    ...policy,
    maxTurns: 20,
    maxToolCalls: 20,
    maxDuration: "10 seconds",
  }),
});

const request = (key: string): StartWorkerRequest => ({
  delegationId: Schema.decodeSync(DelegationId)("research"),
  target,
  idempotencyKey: Schema.decodeSync(IdempotencyKey)(key),
  encodedInput: { text: key },
  encodedParameters: { note: key },
  policy,
  budget: {
    caps: SubagentDelegationCaps.make({
      maxTotalChildInvocations: 10,
      maxConcurrentChildren: 1,
      maxTurns: 20,
      maxToolCalls: 20,
      maxDurationMillis: 10_000,
    }),
    allocation: SubagentReservationAmounts.make({
      turns: 2,
      toolCalls: 2,
      durationMillis: 1_000,
      inputTokens: 0,
      outputTokens: 0,
      costMicrousd: 0,
      resultBytes: 1_024,
    }),
  },
  encodedGrant: SubagentGrant.make({ allowedToolNames: [], maxDepth: 1 }),
});

const reportDeclaration = Subagent.make("research", {
  target,
  parameters: Schema.Struct({ note: Schema.String }),
  prepareInput: ({ note }) => Effect.succeed({ text: note }),
});

const rawReport = automaticReporting(reportDeclaration);

const standardReport: WorkerReporting<WorkerReportPreparationFailure> = {
  ...rawReport,
  prepare: (report) =>
    rawReport
      .prepare(report)
      .pipe(Effect.mapError(() => WorkerReportPreparationFailure.make({ stage: "projection" }))),
};

const reportWith = (
  prepare: WorkerReporting<WorkerReportPreparationFailure>["prepare"] = standardReport.prepare,
): WorkerReporting<WorkerReportPreparationFailure> => ({ ...standardReport, prepare });

const harness = Effect.fn("workerHostHarness")(function* (
  options: {
    readonly independentBudget?: boolean;
    readonly limits?: Partial<typeof WorkerHostConfig.Service>;
    readonly authorize?: (typeof WorkerHostAuthorizer.Service)["authorize"];
    readonly beforeRead?: (request: ThreadReadRequest) => Effect.Effect<void, ThreadStoreError>;
    readonly afterIdentity?: (snapshot: ThreadIdentity) => Effect.Effect<void, ThreadStoreError>;
    readonly sourceReports?: ReadonlyArray<WorkerReporting<WorkerReportPreparationFailure>>;
    readonly targetReports?: ReadonlyArray<WorkerReporting<WorkerReportPreparationFailure>>;
  } = {},
) {
  const now = yield* Clock.currentTimeMillis;
  const crypto = yield* Crypto.Crypto;
  const logs = new Map<ThreadId, Array<CanonicalRecordEnvelope>>();
  const epochs = new Map<ThreadId, ProducerEpoch>();
  const appendAttempts: Array<{ readonly threadId: ThreadId; readonly epoch: ProducerEpoch }> = [];
  const rejectedAppends = { tail: 0, epoch: 0 };
  const deliveries = new Map<string, MessageDeliveryRecord>();
  const submissions = new Map<SubmissionId, SubmissionSnapshot>();
  const settlements = new Map<SubmissionId, Settlement>();
  const stopped = new Set<ThreadId>();
  const fundingOwners = new Map<SubmissionId, SubmissionId>();
  let rejectAdmission = false;
  let failpoint: DurableRuntimeFailpointLocation | undefined;
  let denied: "read" | "send" | "report" | "control" | undefined;

  let sequence = 0;

  const push = (threadId: ThreadId, payload: CanonicalRecordPayload, id: string, batchId = id) => {
    const records = logs.get(threadId) ?? [];

    records.push(
      CanonicalRecordEnvelope.make({
        threadId,
        batchId: Schema.decodeSync(BatchId)(batchId),
        sequence: Schema.decodeSync(CanonicalSequence)(records.length + 1),
        offset: Schema.decodeSync(ObservationOffset)(`${records.length + 1}`),
        record: RecordEnvelope.make({
          recordId: Schema.decodeSync(RecordEnvelope.fields.recordId)(id),
          schemaVersion: 1,
          family: "thread",
          createdAt: DateTime.makeUnsafe(now),
          deploymentId: Schema.decodeSync(DeploymentId)("test"),
          payload,
        }),
      }),
    );
    logs.set(threadId, records);
    if (!epochs.has(threadId)) epochs.set(threadId, Schema.decodeSync(ProducerEpoch)(0));
  };

  push(sourceId, ThreadCreated.make({ agentId: sourceAgent.id, definitions }), "source-created");
  const lookup = (id: SubmissionId) => Option.fromNullishOr(submissions.get(id));

  const status: WorkerInputControl["Service"]["status"] = (receipt) =>
    Effect.sync(() => {
      const settlement = settlements.get(receipt.submissionId);

      return settlement === undefined
        ? PendingSubmission.make({})
        : SettledSubmission.make({ settlement });
    });

  let runtime: Effect.Success<ReturnType<typeof makeWorkerRuntime>>;

  const liveInputs = (threadId: ThreadId) => {
    const all = logs.get(threadId) ?? [];

    return all.filter((entry) => {
      const request = entry.record.payload;

      return (
        request._tag === "WorkerInputRequested" &&
        !all.some(
          ({ record: { payload } }) =>
            (payload._tag === "WorkerInputCompleted" &&
              payload.effectsResolved === true &&
              payload.messageId === request.admission.messageId) ||
            (payload._tag === "WorkerInputRefused" &&
              payload.reservation.recordId === entry.record.recordId),
        )
      );
    });
  };

  const fixtureStore = ThreadStore.of({
    readWorkerCapacity: (request) =>
      Effect.suspend(() => {
        const all = logs.get(request.threadId) ?? [];

        if (all.length !== request.expectedTailSequence)
          return ThreadStoreError.make({ operation: "worker capacity", message: "Tail changed" });

        const rows = liveInputs(request.threadId).flatMap(({ record: { payload } }) =>
          payload._tag === "WorkerInputRequested" ? [payload.admission] : [],
        );

        return Effect.succeed(
          ThreadWorkerCapacity.make({
            threadId: request.threadId,
            tailSequence: request.expectedTailSequence,
            tailDigest: digest,
            producerEpoch: epochs.get(request.threadId)!,
            activeWorkers: Math.min(
              new Set(rows.map((row) => row.origin.worker.threadId)).size,
              request.activeLimit + 1,
            ),
            workerActive: rows.some((row) => row.origin.worker.threadId === request.workerThreadId),
            pendingInputs: Math.min(
              rows.filter(
                (row) =>
                  row.origin.worker.threadId === request.workerThreadId &&
                  (row.reportKind === "update") === request.update,
              ).length,
              request.pendingLimit + 1,
            ),
          }),
        );
      }),
    work: {
      threads: () => Effect.die("Worker fixture does not enumerate global owners"),
      rebuild: () => Effect.die("Worker fixture has no disposable persisted index"),
      page: ({ threadId, limit }) =>
        Effect.sync(() => ({
          entries: [...deliveries.values()]
            .filter(
              (row) =>
                row.key.ownerThreadId === threadId &&
                row.status !== "processed" &&
                row.status !== "refused",
            )
            .slice(0, limit)
            .map((row) =>
              Schema.decodeSync(ThreadWorkEntry)({
                id: workId("delivery", row.key.messageId),
                owner: { _tag: "Delivery", messageId: row.key.messageId },
                stateReference: {
                  _tag: "Delivery",
                  messageId: row.key.messageId,
                  version: row.version,
                },
                state: "ready",
                partition:
                  row.envelope.workerAdmission?.reportKind === "update" ? "update" : "ordinary",
              }),
            ),
        })),
    },
    readIdentity: ({ threadId }) =>
      Effect.gen(function* () {
        const records = logs.get(threadId);

        if (records === undefined) return yield* ThreadNotMaterialized.make({ threadId });

        const selected: Array<CanonicalRecordEnvelope> = [];

        for (const entry of [
          records[0],
          records.find(({ record }) => record.recordId === workerOriginRecordId(threadId)),
          records.find(({ record }) => record.recordId === subagentLineageRecordId(threadId)),
        ])
          if (entry !== undefined && !selected.includes(entry)) selected.push(entry);

        const snapshot = ThreadIdentity.make({
          admissions: [...submissions.values()].filter((row) => row.threadId === threadId).length,
          threadId,
          tailSequence: Schema.decodeSync(CanonicalSequence)(records.length),
          tailDigest: digest,
          producerEpoch: epochs.get(threadId)!,
          records: selected,
        });

        yield* options.afterIdentity?.(snapshot) ?? Effect.void;

        return snapshot;
      }),
    read: (request) =>
      Stream.suspend(() => {
        const all = logs.get(request.threadId) ?? [];
        const selection = "selection" in request ? request.selection : undefined;
        let records = all;

        if (selection?._tag === "RecordId") {
          records = all.filter((entry) => entry.record.recordId === selection.recordId);
        } else if (selection?._tag === "WorkerExecution") {
          records = ["UserInputRecorded", "RunStarted"]
            .flatMap((tag) =>
              all
                .filter(
                  ({ record: { payload } }) =>
                    payload._tag === tag && "runId" in payload && payload.runId !== undefined,
                )
                .slice(-1),
            )
            .sort((a, b) => a.sequence - b.sequence);
        } else if (selection?._tag === "RunInput") {
          records = all.filter(
            ({ record: { payload } }) =>
              payload._tag === "UserInputRecorded" &&
              payload.kind === "user" &&
              payload.runId === selection.runId,
          );
        } else if (selection?._tag === "WorkerState") {
          const reservations = all.flatMap(({ record: { payload } }) =>
            payload._tag === "SubtreeBudgetReserved" &&
            selection.sourceSubmissionId !== undefined &&
            payload.sourceSubmissionId === selection.sourceSubmissionId
              ? [payload.reservationId]
              : [],
          );

          records = all.filter(
            ({ record: { payload } }) =>
              ["ThreadCreated", "WorkerOriginRecorded", "SubagentLineageRecorded"].includes(
                payload._tag,
              ) ||
              (payload._tag === "SubtreeBudgetReserved" &&
                reservations.includes(payload.reservationId)) ||
              (payload._tag === "SubagentJoined" &&
                selection.sourceSubmissionId !== undefined &&
                payload.runId === `run:${selection.sourceSubmissionId}`) ||
              (payload._tag === "WorkerInputRequested" &&
                reservations.includes(payload.admission.messageId)) ||
              ((payload._tag === "WorkerInputCompleted" || payload._tag === "WorkerInputRefused") &&
                reservations.includes(payload.messageId)),
          );
        } else if (selection?._tag === "LiveWorkerInputs") {
          records = liveInputs(request.threadId).filter(
            ({ record: { payload } }) =>
              payload._tag === "WorkerInputRequested" &&
              (selection.workerThreadId === undefined ||
                payload.admission.origin.worker.threadId === selection.workerThreadId),
          );
        } else if (selection?._tag === "WorkerStop") {
          records = all
            .filter(
              ({ sequence, record: { payload } }) =>
                sequence <= selection.throughSequence &&
                payload._tag === "WorkerStopRequested" &&
                payload.command.worker.threadId === selection.workerThreadId,
            )
            .slice(-1);
        } else if (selection?._tag === "RunContinuation") {
          records = all
            .filter(
              ({ sequence, record: { payload } }) =>
                sequence <= selection.throughSequence &&
                payload._tag === "RunContinuation" &&
                payload.runId === selection.runId,
            )
            .slice(-1);
        } else if (selection?._tag === "RunEvidence") {
          records = all.filter(
            ({ sequence, record }) =>
              sequence <= selection.throughSequence &&
              record.payload._tag !== "RunContinuation" &&
              canonicalRunIds(record).includes(selection.runId),
          );
        } else if (
          selection?._tag === "DeliveryPredecessor" ||
          selection?._tag === "LastAgentUpdate"
        ) {
          records = all
            .filter(
              ({ sequence, record: { payload } }) =>
                sequence <= selection.throughSequence &&
                (selection._tag === "LastAgentUpdate"
                  ? payload._tag === "AgentUpdateEmitted"
                  : payload._tag === "WorkerReportPrepared" ||
                    (payload._tag === "AgentUpdateEmitted" && payload.delivery !== undefined)),
            )
            .slice(-1);
        } else if (selection !== undefined)
          return Stream.die("Worker fixture only reads exact identities and accounting");
        const page = "selection" in request ? request.page : request;

        return Stream.fromIterable(
          records
            .filter((entry) => entry.sequence > (page.afterSequence ?? 0))
            .slice(0, page.limit)
            .map((entry) => {
              return entry;
            }),
        );
      }).pipe(Stream.onStart(Effect.suspend(() => options.beforeRead?.(request) ?? Effect.void))),
    export: () =>
      ThreadStoreError.make({
        operation: "export",
        message: "Worker fixture does not export Threads",
      }),
    inspectTail: ({ threadId }) =>
      Effect.suspend(() => {
        const records = logs.get(threadId);

        return records === undefined
          ? ThreadNotMaterialized.make({ threadId })
          : Effect.succeed(
              ThreadTail.make({
                threadId,
                tailSequence: Schema.decodeSync(CanonicalSequence)(records.length),
                tailDigest: digest,
                producerEpoch: epochs.get(threadId)!,
              }),
            );
      }),
    append: (request) =>
      Effect.gen(function* () {
        yield* Effect.yieldNow;
        const records = logs.get(request.threadId) ?? [];

        appendAttempts.push({ threadId: request.threadId, epoch: request.producerEpoch });
        const currentEpoch = epochs.get(request.threadId)!;

        if (currentEpoch !== request.producerEpoch) {
          rejectedAppends.epoch++;

          return yield* FenceRejected.make({
            threadId: request.threadId,
            attemptedEpoch: request.producerEpoch,
            actualEpoch: currentEpoch,
          });
        }
        if (
          records.length !== request.expectedTailSequence ||
          digest !== request.expectedTailDigest
        ) {
          rejectedAppends.tail++;

          return yield* AppendConflict.make({
            threadId: request.threadId,
            batchId: request.batch.batchId,
            reason: "tail",
          });
        }
        const first = records.length + 1;

        for (const record of request.batch.records)
          push(request.threadId, record.payload, record.recordId, request.batch.batchId);

        return AppendResult.make({
          firstSequence: Schema.decodeSync(CanonicalSequence)(first),
          lastSequence: Schema.decodeSync(CanonicalSequence)(records.length),
          tailDigest: digest,
          replayed: false,
        });
      }),

    materialize: () => Effect.die("Worker fixture materializes through input control"),
    observe: () => Stream.die("Worker fixture uses finite canonical reads"),
  });

  const updateStore = ThreadStore.of({
    ...fixtureStore,
    append: (request) =>
      Effect.gen(function* () {
        const writer = yield* makeProgressWriter(request.threadId, DeploymentId.make("test")).pipe(
          Effect.provideService(ThreadReader, ThreadReader.fromStore(fixtureStore)),
          Effect.provideService(Crypto.Crypto, crypto),
        );

        return yield* writer.commit(request.batch).pipe(
          Effect.provideService(CurrentRunWriter, {
            threadId: request.threadId,
            tail: Effect.succeed({
              sequence: request.expectedTailSequence,
              digest: request.expectedTailDigest,
            }),
            append: (batch) => fixtureStore.append({ ...request, batch }),
          }),
        );
      }),
  });

  const runtimes = yield* makeWorkerRuntime({
    deploymentId: Schema.decodeSync(DeploymentId)("test"),
    producerId: Schema.decodeSync(ProducerId)("test"),
    settlementPollInterval: Duration.millis(5),
    bindings: [sourceAgent, target].map((definition) => ({
      definition,
      agentId: definition.id,
      digests: definitions,
      attempt: () => Effect.succeed(Option.none()),
      reporting:
        definition === sourceAgent ? (options.sourceReports ?? []) : (options.targetReports ?? []),
    })),
  }).pipe(
    Effect.flatMap((runtime) =>
      makeAgentUpdateRuntime({
        deploymentId: Schema.decodeSync(DeploymentId)("test"),
        producerId: Schema.decodeSync(ProducerId)("test"),
      }).pipe(
        Effect.provideService(ThreadStore, updateStore),
        Effect.provideService(WorkerRuntime, runtime),
        Effect.provide(WakeScheduler.layerNoop),
        Effect.map((updates) => ({ runtime, updates })),
      ),
    ),
    Effect.provideService(WorkerBudgetAuthorizer, {
      authorize: () =>
        options.independentBudget !== false
          ? Effect.void
          : WorkerError.make({ operation: "start", reason: "denied" }),
    }),
    Effect.provideService(WorkerHostConfig, {
      maxActiveWorkersPerSource: 2,
      maxPendingInputsPerWorker: 2,
      lifetimeMillis: 60_000,
      ...options.limits,
    }),
    Effect.provideService(WorkerHostAuthorizer, {
      authorize: (request) =>
        Effect.suspend(() => {
          return request.access === denied ||
            request.principal !== principal ||
            !logs.has(request.sourceThreadId)
            ? WorkerError.make({ operation: request.operation, reason: "denied" })
            : options.authorize === undefined
              ? Effect.succeed(principal)
              : options.authorize(request);
        }),
    }),
    Effect.provideService(DurableRuntimeFailpoint, {
      hit: (point) =>
        Effect.suspend(() =>
          point === failpoint
            ? DurableRuntimeFailpointError.make({ location: point })
            : Effect.void,
        ),
    }),
    Effect.provideService(ThreadStore, fixtureStore),
    Effect.provideService(MessageDeliveryStore, {
      limits: defaultMessageDeliveryStoreLimits,
      maxStoredValueBytes: 16 * 1_024 * 1_024,
      get: ({ messageId }) => Effect.sync(() => deliveries.get(messageId) ?? null),
      insert: (record) =>
        Effect.sync(() => {
          deliveries.set(record.key.messageId, record);

          return record;
        }),
      change: ({ messageId }, change) =>
        Effect.gen(function* () {
          const changed = yield* Effect.fromResult(
            applyMessageDeliveryChange(deliveries.get(messageId)!, change),
          );

          deliveries.set(messageId, changed);

          return changed;
        }),
      list: ({ ownerThreadId, limit, after, workerStarts, pendingWorker }) =>
        Effect.sync(() => {
          const rows = [...deliveries.values()]
            .filter((record) => {
              const origin = record.envelope.workerAdmission?.origin;

              return (
                record.key.ownerThreadId === ownerThreadId &&
                (after === undefined || record.key.messageId > after) &&
                (workerStarts === undefined ||
                  (origin?.worker.delegationId === workerStarts.delegationId &&
                    origin.worker.targetAgentId === workerStarts.targetAgentId &&
                    origin.firstMessageId === record.key.messageId)) &&
                (pendingWorker === undefined ||
                  (origin?.worker.threadId === pendingWorker &&
                    record.receipt === null &&
                    (record.status === "pending" || record.status === "parked")))
              );
            })
            .sort((a, b) => a.key.messageId.localeCompare(b.key.messageId));

          return {
            items: rows.slice(0, limit),
            next: rows.length > limit ? rows[limit - 1]!.key.messageId : null,
          };
        }),
      due: () =>
        Effect.succeed(
          [...deliveries.values()]
            .filter((record) => record.status === "pending")
            .map((record) => record.key),
        ),
      nextDeadline: () => Effect.succeed(null),
    }),
    Effect.provideService(SubmissionLedger, {
      inspectWorker: (threadId) =>
        Effect.sync(() => {
          const rows = [...submissions.values()]
            .filter((row) => row.threadId === threadId)
            .sort((a, b) => a.queueSequence - b.queueSequence);

          return {
            latest: rows.at(-1) ?? null,
            active: rows.find((row) => row.state !== "settled") ?? null,
            stopped: stopped.has(threadId),
          };
        }),
      lookup: (request) =>
        Effect.sync(() => {
          return request._tag === "SubmissionLookupById"
            ? lookup(request.submissionId)
            : Option.fromNullishOr(
                [...submissions.values()].find(
                  (row) =>
                    row.threadId === request.threadId &&
                    row.principal === request.principal &&
                    row.idempotencyKey === request.idempotencyKey,
                ),
              );
        }),

      capabilities: Effect.die("Worker fixture only implements ledger lookup"),
      scanNonterminal: Stream.die("Worker fixture only implements ledger lookup"),
      admit: () => Effect.die("Worker fixture only implements ledger lookup"),
      markReady: () => Effect.die("Worker fixture only implements ledger lookup"),
      resolveFundingOwner: ({ threadId, submissionId }) =>
        Effect.suspend(() => {
          const selected = submissions.get(submissionId);
          const owner = submissions.get(fundingOwners.get(submissionId) ?? submissionId);

          return selected === undefined ||
            owner === undefined ||
            selected.threadId !== threadId ||
            owner.threadId !== threadId
            ? Effect.fail(
                LedgerError.make({ operation: "funding owner", message: "Missing exact owner" }),
              )
            : Effect.succeed(FundingOwner.make({ selected, owner }));
        }),
      stopWorker: ({ threadId }) =>
        Effect.sync(() => {
          stopped.add(threadId);

          return 0;
        }),
      resolveAdmission: ({ threadId, principal, idempotencyKey }) =>
        Effect.sync(() => {
          const found = [...submissions.values()].find(
            (row) =>
              row.threadId === threadId &&
              row.principal === principal &&
              row.idempotencyKey === idempotencyKey,
          );

          return found === undefined
            ? AdmissionNotAdmitted.make()
            : AdmissionAdmitted.make({ submission: found });
        }),
      claim: () => Effect.die("Worker fixture only implements ledger lookup"),
      renewOwnership: () => Effect.die("Worker fixture only implements ledger lookup"),
      releaseOwnership: () => Effect.die("Worker fixture only implements ledger lookup"),
      markInputApplied: () => Effect.die("Worker fixture only implements ledger lookup"),
      finalizeSettlement: () => Effect.die("Worker fixture only implements ledger lookup"),
      requestAbort: () => Effect.die("Worker fixture only implements ledger lookup"),
      readAbortIntent: () => Effect.die("Worker fixture only implements ledger lookup"),
      claimJoining: () => Effect.die("Worker fixture only implements ledger lookup"),
      markJoined: () => Effect.die("Worker fixture only implements ledger lookup"),
      revertJoining: () => Effect.die("Worker fixture only implements ledger lookup"),
      suspend: () => Effect.die("Worker fixture only implements ledger lookup"),
      recordApprovalDecision: () => Effect.die("Worker fixture only implements ledger lookup"),
      markUnknown: () => Effect.die("Worker fixture only implements ledger lookup"),
      recordUnknownResolution: () => Effect.die("Worker fixture only implements ledger lookup"),
      recordChildSettled: () => Effect.die("Worker fixture only implements ledger lookup"),
      reserveChildBudget: () => Effect.die("Worker fixture only implements ledger lookup"),
      attachChildToReservation: () => Effect.die("Worker fixture only implements ledger lookup"),
      beginChildBudgetRelease: () => Effect.die("Worker fixture only implements ledger lookup"),
      releaseChildBudget: () => Effect.die("Worker fixture only implements ledger lookup"),
      loadRecoverySnapshot: () => Effect.die("Worker fixture only implements ledger lookup"),
    }),
    Effect.provideService(WorkerInputControl, {
      submit: (envelope) =>
        Effect.gen(function* () {
          const metadata = envelope.workerAdmission!;

          const options = {
            threadId: envelope.threadId,
            definitions: envelope.definitions,
            principal: envelope.deliveryPrincipal,
            idempotencyKey: envelope.admissionKey,
            workerAdmission: metadata,
          };

          yield* runtime
            .validateAdmission(
              metadata,
              options,
              envelope.agentId,
              envelope.inputDigest,
              envelope.input,
            )
            .pipe(
              Effect.mapError((error) =>
                AdmissionPolicyError.make({
                  reason:
                    error.reason === "storage" || error.reason === "unavailable"
                      ? "unavailable"
                      : "refused",
                  code: `worker-${error.reason}`,
                  cause: error,
                }),
              ),
            );

          const existing = [...submissions.values()].find(
            (row) => row.idempotencyKey === envelope.admissionKey,
          );

          if (existing !== undefined)
            return Receipt.make({
              threadId: existing.threadId,
              submissionId: existing.submissionId,
              receiptId: existing.receiptId,
              queueSequence: existing.queueSequence,
            });
          if (rejectAdmission || stopped.has(envelope.threadId))
            return yield* AdmissionPolicyError.make({
              reason: "refused",
              code: "receiver-refused",
            });
          if (!logs.has(envelope.threadId))
            push(
              envelope.threadId,
              ThreadCreated.make({ agentId: target.id, definitions }),
              "child-created",
            );
          yield* runtime.ensureOrigin(metadata.origin).pipe(Effect.orDie);
          sequence++;
          const submissionId = Schema.decodeSync(SubmissionId)(`submission-${sequence}`);

          const receipt = Receipt.make({
            threadId: envelope.threadId,
            submissionId,
            receiptId: Schema.decodeSync(ReceiptId)(`receipt-${sequence}`),
            queueSequence: Schema.decodeSync(QueueSequence)(sequence),
          });

          submissions.set(
            submissionId,
            SubmissionSnapshot.make({
              ...receipt,
              principal,
              idempotencyKey: envelope.admissionKey,
              agentId: target.id,
              agentDigests: definitions,
              deploymentId: Schema.decodeSync(DeploymentId)("test"),
              inputPayload: envelope.input,
              inputDigest: envelope.inputDigest,
              state: "ready",
              createdAt: DateTime.makeUnsafe(now),
              workerAdmission: metadata,
            }),
          );

          return receipt;
        }),
      status,
      abort: (command) =>
        Effect.suspend(() =>
          Effect.succeed(AbortIntent.make({ ...command, requestedAt: DateTime.makeUnsafe(now) })),
        ),
    }),
  );

  runtime = runtimes.runtime;
  const ownerId = Schema.decodeSync(SubmissionId)("report-owner");

  if (options.sourceReports?.length)
    submissions.set(
      ownerId,
      SubmissionSnapshot.make({
        submissionId: ownerId,
        threadId: sourceId,
        queueSequence: Schema.decodeSync(QueueSequence)(1),
        principal,
        idempotencyKey: Schema.decodeSync(IdempotencyKey)("report-owner"),
        agentId: sourceAgent.id,
        agentDigests: definitions,
        deploymentId: Schema.decodeSync(DeploymentId)("test"),
        inputPayload: "original parent input",
        inputDigest: digest,
        receiptId: Schema.decodeSync(ReceiptId)("report-owner"),
        state: "settled",
        createdAt: DateTime.makeUnsafe(now),
      }),
    );

  const host = yield* runtime.acquire({
    sourceThreadId: sourceId,
    principal,
    ...(options.sourceReports?.length ? { sourceSubmissionId: ownerId } : {}),
  });

  const commit = Effect.fnUntraced(function* (
    threadId: ThreadId,
    payload: CanonicalRecordPayload,
    id: string,
  ) {
    const store = fixtureStore;

    const writer = yield* makeProgressWriter(
      threadId,
      Schema.decodeSync(DeploymentId)("test"),
    ).pipe(Effect.provideService(ThreadReader, ThreadReader.fromStore(store)));

    yield* writer
      .commit(
        CanonicalBatch.make({
          batchId: BatchId.make(id),
          producerId: ProducerId.make("test"),
          records: [
            RecordEnvelope.make({
              recordId: Schema.decodeSync(RecordEnvelope.fields.recordId)(id),
              family: "thread",
              schemaVersion: 1,
              createdAt: DateTime.makeUnsafe(now),
              deploymentId: DeploymentId.make("test"),
              payload,
            }),
          ],
        }),
      )
      .pipe(
        Effect.provideService(CurrentRunWriter, {
          threadId: threadId,
          tail: Effect.sync(() => ({
            sequence: CanonicalSequence.make(logs.get(threadId)?.length ?? 0),
            digest,
          })),
          append: (batch) =>
            Effect.flatMap(store.inspectTail({ threadId: threadId }), (tail) =>
              store.append({
                threadId: threadId,
                producerEpoch: tail.producerEpoch,
                expectedTailSequence: tail.tailSequence,
                expectedTailDigest: tail.tailDigest,
                batch,
              }),
            ),
        }),
      );
  });

  const settle = Effect.fn("workerHostHarness.settle")(function* (
    receipt: Receipt,
    result = "done",
    options: { readonly host?: Receipt } = {},
  ) {
    const row = submissions.get(receipt.submissionId)!;
    const settlementId = submissionSettlementId(row.submissionId);
    const outcome = "completed";

    const runId = Schema.decodeSync(SubmissionSettled.fields.runId)(
      `run:${options.host?.submissionId ?? row.submissionId}`,
    );

    if (
      runId !== undefined &&
      !logs.get(row.threadId)?.some(({ record }) => record.recordId === `input:${row.submissionId}`)
    )
      push(
        row.threadId,
        UserInputRecorded.make({
          submissionId: row.submissionId,
          kind: options.host === undefined ? "user" : "steering",
          runId,
          input: row.inputPayload,
        }),
        `input:${row.submissionId}`,
      );

    const payload = Schema.decodeUnknownSync(Schema.toType(SubmissionSettledRecord))(
      SubmissionSettled.make({
        submissionId: row.submissionId,
        receiptId: row.receiptId,
        settlementId,
        outcome,
        ...(options.host === undefined && outcome === "completed" ? { result } : {}),
        ...(runId === undefined ? {} : { runId }),
      }),
    );

    yield* commit(row.threadId, payload, `settled:${row.submissionId}`);
    yield* runtime.completeInput(row);

    const settlement = Settlement.make({
      submissionId: row.submissionId,
      receiptId: row.receiptId,
      settlementId,
      outcome,
      settledAt: DateTime.makeUnsafe(now),
    });

    settlements.set(row.submissionId, settlement);
    submissions.set(row.submissionId, SubmissionSnapshot.make({ ...row, state: "settled" }));
  });

  const startRun = Effect.fnUntraced(function* (receipt: Receipt) {
    const row = submissions.get(receipt.submissionId)!;
    const runId = RunId.make(`run:${row.submissionId}`);

    push(
      row.threadId,
      UserInputRecorded.make({
        submissionId: row.submissionId,
        kind: "user",
        runId,
        input: row.inputPayload,
      }),
      `input:${row.submissionId}`,
    );
    yield* commit(
      row.threadId,
      RunStartedRecord.make({ runId, policyAccountingVersion: 1, maxDurationMillis: 10_000 }),
      `started:${row.submissionId}`,
    );
  });

  return {
    startRun,
    runtime,
    updates: runtimes.updates,
    host,
    deliveries,
    logs,
    epochs,
    appendAttempts,
    rejectedAppends,
    submissions,
    fundingOwners,
    rejectAdmission: (value: boolean) => {
      rejectAdmission = value;
    },
    settle,
    push,
    deny: (value: typeof denied) => {
      denied = value;
    },
    fail: (value: typeof failpoint) => {
      failpoint = value;
    },
  };
});

layer(NodeCrypto.layer)((it) => {
  // Regression: https://github.com/yielded-dev/agent/commit/4ff21e2a4
  it.effect(
    "reuses one start admission through preparation and rejects a later revoked start",
    () =>
      Effect.gen(function* () {
        let sourceReads = 0;

        const h = yield* harness({
          authorize: (request) => {
            if (
              request.operation === "start" &&
              request.access === "send" &&
              !("admission" in request)
            )
              sourceReads++;

            return Effect.succeed(principal);
          },
        });

        const command = request("one-admission");

        const started = yield* h.host.start({
          ...command,
          prepare: () =>
            Effect.gen(function* () {
              yield* h.host.resolveTargetPolicy({ target, encodedInput: command.encodedInput });

              return { encodedInput: command.encodedInput, policy, budget: command.budget };
            }),
        });

        expect(started.delivery.status).toBe("parked");
        expect(sourceReads).toBe(1);
        h.deny("send");
        const denied = yield* h.host.start(command).pipe(Effect.flip);

        expect(denied.reason).toBe("denied");
        expect(h.submissions.size).toBe(1);
      }),
  );

  // Regression: https://github.com/yielded-dev/agent/pull/621
  for (const advance of ["tail", "epoch"] as const)
    it.effect(`origin establishment retries a ${advance} advance after its identity snapshot`, () =>
      Effect.gen(function* () {
        let onIdentity: Effect.Effect<void, ThreadStoreError> = Effect.void;
        const h = yield* harness({ afterIdentity: () => Effect.suspend(() => onIdentity) });
        const started = yield* h.host.start(request(`origin-${advance}-takeover`));
        const threadId = started.worker.threadId;

        const origin = h.submissions.get(started.delivery.receipt!.submissionId)!.workerAdmission!
          .origin;

        h.logs.set(threadId, h.logs.get(threadId)!.slice(0, 1));
        h.appendAttempts.length = 0;

        onIdentity = Effect.sync(() => {
          onIdentity = Effect.void;
          if (advance === "epoch") h.epochs.set(threadId, Schema.decodeSync(ProducerEpoch)(1));
          else
            h.push(threadId, UserInputRecorded.make({ kind: "steering", input: "raced" }), "raced");
        });
        yield* h.runtime.ensureOrigin(origin);
        expect(h.rejectedAppends[advance]).toBe(1);
        expect(h.appendAttempts.map((attempt) => attempt.epoch)).toEqual(
          advance === "epoch" ? [0, 1] : [0, 0],
        );
        expect(h.logs.get(threadId)!.map(({ record }) => record.payload._tag)).toEqual(
          advance === "epoch"
            ? ["ThreadCreated", "WorkerOriginRecorded"]
            : ["ThreadCreated", "UserInputRecorded", "WorkerOriginRecorded"],
        );
      }),
    );

  // Regression: https://github.com/yielded-dev/agent/commit/6a4f4f870
  it.effect(
    "origin establishment replays the same identity and refuses incompatible canonical ancestry",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();
        const started = yield* h.host.start(request("origin-identity"));
        const threadId = started.worker.threadId;

        const origin = h.submissions.get(started.delivery.receipt!.submissionId)!.workerAdmission!
          .origin;

        const history = [...h.logs.get(threadId)!];

        yield* h.runtime.ensureOrigin(origin);
        expect(h.logs.get(threadId)).toEqual(history);

        const mismatch = yield* h.runtime
          .ensureOrigin({
            ...origin,
            firstMessageId: Schema.decodeSync(IdempotencyKey)("different-origin"),
          })
          .pipe(Effect.flip);

        expect(mismatch.reason).toBe("worker-mismatch");
        expect(h.logs.get(threadId)).toEqual(history);

        {
          h.logs.set(threadId, []);
          h.push(
            threadId,
            ThreadCreated.make({
              agentId: target.id,
              definitions: definitions,
            }),
            "child-created",
          );
          h.push(
            threadId,
            SubagentLineageRecorded.make({
              parentLink: SubagentParentLink.make({
                delegationId: origin.worker.delegationId,
                parentAgentId: sourceAgent.id,
                parentThreadId: sourceId,
                parentRunId: Schema.decodeSync(RunId)("parent-run"),
                parentToolCallId: Schema.decodeSync(ToolCallId)("parent-call"),
                depth: 1,
              }),
              parentSubmissionId: Schema.decodeSync(SubmissionId)("parent-submission"),
              childDefinitionDigests: definitions,
              childInputDigest: digest,
              grantDigest: digest,
              policy: origin.policy,
              budget: origin.budget,
              grant: origin.grant,
            }),
            subagentLineageRecordId(threadId),
          );
          const before = [...h.logs.get(threadId)!];
          const refused = yield* h.runtime.ensureOrigin(origin).pipe(Effect.flip);

          expect(refused.reason).toBe("worker-mismatch");
          expect(h.logs.get(threadId)).toEqual(before);
        }
        h.logs.delete(threadId);
        expect((yield* h.runtime.ensureOrigin(origin).pipe(Effect.flip)).reason).toBe("storage");
      }),
  );

  // Regression: https://github.com/yielded-dev/agent/commit/4ff21e2a4c3735be34955e7d5caf64f623d33f81
  // Reuse the public-start retained-command fixture from #568 for follow-up correction provenance.
  it.effect(`public follow-up replays its pending correction across Runs without preparation`, () =>
    Effect.gen(function* () {
      class PreparationFailed extends Schema.TaggedError<PreparationFailed>()(
        "PreparationFailed",
        {},
      ) {}
      let preparations = 0;
      let throwPreparation = false;
      const h = yield* harness();
      const first = yield* h.host.start(request("original task"));

      const declaration = Subagent.make("research", {
        target,
        parameters: Schema.Struct({ note: Schema.String }),
        failure: PreparationFailed,
        prepareInput: ({ note }, caller) =>
          Effect.gen(function* () {
            preparations++;
            if (throwPreparation) return yield* new PreparationFailed();

            return {
              text: `${note}:${caller.source === "tool" ? caller.parent.runId : "programmatic"}`,
            };
          }),
      });

      const facet = (run: string) =>
        h.runtime.facet(
          {
            source: {
              _tag: "tool",
              agentId: sourceAgent.id,
              threadId: sourceId,
              runId: Schema.decodeSync(RunId)(run),
              toolCallId: Schema.decodeSync(ToolCallId)(`call:${run}`),
            },
            policy: sourceAgent.policy,
            depth: 0,
          },
          principal,
        );

      const worker = Schema.decodeSync(Subagent.Worker(declaration))(first.worker);
      const key = Schema.decodeSync(IdempotencyKey)("explicit-correction");

      const followUp = Subagent.followUp(
        declaration,
        worker,
        { note: "corrected brief" },
        { idempotencyKey: key },
      );

      h.fail("worker:before-source-append");

      const correction = yield* followUp.pipe(
        Effect.provideService(SubagentHost, facet("human-correction-run")),
      );

      const envelope = structuredClone(h.deliveries.get(correction.message.messageId)!.envelope);

      expect(envelope.input).toEqual({ text: "corrected brief:human-correction-run" });
      expect(envelope.workerAdmission?.origin).toEqual(
        h.deliveries.get(first.delivery.message.messageId)!.envelope.workerAdmission?.origin,
      );
      expect(correction.receipt === null).toBe(true);
      h.fail(undefined);
      yield* TestClock.adjust("31 seconds");
      const later = facet("later-run");
      const replay = yield* followUp.pipe(Effect.provideService(SubagentHost, later));

      expect(replay.message).toEqual(correction.message);
      expect(replay.receipt).not.toBeNull();
      throwPreparation = true;
      expect(yield* followUp.pipe(Effect.provideService(SubagentHost, later))).toEqual(replay);
      expect(
        yield* Subagent.followUp(
          declaration,
          worker,
          { note: "changed brief" },
          { idempotencyKey: key },
        ).pipe(Effect.provideService(SubagentHost, later), Effect.flip),
      ).toMatchObject({ reason: "idempotency-conflict" });
      for (const changed of [
        {
          encodedInput: { text: "different capture" },
          encodedParameters: { note: "corrected brief" },
        },
        { encodedInput: envelope.input, encodedParameters: { note: "different parameters" } },
      ])
        expect(
          yield* later
            .followUp({ worker, target, idempotencyKey: key, ...changed })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "idempotency-conflict" });
      h.deny("send");
      expect(
        yield* followUp.pipe(Effect.provideService(SubagentHost, later), Effect.flip),
      ).toMatchObject({ reason: "denied" });
      h.deny(undefined);
      expect(preparations).toBe(1);
      expect(h.deliveries.get(correction.message.messageId)!.envelope).toEqual(envelope);
      expect(h.submissions.get(replay.receipt!.submissionId)?.inputPayload).toEqual(envelope.input);
      expect(h.submissions.size).toBe(2);
      expect(
        yield* Subagent.followUp(
          declaration,
          worker,
          { note: "new correction" },
          {
            idempotencyKey: Schema.decodeSync(IdempotencyKey)("fresh-command"),
          },
        ).pipe(Effect.provideService(SubagentHost, later), Effect.flip),
      ).toEqual(new PreparationFailed());
      expect(h.deliveries.size).toBe(2);
    }),
  );

  // Equal-input regression: https://github.com/yielded-dev/agent/commit/3ab9045fc293d09a22801c7d881c4d89e562461a
  for (const [preparation, denied] of [
    ["changed", undefined],
    ["failed", undefined],
    ["equal", "caller"],
    ["equal", "policy"],
  ] as const)
    it.effect(
      `public follow-up reconciles concurrent retention with current authority (${preparation}, denied=${denied ?? "none"})`,
      () =>
        Effect.gen(function* () {
          class PreparationFailed extends Schema.TaggedError<PreparationFailed>()(
            "PreparationFailed",
            {},
          ) {}
          let denyPolicy = false;

          const h = yield* harness().pipe(
            Effect.provideService(WorkerPolicyResolver, {
              resolveSource: () => Effect.succeed(Option.none()),
              resolveTarget: () =>
                denyPolicy
                  ? WorkerError.make({ operation: "followUp", reason: "denied" })
                  : Effect.succeed(Option.none()),
            }),
          );

          const first = yield* h.host.start(request("original task"));
          const preparing = yield* Deferred.make<void>();
          const resume = yield* Deferred.make<void>();

          const declaration = Subagent.make("research", {
            target,
            parameters: Schema.Struct({ note: Schema.String }),
            failure: PreparationFailed,
            prepareInput: ({ note }) => Effect.succeed({ text: note }),
          });

          const worker = Schema.decodeSync(Subagent.Worker(declaration))(first.worker);
          const key = Schema.decodeSync(IdempotencyKey)("racing-correction");

          const loser = yield* Subagent.followUp(
            {
              ...declaration,
              prepareInput: () =>
                Effect.gen(function* () {
                  yield* Deferred.succeed(preparing, undefined);
                  yield* Deferred.await(resume);
                  if (preparation === "failed") return yield* new PreparationFailed();

                  return {
                    text:
                      preparation === "equal" ? "original correction" : "later correction capture",
                  };
                }),
            },
            worker,
            { note: "original correction" },
            { idempotencyKey: key },
          ).pipe(Effect.provideService(SubagentHost, h.host), Effect.result, Effect.forkChild);

          yield* Deferred.await(preparing);

          const winner = yield* Subagent.followUp(
            declaration,
            worker,
            { note: "original correction" },
            {
              idempotencyKey: key,
            },
          ).pipe(Effect.provideService(SubagentHost, h.host));

          const envelope = structuredClone(h.deliveries.get(winner.message.messageId)!.envelope);

          if (denied === "caller") h.deny("send");
          if (denied === "policy") denyPolicy = true;
          yield* Deferred.succeed(resume, undefined);
          const result = yield* Fiber.join(loser);

          if (denied !== undefined)
            expect(result).toMatchObject({ _tag: "Failure", failure: { reason: "denied" } });
          else expect(result).toMatchObject({ _tag: "Success", success: winner });
          expect(h.deliveries.get(winner.message.messageId)!.envelope).toEqual(envelope);
          expect(envelope.input).toEqual({ text: "original correction" });
          expect(h.deliveries.size).toBe(2);
          expect(h.submissions.size).toBe(2);
        }),
    );

  // Regression: https://github.com/yielded-dev/agent/commit/43882d187248665eaf7fd46950b3bc617edcb73d
  it.effect(
    "serializes source-aware active slots across raced starts, steering and idle reactivation",
    () =>
      Effect.gen(function* () {
        let limit = 1;

        const h = yield* harness({
          independentBudget: true,
          limits: { maxActiveWorkersPerSource: 2, maxPendingInputsPerWorker: 3 },
        }).pipe(
          Effect.provideService(WorkerConcurrencyResolver, {
            resolve: (request) =>
              Effect.sync(() => {
                expect(request.source.threadId).toBe(sourceId);
                expect(request.principal).toBe(principal);
                expect(request.sourceSubmission).toBeUndefined();

                return Option.some({ maxActiveWorkersPerSource: limit });
              }),
          }),
        );

        const start = (key: string) => h.host.start({ ...request(key), budgetScope: "worker-run" });

        const raced = yield* Effect.forEach(
          [start("slot-a"), start("slot-b")],
          (effect) => effect,
          { concurrency: "unbounded" },
        );

        expect(raced.map((result) => result.delivery.status).sort()).toEqual(["parked", "refused"]);
        const first = raced.find((result) => result.delivery.status === "parked")!;

        limit = 0;

        const follow = (key: string) =>
          h.host.followUp({
            worker: first.worker,
            target,
            idempotencyKey: Schema.decodeSync(IdempotencyKey)(key),
            encodedInput: { text: key },
            encodedParameters: { note: key },
          });

        const steering = yield* follow("active-steering");

        expect(yield* follow("active-steering")).toEqual(steering);
        expect((yield* start("blocked-zero")).delivery).toMatchObject({
          status: "refused",
          reason: "worker-capacity",
        });
        yield* h.settle(first.delivery.receipt!);
        // A queued/steering input still owns the slot after the first receipt settles.
        expect((yield* start("blocked-pending")).delivery).toMatchObject({
          status: "refused",
          reason: "worker-capacity",
        });
        yield* h.settle(steering.receipt!, "steered", { host: first.delivery.receipt! });
        expect(yield* follow("idle-blocked")).toMatchObject({
          status: "refused",
          reason: "worker-capacity",
        });
        limit = 1;
        const later = yield* follow("idle-later");

        expect(later.receipt!.threadId).toEqual(first.worker.threadId);
        expect(h.submissions.get(later.receipt!.submissionId)?.workerAdmission?.origin).toEqual(
          h.submissions.get(first.delivery.receipt!.submissionId)?.workerAdmission?.origin,
        );
        yield* h.settle(later.receipt!);
        limit = 1_000;
        yield* start("host-one");
        yield* start("host-two");
        expect((yield* start("host-three")).delivery).toMatchObject({
          status: "refused",
          reason: "worker-capacity",
        });
        for (const saved of [...h.submissions.values()].filter((row) => row.state !== "settled")) {
          yield* h.settle(
            Receipt.make({
              threadId: saved.threadId,
              submissionId: saved.submissionId,
              receiptId: saved.receiptId,
              queueSequence: saved.queueSequence,
            }),
          );
        }
        for (let index = 0; index < 12; index++) {
          const next = yield* start(`sequential-${index}`);

          expect(next.delivery.receipt).not.toBeNull();
          yield* h.settle(next.delivery.receipt!);
        }
        for (let index = 0; index < 12; index++) {
          const next = yield* follow(`sequential-input-${index}`);

          expect(next.receipt).not.toBeNull();
          yield* h.settle(next.receipt!);
        }
        expect((yield* start("slot-a")).worker).toEqual(raced[0].worker);
        expect(yield* follow("active-steering")).toEqual(steering);
      }),
  );

  it.effect("closes only stopped, never-admitted reserved input and replays its refusal", () =>
    Effect.gen(function* () {
      const h = yield* harness();

      h.rejectAdmission(true);
      const command = request("unadmitted");
      const start = yield* h.host.start(command);

      expect(start.delivery.status).toBe("refused");

      const input = h.logs
        .get(sourceId)!
        .find(({ record: { payload } }) => payload._tag === "WorkerInputRequested");

      if (input?.record.payload._tag !== "WorkerInputRequested")
        return yield* Effect.die("Missing reservation");
      expect(yield* h.runtime.repairInput(sourceId, input.record.payload)).toBe(false);
      yield* h.host.stop({
        worker: start.worker,
        target,
        idempotencyKey: IdempotencyKey.make("stop-unadmitted"),
      });
      expect(h.logs.has(start.worker.threadId)).toBe(false);
      expect(yield* h.runtime.repairInput(sourceId, input.record.payload)).toBe(true);
      expect(yield* h.runtime.repairInput(sourceId, input.record.payload)).toBe(true);
      expect(
        h.logs
          .get(sourceId)!
          .filter(({ record: { payload } }) => payload._tag === "WorkerInputRefused"),
      ).toHaveLength(1);
      expect(yield* h.host.start(command)).toEqual(start);
      expect(h.submissions.size).toBe(0);

      const pending = yield* harness();

      pending.fail("worker:before-source-append");
      const retained = yield* pending.host.start(request("retained-before-reservation"));

      expect(retained.delivery.receipt).toBeNull();
      expect(
        pending.logs
          .get(sourceId)!
          .some(({ record }) => record.payload._tag === "WorkerInputRequested"),
      ).toBe(false);
      pending.fail(undefined);
      yield* pending.host.stop({
        worker: retained.worker,
        target,
        idempotencyKey: IdempotencyKey.make("stop-retained"),
      });

      const stop = pending.logs
        .get(sourceId)!
        .find(({ record }) => record.payload._tag === "WorkerStopRequested")!;

      if (stop.record.payload._tag !== "WorkerStopRequested")
        return yield* Effect.die("Missing retained stop");
      expect(
        yield* pending.runtime.repairStop(sourceId, stop.record.recordId, stop.record.payload),
      ).toBe(true);
      expect(pending.logs.has(retained.worker.threadId)).toBe(false);
    }),
  );

  // Regression: https://github.com/yielded-dev/agent/commit/43882d187248665eaf7fd46950b3bc617edcb73d
  it.effect(
    "revalidates captured authority across a reserved admission retry and freezes later worker policy",
    () =>
      Effect.gen(function* () {
        const captured = AgentPolicy.make({
          ...policy,
          maxTurns: 7,
          maxToolCalls: 6,
          maxDuration: "4 seconds",
        });

        let unavailable = false;
        let replaceRetained = false;
        let initialCalls = 0;

        const h = yield* harness({ independentBudget: true }).pipe(
          Effect.provideService(WorkerPolicyResolver, {
            resolveSource: () => Effect.succeed(Option.none()),
            resolveTarget: (input) =>
              Effect.gen(function* () {
                if (unavailable)
                  return yield* WorkerError.make({ operation: "start", reason: "unavailable" });
                if (input._tag === "RetainedWorker")
                  return Option.some(replaceRetained ? policy : input.origin.policy);
                initialCalls++;
                expect(input.input).toEqual({ text: "captured" });
                expect(input.definition).toBe(target);

                return Option.some(captured);
              }),
          }),
        );

        const base = request("captured");

        const start: StartWorkerRequest = {
          ...base,
          policy: captured,
          budgetScope: "worker-run",
          budget: {
            caps: SubagentDelegationCaps.make({
              maxTotalChildInvocations: 1,
              maxConcurrentChildren: 1,
              maxTurns: 8,
              maxToolCalls: 7,
              maxDurationMillis: 5_000,
            }),
            allocation: SubagentReservationAmounts.make({
              ...base.budget.allocation,
              turns: 8,
              toolCalls: 7,
              durationMillis: 5_000,
            }),
            descendantInvocations: 1,
          },
        };

        h.fail("worker:after-source-append");
        expect((yield* h.host.start(start)).delivery).toMatchObject({
          status: "pending",
          reason: "storage",
        });
        h.fail(undefined);
        const delivery = [...h.deliveries.values()][0]!;
        const metadata = delivery.envelope.workerAdmission!;

        const admissionOptions = {
          threadId: delivery.envelope.threadId,
          definitions: delivery.envelope.definitions,
          principal,
          idempotencyKey: delivery.envelope.admissionKey,
        };

        unavailable = true;
        expect(
          (yield* h.runtime
            .validateAdmission(
              metadata,
              admissionOptions,
              target.id,
              delivery.envelope.inputDigest,
              delivery.envelope.input,
            )
            .pipe(Effect.flip)).reason,
        ).toBe("unavailable");
        expect(h.submissions.size).toBe(0);
        unavailable = false;
        yield* TestClock.adjust("1 second");
        const started = yield* h.host.start(start);

        expect(initialCalls).toBeGreaterThanOrEqual(3);

        const origin = h.submissions.get(started.delivery.receipt!.submissionId)!.workerAdmission!
          .origin;

        expect(origin.policy).toEqual(captured);
        expect(
          h.logs
            .get(sourceId)!
            .filter(({ record }) => record.payload._tag === "WorkerInputRequested"),
        ).toHaveLength(1);
        yield* h.settle(started.delivery.receipt!);

        const followup = {
          worker: started.worker,
          target,
          idempotencyKey: Schema.decodeSync(IdempotencyKey)("later"),
          encodedInput: { text: "different later input" },
          encodedParameters: { note: "later" },
        };

        replaceRetained = true;
        expect((yield* h.host.followUp(followup).pipe(Effect.flip)).reason).toBe("worker-mismatch");
        expect(h.deliveries.size).toBe(1);
        replaceRetained = false;
        const next = yield* h.host.followUp(followup);

        expect(h.submissions.get(next.receipt!.submissionId)!.workerAdmission!.origin).toEqual(
          origin,
        );
      }),
  );

  it.effect("worker summaries retry a selected-read failure after a concurrent append", () =>
    Effect.gen(function* () {
      let onRead: Effect.Effect<void, ThreadStoreError> = Effect.void;

      const h = yield* harness({
        beforeRead: (request) =>
          Effect.suspend(() => {
            if (!("selection" in request) || request.selection._tag !== "WorkerExecution")
              return Effect.void;

            return onRead;
          }),
      });

      const first = yield* h.host.start(request("concurrent-summary"));
      const receipt = first.delivery.receipt;

      if (receipt === null) return yield* Effect.die("Expected an admitted worker input");
      const runId = Schema.decodeSync(RunId)(`run:${receipt.submissionId}`);

      onRead = Effect.sync(() => {
        onRead = Effect.void;
        h.push(
          first.worker.threadId,
          UserInputRecorded.make({
            submissionId: receipt.submissionId,
            kind: "user",
            runId,
            input: { text: "concurrent-summary" },
          }),
          "concurrent-input",
        );
      }).pipe(
        Effect.andThen(
          ThreadStoreError.make({ operation: "selected read", message: "Canonical tail changed" }),
        ),
      );

      const summary = yield* h.host.summary({ worker: first.worker, target });

      expect(summary.appliedInput).toMatchObject({
        receipt,
        messageId: first.delivery.message.messageId,
        runId,
      });
      expect(summary.watermark.canonicalSequence).toBe(summary.appliedInput?.sequence);
    }),
  );

  for (const watermark of ["unchanged", "unavailable"] as const)
    it.effect(
      `worker summaries preserve a selected-read error when the watermark is ${watermark}`,
      () =>
        Effect.gen(function* () {
          const readFailure = ThreadStoreError.make({
            operation: "selected read",
            message: "Canonical tail changed",
          });

          let onRead: Effect.Effect<void, ThreadStoreError> = readFailure;

          const h = yield* harness({
            beforeRead: (request) =>
              Effect.suspend(() => {
                if (!("selection" in request) || request.selection._tag !== "WorkerExecution")
                  return Effect.void;

                return onRead;
              }),
          });

          const first = yield* h.host.start(request("failed-summary"));

          if (watermark === "unavailable")
            onRead = Effect.sync(() => h.logs.delete(first.worker.threadId)).pipe(
              Effect.andThen(readFailure),
            );

          const failure = yield* h.host
            .list({ delegationId: first.worker.delegationId, target, limit: 1 })
            .pipe(Effect.flip);

          expect(failure).toMatchObject({ operation: "list", reason: "storage" });
          expect(failure.cause).toBe(readFailure);
        }),
    );

  it.effect(
    "worker summaries choose the latest destination receipt when source intents arrive out of order",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();
        const first = yield* h.host.start(request("initial"));

        yield* h.settle(first.delivery.receipt!);

        const followUp = (key: string) =>
          h.host.followUp({
            worker: first.worker,
            target,
            idempotencyKey: Schema.decodeSync(IdempotencyKey)(key),
            encodedInput: { text: key },
            encodedParameters: { note: key },
          });

        h.fail("worker:after-source-append");
        expect(yield* followUp("earlier-intent")).toMatchObject({
          status: "pending",
          reason: "storage",
        });
        h.fail(undefined);
        expect(yield* h.host.summary({ worker: first.worker, target })).toMatchObject({
          worker: first.worker,
          latestReceipt: first.delivery.receipt!,
          state: "starting",
        });
        const earlierReceipt = yield* followUp("later-intent");

        // The interrupted delivery's expiring claim must elapse before its admission replay.
        yield* TestClock.adjust("31 seconds");
        const latestReceipt = yield* followUp("earlier-intent");

        expect(latestReceipt.receipt!.queueSequence).toBeGreaterThan(
          earlierReceipt.receipt!.queueSequence,
        );
        expect(
          h.logs
            .get(sourceId)!
            .flatMap(({ record }) =>
              record.payload._tag === "WorkerInputRequested"
                ? [record.payload.admission.parameters]
                : [],
            ),
        ).toEqual([{ note: "initial" }, { note: "earlier-intent" }, { note: "later-intent" }]);

        const active = {
          worker: first.worker,
          latestReceipt: latestReceipt.receipt,
          state: "active",
        };

        expect(yield* h.host.summary({ worker: first.worker, target })).toMatchObject(active);
        expect(
          (yield* h.host.list({ delegationId: first.worker.delegationId, target, limit: 10 }))
            .items,
        ).toMatchObject([active]);
        yield* h.settle(earlierReceipt.receipt!);
        yield* h.settle(latestReceipt.receipt!);
        expect(yield* h.host.summary({ worker: first.worker, target })).toMatchObject({
          ...active,
          state: "idle",
        });
      }),
  );

  it.effect("bounds report preparation and finalizes its resources on timeout", () =>
    Effect.gen(function* () {
      let finalized = 0;
      const entered = yield* Deferred.make<void>();

      const h = yield* harness({
        sourceReports: [
          reportWith(() =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(
                Effect.sync(() => {
                  finalized++;
                }),
              ),
            ),
          ),
        ],
      });

      const first = yield* h.host.start(request("timeout"));
      const fiber = yield* h.settle(first.delivery.receipt!).pipe(Effect.forkChild);

      yield* Deferred.await(entered);
      yield* TestClock.adjust("5 seconds");
      yield* Fiber.join(fiber);
      expect(finalized).toBe(1);
      expect(
        h.logs
          .get(first.worker.threadId)!
          .find(({ record }) => record.payload._tag === "WorkerReportRefused")?.record.payload,
      ).toMatchObject({ reason: "timeout" });
    }),
  );

  it.effect("interrupted preparation leaves repairable work without a false refusal", () =>
    Effect.gen(function* () {
      let block = true;
      let finalized = 0;
      const entered = yield* Deferred.make<void>();

      const h = yield* harness({
        sourceReports: [
          reportWith((report) =>
            (block
              ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
              : standardReport.prepare(report)
            ).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  finalized++;
                }),
              ),
            ),
          ),
        ],
      });

      const first = yield* h.host.start(request("interrupted"));
      const fiber = yield* h.settle(first.delivery.receipt!).pipe(Effect.forkChild);

      yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);
      block = false;
      yield* h.runtime.completeInput(h.submissions.get(first.delivery.receipt!.submissionId)!);
      expect(finalized).toBe(2);
      expect(
        h.logs
          .get(first.worker.threadId)!
          .filter(({ record }) => record.payload._tag === "WorkerReportRefused"),
      ).toHaveLength(0);
      expect(
        h.logs
          .get(first.worker.threadId)!
          .filter(({ record }) => record.payload._tag === "WorkerReportPrepared"),
      ).toHaveLength(1);
    }),
  );

  it.effect("validates report authority against the receiving worker's original ancestor", () =>
    Effect.gen(function* () {
      const scoutId = Schema.decodeSync(DelegationId)("scout");

      const scoutReport = automaticReporting(
        Subagent.make("scout", {
          target,
          parameters: reportDeclaration.parameters,
          prepareInput: reportDeclaration.prepareInput,
        }),
      );

      const h = yield* harness({
        limits: {
          maxPendingUpdateInputsPerWorker: 1,
        },
        targetReports: [
          {
            ...scoutReport,
            prepare: (report) =>
              scoutReport
                .prepare(report)
                .pipe(
                  Effect.mapError(() =>
                    WorkerReportPreparationFailure.make({ stage: "projection" }),
                  ),
                ),
          },
        ],
      });

      const initial = request("report-builder");
      const grant = SubagentGrant.make({ allowedToolNames: [], maxDepth: 2 });

      const builder = yield* h.host.start({
        ...initial,
        encodedGrant: grant,
        budget: {
          ...initial.budget,
          descendantInvocations: 1,
          caps: SubagentDelegationCaps.make({ ...initial.budget.caps, maxConcurrentChildren: 2 }),
          allocation: SubagentReservationAmounts.make({
            ...initial.budget.allocation,
            turns: 3,
            toolCalls: 3,
            durationMillis: 1_500,
            resultBytes: 1_280,
          }),
        },
      });

      const childPolicy = AgentPolicy.make({
        ...policy,
        maxTurns: 1,
        maxToolCalls: 1,
        maxDuration: "500 millis",
        toolConcurrency: 1,
        toolResultBounds: ToolResultBounds.make({ maxBytes: 256 }),
      });

      const fundingOwner = builder.delivery.receipt!.submissionId;
      const joinedId = SubmissionId.make("joined-builder-input");

      h.submissions.set(
        joinedId,
        SubmissionSnapshot.make({
          ...h.submissions.get(fundingOwner)!,
          submissionId: joinedId,
          state: "joined",
        }),
      );
      h.fundingOwners.set(joinedId, fundingOwner);

      const nested = h.runtime.facet(
        {
          source: {
            _tag: "tool",
            agentId: target.id,
            threadId: builder.worker.threadId,
            runId: Schema.decodeSync(RunId)("builder-run"),
            toolCallId: Schema.decodeSync(ToolCallId)("scout-call"),
          },
          policy,
          depth: 1,
          grant,
        },
        principal,
        joinedId,
      );

      const scout = yield* nested.start({
        ...request("report-scout"),
        delegationId: scoutId,
        policy: childPolicy,
        encodedGrant: grant,
        budget: {
          caps: SubagentDelegationCaps.make({
            maxTotalChildInvocations: 1,
            maxConcurrentChildren: 1,
            maxTurns: 1,
            maxToolCalls: 1,
            maxDurationMillis: 500,
            maxResultBytes: 256,
          }),
          allocation: SubagentReservationAmounts.make({
            turns: 1,
            toolCalls: 1,
            durationMillis: 500,
            resultBytes: 256,
            inputTokens: 0,
            outputTokens: 0,
            costMicrousd: 0,
          }),
        },
      });

      expect(
        h.submissions.get(scout.delivery.receipt!.submissionId)?.workerAdmission
          ?.sourceSubmissionId,
      ).toBe(fundingOwner);
      expect(
        h.logs
          .get(builder.worker.threadId)!
          .find(({ record }) => record.payload._tag === "SubtreeBudgetReserved")?.record.payload,
      ).toMatchObject({ sourceSubmissionId: fundingOwner });

      {
        const submission = h.submissions.get(scout.delivery.receipt!.submissionId)!;
        const runId = Schema.decodeSync(RunId)(`run:${submission.submissionId}`);

        yield* h.startRun(scout.delivery.receipt!);
        yield* h.updates.emit({
          submission,
          runId,
          producerEpoch: Schema.decodeSync(ProducerEpoch)(0),
          definitions,
          updateId: Schema.decodeSync(IdempotencyKey)("scout-finding"),
          value: { finding: "area concern" },
        });

        const update = [...h.deliveries.values()].find((row) =>
          Schema.is(WorkerUpdate)(row.envelope.messageAdmission),
        )!;

        const metadata = update.envelope.workerAdmission!;

        expect(metadata.reportKind).toBe("update");

        const options = {
          threadId: update.envelope.threadId,
          principal: update.envelope.deliveryPrincipal,
          idempotencyKey: update.envelope.admissionKey,
          definitions: update.envelope.definitions,
          workerAdmission: metadata,
          messageAdmission: update.envelope.messageAdmission,
        };

        expect(
          yield* h.runtime
            .validateAdmission(
              metadata,
              { ...options, messageAdmission: undefined },
              update.envelope.agentId,
              update.envelope.inputDigest,
              update.envelope.input,
            )
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "denied" });
        yield* h.runtime.validateAdmission(
          metadata,
          options,
          update.envelope.agentId,
          update.envelope.inputDigest,
          update.envelope.input,
        );
      }

      yield* h.settle(scout.delivery.receipt!);

      const report = [...h.deliveries.values()].find(
        (row) =>
          row.key.ownerThreadId === scout.worker.threadId &&
          !Schema.is(WorkerUpdate)(row.envelope.messageAdmission),
      )!;

      expect(report.envelope.threadId).toBe(builder.worker.threadId);
      expect(report.envelope.workerAdmission?.parameters).toEqual({
        note: "report-builder",
      });
      expect(report.envelope.workerAdmission?.origin).toEqual(
        h.submissions.get(builder.delivery.receipt!.submissionId)?.workerAdmission?.origin,
      );
      expect(report.envelope.workerAdmission?.sourceSubmissionId).toBeUndefined();
      {
        expect(report.envelope.input).toEqual({ text: "report-builder" });
        expect(Schema.is(WorkerCompletion)(report.envelope.messageAdmission)).toBe(true);
      }
      const metadata = report.envelope.workerAdmission!;

      const options = {
        threadId: report.envelope.threadId,
        principal: report.envelope.deliveryPrincipal,
        idempotencyKey: report.envelope.admissionKey,
        definitions: report.envelope.definitions,
        workerAdmission: metadata,
      };

      {
        const message = yield* Schema.decodeUnknownEffect(WorkerCompletion)(
          report.envelope.messageAdmission,
        );

        const validate = (
          completion = message,
          admission: Parameters<typeof h.runtime.validateCompletion>[1] = options,
          inputDigest = report.envelope.inputDigest,
        ) =>
          h.runtime.validateCompletion(completion, admission, report.envelope.agentId, inputDigest);

        expect(yield* validate()).toEqual(message);
        for (const changed of [
          { ...options, threadId: sourceId },
          { ...options, principal: Schema.decodeSync(Principal)("other") },
          { ...options, idempotencyKey: Schema.decodeSync(IdempotencyKey)("other") },
          { ...options, workerAdmission: undefined },
          {
            ...options,
            definitions: DefinitionDigests.make({
              ...options.definitions,
              agent: Schema.decodeSync(Digest)("b".repeat(64)),
            }),
          },
        ])
          expect((yield* validate(message, changed).pipe(Effect.flip)).reason).toBe("denied");
        expect(
          (yield* validate({ ...message, budgetExhausted: true }).pipe(Effect.flip)).reason,
        ).toBe("denied");
        expect(
          (yield* validate({
            ...message,
            report: { ...message.report, runId: Schema.decodeSync(RunId)("unprepared-run") },
          }).pipe(Effect.flip)).reason,
        ).toBe("denied");
        expect(
          (yield* validate(message, options, Schema.decodeSync(Digest)("b".repeat(64))).pipe(
            Effect.flip,
          )).reason,
        ).toBe("denied");
        h.deny("report");
        expect((yield* validate().pipe(Effect.flip)).reason).toBe("denied");
        h.deny(undefined);
      }
    }),
  );
});
