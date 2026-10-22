import {
  decodePortRequest,
  encodePortResponse,
  LedgerLookupResult,
  PortFailed,
  PortProtocolError,
  PortSucceeded,
  type PortRequest,
  type PortResponse,
} from "@yielded/agent-storage-cloudflare/port-protocol";
import {
  IntegrityReport,
  ObligationReport,
  ObligationThresholds,
  RecoveryExplanation,
  RetryCommand,
  RetryRefused,
} from "@yielded/agent/admin";
import { DigestError } from "@yielded/agent/digest";
import {
  ApprovalSuspensionError,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  RecoveryReport,
  RecoveryBlocked,
  type DurableSubmitAgent,
} from "@yielded/agent/durable-agent-runtime";
import { DurableRuntimeFailpointError } from "@yielded/agent/durable-failpoint";
import { type AgentId, type ThreadId } from "@yielded/agent/identifiers";
import { SubmissionId } from "@yielded/agent/identifiers";
import {
  OperationAuthorizationRequest,
  OperationAuthorizer,
  OperationDenied,
} from "@yielded/agent/operation-authorizer";
import { PersistedJson } from "@yielded/agent/records";
import { RunJournalError } from "@yielded/agent/run-journal";
import {
  AdmissionPolicyError,
  LedgerError,
  OwnershipLost,
  SettlementConflict,
  SubmissionLedger,
  SubmissionLookupByKey,
} from "@yielded/agent/submission-ledger";
import {
  AppendConflict,
  ThreadNotMaterialized,
  ThreadRead,
  ThreadStore,
  ThreadStoreError,
} from "@yielded/agent/thread-store";
import { WorkDiscoveryUnavailable } from "@yielded/agent/thread-work";
import { WakeScheduler } from "@yielded/agent/wake-scheduler";
import {
  DurableObject as EffectCfDurableObject,
  DurableObjectAlarm,
  DurableObjectState as EffectCfDurableObjectState,
  WorkerEnvironment,
} from "effect-cf";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import {
  ThreadMaintenance,
  ThreadMaintenanceAlarm,
  DurableAlarmError,
  DurableAlarmService,
  ThreadMutationGate,
  publishCommitted,
  type MaintenancePassFailure,
} from "./Alarm.ts";
import {
  ThreadObjectIdentity,
  ThreadObjectPlacement,
  DurableObjectContext,
  ThreadObjectNamespace,
  threadNamespaceFromEnv,
  type CloudflareBindingError,
  type ThreadObjectRpc,
} from "./CloudflareBindings.ts";
import { AdmissionLimitExceeded, CloudflareDurableRuntimeConfig } from "./CloudflareConfig.ts";
import { boundHostDiagnostic, HostProtocolError } from "./internal/host-protocol-error.ts";
import {
  AbortRecorded,
  ApprovalRecorded,
  HostFailed,
  ObservedPage,
  ProgressObserved,
  ProgressCancelled,
  SettlementReached,
  SettlementRecordReached,
  SubmissionStatusResponse,
  SubmitSucceeded,
  UnknownResolutionRecorded,
  encodeHostResponse,
  hostOperations,
  type AwaitSettlementRequest,
  type CancelProgressRequest,
  type HostFailure,
  type HostOperation,
  type HostResponse,
  type HostSuccess,
  type SubmitRequest,
} from "./internal/host-protocol.ts";
import {
  layerConfig,
  ThreadObjectPorts,
  type CloudflareDurableRuntimeInitializationError,
  type CloudflareDurableRuntimeOptions,
  type CloudflareDurableRuntimeServices,
  type CloudflareBootstrapServices,
} from "./internal/layers.ts";
import { Request as WatchTextRequest } from "./internal/live-text-protocol.ts";
import { LiveTextHub } from "./internal/live-text.ts";
import { ProgressWaitRegistry } from "./internal/progress-wait.ts";

export {
  layer,
  layerConfig,
  layerHostConfig,
  layerInHost,
  ThreadObjectPorts,
  type ThreadPublicationOptions as PublicationOptions,
  type ThreadHostMutation,
  type CloudflareDurableRuntimeOptions as RuntimeOptions,
  type CloudflareDurableRuntimeServices as Services,
  type CloudflareDurableRuntimeInitializationError as InitializationError,
  type CloudflareBootstrapServices as BootstrapServices,
} from "./internal/layers.ts";

/**
 * `ThreadObject.make(application, options)` — the Thread Durable Object
 * (plan §1.4,
 * D-P6-1): a factory returning a class that applications export from their Worker entry.
 * One SQLite-backed Object per Thread is the serialized owner (durability §6); the
 * Object never runs `runResolvedWorker`'s infinite loop — each ingress event or alarm runs
 * ONE bounded maintenance event with selected-Thread recovery and old cleanup, and the persisted alarm
 * (the single multiplexed slot, D-P6-2) finishes accepted work across evictions WITHOUT any
 * incoming request.
 * `Services` exposes the same owner `SqlClient` used by the Thread stores. Compose optional
 * local repositories after `ThreadObject.layer`; never acquire another independently locked
 * SQL client for the same Object. Exposing the client installs no additional storage schemas.
 *
 * Constructor gate (`blockConcurrencyWhile`) is LOCAL-ONLY: schema migration and the
 * exact-version check, configuration decode, and the defensive ensure-alarm half of the
 * alarm invariant. It deliberately does NOT run the recovery pass: parent recovery can
 * require child-Object reads and vice versa, and two Objects blocked in constructor gates
 * awaiting each other's RPC would deadlock (plan §1.4). Instead every pass runs
 * `runRecovery({ threadId })` BEFORE that Thread's claim; old recovery cannot gate fresh dispatch.
 */

type ApplicationAlarmRegistration<ApplicationServices, EventServices, AlarmServices, AlarmError> =
  DurableObjectAlarm.AlarmRegistration<
    AlarmServices,
    | RuntimeServices
    | ApplicationServices
    | EventServices
    | NoInfer<AlarmServices>
    | EffectCfDurableObjectState.DurableObjectState
    | WorkerEnvironment
    | Scope.Scope,
    AlarmError
  >;

/** Construction options for one deployed Thread Object class. */
export type Options<
  ApplicationServices = never,
  EventServices = never,
  EventLayerError = never,
  AlarmServices = never,
  AlarmError = never,
> = BaseOptions<ApplicationServices, EventServices, EventLayerError, AlarmServices> &
  ([AlarmServices] extends [never]
    ? {
        readonly alarms?: ApplicationAlarmRegistration<
          ApplicationServices,
          EventServices,
          AlarmServices,
          AlarmError
        >;
      }
    : {
        readonly alarms: ApplicationAlarmRegistration<
          ApplicationServices,
          EventServices,
          AlarmServices,
          AlarmError
        >;
      });

interface BaseOptions<
  ApplicationServices,
  EventServices,
  EventLayerError,
  AlarmServices,
> extends CloudflareDurableRuntimeOptions {
  /** Accept transient native RPC tracing through effect-cf; disabled by default. */
  readonly rpcTracing?: boolean;
  /**
   * Name of the Worker `env` binding carrying THIS class's `DurableObjectNamespace` — the
   * Object's route back to sibling Thread Objects for the WP2 cross-Object port calls
   * and remote wakes (DEPLOY-010: the binding enters through a Layer, never ambiently).
   */
  readonly namespaceBinding: string;
  /** Acquired and finalized per native event, with access to the complete application runtime. */
  readonly eventLayer?: Layer.Layer<
    EventServices,
    EventLayerError,
    | RuntimeServices
    | ApplicationServices
    | NoInfer<AlarmServices>
    | EffectCfDurableObjectState.DurableObjectState
    | WorkerEnvironment
  >;
}

type EndpointServices =
  | CloudflareDurableRuntimeServices
  | CloudflareBootstrapServices
  | DurableObjectContext;
type RuntimeServices = EndpointServices | ThreadObjectNamespace;
type ThreadObjectInitializationError =
  | CloudflareDurableRuntimeInitializationError
  | CloudflareBindingError
  | MaintenancePassFailure;

/** Classify only a decoded port request so new protocol members cannot bypass pre-arming. */
const isMutatingPortRequest = (request: PortRequest): boolean => {
  switch (request._tag) {
    case "WorkerAdmit":
    case "LedgerAdmit":
    case "LedgerMarkReady":
    case "LedgerStopWorker":
    case "LedgerRequestAbort":
    case "LedgerRecordChildSettled":
    case "StoreMaterialize":
    case "StoreAppend":
    case "StoreWorkRebuild":
    case "StoreRangeSeal":
    case "StoreRangeArchive":
    case "SettlementPublish":
    case "MessageDeliveryComplete":
      return true;
    case "LedgerInspectWorker":
    case "LedgerLookup":
    case "LedgerResolveAdmission":
    case "LedgerFundingOwner":
    case "StoreReadPage":
    case "StoreReadPrompt":
    case "StoreReadExecution":
    case "StoreWorkPage":
    case "StoreInspectTail":
    case "StoreReadIdentity":
    case "StoreCountPeerMessages":
    case "StoreExport":
    case "StoreRangePage":
    case "StoreRangeVerify":
    case "StoreVerification":
    case "StoreWorkerCapacity":
    case "MessageDeliveryList":
      return false;
  }
  request satisfies never;

  return false;
};

/** The literal encoded `PortFailed(PortProtocolError)` fallback (same shape as WP2's). */
const encodedPortProtocolFailure = (message: string): unknown => ({
  _tag: "PortFailed",
  failure: { _tag: "PortProtocolError", message: boundHostDiagnostic(message) },
});

const protocolFailure = (context: string) => (error: { readonly message: string }) =>
  HostProtocolError.make({
    message: boundHostDiagnostic(`${context}: ${error.message}`),
  });

/** An invalid outgoing envelope still fulfills RPC with the protocol's typed failure. */
const responseEncodingFailure = (error: Schema.SchemaError) =>
  Effect.succeed<unknown>({
    _tag: "HostFailed",
    failure: {
      _tag: "HostProtocolError",
      message: boundHostDiagnostic(`The host response could not be encoded: ${error.message}`),
    },
  });

const encodeResponse = (response: HostResponse): Effect.Effect<unknown> =>
  encodeHostResponse(response).pipe(Effect.catch(responseEncodingFailure));

/** Decode once at ingress, then encode the operation's success or typed failure. */
const endpoint = <Request, Success extends HostSuccess, Failure extends HostFailure>(
  contract: HostOperation<Request, Success, Failure>,
  handle: (
    request: NoInfer<Request>,
  ) => Effect.Effect<NoInfer<Success>, NoInfer<Failure> | HostProtocolError, EndpointServices>,
) =>
  Effect.fnUntraced(function* (encoded: unknown) {
    const response = yield* contract.decodeRequest(encoded).pipe(
      Effect.mapError(protocolFailure(`The ${contract.name} request could not be decoded`)),
      Effect.flatMap(handle),
      Effect.catch((failure) => Effect.succeed({ _tag: "HostFailed" as const, failure })),
    );

    return yield* contract.encodeResponse(response).pipe(Effect.catch(responseEncodingFailure));
  });

const utf8Bytes = (value: PersistedJson): number =>
  new TextEncoder().encode(JSON.stringify(value)).length;

/**
 * The admission-limits gate, BEFORE `runtime.submit` touches the ledger (exit gate
 * "resource limits are checked before admission"; DEPLOY-007). A replayed idempotency key is
 * exempt: its accepted-work obligation already exists, and returning the original Receipt
 * consumes no new quota. Refusals are typed `AdmissionLimitExceeded` and nothing is written.
 */
const gateAdmissionLimits = Effect.fnUntraced(function* (
  threadId: ThreadId,
  request: {
    readonly principal: SubmissionLookupByKey["principal"];
    readonly idempotencyKey: SubmissionLookupByKey["idempotencyKey"];
    readonly inputPayload: PersistedJson;
  },
) {
  const config = yield* CloudflareDurableRuntimeConfig;
  const ledger = yield* SubmissionLedger;
  const { ctx } = yield* DurableObjectContext;

  // Admission checks the key durably. The host needs an extra lookup only to
  // exempt a retained receipt when a current limit would otherwise refuse it.
  const refuseFresh = (error: AdmissionLimitExceeded) =>
    ledger
      .lookup(
        SubmissionLookupByKey.make({
          threadId,
          principal: request.principal,
          idempotencyKey: request.idempotencyKey,
        }),
      )
      .pipe(
        Effect.flatMap((existing) => (Option.isSome(existing) ? Effect.void : Effect.fail(error))),
      );

  const inputBytes = utf8Bytes(request.inputPayload);

  if (inputBytes > config.limits.maxInputBytes) {
    return yield* refuseFresh(
      AdmissionLimitExceeded.make({
        limit: "input-bytes",
        actual: inputBytes,
        maximum: config.limits.maxInputBytes,
      }),
    );
  }

  const queueDepth = yield* Stream.runFold(
    ledger.scanNonterminal,
    () => 0,
    (depth, submission) => depth + (submission.threadId === threadId ? 1 : 0),
  );

  if (queueDepth >= config.limits.maxQueueDepthPerLane) {
    return yield* refuseFresh(
      AdmissionLimitExceeded.make({
        limit: "queue-depth",
        actual: queueDepth,
        maximum: config.limits.maxQueueDepthPerLane,
      }),
    );
  }

  const databaseBytes = yield* Effect.sync(() => ctx.storage.sql.databaseSize);

  if (databaseBytes > config.limits.maxDatabaseBytes) {
    return yield* refuseFresh(
      AdmissionLimitExceeded.make({
        limit: "database-bytes",
        actual: databaseBytes,
        maximum: config.limits.maxDatabaseBytes,
      }),
    );
  }
});

/**
 * The submit-capable projection of an Agent Binding on the OBJECT side: the input arrived
 * already encoded through the real input schema on the Worker side (`client.ts`), so the
 * Object admits the canonical `PersistedJson` payload as-is; the resolved Binding re-derives
 * everything else from the stored `(agentId, agentDigests)` at claim time (SUB-023).
 */
const passthroughSubmitAgent = (agentId: AgentId): DurableSubmitAgent<typeof PersistedJson> => ({
  definition: {
    id: agentId,
    input: PersistedJson,
  },
});

/** A physical owner may hold other Threads; an addressed request cannot act on their IDs. */
const lookupAddressedSubmission = Effect.fnUntraced(function* (submissionId: SubmissionId) {
  const { threadId } = yield* ThreadObjectIdentity;
  const ports = yield* ThreadObjectPorts;
  const submission = yield* ports.lookupSubmission(submissionId);

  if (Option.isSome(submission) && submission.value.threadId !== threadId)
    return yield* HostProtocolError.make({ message: "The Submission belongs to another Thread" });

  return submission;
});

const requireSubmissionThread = Effect.fnUntraced(function* (submissionId: SubmissionId) {
  const submission = yield* lookupAddressedSubmission(submissionId);

  if (Option.isNone(submission))
    return yield* LedgerError.make({
      operation: "addressed Submission lookup",
      message: "The addressed Thread has no such Submission",
    });
});

const requireReceiptThread = Effect.fnUntraced(function* (threadId: ThreadId) {
  const identity = yield* ThreadObjectIdentity;

  if (threadId !== identity.threadId)
    return yield* HostProtocolError.make({ message: "The Receipt belongs to another Thread" });
});

const requirePortThread = (request: PortRequest) => {
  switch (request._tag) {
    case "SettlementPublish":
      return requireReceiptThread(request.request.append.threadId).pipe(
        Effect.andThen(requireSubmissionThread(request.request.submissionId)),
      );
    case "MessageDeliveryComplete":
      return requireReceiptThread(request.key.ownerThreadId);
    case "MessageDeliveryList":
      return requireReceiptThread(request.request.ownerThreadId);
    case "LedgerLookup":
      return request.request._tag === "SubmissionLookupById"
        ? lookupAddressedSubmission(request.request.submissionId).pipe(Effect.asVoid)
        : requireReceiptThread(request.request.threadId);
    case "LedgerMarkReady":
    case "LedgerRequestAbort":
      return requireSubmissionThread(request.request.submissionId);
    case "LedgerRecordChildSettled":
      return requireSubmissionThread(request.request.parentSubmissionId);
    case "LedgerFundingOwner":
      return requireReceiptThread(request.request.threadId).pipe(
        Effect.andThen(requireSubmissionThread(request.request.submissionId)),
      );
    case "LedgerInspectWorker":
    case "LedgerStopWorker":
    case "WorkerAdmit":
    case "LedgerAdmit":
    case "LedgerResolveAdmission":
    case "StoreMaterialize":
    case "StoreAppend":
    case "StoreReadPage":
    case "StoreReadPrompt":
    case "StoreReadExecution":
    case "StoreInspectTail":
    case "StoreReadIdentity":
    case "StoreCountPeerMessages":
    case "StoreExport":
    case "StoreWorkPage":
    case "StoreWorkRebuild":
    case "StoreRangePage":
    case "StoreRangeSeal":
    case "StoreRangeArchive":
    case "StoreRangeVerify":
    case "StoreVerification":
    case "StoreWorkerCapacity":
      return requireReceiptThread(request.request.threadId);
  }
  request satisfies never;
};

/**
 * Admit an already Schema-decoded request to a logical Thread in this physical owner.
 * Custom hosts validate local placement before calling this Effect and provide their same
 * runtime, maintenance and alarm instances. The native endpoint uses this path too: queue limits,
 * idempotent receipts and the pre-admission generation/alarm commit have one owner.
 */
export const submit = Effect.fnUntraced(function* (threadId: ThreadId, request: SubmitRequest) {
  const placement = yield* ThreadObjectPlacement;

  if (!placement.ownsThread(threadId))
    return yield* HostProtocolError.make({ message: "The Thread belongs to another Object" });
  const mutations = yield* ThreadMutationGate;
  const runtime = yield* DurableAgentRuntime;
  const alarm = yield* DurableAlarmService;

  yield* gateAdmissionLimits(threadId, request);

  // The mutation gate pre-arms durable maintenance; publication keeps its own wake behavior.
  return yield* mutations.withMutation(
    runtime
      .submit(passthroughSubmitAgent(request.agentId), request.inputPayload, {
        threadId,
        principal: request.principal,
        idempotencyKey: request.idempotencyKey,
        ...(request.admissionGroup === undefined ? {} : { admissionGroup: request.admissionGroup }),
        ...(request.admissionFence === undefined ? {} : { admissionFence: request.admissionFence }),
        ...(request.workerAdmission === undefined
          ? {}
          : { workerAdmission: request.workerAdmission }),
        ...(request.messageAdmission === undefined
          ? {}
          : { messageAdmission: request.messageAdmission }),
        definitions: request.definitions,
      })
      .pipe(
        alarm.withHintsDeferred,
        Effect.tap(() => publishCommitted),
      ),
  );
});

const submitEndpoint = endpoint(
  hostOperations.submit,
  Effect.fnUntraced(function* (request) {
    const identity = yield* ThreadObjectIdentity;
    const receipt = yield* submit(identity.threadId, request);
    const scheduling = yield* DurableObjectAlarm.ScheduleConfiguration;

    if (scheduling.dispatchAfterEvent === true) {
      const maintenance = yield* ThreadMaintenance;

      // effect-cf owns the background scope and native event lifetime. Admission is
      // already durable; neither the Attempt nor its model request delays this receipt.
      yield* maintenance.start(receipt);
    }

    return SubmitSucceeded.make({ receipt });
  }),
);

const submissionStatusEndpoint = endpoint(
  hostOperations.submissionStatus,
  Effect.fnUntraced(function* (receipt) {
    const authorizer = yield* OperationAuthorizer;

    yield* authorizer.authorize(
      OperationAuthorizationRequest.make({
        operation: "awaitSettlement",
        threadId: receipt.threadId,
        submissionId: receipt.submissionId,
      }),
    );
    yield* requireReceiptThread(receipt.threadId);
    yield* requireSubmissionThread(receipt.submissionId);
    const runtime = yield* DurableAgentRuntime;

    return SubmissionStatusResponse.make({ status: yield* runtime.submissionStatus(receipt) });
  }),
);

const settlementEndpoint = <Success extends SettlementReached | SettlementRecordReached>(
  contract: HostOperation<
    AwaitSettlementRequest,
    Success | ProgressCancelled,
    typeof hostOperations.awaitSettlement.failure.Type
  >,
  settle: (
    runtime: DurableAgentRuntime["Service"],
    receipt: AwaitSettlementRequest,
  ) => Effect.Effect<Success, typeof hostOperations.awaitSettlement.failure.Type>,
  includeRecord = false,
) =>
  endpoint(
    contract,
    Effect.fnUntraced(function* (receipt) {
      const authorizer = yield* OperationAuthorizer;

      yield* authorizer.authorize(
        OperationAuthorizationRequest.make({
          operation: "awaitSettlement",
          threadId: receipt.threadId,
          submissionId: receipt.submissionId,
        }),
      );
      if (includeRecord)
        yield* authorizer.authorize(
          OperationAuthorizationRequest.make({
            operation: "observe",
            threadId: receipt.threadId,
          }),
        );
      yield* requireReceiptThread(receipt.threadId);
      yield* requireSubmissionThread(receipt.submissionId);
      const runtime = yield* DurableAgentRuntime;

      const wait = settle(runtime, receipt);

      if (receipt.waiterId === undefined) return yield* wait;
      const registry = yield* ProgressWaitRegistry;

      return yield* Effect.scoped(
        Effect.gen(function* () {
          const cancelled = yield* registry.subscribe(
            JSON.stringify([receipt.threadId, receipt.waiterId]),
          );

          return yield* Effect.raceFirst(wait, cancelled.pipe(Effect.as(ProgressCancelled.make())));
        }),
      );
    }),
  );

const awaitSettlementEndpoint = settlementEndpoint(
  hostOperations.awaitSettlement,
  (runtime, receipt) =>
    runtime
      .awaitSettlement(receipt)
      .pipe(Effect.map((settlement) => SettlementReached.make({ settlement }))),
);

const awaitSettlementRecordEndpoint = settlementEndpoint(
  hostOperations.awaitSettlementRecord,
  (runtime, receipt) =>
    runtime
      .awaitSettlementRecord(receipt)
      .pipe(Effect.map((record) => SettlementRecordReached.make({ record }))),
  true,
);

const awaitProgressEndpoint = endpoint(
  hostOperations.awaitProgress,
  Effect.fnUntraced(function* (request) {
    const identity = yield* ThreadObjectIdentity;
    const runtime = yield* DurableAgentRuntime;
    const registry = yield* ProgressWaitRegistry;

    yield* Effect.scoped(
      Effect.gen(function* () {
        const cancelled = yield* registry.subscribe(
          JSON.stringify([identity.threadId, request.waiterId]),
        );

        yield* Effect.raceFirst(
          runtime.awaitProgress(identity.threadId, request.afterSequence),
          cancelled,
        );
      }),
    );

    return ProgressObserved.make();
  }),
);

const cancelProgressEndpoint = endpoint<
  CancelProgressRequest,
  ProgressCancelled,
  HostProtocolError
>(
  hostOperations.cancelProgress,
  Effect.fnUntraced(function* (request) {
    const identity = yield* ThreadObjectIdentity;
    const registry = yield* ProgressWaitRegistry;

    yield* registry.cancel(JSON.stringify([identity.threadId, request.waiterId]));

    return ProgressCancelled.make();
  }),
);

const observePageEndpoint = endpoint(
  hostOperations.observePage,
  Effect.fnUntraced(function* (request) {
    const identity = yield* ThreadObjectIdentity;
    const store = yield* ThreadStore;
    // The same fail-closed authorization seam the runtime's `observe` consults (P7 WP1);
    // the default reference preserves the possession behavior.
    const authorizer = yield* OperationAuthorizer;

    yield* authorizer.authorize(
      OperationAuthorizationRequest.make({
        operation: "observe",
        threadId: identity.threadId,
      }),
    );

    const records = yield* Stream.runCollect(
      store.read(
        ThreadRead.make({
          threadId: identity.threadId,
          ...(request.afterSequence === undefined ? {} : { afterSequence: request.afterSequence }),
          limit: request.limit,
        }),
      ),
    );

    return ObservedPage.make({ records: [...records] });
  }),
);

const watchTextEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  Schema.decodeUnknownEffect(WatchTextRequest)(encoded).pipe(
    Effect.mapError(protocolFailure("The provisional text request could not be decoded")),
    Effect.flatMap(() =>
      Effect.gen(function* () {
        const { threadId } = yield* ThreadObjectIdentity;
        const authorizer = yield* OperationAuthorizer;

        yield* authorizer.authorize(
          OperationAuthorizationRequest.make({ operation: "observe", threadId }),
        );
        const hub = yield* LiveTextHub;

        return yield* hub.open(threadId);
      }),
    ),
    Effect.catch((failure) => encodeResponse(HostFailed.make({ failure }))),
  );

const abortEndpoint = endpoint(
  hostOperations.abort,
  Effect.fnUntraced(function* (command) {
    const authorizer = yield* OperationAuthorizer;

    yield* authorizer.authorize(
      OperationAuthorizationRequest.make({
        operation: "abort",
        submissionId: command.submissionId,
      }),
    );
    yield* requireSubmissionThread(command.submissionId);
    const maintenance = yield* ThreadMaintenance;
    const runtime = yield* DurableAgentRuntime;
    const intent = yield* maintenance.withMutation(runtime.abort(command));

    return AbortRecorded.make({ intent });
  }),
);

const resolveApprovalEndpoint = endpoint(
  hostOperations.resolveApproval,
  Effect.fnUntraced(function* (command) {
    const authorizer = yield* OperationAuthorizer;

    yield* authorizer.authorize(
      OperationAuthorizationRequest.make({
        operation: "resolveApproval",
        submissionId: command.submissionId,
      }),
    );
    yield* requireSubmissionThread(command.submissionId);
    const maintenance = yield* ThreadMaintenance;
    const runtime = yield* DurableAgentRuntime;
    const intent = yield* maintenance.withMutation(runtime.resolveApproval(command));

    return ApprovalRecorded.make({ intent });
  }),
);

const resolveUnknownEndpoint = endpoint(
  hostOperations.resolveUnknown,
  Effect.fnUntraced(function* (command) {
    const authorizer = yield* OperationAuthorizer;

    yield* authorizer.authorize(
      OperationAuthorizationRequest.make({
        operation: "resolveUnknown",
        submissionId: command.submissionId,
      }),
    );
    yield* requireSubmissionThread(command.submissionId);
    const maintenance = yield* ThreadMaintenance;
    const runtime = yield* DurableAgentRuntime;
    const intent = yield* maintenance.withMutation(runtime.resolveUnknown(command));

    return UnknownResolutionRecorded.make({ intent });
  }),
);

// ---------------------------------------------------------------------------
// P7 administrative entry points (plan §3): explain/verify/retry/obligations over the SAME
// envelope discipline as the host protocol — closed request/response Schema unions, typed
// failures that re-decode to identical tags, protocol anomalies answered typed. The envelopes
// live here (not `client.ts`) because no Worker-side client consumption exists yet; `wake`
// already exists as the `wake()` entry point.
// ---------------------------------------------------------------------------

/** Explain one Submission (`submissionId` present) or every nonterminal lane member. */
export class AdminExplainRequest extends Schema.Class<AdminExplainRequest>(
  "@effect-agent/platform-cloudflare/AdminExplainRequest",
)({
  submissionId: Schema.optionalKey(SubmissionId),
}) {}

/** Verify carries no parameters — the addressed Object IS the lane. */
export class AdminVerifyRequest extends Schema.Class<AdminVerifyRequest>(
  "@effect-agent/platform-cloudflare/AdminVerifyRequest",
)({}) {}

/** Every typed failure of the four admin entry points, plus the protocol's own errors. */
export const AdminFailure = Schema.Union([
  AdmissionPolicyError,
  ApprovalSuspensionError,
  OperationDenied,
  RetryRefused,
  LedgerError,
  RunJournalError,
  DigestError,
  OwnershipLost,
  SettlementConflict,
  ThreadStoreError,
  ThreadNotMaterialized,
  AppendConflict,
  DurableRuntimeFailpointError,
  DurableAlarmError,
  HostProtocolError,
  RecoveryBlocked,
  WorkDiscoveryUnavailable,
]);

export type AdminFailure = typeof AdminFailure.Type;

export class ExplainedRecovery extends Schema.TaggedClass<ExplainedRecovery>(
  "@effect-agent/platform-cloudflare/ExplainedRecovery",
)("ExplainedRecovery", {
  explanations: Schema.Array(RecoveryExplanation).check(Schema.isMaxLength(1_024)),
}) {}

export class VerifiedIntegrity extends Schema.TaggedClass<VerifiedIntegrity>(
  "@effect-agent/platform-cloudflare/VerifiedIntegrity",
)("VerifiedIntegrity", {
  report: IntegrityReport,
}) {}

export class RetryExecuted extends Schema.TaggedClass<RetryExecuted>(
  "@effect-agent/platform-cloudflare/RetryExecuted",
)("RetryExecuted", {
  report: RecoveryReport,
}) {}

export class ObligationsScanned extends Schema.TaggedClass<ObligationsScanned>(
  "@effect-agent/platform-cloudflare/ObligationsScanned",
)("ObligationsScanned", {
  report: ObligationReport,
}) {}

/** The admin entry point failed TYPED on the Object; the failure re-decodes verbatim. */
export class AdminFailed extends Schema.TaggedClass<AdminFailed>(
  "@effect-agent/platform-cloudflare/AdminFailed",
)("AdminFailed", {
  failure: AdminFailure,
}) {}

/** The uniform answer of one admin entry point. Callers narrow by the tag their call implies. */
export const AdminResponse = Schema.Union([
  ExplainedRecovery,
  VerifiedIntegrity,
  RetryExecuted,
  ObligationsScanned,
  AdminFailed,
]);

export type AdminResponse = typeof AdminResponse.Type;

export const decodeAdminExplainRequest = Schema.decodeUnknownEffect(AdminExplainRequest);
export const decodeAdminVerifyRequest = Schema.decodeUnknownEffect(AdminVerifyRequest);
export const decodeRetryCommand = Schema.decodeUnknownEffect(RetryCommand);
export const decodeObligationThresholds = Schema.decodeUnknownEffect(ObligationThresholds);

export const encodeAdminResponse: ReturnType<typeof Schema.encodeEffect<typeof AdminResponse>> = (
  ...args
) => Schema.encodeEffect(AdminResponse)(...args);

export const decodeAdminResponse = Schema.decodeUnknownEffect(AdminResponse);

/** Fold one admin endpoint's typed failures into the uniform `AdminResponse` envelope. */
const respondAdmin = <Result extends AdminResponse, Failure extends AdminFailure>(
  effect: Effect.Effect<Result, Failure, EndpointServices>,
): Effect.Effect<AdminResponse, never, EndpointServices> =>
  effect.pipe(
    Effect.map((result): AdminResponse => result),
    Effect.catch((failure) => Effect.succeed<AdminResponse>(AdminFailed.make({ failure }))),
  );

/** Encode the admin response envelope; an unencodable response degrades to a protocol failure. */
const encodeAdminResponseTotal = (response: AdminResponse): Effect.Effect<unknown> =>
  encodeAdminResponse(response).pipe(
    Effect.catch((error) =>
      Effect.succeed<unknown>({
        _tag: "AdminFailed",
        failure: {
          _tag: "HostProtocolError",
          message: boundHostDiagnostic(`The admin response could not be encoded: ${error.message}`),
        },
      }),
    ),
  );

const explainEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeAdminExplainRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The explain request could not be decoded")),
    Effect.flatMap((request) =>
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;
        const runtime = yield* DurableAgentRuntime;

        const explanations =
          request.submissionId === undefined
            ? yield* runtime.explainThread(identity.threadId)
            : [yield* runtime.explain(request.submissionId)];

        return ExplainedRecovery.make({ explanations });
      }),
    ),
    respondAdmin,
    Effect.flatMap(encodeAdminResponseTotal),
  );

const verifyEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeAdminVerifyRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The verify request could not be decoded")),
    Effect.flatMap(() =>
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;
        const runtime = yield* DurableAgentRuntime;
        const report = yield* runtime.verify(identity.threadId);

        return VerifiedIntegrity.make({ report });
      }),
    ),
    respondAdmin,
    Effect.flatMap(encodeAdminResponseTotal),
  );

const retryEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeRetryCommand(encoded).pipe(
    Effect.mapError(protocolFailure("The retry command could not be decoded")),
    Effect.flatMap((command) =>
      Effect.gen(function* () {
        const maintenance = yield* ThreadMaintenance;
        const runtime = yield* DurableAgentRuntime;
        // Retry may repair durable state, so its generation + alarm commit before the mutation.
        const report = yield* maintenance.withMutation(runtime.retry(command));

        return RetryExecuted.make({ report });
      }),
    ),
    respondAdmin,
    Effect.flatMap(encodeAdminResponseTotal),
  );

const obligationsEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeObligationThresholds(encoded).pipe(
    Effect.mapError(protocolFailure("The obligation thresholds could not be decoded")),
    Effect.flatMap((thresholds) =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const report = yield* runtime.scanObligations(thresholds);

        return ObligationsScanned.make({ report });
      }),
    ),
    respondAdmin,
    Effect.flatMap(encodeAdminResponseTotal),
  );

/**
 * Owner-side `portCall`: wrap a mutating envelope in the same pre-armed generation protocol as
 * public RPC (a routed mutation committed by THIS Object must already carry the alarm that will
 * finish it), execute on the LOCAL facets (never the routed decorators), then arm an immediate
 * alarm so the mutated lane is processed promptly. Protocol anomalies answer
 * `PortFailed(PortProtocolError)`.
 */
const encodePortResponseTotal = (response: PortResponse) =>
  encodePortResponse(response).pipe(
    Effect.catch((error) =>
      Effect.succeed(
        encodedPortProtocolFailure(`The port response could not be encoded: ${error.message}`),
      ),
    ),
  );

const portGuardFailure = (failure: LedgerError | HostProtocolError): PortFailed =>
  PortFailed.make({
    failure:
      failure._tag === "LedgerError"
        ? failure
        : PortProtocolError.make({ message: "The port request is not for the addressed Thread" }),
  });

export const portCall = (
  encoded: unknown,
): Effect.Effect<
  unknown,
  never,
  ThreadObjectPorts | ThreadMaintenance | DurableAlarmService | ThreadObjectIdentity
> =>
  Effect.gen(function* () {
    const ports = yield* ThreadObjectPorts;
    const maintenance = yield* ThreadMaintenance;
    const alarm = yield* DurableAlarmService;

    const decoded = yield* decodePortRequest(encoded).pipe(
      Effect.map((request) => ({ _tag: "success" as const, request })),
      Effect.catch((error) => Effect.succeed({ _tag: "failure" as const, message: error.message })),
    );

    if (decoded._tag === "failure") {
      return encodedPortProtocolFailure(
        `The port request could not be decoded: ${decoded.message}`,
      );
    }
    if (
      decoded.request._tag === "LedgerLookup" &&
      decoded.request.request._tag === "SubmissionLookupById"
    ) {
      const response = yield* lookupAddressedSubmission(decoded.request.request.submissionId).pipe(
        Effect.map((submission) =>
          PortSucceeded.make({
            result: LedgerLookupResult.make(
              Option.isSome(submission) ? { submission: submission.value } : {},
            ),
          }),
        ),
        Effect.catch((failure) => Effect.succeed(portGuardFailure(failure))),
      );

      return yield* encodePortResponseTotal(response);
    }
    const identityCheck = yield* requirePortThread(decoded.request).pipe(Effect.result);

    if (identityCheck._tag === "Failure")
      return yield* encodePortResponseTotal(portGuardFailure(identityCheck.failure));
    const mutating = isMutatingPortRequest(decoded.request);

    const handled = yield* (
      mutating
        ? maintenance.withMutation(ports.handle(decoded.request))
        : ports.handle(decoded.request)
    ).pipe(Effect.exit);

    if (handled._tag === "Failure") {
      // Without the committed generation/alarm the invariant cannot be promised; refuse before
      // the port mutation runs. `ports.handle` itself is total, so this is the maintenance error.
      return encodedPortProtocolFailure(
        "The owner Object could not arm its maintenance alarm before the mutation.",
      );
    }

    const response = yield* encodePortResponseTotal(handled.value);

    if (mutating) {
      // Prompt processing hint; the pre-armed alarm already guarantees convergence.
      yield* alarm.scheduleNow.pipe(
        Effect.catch((error) =>
          Effect.logWarning("ThreadObject.portCall: immediate re-arm failed", error),
        ),
      );
    }

    return response;
  });

const wakeEndpoint: Effect.Effect<void, never, EndpointServices> = Effect.gen(function* () {
  const identity = yield* ThreadObjectIdentity;
  const wake = yield* WakeScheduler;

  // Route the remote hint through this incarnation's scheduler so scoped progress waiters and
  // the alarm receive the same hint. Delivery remains droppable; canonical storage is authority.
  yield* wake.notify(identity.threadId);
});

/** The per-Thread wire operations supported by native and application-owned endpoints. */
export const ThreadRpcOperation = Schema.Literals([
  ...Object.values(hostOperations).map((operation) => operation.method),
  "watchTextEncoded",
  "portCall",
  "wake",
]);

export type ThreadRpcOperation = typeof ThreadRpcOperation.Type;

const threadRpc = {
  submitEncoded: submitEndpoint,
  submissionStatusEncoded: submissionStatusEndpoint,
  awaitSettlementEncoded: awaitSettlementEndpoint,
  awaitSettlementRecordEncoded: awaitSettlementRecordEndpoint,
  awaitProgressEncoded: awaitProgressEndpoint,
  cancelProgressEncoded: cancelProgressEndpoint,
  watchTextEncoded: watchTextEndpoint,
  observePage: observePageEndpoint,
  abortEncoded: abortEndpoint,
  resolveApprovalEncoded: resolveApprovalEndpoint,
  resolveUnknownEncoded: resolveUnknownEndpoint,
  portCall,
  wake: () => wakeEndpoint,
} satisfies Record<
  ThreadRpcOperation,
  (encoded: unknown) => Effect.Effect<unknown, never, EndpointServices>
>;

/**
 * Bind an addressed request to its logical Thread while sharing one physical runtime. This
 * validates local placement before invoking the same native handlers. Receipts, Submission
 * commands and port envelopes must match this identity; progress cancellation is Thread-scoped.
 * The producer comes from the actual runtime configuration, never from caller input. These
 * guards supplement the existing current model/Tool and operation authorization policies.
 */
export const handleRpc = Effect.fnUntraced(function* (
  threadId: ThreadId,
  operation: ThreadRpcOperation,
  encoded: unknown,
) {
  const placement = yield* ThreadObjectPlacement;

  if (!placement.ownsThread(threadId))
    return yield* HostProtocolError.make({ message: "The Thread belongs to another Object" });
  const { producerId } = yield* DurableRuntimeConfig;

  return yield* threadRpc[operation](encoded).pipe(
    Effect.provideService(ThreadObjectIdentity, { threadId, producerId }),
  );
});

/** Compose with application registrations on a shared physical object. */
export const alarms = ThreadMaintenanceAlarm.handlers({
  "@yielded/agent/thread-maintenance": () =>
    Effect.flatMap(ThreadMaintenance, (maintenance) => Effect.asVoid(maintenance.pass)),
});

const gateEndpoint: Effect.Effect<void, MaintenancePassFailure, EndpointServices> = Effect.gen(
  function* () {
    // Forcing ThreadMaintenance forces the whole Layer stack: migration + exact-version
    // check + configuration decode (DEPLOY-008 fails typed here, before any mutation), then
    // the defensive local ensure-alarm half of the invariant. LOCAL-ONLY by construction.
    const maintenance = yield* ThreadMaintenance;

    yield* maintenance.ensureAlarm;
  },
);

/**
 * Adapter from effect-cf's native Durable Object services to Effect Agent's existing platform
 * ports. effect-cf owns the cached ManagedRuntime and supplies these values once per Object
 * incarnation; the durable runtime continues to depend only on the narrow services below.
 */
const effectCfPlatformLayer = (
  namespaceBinding: string,
  rpcTracing = false,
): Layer.Layer<
  DurableObjectContext | ThreadObjectNamespace,
  CloudflareBindingError,
  EffectCfDurableObjectState.DurableObjectState | WorkerEnvironment
> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const state = yield* EffectCfDurableObjectState.DurableObjectState;
      const env = yield* WorkerEnvironment;
      const binding = yield* threadNamespaceFromEnv(env, namespaceBinding);

      return Context.make(DurableObjectContext, { ctx: state.raw, env }).pipe(
        Context.add(
          ThreadObjectNamespace,
          ThreadObjectNamespace.of({
            get: (threadId) => binding.get(binding.idFromName(threadId)),
            ...(rpcTracing === true ? { rpcTracing: namespaceBinding } : {}),
          }),
        ),
      );
    }),
  );

/** The public endpoints and effect-cf invocation hook of one Thread Object instance. */
export interface Instance<EventServices = never>
  extends
    ThreadObjectRpc,
    InstanceType<
      EffectCfDurableObject.DurableObjectClass<
        Record<never, never>,
        RuntimeServices | EventServices | ThreadMaintenanceAlarm
      >
    > {
  explainEncoded(encoded: unknown): Promise<unknown>;
  verifyEncoded(encoded: unknown): Promise<unknown>;
  retryEncoded(encoded: unknown): Promise<unknown>;
  obligationsEncoded(encoded: unknown): Promise<unknown>;
  alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> | void;
}

/** The constructor shape workerd instantiates for each Thread Object. */
export interface Class<EventServices = never> {
  new (ctx: DurableObjectState, env: Cloudflare.Env): Instance<EventServices>;
}

/**
 * Export a composed application Layer as a native Durable Object class.
 * Bootstrap services are provided to the whole graph before it acquires, so application Layers
 * can yield effect-cf's WorkerEnvironment and DurableObjectState, derived identity, and Crypto.
 * Effect Config reads scalar Worker vars and secrets through effect-cf's environment provider;
 * WorkerEnvironment exposes resource bindings without a separate config Layer.
 * Application dependencies remain visible until Layer.provide satisfies them. effect-cf owns the
 * cached ManagedRuntime, native RPC methods, event scopes, and telemetry flushing.
 * Initialization is local and bounded inside the constructor gate. Cloudflare eviction does not
 * guarantee finalizers; put resources requiring timely release in scoped operations or eventLayer.
 */
export const make = <
  ApplicationServices,
  ApplicationError,
  EventServices = never,
  EventLayerError = never,
  AlarmServices = never,
  AlarmError = never,
>(
  applicationLayer: Layer.Layer<
    CloudflareDurableRuntimeServices | ApplicationServices,
    ApplicationError,
    | CloudflareBootstrapServices
    | ThreadMaintenanceAlarm
    | NoInfer<AlarmServices>
    | EffectCfDurableObjectState.DurableObjectState
    | WorkerEnvironment
    | DurableObjectContext
    | ThreadObjectNamespace
  >,
  options: Options<ApplicationServices, EventServices, EventLayerError, AlarmServices, AlarmError>,
): Class<ApplicationServices | EventServices | AlarmServices> => {
  const application = applicationLayer.pipe(
    Layer.provideMerge(layerConfig(options)),
    Layer.provideMerge(effectCfPlatformLayer(options.namespaceBinding, options.rpcTracing)),
  );

  // effect-cf builds this entire Layer inside its constructor gate. Keep alarm inspection in
  // that build, after application acquisition, using the same per-Object MemoMap and Scope.
  const runtime: Layer.Layer<
    RuntimeServices | ApplicationServices,
    ThreadObjectInitializationError | ApplicationError,
    | EffectCfDurableObjectState.DurableObjectState
    | WorkerEnvironment
    | ThreadMaintenanceAlarm
    | AlarmServices
  > = Layer.flatMap(application, (applicationServices) => {
    const scheduling = Context.get(applicationServices, DurableObjectAlarm.ScheduleConfiguration);
    const config = Context.get(applicationServices, CloudflareDurableRuntimeConfig);

    const services = Context.add(applicationServices, DurableObjectAlarm.ScheduleConfiguration, {
      ...scheduling,
      dispatchAfterEvent: scheduling.dispatchAfterEvent ?? true,
      inFlightRecovery: config.recoveryWindowMillis || (scheduling.inFlightRecovery ?? 1_000),
    });

    return Layer.effectContext(gateEndpoint.pipe(Effect.provide(services), Effect.as(services)));
  });

  const rpc = {
    ...threadRpc,
    explainEncoded: (encoded: unknown) => explainEndpoint(encoded),
    verifyEncoded: (encoded: unknown) => verifyEndpoint(encoded),
    retryEncoded: (encoded: unknown) => retryEndpoint(encoded),
    obligationsEncoded: (encoded: unknown) => obligationsEndpoint(encoded),
  } satisfies EffectCfDurableObject.DurableObjectRpc<
    RuntimeServices | ApplicationServices | EventServices
  >;

  type NativeOptions = EffectCfDurableObject.DurableObjectOptions<
    RuntimeServices | ApplicationServices,
    EventServices,
    EventLayerError,
    typeof rpc,
    ThreadMaintenanceAlarm | AlarmServices
  >;

  return EffectCfDurableObject.make<
    RuntimeServices | ApplicationServices,
    ThreadObjectInitializationError | ApplicationError,
    EventServices,
    EventLayerError,
    typeof rpc,
    ThreadMaintenanceAlarm | AlarmServices
  >(runtime, {
    ...(options.rpcTracing === true ? { rpcTracing: { service: options.namespaceBinding } } : {}),
    ...(options.eventLayer === undefined ? {} : { eventLayer: options.eventLayer }),
    // Force the gated runtime Layer when Cloudflare loads this Object incarnation. Recovery stays
    // in each bounded pass so cross-Object initialization cannot deadlock.
    initialize: Effect.void,
    rpc,
    alarms:
      options.alarms === undefined ? alarms : DurableObjectAlarm.mergeAll(options.alarms, alarms),
    // The named registration supplies its scheduler before the constructor builds application services.
    // Upstream's conditional alarm-registration check cannot reduce over generic application
    // services. Options and the rpc satisfies check above retain their Effect requirements.
  } as NativeOptions);
};
