import { AgentInputError } from "@yielded/agent/agent-error";
import { DigestError } from "@yielded/agent/digest";
import { DurableRuntimeFailpointError } from "@yielded/agent/durable-failpoint";
import { AgentId } from "@yielded/agent/identifiers";
import { InputMessage } from "@yielded/agent/messaging";
import { OperationDenied } from "@yielded/agent/operation-authorizer";
import { Receipt } from "@yielded/agent/receipt";
import {
  CanonicalRecordEnvelope,
  CanonicalSequence,
  DefinitionDigests,
  PersistedJson,
  SubmissionSettledRecord,
  WorkerAdmission,
} from "@yielded/agent/records";
import {
  AdmissionFence,
  AdmissionGroup,
  AbortCommand,
  AbortIntent,
  AdmissionConflict,
  AdmissionPolicyError,
  ApprovalConflict,
  ApprovalDecisionCommand,
  ApprovalDecisionIntent,
  IdempotencyKey,
  JoinedToHost,
  LedgerError,
  OwnershipLost,
  Principal,
  Settlement,
  SettlementConflict,
  UnknownResolutionCommand,
  UnknownResolutionConflict,
  UnknownResolutionIntent,
} from "@yielded/agent/submission-ledger";
import { SubmissionStatus } from "@yielded/agent/submission-status";
import {
  AppendConflict,
  ThreadNotMaterialized,
  ThreadStoreError,
} from "@yielded/agent/thread-store";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { DurableAlarmError, type ThreadObjectRpc } from "../CloudflareBindings.ts";
import { AdmissionLimitExceeded } from "../CloudflareConfig.ts";
import { HostProtocolError } from "./host-protocol-error.ts";

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/**
 * One durable submission, input ALREADY encoded by the caller through the Agent Binding's
 * input schema (the Worker bundles the same Agent definitions as the Object, so schema
 * validation happens client-side; the resolved Binding re-validates at claim time). The
 * Thread identity is deliberately absent — the addressed Object IS the lane.
 */
export class SubmitRequest extends Schema.Class<SubmitRequest>(
  "@effect-agent/platform-cloudflare/SubmitRequest",
)({
  agentId: AgentId,
  principal: Principal,
  idempotencyKey: IdempotencyKey,
  admissionGroup: Schema.optionalKey(AdmissionGroup),
  admissionFence: Schema.optionalKey(AdmissionFence),
  workerAdmission: Schema.optionalKey(WorkerAdmission),
  messageAdmission: Schema.optionalKey(InputMessage),
  definitions: DefinitionDigests,
  inputPayload: PersistedJson,
}) {}

/** One bounded page of canonical records after an optional sequence. */
export class ObservePageRequest extends Schema.Class<ObservePageRequest>(
  "@effect-agent/platform-cloudflare/ObservePageRequest",
)({
  afterSequence: Schema.optionalKey(CanonicalSequence),
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

/** Receipt-shaped observation request; the optional identity owns only the remote wait. */
export class AwaitSettlementRequest extends Receipt.extend<AwaitSettlementRequest>(
  "@effect-agent/platform-cloudflare/AwaitSettlementRequest",
)({
  waiterId: Schema.optionalKey(Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256))),
}) {}

/** One event-driven wait for canonical progress strictly after this sequence. */
export class AwaitProgressRequest extends Schema.Class<AwaitProgressRequest>(
  "@effect-agent/platform-cloudflare/AwaitProgressRequest",
)({
  afterSequence: CanonicalSequence,
  waiterId: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256)),
}) {}

/** Best-effort cancellation of one in-flight progress or settlement observation. */
export class CancelProgressRequest extends Schema.Class<CancelProgressRequest>(
  "@effect-agent/platform-cloudflare/CancelProgressRequest",
)({
  waiterId: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256)),
}) {}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/**
 * Every typed failure a host entry point can produce, plus the protocol's own errors. Same
 * closed-union discipline as the WP2 `PortFailure`: members re-decode to the SAME tagged
 * classes on the Worker side; `cause` chains travel as Schema defects without instance
 * fidelity (plan §2.8).
 */
export const HostFailure = Schema.Union([
  AgentInputError,
  DigestError,
  AdmissionConflict,
  AdmissionPolicyError,
  SettlementConflict,
  ApprovalConflict,
  UnknownResolutionConflict,
  JoinedToHost,
  LedgerError,
  OwnershipLost,
  ThreadStoreError,
  ThreadNotMaterialized,
  AppendConflict,
  DurableRuntimeFailpointError,
  AdmissionLimitExceeded,
  DurableAlarmError,
  OperationDenied,
  HostProtocolError,
]);

export type HostFailure = typeof HostFailure.Type;

export class SubmitSucceeded extends Schema.TaggedClass<SubmitSucceeded>(
  "@effect-agent/platform-cloudflare/SubmitSucceeded",
)("SubmitSucceeded", {
  receipt: Receipt,
}) {}

export class SubmissionStatusResponse extends Schema.TaggedClass<SubmissionStatusResponse>(
  "@effect-agent/platform-cloudflare/SubmissionStatusResponse",
)("SubmissionStatusResponse", { status: SubmissionStatus }) {}

export class SettlementReached extends Schema.TaggedClass<SettlementReached>(
  "@effect-agent/platform-cloudflare/SettlementReached",
)("SettlementReached", {
  settlement: Settlement,
}) {}

/** The exact canonical record of an operationally finalized Submission. */
export class SettlementRecordReached extends Schema.TaggedClass<SettlementRecordReached>(
  "@effect-agent/platform-cloudflare/SettlementRecordReached",
)("SettlementRecordReached", {
  record: SubmissionSettledRecord,
}) {}

export class ObservedPage extends Schema.TaggedClass<ObservedPage>(
  "@effect-agent/platform-cloudflare/ObservedPage",
)("ObservedPage", {
  records: Schema.Array(CanonicalRecordEnvelope).check(Schema.isMaxLength(1_024)),
}) {}

/** A record was already committed or an incarnation-local hint says the caller should re-read. */
export class ProgressObserved extends Schema.TaggedClass<ProgressObserved>(
  "@effect-agent/platform-cloudflare/ProgressObserved",
)("ProgressObserved", {}) {}

export class ProgressCancelled extends Schema.TaggedClass<ProgressCancelled>(
  "@effect-agent/platform-cloudflare/ProgressCancelled",
)("ProgressCancelled", {}) {}

export class AbortRecorded extends Schema.TaggedClass<AbortRecorded>(
  "@effect-agent/platform-cloudflare/AbortRecorded",
)("AbortRecorded", {
  intent: AbortIntent,
}) {}

export class ApprovalRecorded extends Schema.TaggedClass<ApprovalRecorded>(
  "@effect-agent/platform-cloudflare/ApprovalRecorded",
)("ApprovalRecorded", {
  intent: ApprovalDecisionIntent,
}) {}

export class UnknownResolutionRecorded extends Schema.TaggedClass<UnknownResolutionRecorded>(
  "@effect-agent/platform-cloudflare/UnknownResolutionRecorded",
)("UnknownResolutionRecorded", {
  intent: UnknownResolutionIntent,
}) {}

/** The entry point failed TYPED on the Object; the failure re-decodes verbatim. */
export class HostFailed extends Schema.TaggedClass<HostFailed>(
  "@effect-agent/platform-cloudflare/HostFailed",
)("HostFailed", {
  failure: HostFailure,
}) {}

/** The uniform answer of one host entry point. Callers narrow by the tag their call implies. */
export const HostResponse = Schema.Union([
  SubmitSucceeded,
  SettlementReached,
  SettlementRecordReached,
  SubmissionStatusResponse,
  ObservedPage,
  ProgressObserved,
  ProgressCancelled,
  AbortRecorded,
  ApprovalRecorded,
  UnknownResolutionRecorded,
  HostFailed,
]);

export type HostResponse = typeof HostResponse.Type;

/** Failure surface of `CloudflareThreadClient.submit`. */
const SubmitHostFailure = Schema.Union([
  AgentInputError,
  DigestError,
  AdmissionConflict,
  AdmissionPolicyError,
  LedgerError,
  OwnershipLost,
  ThreadStoreError,
  ThreadNotMaterialized,
  AppendConflict,
  DurableRuntimeFailpointError,
  AdmissionLimitExceeded,
  DurableAlarmError,
]);

const AwaitHostFailure = Schema.Union([
  LedgerError,
  OwnershipLost,
  SettlementConflict,
  OperationDenied,
]);

const ObserveHostFailure = Schema.Union([ThreadStoreError, ThreadNotMaterialized, OperationDenied]);

const AbortHostFailure = Schema.Union([
  OperationDenied,
  LedgerError,
  OwnershipLost,
  SettlementConflict,
  JoinedToHost,
  DurableRuntimeFailpointError,
  DurableAlarmError,
]);

const ApprovalHostFailure = Schema.Union([
  LedgerError,
  OwnershipLost,
  SettlementConflict,
  ApprovalConflict,
  OperationDenied,
  DurableAlarmError,
]);

const UnknownHostFailure = Schema.Union([
  LedgerError,
  OwnershipLost,
  SettlementConflict,
  UnknownResolutionConflict,
  JoinedToHost,
  DurableRuntimeFailpointError,
  OperationDenied,
  DurableAlarmError,
]);

/** A host method owns its request, success and typed failure codecs on both sides of RPC. */
const operation = <
  const Method extends keyof ThreadObjectRpc,
  Request extends Schema.Codec<unknown, unknown>,
  Success extends Schema.Codec<HostSuccess, unknown>,
  Failure extends Schema.Codec<HostFailure, unknown>,
>(
  method: Method,
  request: Request,
  success: Success,
  failure: Failure,
) => {
  const failures = Schema.Union([failure, HostProtocolError]);

  const response = Schema.Union([
    success,
    Schema.TaggedStruct("HostFailed", { failure: failures }),
  ]);

  return {
    method,
    name: method.replace(/Encoded$/, ""),
    failure: failures,
    decodeRequest: Schema.decodeUnknownEffect(request),
    encodeRequest: Schema.encodeEffect(request),
    decodeResponse: Schema.decodeUnknownEffect(response),
    encodeResponse: Schema.encodeEffect(response),
  };
};

export type HostSuccess = Exclude<HostResponse, HostFailed>;

export const hostOperations = {
  submit: {
    ...operation("submitEncoded", SubmitRequest, SubmitSucceeded, SubmitHostFailure),
    // Encode fields directly instead of constructing and validating the same Class twice.
    encodeRequest: Schema.encodeEffect(Schema.Struct(SubmitRequest.fields)),
  },
  submissionStatus: operation(
    "submissionStatusEncoded",
    Receipt,
    SubmissionStatusResponse,
    AwaitHostFailure,
  ),
  awaitSettlement: operation(
    "awaitSettlementEncoded",
    AwaitSettlementRequest,
    Schema.Union([SettlementReached, ProgressCancelled]),
    AwaitHostFailure,
  ),
  awaitSettlementRecord: operation(
    "awaitSettlementRecordEncoded",
    AwaitSettlementRequest,
    Schema.Union([SettlementRecordReached, ProgressCancelled]),
    AwaitHostFailure,
  ),
  awaitProgress: operation(
    "awaitProgressEncoded",
    AwaitProgressRequest,
    ProgressObserved,
    ObserveHostFailure,
  ),
  cancelProgress: operation(
    "cancelProgressEncoded",
    CancelProgressRequest,
    ProgressCancelled,
    Schema.Never,
  ),
  observePage: operation("observePage", ObservePageRequest, ObservedPage, ObserveHostFailure),
  abort: operation("abortEncoded", AbortCommand, AbortRecorded, AbortHostFailure),
  resolveApproval: operation(
    "resolveApprovalEncoded",
    ApprovalDecisionCommand,
    ApprovalRecorded,
    ApprovalHostFailure,
  ),
  resolveUnknown: operation(
    "resolveUnknownEncoded",
    UnknownResolutionCommand,
    UnknownResolutionRecorded,
    UnknownHostFailure,
  ),
};

export type HostRpcMethod = (typeof hostOperations)[keyof typeof hostOperations]["method"];

export interface HostOperation<Request, Success extends HostSuccess, Failure extends HostFailure> {
  readonly method: HostRpcMethod;
  readonly name: string;
  readonly encodeRequest: (request: Request) => Effect.Effect<unknown, Schema.SchemaError>;
  readonly decodeRequest: (encoded: unknown) => Effect.Effect<Request, Schema.SchemaError>;
  readonly decodeResponse: (
    encoded: unknown,
  ) => Effect.Effect<
    Success | { readonly _tag: "HostFailed"; readonly failure: Failure | HostProtocolError },
    Schema.SchemaError
  >;
  readonly encodeResponse: (
    response:
      | Success
      | { readonly _tag: "HostFailed"; readonly failure: Failure | HostProtocolError },
  ) => Effect.Effect<unknown, Schema.SchemaError>;
}

// Preserve the public codec exports while endpoints use their operation's contract.
export const decodeSubmitRequest = hostOperations.submit.decodeRequest;
export const encodeSubmitRequest = hostOperations.submit.encodeRequest;
export const decodeReceipt = hostOperations.submissionStatus.decodeRequest;
export const encodeReceipt = hostOperations.submissionStatus.encodeRequest;
export const decodeAwaitSettlementRequest = hostOperations.awaitSettlement.decodeRequest;
export const encodeAwaitSettlementRequest = hostOperations.awaitSettlement.encodeRequest;
export const decodeObservePageRequest = hostOperations.observePage.decodeRequest;
export const encodeObservePageRequest = hostOperations.observePage.encodeRequest;
export const decodeAwaitProgressRequest = hostOperations.awaitProgress.decodeRequest;
export const encodeAwaitProgressRequest = hostOperations.awaitProgress.encodeRequest;
export const decodeCancelProgressRequest = hostOperations.cancelProgress.decodeRequest;
export const encodeCancelProgressRequest = hostOperations.cancelProgress.encodeRequest;
export const decodeAbortCommand = hostOperations.abort.decodeRequest;
export const encodeAbortCommand = hostOperations.abort.encodeRequest;
export const decodeApprovalDecisionCommand = hostOperations.resolveApproval.decodeRequest;
export const encodeApprovalDecisionCommand = hostOperations.resolveApproval.encodeRequest;
export const decodeUnknownResolutionCommand = hostOperations.resolveUnknown.decodeRequest;
export const encodeUnknownResolutionCommand = hostOperations.resolveUnknown.encodeRequest;
export const encodeHostResponse = Schema.encodeEffect(HostResponse);
export const decodeHostResponse = Schema.decodeUnknownEffect(HostResponse);
