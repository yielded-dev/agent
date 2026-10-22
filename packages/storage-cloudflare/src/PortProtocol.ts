/**
 * Schema-validated requests and responses for routing storage operations to their owner.
 *
 * @since 0.1.0
 */
import { IntegrityReport } from "@yielded/agent/admin";
import { DurableRuntimeFailpointError } from "@yielded/agent/durable-failpoint";
import { ThreadId } from "@yielded/agent/identifiers";
import {
  MessageDeliveryError,
  MessageDeliveryFailpointError,
  MessageDeliveryKey,
  MessageDeliveryCompletion,
  MessageDeliveryRecord,
  MessageDeliveryPageRequest,
  MessageDeliveryPage,
} from "@yielded/agent/message-delivery";
import { ReadEnvelope } from "@yielded/agent/record-format";
import { PromptRecordEnvelope } from "@yielded/agent/records";
import {
  SettlementPublication,
  SettlementPublicationResult,
} from "@yielded/agent/settlement-publisher";
import {
  AbortCommand,
  WorkerStopCommand,
  WorkerLedgerState,
  AbortIntent,
  AdmissionConflict,
  AdmissionPolicyError,
  AdmissionRequest,
  AdmissionResolution,
  AdmissionResult,
  ChildSettledNotification,
  ChildSettledOutcome,
  JoinedToHost,
  LedgerError,
  OwnershipLost,
  MarkReadyRequest,
  SettlementConflict,
  SubmissionLookup,
  SubmissionLookupByKey,
  SubmissionSnapshot,
  FundingOwnerRequest,
  FundingOwner,
} from "@yielded/agent/submission-ledger";
import {
  ThreadArchiveRange,
  ThreadArchiveRangePage,
  ThreadArchiveRangeRead,
  ThreadArchiveRangeRequest,
  ThreadArchiveRangeSeal,
  ThreadArchiveRangePublish,
} from "@yielded/agent/thread-archive-range";
import {
  AppendConflict,
  ExecutionRead,
  ExecutionCheckpoint,
  AppendResult,
  ThreadExport,
  ThreadPeerCountRequest,
  ThreadExportRequest,
  ThreadIdentity,
  ThreadIdentityRequest,
  ThreadWorkerCapacityRequest,
  ThreadWorkerCapacity,
  ThreadVerificationRequest,
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadExportRejected,
  ThreadReadRequest,
  ThreadPromptRead,
  ThreadStoreError,
  ThreadTail,
  ThreadTailRequest,
  AppendRequest,
} from "@yielded/agent/thread-store";
import {
  ThreadWorkRequest,
  ThreadWorkPage,
  WorkIndexRebuildRequest,
  WorkIndexProgress,
  WorkDiscoveryUnavailable,
} from "@yielded/agent/thread-work";
import { WorkerAdmissionRequest } from "@yielded/agent/worker-admission";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";

/**
 * The cross-Durable-Object port protocol (plan §1.3, D-P6-3): Schema request/response/error
 * envelopes for the CLOSED route-capable subset of the thread ports. One Thread's
 * Durable Object executes another Thread's request against its OWN local facets; the
 * envelopes here are the only values that cross the Object boundary, and they are
 * transport-agnostic — native Durable Object JS RPC is the shipped carrier, fetch-with-JSON
 * the documented fallback, and both move the same Schema-encoded JSON.
 *
 * The closed subset is exactly the set of operations the durable coordinator performs against
 * a FOREIGN Thread (parent/child establishment, status checks, abort propagation,
 * child-settlement notification, and the child-thread store operations used by
 * establishment, `verifySettledChild`, and result projection):
 *
 * - worker: `admitWorker` (ledger admission, materialization, creation, origin, readiness);
 * - ledger: `admit`, `markReady`, `lookup`, `resolveAdmission`, `requestAbort`,
 *   `recordChildSettled`;
 * - publisher: `publish`;
 * - store: `materialize`, `append`, `read` and `readPrompt` (one page), `readIdentity`,
 *   `inspectTail`, `export`, `countPeerMessages`, `work.page`, `work.rebuild`.
 *
 * Every other port operation is lane-local and has no envelope. Foreign operations
 * fail fast and typed instead of widening the distributed surface.
 *
 * Failures cross the boundary as the `PortFailure` union and re-decode on the caller side to
 * the SAME tagged error types the local facet would have produced, so routed calls keep
 * error-tag fidelity. `cause` chains inside `LedgerError`/`ThreadStoreError` travel as
 * Schema defects and do not claim instance fidelity across Objects (plan §2.8).
 */

/**
 * Ceiling for protocol diagnostic strings; matches `AdmissionIndeterminate.reason`.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MAX_PORT_DIAGNOSTIC_LENGTH = 4_096;

const BoundedDiagnostic = Schema.String.check(Schema.isMaxLength(MAX_PORT_DIAGNOSTIC_LENGTH));

/**
 * Truncate a diagnostic string to the protocol's bounded diagnostic length.
 *
 * @category models
 * @since 0.1.0
 */
export const boundPortDiagnostic = (value: string): string =>
  value.length > MAX_PORT_DIAGNOSTIC_LENGTH
    ? `${value.slice(0, MAX_PORT_DIAGNOSTIC_LENGTH - 3)}...`
    : value;

/**
 * The envelope itself could not be honored: the receiving Object could not decode the
 * request, or a response could not be encoded/decoded. It never carries port semantics —
 * callers fold it into the operation's base error (`LedgerError`/`ThreadStoreError`),
 * except `resolveAdmission`, which folds it into `AdmissionIndeterminate` because a
 * non-answer is never proof of absence (SUB-031).
 *
 * @category errors
 * @since 0.1.0
 */
export class PortProtocolError extends Schema.TaggedError<PortProtocolError>()(
  "PortProtocolError",
  {
    message: BoundedDiagnostic,
  },
) {}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/**
 * One destination-owned worker admission, including canonical initialization and readiness.
 *
 * @category schemas
 * @since 0.1.0
 */
export class WorkerAdmitCall extends Schema.TaggedClass<WorkerAdmitCall>()("WorkerAdmit", {
  request: WorkerAdmissionRequest,
}) {}

/**
 * Routed `SubmissionLedger.admit` — child establishment admits INTO the owning Object.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerAdmitCall extends Schema.TaggedClass<LedgerAdmitCall>(
  "@effect-agent/storage-cloudflare/LedgerAdmitCall",
)("LedgerAdmit", {
  request: AdmissionRequest,
}) {}

/**
 * Routed `SubmissionLedger.markReady` for a Submission owned by another Object.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerMarkReadyCall extends Schema.TaggedClass<LedgerMarkReadyCall>(
  "@effect-agent/storage-cloudflare/LedgerMarkReadyCall",
)("LedgerMarkReady", {
  request: MarkReadyRequest,
}) {}

/**
 * Routed `SubmissionLedger.lookup` (by identity or scoped idempotency key).
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerLookupCall extends Schema.TaggedClass<LedgerLookupCall>(
  "@effect-agent/storage-cloudflare/LedgerLookupCall",
)("LedgerLookup", {
  request: SubmissionLookup,
}) {}

/**
 * Owner-routed request to read a submission funding owner.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerFundingOwnerCall extends Schema.TaggedClass<LedgerFundingOwnerCall>()(
  "LedgerFundingOwner",
  { request: FundingOwnerRequest },
) {}

/**
 * Routed `SubmissionLedger.resolveAdmission` — the SUB-031 tri-state authority call.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerResolveAdmissionCall extends Schema.TaggedClass<LedgerResolveAdmissionCall>(
  "@effect-agent/storage-cloudflare/LedgerResolveAdmissionCall",
)("LedgerResolveAdmission", {
  request: SubmissionLookupByKey,
}) {}

/**
 * Owner-routed request to inspect worker state.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerInspectWorkerCall extends Schema.TaggedClass<LedgerInspectWorkerCall>()(
  "LedgerInspectWorker",
  { request: Schema.Struct({ threadId: ThreadId }) },
) {}

/**
 * Owner response to a request to inspect worker state.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerInspectWorkerResult extends Schema.TaggedClass<LedgerInspectWorkerResult>()(
  "LedgerInspectWorkerResult",
  { state: WorkerLedgerState },
) {}

/**
 * Owner-routed request to stop a worker.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerStopWorkerCall extends Schema.TaggedClass<LedgerStopWorkerCall>()(
  "LedgerStopWorker",
  { request: WorkerStopCommand },
) {}

/**
 * Owner response to a request to stop a worker.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerStopWorkerResult extends Schema.TaggedClass<LedgerStopWorkerResult>()(
  "LedgerStopWorkerResult",
  { owned: Schema.Natural },
) {}

/**
 * Routed `SubmissionLedger.requestAbort` — abort propagation across Objects.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerRequestAbortCall extends Schema.TaggedClass<LedgerRequestAbortCall>(
  "@effect-agent/storage-cloudflare/LedgerRequestAbortCall",
)("LedgerRequestAbort", {
  request: AbortCommand,
}) {}

/**
 * Routed `SubmissionLedger.recordChildSettled` — the child→parent durable notification.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerRecordChildSettledCall extends Schema.TaggedClass<LedgerRecordChildSettledCall>(
  "@effect-agent/storage-cloudflare/LedgerRecordChildSettledCall",
)("LedgerRecordChildSettled", {
  request: ChildSettledNotification,
}) {}

/**
 * Publish canonical settlement intent at the owning Thread.
 *
 * @category schemas
 * @since 0.1.0
 */
export class SettlementPublishCall extends Schema.TaggedClass<SettlementPublishCall>()(
  "SettlementPublish",
  { request: SettlementPublication },
) {}

/**
 * Owner response to a request to publish terminal settlement.
 *
 * @category schemas
 * @since 0.1.0
 */
export class SettlementPublishResult extends Schema.TaggedClass<SettlementPublishResult>()(
  "SettlementPublishResult",
  { result: SettlementPublicationResult },
) {}

/**
 * Routed `ThreadStore.materialize` against the owning Object.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreMaterializeCall extends Schema.TaggedClass<StoreMaterializeCall>(
  "@effect-agent/storage-cloudflare/StoreMaterializeCall",
)("StoreMaterialize", {
  request: ThreadMaterialization,
}) {}

/**
 * Routed `ThreadStore.append` against the owning Object.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreAppendCall extends Schema.TaggedClass<StoreAppendCall>(
  "@effect-agent/storage-cloudflare/StoreAppendCall",
)("StoreAppend", {
  request: AppendRequest,
}) {}

/**
 * Routed one-page `ThreadStore.read`; the page bound is the request's own `limit`.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreReadPageCall extends Schema.TaggedClass<StoreReadPageCall>(
  "@effect-agent/storage-cloudflare/StoreReadPageCall",
)("StoreReadPage", {
  request: ThreadReadRequest,
}) {}

/**
 * Owner-routed request to read prompt history.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreReadPromptCall extends Schema.TaggedClass<StoreReadPromptCall>()(
  "StoreReadPrompt",
  { request: ThreadPromptRead },
) {}

/**
 * Routed `ThreadStore.inspectTail` against the owning Object.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreInspectTailCall extends Schema.TaggedClass<StoreInspectTailCall>(
  "@effect-agent/storage-cloudflare/StoreInspectTailCall",
)("StoreInspectTail", {
  request: ThreadTailRequest,
}) {}

/**
 * Routed bounded `ThreadStore.readIdentity` against the owning Object.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreReadIdentityCall extends Schema.TaggedClass<StoreReadIdentityCall>(
  "@effect-agent/storage-cloudflare/StoreReadIdentityCall",
)("StoreReadIdentity", {
  request: ThreadIdentityRequest,
}) {}

/**
 * Owner-routed request to read execution state.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreReadExecutionCall extends Schema.TaggedClass<StoreReadExecutionCall>()(
  "StoreReadExecution",
  { request: ExecutionRead },
) {}

/**
 * Owner-routed request to read worker capacity.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreWorkerCapacityCall extends Schema.TaggedClass<StoreWorkerCapacityCall>()(
  "StoreWorkerCapacity",
  { request: ThreadWorkerCapacityRequest },
) {}

/**
 * Owner-routed request to verify retained Thread state.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreVerificationCall extends Schema.TaggedClass<StoreVerificationCall>()(
  "StoreVerification",
  { request: ThreadVerificationRequest },
) {}

/**
 * Routed `ThreadStore.export` against the owning Object.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreExportCall extends Schema.TaggedClass<StoreExportCall>(
  "@effect-agent/storage-cloudflare/StoreExportCall",
)("StoreExport", {
  request: ThreadExportRequest,
}) {}

/**
 * Owner-routed request to count peer messages.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreCountPeerMessagesCall extends Schema.TaggedClass<StoreCountPeerMessagesCall>()(
  "StoreCountPeerMessages",
  { request: ThreadPeerCountRequest },
) {}

/**
 * Work maintenance is routed to the exact Thread's authoritative owner.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreWorkPageCall extends Schema.TaggedClass<StoreWorkPageCall>()("StoreWorkPage", {
  request: ThreadWorkRequest,
}) {}

/**
 * Owner-routed request to rebuild the work index.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreWorkRebuildCall extends Schema.TaggedClass<StoreWorkRebuildCall>()(
  "StoreWorkRebuild",
  { request: WorkIndexRebuildRequest },
) {}

/**
 * Owner-routed request to list message delivery obligations.
 *
 * @category schemas
 * @since 0.1.0
 */
export class MessageDeliveryListCall extends Schema.TaggedClass<MessageDeliveryListCall>()(
  "MessageDeliveryList",
  { request: MessageDeliveryPageRequest },
) {}

/**
 * Owner-routed request to complete a message delivery.
 *
 * @category schemas
 * @since 0.1.0
 */
export class MessageDeliveryCompleteCall extends Schema.TaggedClass<MessageDeliveryCompleteCall>()(
  "MessageDeliveryComplete",
  { key: MessageDeliveryKey, completion: MessageDeliveryCompletion },
) {}

/**
 * Owner-routed request to read a canonical range.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreRangePageCall extends Schema.TaggedClass<StoreRangePageCall>()("StoreRangePage", {
  request: ThreadArchiveRangeRead,
}) {}

/**
 * Owner-routed request to seal a canonical range.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreRangeSealCall extends Schema.TaggedClass<StoreRangeSealCall>()("StoreRangeSeal", {
  request: ThreadArchiveRangeSeal,
}) {}

/**
 * Owner-routed request to archive a canonical range.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreRangeArchiveCall extends Schema.TaggedClass<StoreRangeArchiveCall>()(
  "StoreRangeArchive",
  { request: ThreadArchiveRangePublish },
) {}

/**
 * Owner-routed request to verify a canonical range.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreRangeVerifyCall extends Schema.TaggedClass<StoreRangeVerifyCall>()(
  "StoreRangeVerify",
  { request: ThreadArchiveRangeRequest },
) {}

/**
 * Every request that may cross a Durable Object boundary — the CLOSED route-capable subset.
 *
 * @category schemas
 * @since 0.1.0
 */
export const PortRequest = Schema.Union([
  SettlementPublishCall,
  WorkerAdmitCall,
  MessageDeliveryListCall,
  MessageDeliveryCompleteCall,
  LedgerAdmitCall,
  LedgerMarkReadyCall,
  LedgerLookupCall,
  LedgerFundingOwnerCall,
  LedgerResolveAdmissionCall,
  LedgerRequestAbortCall,
  LedgerStopWorkerCall,
  LedgerInspectWorkerCall,
  LedgerRecordChildSettledCall,
  StoreRangePageCall,
  StoreRangeSealCall,
  StoreRangeArchiveCall,
  StoreRangeVerifyCall,
  StoreMaterializeCall,
  StoreAppendCall,
  StoreReadPageCall,
  StoreReadPromptCall,
  StoreInspectTailCall,
  StoreReadIdentityCall,
  StoreReadExecutionCall,
  StoreWorkerCapacityCall,
  StoreVerificationCall,
  StoreExportCall,
  StoreCountPeerMessagesCall,
  StoreWorkPageCall,
  StoreWorkRebuildCall,
]);

/**
 * Decoded value of the PortRequest schema.
 *
 * @category models
 * @since 0.1.0
 */
export type PortRequest = typeof PortRequest.Type;

/**
 * The wire form of one port request (what a transport actually carries).
 *
 * @category utility types
 * @since 0.1.0
 */
export type PortRequestEnvelope = typeof PortRequest.Encoded;

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

/**
 * Owner response to a request to admit worker input.
 *
 * @category schemas
 * @since 0.1.0
 */
export class WorkerAdmitResult extends Schema.TaggedClass<WorkerAdmitResult>()(
  "WorkerAdmitResult",
  {
    result: AdmissionResult,
  },
) {}

/**
 * Owner response to a request to admit input.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerAdmitResult extends Schema.TaggedClass<LedgerAdmitResult>(
  "@effect-agent/storage-cloudflare/LedgerAdmitResult",
)("LedgerAdmitResult", {
  result: AdmissionResult,
}) {}

/**
 * Owner response to a request to mark a submission ready.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerMarkReadyResult extends Schema.TaggedClass<LedgerMarkReadyResult>(
  "@effect-agent/storage-cloudflare/LedgerMarkReadyResult",
)("LedgerMarkReadyResult", {}) {}

/**
 * `submission` is absent exactly when the lookup answered `Option.none`.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerLookupResult extends Schema.TaggedClass<LedgerLookupResult>(
  "@effect-agent/storage-cloudflare/LedgerLookupResult",
)("LedgerLookupResult", {
  submission: Schema.optionalKey(SubmissionSnapshot),
}) {}

/**
 * Owner response to a request to resolve an uncertain admission.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerResolveAdmissionResult extends Schema.TaggedClass<LedgerResolveAdmissionResult>(
  "@effect-agent/storage-cloudflare/LedgerResolveAdmissionResult",
)("LedgerResolveAdmissionResult", {
  resolution: AdmissionResolution,
}) {}

/**
 * Owner response to a request to retain an abort request.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerRequestAbortResult extends Schema.TaggedClass<LedgerRequestAbortResult>(
  "@effect-agent/storage-cloudflare/LedgerRequestAbortResult",
)("LedgerRequestAbortResult", {
  intent: AbortIntent,
}) {}

/**
 * Owner response to a request to record child settlement.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerRecordChildSettledResult extends Schema.TaggedClass<LedgerRecordChildSettledResult>(
  "@effect-agent/storage-cloudflare/LedgerRecordChildSettledResult",
)("LedgerRecordChildSettledResult", {
  outcome: ChildSettledOutcome,
}) {}

/**
 * Owner response to a request to materialize a Thread.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreMaterializeResult extends Schema.TaggedClass<StoreMaterializeResult>(
  "@effect-agent/storage-cloudflare/StoreMaterializeResult",
)("StoreMaterializeResult", {}) {}

/**
 * Owner response to a request to append canonical records.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreAppendResult extends Schema.TaggedClass<StoreAppendResult>(
  "@effect-agent/storage-cloudflare/StoreAppendResult",
)("StoreAppendResult", {
  result: AppendResult,
}) {}

/**
 * One page of canonical records, bounded by the request's `limit` (≤ 1,024).
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreReadPageResult extends Schema.TaggedClass<StoreReadPageResult>(
  "@effect-agent/storage-cloudflare/StoreReadPageResult",
)("StoreReadPageResult", {
  records: Schema.Array(ReadEnvelope).check(Schema.isMaxLength(1_024)),
}) {}

/**
 * Owner response to a request to read prompt history.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreReadPromptResult extends Schema.TaggedClass<StoreReadPromptResult>()(
  "StoreReadPromptResult",
  {
    records: Schema.Array(PromptRecordEnvelope).check(Schema.isMaxLength(1_024)),
  },
) {}

/**
 * Owner response to a request to inspect the committed tail.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreInspectTailResult extends Schema.TaggedClass<StoreInspectTailResult>(
  "@effect-agent/storage-cloudflare/StoreInspectTailResult",
)("StoreInspectTailResult", {
  tail: ThreadTail,
}) {}

/**
 * Owner response to a request to read execution state.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreReadExecutionResult extends Schema.TaggedClass<StoreReadExecutionResult>()(
  "StoreReadExecutionResult",
  { checkpoint: Schema.OptionFromNullOr(ExecutionCheckpoint) },
) {}

/**
 * Owner response to a request to read Thread identity.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreReadIdentityResult extends Schema.TaggedClass<StoreReadIdentityResult>(
  "@effect-agent/storage-cloudflare/StoreReadIdentityResult",
)("StoreReadIdentityResult", {
  identity: Schema.Struct({
    ...ThreadIdentity.fields,
    records: Schema.Array(ReadEnvelope).check(Schema.isMaxLength(3)),
  }).pipe(
    Schema.decodeTo(Schema.toType(ThreadIdentity), {
      decode: SchemaGetter.transform((fields) => ThreadIdentity.make(fields)),
      encode: SchemaGetter.transform((identity) => identity),
    }),
  ),
}) {}

/**
 * Owner response to a request to read a submission funding owner.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LedgerFundingOwnerResult extends Schema.TaggedClass<LedgerFundingOwnerResult>()(
  "LedgerFundingOwnerResult",
  { funding: FundingOwner },
) {}

/**
 * Owner response to a request to read worker capacity.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreWorkerCapacityResult extends Schema.TaggedClass<StoreWorkerCapacityResult>()(
  "StoreWorkerCapacityResult",
  { capacity: ThreadWorkerCapacity },
) {}

/**
 * Owner response to a request to verify retained Thread state.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreVerificationResult extends Schema.TaggedClass<StoreVerificationResult>()(
  "StoreVerificationResult",
  { report: IntegrityReport },
) {}

/**
 * Owner response to a request to export an idle Thread.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreExportResult extends Schema.TaggedClass<StoreExportResult>(
  "@effect-agent/storage-cloudflare/StoreExportResult",
)("StoreExportResult", {
  export: ThreadExport,
}) {}

/**
 * Owner response to a request to count peer messages.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreCountPeerMessagesResult extends Schema.TaggedClass<StoreCountPeerMessagesResult>()(
  "StoreCountPeerMessagesResult",
  { count: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1000 })) },
) {}

/**
 * Owner response to a request to read a page of pending work.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreWorkPageResult extends Schema.TaggedClass<StoreWorkPageResult>()(
  "StoreWorkPageResult",
  { page: ThreadWorkPage },
) {}

/**
 * Owner response to a request to rebuild the work index.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreWorkRebuildResult extends Schema.TaggedClass<StoreWorkRebuildResult>()(
  "StoreWorkRebuildResult",
  { progress: WorkIndexProgress },
) {}

/**
 * Owner response to a request to list message delivery obligations.
 *
 * @category schemas
 * @since 0.1.0
 */
export class MessageDeliveryListResult extends Schema.TaggedClass<MessageDeliveryListResult>()(
  "MessageDeliveryListResult",
  { page: MessageDeliveryPage },
) {}

/**
 * Owner response to a request to complete a message delivery.
 *
 * @category schemas
 * @since 0.1.0
 */
export class MessageDeliveryCompleteResult extends Schema.TaggedClass<MessageDeliveryCompleteResult>()(
  "MessageDeliveryCompleteResult",
  { record: MessageDeliveryRecord },
) {}

/**
 * Owner response to a request to read a canonical range.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreRangePageResult extends Schema.TaggedClass<StoreRangePageResult>()(
  "StoreRangePageResult",
  { page: ThreadArchiveRangePage },
) {}

/**
 * Owner response to a request to seal a canonical range.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreRangeSealResult extends Schema.TaggedClass<StoreRangeSealResult>()(
  "StoreRangeSealResult",
  { range: Schema.Option(ThreadArchiveRange) },
) {}

/**
 * Owner response to a request to archive a canonical range.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreRangeArchiveResult extends Schema.TaggedClass<StoreRangeArchiveResult>()(
  "StoreRangeArchiveResult",
  { range: ThreadArchiveRange },
) {}

/**
 * Owner response to a request to verify a canonical range.
 *
 * @category schemas
 * @since 0.1.0
 */
export class StoreRangeVerifyResult extends Schema.TaggedClass<StoreRangeVerifyResult>()(
  "StoreRangeVerifyResult",
  { range: ThreadArchiveRange },
) {}

/**
 * Every successful routed result. Callers narrow by the tag their request implies.
 *
 * @category schemas
 * @since 0.1.0
 */
export const PortResult = Schema.Union([
  SettlementPublishResult,
  WorkerAdmitResult,
  MessageDeliveryListResult,
  MessageDeliveryCompleteResult,
  LedgerAdmitResult,
  LedgerMarkReadyResult,
  LedgerLookupResult,
  LedgerFundingOwnerResult,
  LedgerResolveAdmissionResult,
  LedgerRequestAbortResult,
  LedgerStopWorkerResult,
  LedgerInspectWorkerResult,
  LedgerRecordChildSettledResult,
  StoreRangePageResult,
  StoreRangeSealResult,
  StoreRangeArchiveResult,
  StoreRangeVerifyResult,
  StoreMaterializeResult,
  StoreAppendResult,
  StoreReadPageResult,
  StoreReadPromptResult,
  StoreInspectTailResult,
  StoreReadIdentityResult,
  StoreReadExecutionResult,
  StoreWorkerCapacityResult,
  StoreVerificationResult,
  StoreExportResult,
  StoreCountPeerMessagesResult,
  StoreWorkPageResult,
  StoreWorkRebuildResult,
]);

/**
 * Decoded value of the PortResult schema.
 *
 * @category models
 * @since 0.1.0
 */
export type PortResult = typeof PortResult.Type;

// ---------------------------------------------------------------------------
// Failures and the response envelope
// ---------------------------------------------------------------------------

/**
 * Every typed failure a route-capable operation can produce on its owning Object, plus the
 * protocol's own `PortProtocolError`. Members re-decode to the SAME tagged classes the
 * thread ports declare, so a routed caller observes identical error tags and fields.
 *
 * @category schemas
 * @since 0.1.0
 */
export const PortFailure = Schema.Union([
  OwnershipLost,
  DurableRuntimeFailpointError,
  MessageDeliveryError,
  MessageDeliveryFailpointError,
  AdmissionConflict,
  AdmissionPolicyError,
  SettlementConflict,
  JoinedToHost,
  LedgerError,
  ThreadStoreError,
  WorkDiscoveryUnavailable,
  ThreadNotMaterialized,
  ThreadExportRejected,
  AppendConflict,
  PortProtocolError,
]);

/**
 * Typed failures returned by owner-routed storage operations.
 *
 * @category errors
 * @since 0.1.0
 */
export type PortFailure = typeof PortFailure.Type;

/**
 * The routed operation succeeded on its owning Object.
 *
 * @category schemas
 * @since 0.1.0
 */
export class PortSucceeded extends Schema.TaggedClass<PortSucceeded>(
  "@effect-agent/storage-cloudflare/PortSucceeded",
)("PortSucceeded", {
  result: PortResult,
}) {}

/**
 * The routed operation failed TYPED on its owning Object; the failure re-decodes verbatim.
 *
 * @category schemas
 * @since 0.1.0
 */
export class PortFailed extends Schema.TaggedClass<PortFailed>(
  "@effect-agent/storage-cloudflare/PortFailed",
)("PortFailed", {
  failure: PortFailure,
}) {}

/**
 * The uniform answer of one `portCall`: op-specific success or a re-decodable typed failure.
 *
 * @category schemas
 * @since 0.1.0
 */
export const PortResponse = Schema.Union([PortSucceeded, PortFailed]);
/**
 * Decoded value of the PortResponse schema.
 *
 * @category models
 * @since 0.1.0
 */
export type PortResponse = typeof PortResponse.Type;

/**
 * The wire form of one port response (what a transport actually carries).
 *
 * @category utility types
 * @since 0.1.0
 */
export type PortResponseEnvelope = typeof PortResponse.Encoded;

// ---------------------------------------------------------------------------
// Codecs
// ---------------------------------------------------------------------------

// Remote codecs derive their memoized encoding AST only when a port is used.
/**
 * Encode port request through its RPC schema.
 *
 * @category constructors
 * @since 0.1.0
 */
export const encodePortRequest: ReturnType<typeof Schema.encodeEffect<typeof PortRequest>> = (
  ...args
) => Schema.encodeEffect(PortRequest)(...args);

/**
 * Decode and validate port request through its RPC schema.
 *
 * @category constructors
 * @since 0.1.0
 */
export const decodePortRequest = Schema.decodeUnknownEffect(PortRequest);

/**
 * Encode port response through its RPC schema.
 *
 * @category constructors
 * @since 0.1.0
 */
export const encodePortResponse: ReturnType<typeof Schema.encodeEffect<typeof PortResponse>> = (
  ...args
) => Schema.encodeEffect(PortResponse)(...args);

/**
 * Decode and validate port response through its RPC schema.
 *
 * @category constructors
 * @since 0.1.0
 */
export const decodePortResponse = Schema.decodeUnknownEffect(PortResponse);
