/**
 * Shared submission row fields with adapter-supplied text and sequence codecs.
 *
 * @since 0.1.0
 */
import {
  ApprovalDecision,
  Digest,
  RecordId,
  SettlementOutcome,
  type CanonicalSequence,
} from "@yielded/agent/records";
import {
  AdmissionGroup,
  ChildReservationStatus,
  SubmissionState,
  UnknownResolutionKind,
  type QueueSequence,
} from "@yielded/agent/submission-ledger";
import * as Schema from "effect/Schema";

/**
 * Nonempty persisted identifier with a maximum length of 1,024 UTF-16 code units.
 *
 * @category schemas
 * @since 0.1.0
 */
export const BoundedIdentifier = Schema.NonEmptyString.check(Schema.isMaxLength(1024));
/**
 * Nonempty persisted timestamp with a maximum length of 128 UTF-16 code units.
 *
 * @category schemas
 * @since 0.1.0
 */
export const BoundedTimestamp = Schema.NonEmptyString.check(Schema.isMaxLength(128));

/**
 * Shared ledger row fields with adapter-owned text bounds and native sequence codecs.
 * Keep row classes and decoders in each adapter so cached row identities and diagnostics
 * remain local. This does not normalize native values before their schema validates them.
 *
 * @category constructors
 * @since 0.1.0
 */
export const makeSubmissionRowFields = <
  Text extends Schema.Codec<string>,
  Queue extends Schema.Codec<QueueSequence, number | bigint>,
  Sequence extends Schema.Codec<CanonicalSequence, number | bigint>,
>({
  storedText,
  queueSequence,
  canonicalSequence,
}: {
  readonly storedText: Text;
  readonly queueSequence: Queue;
  readonly canonicalSequence: Sequence;
}) => ({
  submission: {
    submission_id: BoundedIdentifier,
    thread_id: BoundedIdentifier,
    queue_sequence: queueSequence,
    principal: BoundedIdentifier,
    idempotency_key: BoundedIdentifier,
    agent_id: BoundedIdentifier,
    agent_digests_json: storedText,
    deployment_id: BoundedIdentifier,
    input_json: storedText,
    input_digest: Digest,
    receipt_id: BoundedIdentifier,
    state: SubmissionState,
    settled_outcome: Schema.NullOr(SettlementOutcome),
    settled_record_id: Schema.NullOr(BoundedIdentifier.pipe(Schema.decodeTo(RecordId))),
    finalized_at: Schema.NullOr(BoundedTimestamp),
    created_at: BoundedTimestamp,
    ready_at: Schema.NullOr(BoundedTimestamp),
    input_applied_record_id: Schema.NullOr(BoundedIdentifier),
    input_applied_sequence: Schema.NullOr(canonicalSequence),
    joined_host_submission_id: Schema.NullOr(BoundedIdentifier),
    suspended_reason_json: Schema.NullOr(storedText),
    suspended_at: Schema.NullOr(BoundedTimestamp),
    unknown_reason: Schema.NullOr(storedText),
    unknown_tool_call_ids_json: Schema.NullOr(storedText),
    parent_submission_id: Schema.NullOr(BoundedIdentifier),
    parent_tool_call_id: Schema.NullOr(BoundedIdentifier),
    admission_group: Schema.NullOr(AdmissionGroup),
    admission_fence_json: Schema.NullOr(storedText),
    worker_admission_json: Schema.NullOr(storedText),
    message_admission_json: Schema.NullOr(storedText),
  },
  childReservation: {
    reservation_id: BoundedIdentifier,
    parent_submission_id: BoundedIdentifier,
    parent_tool_call_id: BoundedIdentifier,
    child_submission_id: Schema.NullOr(BoundedIdentifier),
    status: ChildReservationStatus,
    allocation_json: storedText,
    allocation_digest: Digest,
    accounting_json: Schema.NullOr(storedText),
    reserved_at: BoundedTimestamp,
    release_began_at: Schema.NullOr(BoundedTimestamp),
    released_at: Schema.NullOr(BoundedTimestamp),
  },
  approvalDecision: {
    submission_id: BoundedIdentifier,
    tool_call_id: BoundedIdentifier,
    decision: ApprovalDecision,
    resolver: BoundedIdentifier,
    reason: storedText,
    decided_at: BoundedTimestamp,
  },
  unknownResolution: {
    submission_id: BoundedIdentifier,
    tool_call_id: BoundedIdentifier,
    resolution_kind: UnknownResolutionKind,
    author: BoundedIdentifier,
    reason: storedText,
    resolution_json: storedText,
    resolved_at: BoundedTimestamp,
  },
  abortIntent: {
    submission_id: BoundedIdentifier,
    author: BoundedIdentifier,
    reason: storedText,
    requested_at: BoundedTimestamp,
    canonical_record_id: Schema.NullOr(BoundedIdentifier),
  },
});
