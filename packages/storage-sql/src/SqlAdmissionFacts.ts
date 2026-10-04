import {
  AbortIntent,
  ApprovalDecisionIntent,
  UnknownResolutionIntent,
} from "@yielded/agent/submission-ledger";
import { ThreadAdmission } from "@yielded/agent/thread-store";
import { Effect, Schema } from "effect";

const parseJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Json));
const decodeAdmission = Schema.decodeUnknownEffect(ThreadAdmission);
const decodeAbort = Schema.decodeEffect(AbortIntent);
const decodeApproval = Schema.decodeUnknownEffect(ApprovalDecisionIntent);
const decodeResolution = Schema.decodeUnknownEffect(UnknownResolutionIntent);

/**
 * Decode immutable facts after the adapter has validated its native row and storage bounds.
 * Ledger state, canonical application markers, caching and diagnostics remain adapter-owned.
 */
export const decodeAdmissionFact = Effect.fnUntraced(function* (row: {
  readonly submission_id: string;
  readonly thread_id: string;
  readonly receipt_id: string;
  readonly queue_sequence: number;
  readonly principal: string;
  readonly idempotency_key: string;
  readonly agent_id: string;
  readonly agent_digests_json: string;
  readonly deployment_id: string;
  readonly input_json: string;
  readonly input_digest: string;
  readonly created_at: string;
  readonly parent_submission_id: string | null;
  readonly parent_tool_call_id: string | null;
  readonly worker_admission_json: string | null;
  readonly message_admission_json: string | null;
  readonly admission_group: string | null;
  readonly admission_fence_json: string | null;
}): Effect.fn.Return<ThreadAdmission, Schema.SchemaError> {
  return yield* decodeAdmission({
    submissionId: row.submission_id,
    threadId: row.thread_id,
    receiptId: row.receipt_id,
    queueSequence: row.queue_sequence,
    principal: row.principal,
    idempotencyKey: row.idempotency_key,
    agentId: row.agent_id,
    agentDigests: yield* parseJson(row.agent_digests_json),
    deploymentId: row.deployment_id,
    inputPayload: yield* parseJson(row.input_json),
    inputDigest: row.input_digest,
    createdAt: row.created_at,
    // A partial parent identity must reach the schema and fail, never disappear.
    ...(row.parent_submission_id === null && row.parent_tool_call_id === null
      ? {}
      : {
          parentLinkage: {
            parentSubmissionId: row.parent_submission_id,
            parentToolCallId: row.parent_tool_call_id,
          },
        }),
    ...(row.worker_admission_json === null
      ? {}
      : { workerAdmission: yield* parseJson(row.worker_admission_json) }),
    ...(row.message_admission_json === null
      ? {}
      : { messageAdmission: yield* parseJson(row.message_admission_json) }),
    ...(row.admission_group === null ? {} : { admissionGroup: row.admission_group }),
    ...(row.admission_fence_json === null
      ? {}
      : { admissionFence: yield* parseJson(row.admission_fence_json) }),
  });
});

/** Accepted abort fact only; the ledger derives canonicalRecordId from canonical history. */
export const decodeAbortFact = (row: {
  readonly submission_id: string;
  readonly author: string;
  readonly reason: string;
  readonly requested_at: string;
}): Effect.Effect<AbortIntent, Schema.SchemaError> =>
  decodeAbort({
    submissionId: row.submission_id,
    author: row.author,
    reason: row.reason,
    requestedAt: row.requested_at,
  });

/** Accepted approval fact without a canonical application marker. */
export const decodeApprovalFact = (row: {
  readonly submission_id: string;
  readonly tool_call_id: string;
  readonly decision: string;
  readonly resolver: string;
  readonly reason: string;
  readonly decided_at: string;
}): Effect.Effect<ApprovalDecisionIntent, Schema.SchemaError> =>
  decodeApproval({
    submissionId: row.submission_id,
    toolCallId: row.tool_call_id,
    decision: row.decision,
    resolver: row.resolver,
    reason: row.reason,
    decidedAt: row.decided_at,
  });

/** Accepted unknown-resolution fact without a canonical application marker. */
export const decodeResolutionFact = Effect.fnUntraced(function* (row: {
  readonly submission_id: string;
  readonly tool_call_id: string;
  readonly author: string;
  readonly reason: string;
  readonly resolution_json: string;
  readonly resolved_at: string;
}): Effect.fn.Return<UnknownResolutionIntent, Schema.SchemaError> {
  return yield* decodeResolution({
    submissionId: row.submission_id,
    toolCallId: row.tool_call_id,
    author: row.author,
    reason: row.reason,
    resolution: yield* parseJson(row.resolution_json),
    resolvedAt: row.resolved_at,
  });
});
