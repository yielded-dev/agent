import type { RunId } from "../../core/Identifiers.ts";
import { utf8ByteLength } from "../../core/internal/utf8.ts";
import type { ModelCallUsage } from "../../core/Usage.ts";
import type { RecordEnvelope } from "../Records.ts";
import { runIdForSubmission } from "../RunJournal.ts";

/** Index membership comes from canonical owner fields, never parsing opaque identities. */
export const canonicalRunIds = ({ payload }: RecordEnvelope): ReadonlyArray<RunId> => {
  const ids = new Set<RunId>();

  if ("runId" in payload && payload.runId !== undefined) ids.add(payload.runId);
  if (payload._tag === "AbortRequested" || payload._tag === "SubmissionSettled")
    ids.add(runIdForSubmission(payload.submissionId));
  if (payload._tag === "AgentUpdateEmitted") ids.add(payload.update.runId);
  if (payload._tag === "WorkerInputRequested" && payload.admission.executionRunId !== null)
    ids.add(payload.admission.executionRunId);
  if (payload._tag === "SubtreeBudgetReserved" && payload.executionRunId !== null)
    ids.add(payload.executionRunId);
  if (payload._tag === "PeerMessagePrepared" && payload.source._tag === "tool")
    ids.add(payload.source.runId);

  return [...ids];
};

/** A joined Receipt's closure is discoverable from its host, but owns no host execution charge. */
export const executionRunIds = (record: RecordEnvelope): ReadonlyArray<RunId> =>
  record.payload._tag === "SubmissionSettled"
    ? [runIdForSubmission(record.payload.submissionId)]
    : canonicalRunIds(record);

/**
 * These immutable preparations are durable handoff intents before a destination row exists.
 * Their canonical tag/identity index is published with the preparation and survives Run
 * settlement. WorkerInputCompleted closes capacity only with factual effectsResolved evidence;
 * ToolCallSettled/Resolved close operations, and destination delivery state owns transport closure.
 */
export const isWorkHandoff = ({ payload }: RecordEnvelope): boolean =>
  payload._tag === "WorkerInputRequested" ||
  payload._tag === "WorkerReportPrepared" ||
  payload._tag === "PeerMessagePrepared" ||
  payload._tag === "WorkerStopRequested" ||
  payload._tag === "SubtreeBudgetReserved" ||
  payload._tag === "SubagentRequested" ||
  (payload._tag === "AgentUpdateEmitted" && payload.delivery !== undefined);

/** A host can prepare independent work before the source admission ever starts a Run. */
export const isPreContinuationFact = (record: RecordEnvelope): boolean =>
  (isWorkHandoff(record) &&
    record.payload._tag !== "SubagentRequested" &&
    record.payload._tag !== "AgentUpdateEmitted") ||
  (record.payload._tag === "UserInputRecorded" && record.payload.kind === "user") ||
  record.payload._tag === "AbortRequested" ||
  record.payload._tag === "SubmissionSettled";

/** Grouped usage is no larger than its original identities/components plus bounded counters. */
export const terminalUsageCharge = (calls: ReadonlyArray<ModelCallUsage>): number =>
  calls.reduce((bytes, call) => bytes + utf8ByteLength(JSON.stringify(call)) + 512, 0);

/** These facts end accepted work or durably request its termination. */
export const isTerminalBudgetFact = ({ payload }: RecordEnvelope): boolean =>
  payload._tag === "AbortRequested" ||
  payload._tag === "RunFailed" ||
  payload._tag === "RunCompleted" ||
  payload._tag === "SubmissionSettled";
