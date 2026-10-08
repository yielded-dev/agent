/** Throwaway synchronous append rules. Copied from the current shared reducers for this spike. */
import type { RawAppendRequest } from "@yielded/agent-storage-sql/sql-journal";
import type { RunId, ToolCallId, ThreadId } from "@yielded/agent/identifiers";
import type { IdempotencyKey } from "@yielded/agent/receipt";
import {
  EvidenceReference,
  type ContinuationAccounting,
  type CanonicalRecord,
  type CanonicalSequence,
  type RunContinuation,
  MAX_RUN_CONTINUATION_BYTES,
  MAX_RUN_RECOVERY_SUFFIX_RECORDS,
  MAX_RUN_RECOVERY_SUFFIX_BYTES,
} from "@yielded/agent/records";
import {
  canonicalRecordBytes,
  executionRunIds,
  isPreContinuationFact,
} from "@yielded/agent/run-continuation";
import { runIdForSubmission } from "@yielded/agent/run-journal";
import { ThreadStoreError } from "@yielded/agent/thread-store";
import {
  workId,
  type WorkOwner,
  type WorkIndexChange,
  type ThreadWorkEntry,
  type WorkerReportingMode,
} from "@yielded/agent/thread-work";
import { Result, Schema } from "effect";

import type {
  DoAppendConflict,
  DoFenceRejected,
  DoStorageCorruptionError,
  DoStorageError,
  DoValueBoundExceeded,
} from "../DoStorageError.ts";

export type SyncAppendError =
  | ThreadStoreError
  | DoAppendConflict
  | DoFenceRejected
  | DoStorageCorruptionError
  | DoStorageError
  | DoValueBoundExceeded;

/** Internal unwinding only: the transaction adapter catches this identity and returns a tagged Result. */
export class AppendRefusal {
  constructor(readonly error: SyncAppendError) {}
}

export const reject = (error: SyncAppendError): never => {
  throw new AppendRefusal(error);
};

export const failure = (message: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation: "synchronous DO append",
    message,
    ...(cause === undefined ? {} : { cause }),
  });

export const decode = <A, I>(schema: Schema.Codec<A, I>, value: unknown): A => {
  const result = Schema.decodeUnknownResult(schema)(value);

  if (Result.isFailure(result))
    return reject(failure("Stored append metadata is malformed", result.failure));

  return result.success;
};

export const workReferenceTags = new Set<CanonicalRecord["payload"]["_tag"]>([
  "ModelResponseRecorded",
  "ToolCallUnknown",
  "ToolApprovalRequested",
  "ToolApprovalDecided",
  "ToolCallSettled",
  "ToolCallResolved",
  "SubagentRequested",
  "SubagentJoined",
  "WorkerInputRequested",
  "WorkerInputCompleted",
  "WorkerInputRefused",
  "WorkerOriginRecorded",
  "SubmissionSettled",
  "WorkerReportPrepared",
  "WorkerReportRefused",
  "PeerMessagePrepared",
  "AgentUpdateEmitted",
  "WorkerStopRequested",
  "SubtreeBudgetReserved",
  "WorkHandoffCompleted",
]);

export const workChanges = (
  record: CanonicalRecord,
  sequence: CanonicalSequence,
  reporting: WorkerReportingMode,
  ref: EvidenceReference,
): ReadonlyArray<WorkIndexChange> => {
  const payload = record.payload;

  const remove = (kind: string, ...ids: ReadonlyArray<string>): WorkIndexChange => ({
    _tag: "Remove",
    id: workId(kind, ...ids),
  });

  const put = (
    id: string,
    owner: WorkOwner,
    state: ThreadWorkEntry["state"],
    wait?: ThreadWorkEntry["wait"],
  ): WorkIndexChange => ({
    _tag: "Put",
    entry: {
      id,
      owner,
      stateReference: { _tag: "Canonical", ...ref },
      state,
      ...(wait === undefined ? {} : { wait }),
      createdSequence: sequence,
      updatedSequence: sequence,
      originRecordId: record.recordId,
      originDigest: ref.digest,
    },
  });

  const operation = (
    runId: RunId,
    toolCallId: ToolCallId,
    state: ThreadWorkEntry["state"],
    wait?: ThreadWorkEntry["wait"],
  ) =>
    put(
      workId("operation", runId, toolCallId),
      { _tag: "Operation", runId, toolCallId },
      state,
      wait,
    );

  const handoff = (
    kind: Extract<WorkOwner, { _tag: "Handoff" }>["kind"],
    messageId?: IdempotencyKey,
    childThreadId?: ThreadId,
  ) =>
    put(
      workId("handoff", record.recordId),
      {
        _tag: "Handoff",
        recordId: record.recordId,
        kind,
        ...(messageId === undefined ? {} : { messageId }),
        ...(childThreadId === undefined ? {} : { childThreadId }),
      },
      "handoff",
      "destination",
    );

  switch (payload._tag) {
    case "ModelResponseRecorded":
      return payload.toolOperations.map((call) =>
        operation(payload.runId, call.toolCallId, "ready"),
      );
    case "ToolCallUnknown":
      return [operation(payload.runId, payload.toolCallId, "unknown", "effect-resolution")];
    case "ToolApprovalRequested":
      return [operation(payload.runId, payload.toolCallId, "waiting", "approval")];
    case "ToolApprovalDecided":
      return [operation(payload.runId, payload.toolCallId, "ready")];
    case "ToolCallSettled":
      return [remove("operation", payload.runId, payload.toolCallId)];
    case "ToolCallResolved":
      return payload.resolution === "safe-retry"
        ? [operation(payload.runId, payload.toolCallId, "unknown", "effect-resolution")]
        : [remove("operation", payload.runId, payload.toolCallId)];
    case "SubagentRequested":
      return [
        remove("reservation", payload.reservationId),
        put(
          workId("child", payload.runId, payload.toolCallId),
          {
            _tag: "Child",
            runId: payload.runId,
            toolCallId: payload.toolCallId,
            childThreadId: payload.childThreadId,
          },
          "waiting",
          "child",
        ),
      ];
    case "SubagentJoined":
      return [remove("child", payload.runId, payload.toolCallId), handoff("child-accounting")];
    case "WorkerInputRequested":
      return [
        remove("reservation", payload.admission.messageId),
        put(
          workId("worker-input", payload.admission.messageId),
          {
            _tag: "WorkerInput",
            messageId: payload.admission.messageId,
            workerThreadId: payload.admission.origin.worker.threadId,
            update: payload.admission.reportKind === "update",
          },
          "waiting",
          "worker",
        ),
      ];
    case "WorkerInputCompleted":
      return payload.effectsResolved === true
        ? [
            remove("worker-input", payload.messageId),
            remove("worker-effects", payload.submissionId),
          ]
        : [];
    case "WorkerInputRefused":
      return [
        {
          _tag: "Remove",
          id: workId("worker-input", payload.messageId),
          stateRecordId: payload.reservation.recordId,
        },
        {
          _tag: "Remove",
          id: workId("reservation", payload.messageId),
          stateRecordId: payload.reservation.recordId,
        },
      ];
    case "WorkerOriginRecorded":
      return [
        {
          _tag: "WorkerMode",
          mode: payload.origin.reporting?.mode === "standard" ? "standard" : "private",
        },
      ];
    case "SubmissionSettled":
      return reporting === "none"
        ? []
        : [
            put(
              workId("worker-effects", payload.submissionId),
              { _tag: "WorkerEffects", submissionId: payload.submissionId },
              "waiting",
              "effect-resolution",
            ),
            ...(reporting === "standard" &&
            payload.runId === runIdForSubmission(payload.submissionId)
              ? [
                  put(
                    workId("report", payload.runId),
                    { _tag: "Report", runId: payload.runId },
                    "ready",
                  ),
                ]
              : []),
          ];
    case "WorkerReportPrepared":
      return [remove("report", payload.runId), handoff("report", payload.messageId)];
    case "WorkerReportRefused":
      return [remove("report", payload.runId)];
    case "PeerMessagePrepared":
      return [handoff("peer", payload.messageId)];
    case "AgentUpdateEmitted":
      return payload.delivery === undefined ? [] : [handoff("update", payload.delivery.messageId)];
    case "WorkerStopRequested":
      return [handoff("worker-stop", undefined, payload.command.worker.threadId)];
    case "SubtreeBudgetReserved":
      return [
        put(
          workId("reservation", payload.reservationId),
          {
            _tag: "Handoff",
            recordId: record.recordId,
            kind: "reservation",
            childThreadId: payload.childThreadId,
          },
          "waiting",
          "capacity",
        ),
      ];
    case "WorkHandoffCompleted":
      return [{ _tag: "Remove", id: payload.ownerId }];
    default:
      return [];
  }
};

export const validateProgress = (
  records: RawAppendRequest["progress"],
  reader: {
    previous(runId: RunId): RunContinuation | undefined;
    initial(next: RunContinuation): ReadonlyArray<CanonicalRecord>;
  },
): void => {
  const seen = new Set<RunId>();
  let progressStarted = false;

  for (const record of records) {
    const next = record.continuation;

    if (next === undefined) {
      if (progressStarted)
        return reject(failure("Execution facts cannot follow their continuation"));
      continue;
    }
    progressStarted = true;
    if (record.recordBytes > MAX_RUN_CONTINUATION_BYTES)
      return reject(failure("Encoded continuation exceeds its record byte bound"));
    if (seen.has(next.runId))
      return reject(failure("One atomic batch cannot publish conflicting Run progress"));
    seen.add(next.runId);

    const facts = records.filter(
      (fact) => fact.continuation === undefined && fact.runIds.includes(next.runId),
    );

    const frontier = facts.at(-1);
    const prior = reader.previous(next.runId);

    const retained =
      prior === undefined
        ? reader.initial(next).filter((fact) => executionRunIds(fact).includes(next.runId))
        : [];

    const retainedBytes = retained.reduce((bytes, fact) => bytes + canonicalRecordBytes(fact), 0);
    const factBytes = facts.reduce((bytes, fact) => bytes + fact.recordBytes, 0);
    const turnBase = prior?.turn === next.turn ? prior.turnBytes : retainedBytes;
    const closing = facts.filter((fact) => fact.terminal);

    if (
      frontier === undefined ||
      retained.length > MAX_RUN_RECOVERY_SUFFIX_RECORDS ||
      retainedBytes > MAX_RUN_RECOVERY_SUFFIX_BYTES ||
      retained.some(
        (fact) => !isPreContinuationFact(fact) || !executionRunIds(fact).includes(next.runId),
      ) ||
      frontier.recordId !== next.lastFact.recordId ||
      next.revision !== (prior?.revision ?? 0) + 1 ||
      next.recordBytes < (prior?.recordBytes ?? 0) ||
      next.turn < (prior?.turn ?? 0) ||
      (prior !== undefined && next.turn === prior.turn && next.turnBytes <= prior.turnBytes) ||
      next.recordBytes !== (prior?.recordBytes ?? retainedBytes) + factBytes ||
      next.terminalUsageBytes !==
        (prior?.terminalUsageBytes ?? 0) +
          facts.reduce((bytes, fact) => bytes + fact.terminalUsageBytes, 0) ||
      next.turnBytes !== turnBase + factBytes + record.recordBytes ||
      next.recordCount !== (prior?.recordCount ?? retained.length) + facts.length ||
      next.terminalRecords !== (prior?.terminalRecords ?? 0) + closing.length ||
      next.terminalBytes !==
        (prior?.terminalBytes ?? 0) +
          closing.reduce((bytes, fact) => bytes + fact.recordBytes, 0) +
          (closing.length > 0 ? record.recordBytes : 0) ||
      (prior !== undefined &&
        (prior.submissionId !== next.submissionId ||
          !Schema.toEquivalence(EvidenceReference)(prior.originalInput, next.originalInput) ||
          (prior.savedContext !== undefined &&
            (next.savedContext === undefined ||
              !Schema.toEquivalence(EvidenceReference)(prior.savedContext, next.savedContext))))) ||
      (prior !== undefined &&
        (
          [
            "committedTurns",
            "toolCalls",
            "programmaticToolCalls",
            "modelRestarts",
            "modelCalls",
            "inputTokens",
            "outputTokens",
            "costMicrousd",
            "unobservedModelCalls",
            "accountedToolTurn",
          ] satisfies ReadonlyArray<keyof ContinuationAccounting>
        ).some((field) => next.accounting[field] < prior.accounting[field])) ||
      (prior?.accounting.finalizationUsed === true && !next.accounting.finalizationUsed)
    )
      return reject(failure("Run continuation conflicts with its canonical frontier or revision"));
  }
};
