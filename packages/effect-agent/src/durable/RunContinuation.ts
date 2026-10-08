import { Context, Crypto, DateTime, Effect, Option, Schema, Semaphore, Stream } from "effect";
import { Prompt } from "effect/ai";

import { AgentPersistenceCapacityError } from "../core/AgentError.ts";
import type { RunId, SubmissionId, ThreadId } from "../core/Identifiers.ts";
import { ToolCallId } from "../core/Identifiers.ts";
import { utf8ByteLength } from "../core/internal/utf8.ts";
import { summarizeModelUsage } from "../core/Usage.ts";
import { reference, resolveEvidence } from "./internal/evidence.ts";
export { reference, resolveEvidence } from "./internal/evidence.ts";
import {
  captureRecord,
  recordEncoding,
  type ProgressAppendRecord,
} from "./internal/record-encoding.ts";
import {
  canonicalRunIds,
  executionRunIds,
  isPreContinuationFact,
  isTerminalBudgetFact,
} from "./internal/record-ownership.ts";
export { terminalUsageCharge } from "./internal/record-ownership.ts";
import { readRunContext, validateContextBoundary } from "./internal/run-context.ts";
import {
  type CanonicalRecordEnvelope,
  type ContinuationAccounting,
  type DeploymentId,
  CanonicalBatch,
  CanonicalSequence,
  EvidenceReference,
  MAX_RUN_CONTINUATION_BYTES,
  MAX_RUN_EVIDENCE_BYTES,
  MAX_RUN_TERMINAL_BYTES,
  MAX_RUN_EVIDENCE_RECORDS,
  MAX_RUN_RECOVERY_SUFFIX_BYTES,
  MAX_RUN_RECOVERY_SUFFIX_RECORDS,
  MAX_TURN_CANONICAL_BYTES,
  MAX_PERSISTED_JSON_BYTES,
  RUN_TERMINAL_RESERVE_BYTES,
  RUN_TERMINAL_RESERVE_RECORDS,
  RecordEnvelope,
  RecordId,
  RunContinuation,
  type ModelResponseRecorded,
  type ToolCallSettled,
} from "./Records.ts";
import {
  runIdForSubmission,
  runCompletedRecordId,
  toolCallSettledRecordId,
  toolCallResultBatchId,
  subagentRequestedRecordId,
  subagentStartedRecordId,
} from "./RunJournal.ts";
import type { RunStorageSession, RunWriter } from "./RunStorage.ts";
import {
  submissionAbortRecordId,
  submissionInputRecordId,
  submissionSettlementRecordId,
  submissionSettlementBatchId,
} from "./SubmissionLedger.ts";
import { getRecord, getRunInput, ThreadReader, ThreadStoreError } from "./ThreadStore.ts";

export { RunContinuation, RunContextRecorded, EvidenceReference } from "./Records.ts";

/** The fenced writer selected at the owning resource boundary, never execution authority. */
export class CurrentRunWriter extends Context.Service<
  CurrentRunWriter,
  Pick<RunWriter, "threadId" | "tail" | "append">
>()("@effect-agent/thread/CurrentRunWriter") {}

/** Settlement publication keeps its original atomic authority and claim-scoped implementation. */
export class CurrentRunSettlement extends Context.Service<
  CurrentRunSettlement,
  Pick<RunStorageSession, "threadId" | "tail" | "publishSettlement">
>()("@effect-agent/thread/CurrentRunSettlement") {}

/** Canonical reads pinned to the append owner's current transaction or mutation gate. */
export class ProgressAppendReader extends Context.Service<
  ProgressAppendReader,
  {
    readonly previous: (
      runId: RunId,
    ) => Effect.Effect<RunContinuation | undefined, ThreadStoreError>;
    readonly initial: (
      next: RunContinuation,
    ) => Effect.Effect<ReadonlyArray<RecordEnvelope>, ThreadStoreError>;
  }
>()("@effect-agent/thread/ProgressAppendReader") {}

export {
  canonicalRunIds,
  executionRunIds,
  isWorkHandoff,
  isPreContinuationFact,
} from "./internal/record-ownership.ts";

const failure = (message: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation: "RunContinuation",
    message,
    ...(cause === undefined ? {} : { cause }),
  });

const capacityFailure = (message: string) =>
  failure(message, AgentPersistenceCapacityError.make({ message }));

const decodeResponsePrompt = Schema.decodeUnknownEffect(Prompt.Prompt);

const validateBatchHeader = Schema.decodeSync(
  Schema.Struct({
    batchId: CanonicalBatch.fields.batchId,
    producerId: CanonicalBatch.fields.producerId,
  }),
);

const validateBatchCount = Schema.decodeSync(
  Schema.NonEmptyArray(Schema.Unknown).check(Schema.isMaxLength(256)),
);

const ContinuationBytes = Schema.Struct({
  turnBytes: RunContinuation.fields.turnBytes,
  terminalBytes: RunContinuation.fields.terminalBytes,
});

/** One private snapshot for every retry; callers cannot change facts while awaiting the gate. */
const captureFacts = (batch: CanonicalBatch) =>
  Effect.try({
    try: () => {
      const header = validateBatchHeader(batch);

      validateBatchCount(batch.records);
      const records = batch.records.map((record) => captureRecord(record).canonical);

      return Object.freeze(
        new CanonicalBatch(
          { ...header, records: [records[0]!, ...records.slice(1)] },
          { disableChecks: true },
        ),
      );
    },
    catch: (cause) => failure("Cannot capture canonical progress facts", cause),
  });

/** Exact UTF-8 wire accounting reuses the append's privately captured Schema encoding. */
export const canonicalRecordBytes = (record: RecordEnvelope): number =>
  recordEncoding(record).bytes;

export type { ProgressAppendRecord } from "./internal/record-encoding.ts";

/** Capture before acquiring the mutation owner; summaries never cross a wire boundary. */
export const prepareProgressAppend = (
  records: ReadonlyArray<RecordEnvelope>,
): ReadonlyArray<ProgressAppendRecord> =>
  Object.freeze(records.map((record) => captureRecord(record).progress));

const factUsageCharge = (facts: ReadonlyArray<RecordEnvelope>): number =>
  facts.reduce((bytes, record) => bytes + recordEncoding(record).progress.terminalUsageBytes, 0);

export const readContinuation = Effect.fnUntraced(function* (
  threadId: ThreadId,
  runId: RunId,
  throughSequence: CanonicalSequence,
) {
  const reader = yield* ThreadReader;

  const records = yield* Stream.runCollect(
    reader.read({
      threadId,
      selection: { _tag: "RunContinuation", runId, throughSequence },
      page: { limit: 1 },
    }),
  );

  if (records.length > 1) return yield* failure("Ambiguous canonical Run continuation");
  const value = records[0];

  if (value === undefined)
    return Option.none<CanonicalRecordEnvelope & { continuation: RunContinuation }>();
  if (
    value.record.payload._tag !== "RunContinuation" ||
    value.record.payload.runId !== runId ||
    value.threadId !== threadId ||
    value.sequence > throughSequence ||
    canonicalRecordBytes(value.record) > MAX_RUN_CONTINUATION_BYTES
  )
    return yield* failure("Invalid canonical Run continuation locator");

  return Option.some({ ...value, continuation: value.record.payload });
});

/** Each selected page is at most eight records; total resident encoded evidence is bounded. */
export const runEvidence = (
  threadId: ThreadId,
  submissionId: SubmissionId,
  throughSequence: CanonicalSequence,
  afterSequence: CanonicalSequence = CanonicalSequence.make(0),
): Stream.Stream<CanonicalRecordEnvelope, ThreadStoreError, ThreadReader> =>
  Stream.unwrap(
    Effect.map(ThreadReader, (reader) =>
      Stream.suspend(() => {
        let after = afterSequence;
        let count = 0;
        let bytes = 0;
        const runId = runIdForSubmission(submissionId);

        const controls = new Set([
          submissionInputRecordId(submissionId),
          submissionAbortRecordId(submissionId),
          submissionSettlementRecordId(submissionId),
        ]);

        const next = (): Stream.Stream<CanonicalRecordEnvelope, ThreadStoreError> =>
          Stream.unwrap(
            Stream.runCollect(
              reader.read({
                threadId,
                selection: { _tag: "RunEvidence", runId, submissionId, throughSequence },
                page: { limit: 8, ...(after === 0 ? {} : { afterSequence: after }) },
              }),
            ).pipe(
              Effect.mapError((cause) =>
                cause._tag === "ThreadStoreError"
                  ? cause
                  : failure("Selected Run disappeared", cause),
              ),
              Effect.flatMap((page) =>
                Effect.gen(function* () {
                  if (page.length > 8)
                    return yield* failure("Run evidence page exceeds its record bound");
                  for (const entry of page) {
                    if (
                      entry.threadId !== threadId ||
                      entry.sequence <= after ||
                      entry.sequence > throughSequence ||
                      entry.record.payload._tag === "RunContinuation" ||
                      (!canonicalRunIds(entry.record).includes(runId) &&
                        !controls.has(entry.record.recordId))
                    )
                      return yield* failure("Run evidence page has invalid identity or ordering");
                    after = entry.sequence;
                    count += 1;
                    bytes += yield* Effect.try({
                      try: () => canonicalRecordBytes(entry.record),
                      catch: (cause) => failure("Selected Run evidence is malformed", cause),
                    });
                    if (
                      count > MAX_RUN_EVIDENCE_RECORDS + RUN_TERMINAL_RESERVE_RECORDS ||
                      bytes > MAX_RUN_EVIDENCE_BYTES + MAX_RUN_TERMINAL_BYTES
                    )
                      return yield* failure("Selected Run evidence exceeds its recovery bound");
                  }

                  return page.length < 8
                    ? Stream.fromIterable(page)
                    : Stream.concat(Stream.fromIterable(page), Stream.suspend(next));
                }),
              ),
            ),
          );

        return next();
      }),
    ),
  );

/** A bounded selected-Run suffix, never a Thread-wide tail or an implicit replay fallback. */
export const validateSuffix = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  through: CanonicalSequence,
) => {
  const suffix = records.filter((record) => record.sequence > through);

  return (
    suffix.length <= MAX_RUN_RECOVERY_SUFFIX_RECORDS &&
    suffix.reduce((bytes, record) => bytes + canonicalRecordBytes(record.record), 0) <=
      MAX_RUN_RECOVERY_SUFFIX_BYTES
  );
};

/**
 * Selected semantic evidence without expanding model context. Worker acknowledgements and
 * delivery repair must validate locator completeness before treating missing effects as closed.
 */
export const readRunEvidenceSnapshot = Effect.fnUntraced(function* (
  threadId: ThreadId,
  submissionId: SubmissionId,
  throughSequence: CanonicalSequence,
) {
  const records = yield* Stream.runCollect(runEvidence(threadId, submissionId, throughSequence));
  const runId = runIdForSubmission(submissionId);
  const own = records.filter((entry) => executionRunIds(entry.record).includes(runId));
  const progress = yield* readContinuation(threadId, runId, throughSequence);

  if (Option.isNone(progress)) {
    if (
      own.some(
        ({ record }) =>
          !isPreContinuationFact(record) ||
          (record.payload._tag === "SubmissionSettled" && record.payload.runId === runId),
      )
    )
      return yield* failure(
        "Selected execution has no canonical continuation; rebuild its locator explicitly",
      );

    return records;
  }
  const envelope = progress.value;
  const cursor = envelope.continuation;

  if (cursor.submissionId !== submissionId)
    return yield* failure("Selected continuation has another admitted owner");
  const byId = new Map(records.map((entry) => [entry.record.recordId, entry]));

  const resolve = Effect.fnUntraced(function* (ref: EvidenceReference) {
    const found = byId.get(ref.recordId);

    if (found === undefined) {
      yield* resolveEvidence(threadId, ref);

      return yield* failure(
        "Selected canonical locator omits required evidence; rebuild it explicitly",
      );
    }
    if ((yield* reference(found.record)).digest !== ref.digest)
      return yield* failure("Selected canonical evidence has invalid integrity");

    return found;
  });

  const input = yield* resolve(cursor.originalInput);

  if (
    input.record.payload._tag !== "UserInputRecorded" ||
    input.record.payload.submissionId !== submissionId ||
    input.record.payload.kind !== "user" ||
    input.record.payload.runId !== runId
  )
    return yield* failure("Selected continuation has no original accepted input");
  const frontier = yield* resolve(cursor.lastFact);

  if (
    frontier.batchId !== envelope.batchId ||
    frontier.sequence >= envelope.sequence ||
    own.filter((entry) => entry.sequence <= frontier.sequence).length !== cursor.recordCount ||
    !validateSuffix(own, envelope.sequence)
  )
    return yield* failure("Selected canonical evidence is incomplete or exceeds its suffix bound");
  if (cursor.savedContext !== undefined) {
    const context = yield* resolve(cursor.savedContext);

    if (
      context.record.payload._tag !== "RunContextRecorded" ||
      context.record.payload.runId !== runId
    )
      return yield* failure("Selected continuation has invalid saved context evidence");
  }
  if (cursor.latestResponse !== undefined) {
    const response = yield* resolve(cursor.latestResponse);

    if (
      response.record.payload._tag !== "ModelResponseRecorded" ||
      response.record.payload.runId !== runId ||
      response.record.payload.turn !== cursor.accounting.committedTurns
    )
      return yield* failure("Selected continuation has invalid operation evidence");
  }
  if (cursor.terminal !== undefined) {
    const terminal = yield* resolve(cursor.terminal);

    if (
      terminal.record.payload._tag !== "RunCompleted" &&
      terminal.record.payload._tag !== "RunFailed" &&
      terminal.record.payload._tag !== "SubmissionSettled"
    )
      return yield* failure("Selected continuation has invalid terminal evidence");
  }

  return records;
});

/** Called inside the adapter's existing mutation, before publishing any fact or index. */
export const validateProgressAppend = Effect.fnUntraced(function* (
  records: ReadonlyArray<ProgressAppendRecord>,
): Effect.fn.Return<void, ThreadStoreError, ProgressAppendReader> {
  const reader = yield* ProgressAppendReader;
  const seen = new Set<RunId>();
  let progressStarted = false;

  for (const record of records) {
    const next = record.continuation;

    if (next === undefined) {
      if (progressStarted)
        return yield* failure("Execution facts cannot follow their continuation");
      continue;
    }
    progressStarted = true;
    if (record.recordBytes > MAX_RUN_CONTINUATION_BYTES)
      return yield* failure("Encoded continuation exceeds its record byte bound");
    if (seen.has(next.runId))
      return yield* failure("One atomic batch cannot publish conflicting Run progress");
    seen.add(next.runId);

    const facts = records.filter(
      (fact) => fact.continuation === undefined && fact.runIds.includes(next.runId),
    );

    const frontier = facts.at(-1);
    const prior = yield* reader.previous(next.runId);

    const retained =
      prior === undefined
        ? (yield* reader.initial(next)).filter((fact) => executionRunIds(fact).includes(next.runId))
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
      return yield* failure("Run continuation conflicts with its canonical frontier or revision");
  }
});

interface ChildResultCapacity {
  readonly resultBytes: number;
  readonly remainingBytes: number;
  readonly remainingRecords: number;
}

interface ProgressState {
  readonly continuation: RunContinuation;
  readonly record: RecordEnvelope;
  readonly response?: ModelResponseRecorded;
  readonly results: ReadonlyMap<ToolCallId, ToolCallSettled>;
  readonly completionBytes?: number;
  readonly childResults: ReadonlyMap<ToolCallId, ChildResultCapacity>;
}

type CapacityState = Omit<ProgressState, "continuation" | "record"> & {
  readonly continuation: Pick<
    RunContinuation,
    | "runId"
    | "position"
    | "turn"
    | "turnBytes"
    | "recordBytes"
    | "recordCount"
    | "accounting"
    | "terminalUsageBytes"
    | "terminalBytes"
    | "terminalRecords"
  >;
};

/** Headers/identities must themselves fit a continuation. Leave room for a result's cursor too. */
const resultCapacity = (state: CapacityState): { bytes: number; records: number } => {
  const response = state.response;

  if (
    response === undefined ||
    state.continuation.position === "settling" ||
    state.continuation.position === "settled" ||
    state.continuation.accounting.accountedToolTurn >= response.turn
  )
    return { bytes: 0, records: 0 };
  let bytes = 0;
  let records = 0;

  for (const operation of response.toolOperations) {
    if (state.results.has(operation.toolCallId)) continue;

    const identity = JSON.stringify({
      recordId: toolCallSettledRecordId(response.runId, response.turn, operation.toolCallId),
      payload: {
        _tag: "ToolCallSettled",
        runId: response.runId,
        toolCallId: operation.toolCallId,
        toolName: operation.toolName,
        result: null,
        isFailure: false,
      },
    });

    // An uncertain ordinary call may later close with exact supplier/operator truth, rather
    // than the interpreter's truncated result. Attached joins retain their frozen allocation.
    const resultBytes =
      operation.executionKind === "ordinary" && operation.executionClass !== "readonly"
        ? MAX_PERSISTED_JSON_BYTES
        : Math.max(
            response.toolResultMaxBytes,
            state.childResults.get(operation.toolCallId)?.resultBytes ?? 0,
          );

    const joining = state.childResults.get(operation.toolCallId);

    bytes +=
      utf8ByteLength(identity) +
      resultBytes +
      (joining?.remainingBytes ?? 0) +
      response.toolSelectionMaxBytes +
      2 * MAX_RUN_CONTINUATION_BYTES +
      256;
    records += joining?.remainingRecords ?? 1;
  }

  return { bytes, records };
};

const emptyAccounting = (): ContinuationAccounting => ({
  committedTurns: 0,
  toolCalls: 0,
  programmaticToolCalls: 0,
  consecutiveToolFailures: 0,
  finalizationUsed: false,
  modelRestarts: 0,
  modelCalls: 0,
  unobservedModelCalls: 0,
  inputTokens: 0,
  outputTokens: 0,
  lastInputTokens: 0,
  lastOutputTokens: 0,
  costMicrousd: 0,
  accountedToolTurn: 0,
});

/** Recompute semantic progress solely from immutable owning facts. No port grants authority. */
const advanceFacts = Effect.fnUntraced(function* (
  previous: ProgressState | undefined,
  facts: ReadonlyArray<RecordEnvelope>,
  submissionId: SubmissionId,
  initialBytes: number,
) {
  const accounting = { ...(previous?.continuation.accounting ?? emptyAccounting()) };
  let savedContext = previous?.continuation.savedContext;
  let latestResponse = previous?.continuation.latestResponse;
  let terminal = previous?.continuation.terminal;
  let position = previous?.continuation.position ?? "starting";
  let response = previous?.response;
  let responsePrompt: Prompt.Prompt | undefined;
  let results = new Map(previous?.results);
  let childResults = new Map(previous?.childResults);
  let completionBytes = previous?.completionBytes;

  for (const fact of facts) {
    const payload = fact.payload;

    switch (payload._tag) {
      case "RunStarted":
        position = "awaiting-model";
        break;
      case "RunContextRecorded":
        if (savedContext !== undefined)
          return yield* failure("A Run's original context cannot be replaced");
        savedContext = yield* reference(fact);
        position = "awaiting-model";
        break;
      case "CompactionCreated": {
        if (payload.kind !== "native") break;
        if (savedContext === undefined || payload.native === undefined)
          return yield* failure("Native compaction has no saved context or accounted usage");
        const call = payload.native.usage;

        accounting.modelCalls += 1;
        accounting.inputTokens += call.inputTokens.total;
        accounting.outputTokens += call.outputTokens.total;
        accounting.costMicrousd += call.costMicrousd;
        break;
      }
      case "ModelResponseRecorded": {
        if (savedContext === undefined)
          return yield* failure("A model response has no saved original Run context");
        if (payload.turn !== accounting.committedTurns + 1)
          return yield* failure("Canonical Turn progress must advance once");

        const messages = yield* decodeResponsePrompt(payload.messages).pipe(
          Effect.mapError((cause) => failure("Invalid model evidence", cause)),
        );

        responsePrompt = messages;

        const calls = messages.content.flatMap((message) =>
          message.role === "assistant"
            ? message.content.filter((part) => part.type === "tool-call")
            : [],
        );

        const summary =
          payload.modelUsage === undefined
            ? undefined
            : yield* summarizeModelUsage(payload.modelUsage).pipe(
                Effect.mapError((cause) => failure("Invalid usage evidence", cause)),
              );

        accounting.committedTurns = payload.turn;
        accounting.toolCalls += calls.length;
        accounting.modelCalls += summary?.modelCalls ?? 1;
        accounting.inputTokens += summary?.inputTokens.total ?? payload.inputTokens ?? 0;
        accounting.outputTokens += summary?.outputTokens.total ?? payload.outputTokens ?? 0;
        accounting.costMicrousd += summary?.costMicrousd ?? payload.costMicrousd ?? 0;
        accounting.lastInputTokens = summary?.inputTokens.total ?? payload.inputTokens ?? 0;
        accounting.lastOutputTokens = summary?.outputTokens.total ?? payload.outputTokens ?? 0;
        accounting.unobservedModelCalls += payload.unobservedModelCalls ?? 0;
        latestResponse = yield* reference(fact);
        response = payload;
        results = new Map();
        childResults = new Map();
        position = payload.toolOperations.length === 0 ? "awaiting-model" : "processing-operations";
        break;
      }
      case "ToolCallSettled":
        results.set(payload.toolCallId, payload);
        childResults.delete(payload.toolCallId);
        break;
      case "ToolCallUnknown":
        position = "unknown";
        break;
      case "ToolCallResolved":
        position = "processing-operations";
        break;
      case "ToolApprovalRequested":
        position = "waiting-approval";
        break;
      case "ToolApprovalDecided":
        position = "processing-operations";
        break;
      case "SubagentRequested":
        childResults.set(payload.toolCallId, {
          resultBytes: Math.min(
            MAX_PERSISTED_JSON_BYTES,
            payload.budget?.allocation.resultBytes ?? MAX_PERSISTED_JSON_BYTES,
          ),
          remainingBytes: RUN_TERMINAL_RESERVE_BYTES,
          remainingRecords: 4,
        });
        position = "waiting-dependency";
        break;
      case "SubagentStarted":
        {
          const reserved = childResults.get(payload.toolCallId);

          if (reserved !== undefined)
            childResults.set(payload.toolCallId, {
              ...reserved,
              remainingBytes: Math.max(
                0,
                reserved.remainingBytes - canonicalRecordBytes(fact) - MAX_RUN_CONTINUATION_BYTES,
              ),
              remainingRecords: reserved.remainingRecords - 1,
            });
        }
        position = "waiting-dependency";
        break;
      case "RunPolicyUsageReserved":
        if (
          payload.programmaticToolCalls < accounting.programmaticToolCalls ||
          (accounting.finalizationUsed && !payload.finalizationUsed)
        )
          return yield* failure("Run reservations cannot be refunded");
        accounting.programmaticToolCalls = payload.programmaticToolCalls;
        accounting.finalizationUsed = payload.finalizationUsed;
        break;
      case "ModelCallAborted": {
        if (payload.restart !== (accounting.modelRestarts ?? 0) + 1)
          return yield* failure("Model restart progress must advance once");

        const summary = yield* summarizeModelUsage(payload.modelUsage).pipe(
          Effect.mapError((cause) => failure("Invalid cancelled model usage", cause)),
        );

        accounting.modelRestarts = payload.restart;
        accounting.modelCalls += summary.modelCalls;
        accounting.inputTokens += summary.inputTokens.total;
        accounting.outputTokens += summary.outputTokens.total;
        accounting.costMicrousd += summary.costMicrousd;
        accounting.unobservedModelCalls += payload.unobservedModelCalls;
        break;
      }
      case "RunCompleted":
      case "RunFailed":
        completionBytes =
          payload._tag === "RunCompleted"
            ? utf8ByteLength(JSON.stringify(payload.output)) +
              (payload.runDisposition === undefined
                ? 0
                : utf8ByteLength(JSON.stringify(payload.runDisposition)))
            : undefined;
        terminal = yield* reference(fact);
        position = "settling";
        break;
      case "SubmissionSettled":
        if (payload.submissionId !== submissionId) break;
        terminal = yield* reference(fact);
        position = "settled";
        if (payload.usageSummary !== undefined) {
          accounting.modelCalls = payload.usageSummary.modelCalls;
          accounting.inputTokens = payload.usageSummary.inputTokens.total;
          accounting.outputTokens = payload.usageSummary.outputTokens.total;
          accounting.costMicrousd = payload.usageSummary.costMicrousd;
          accounting.unobservedModelCalls =
            payload.usageSummary.unobservedModelCalls ?? accounting.unobservedModelCalls;
        }
        break;
      default:
        break;
    }
  }
  if (
    response !== undefined &&
    response.turn > accounting.accountedToolTurn &&
    response.toolOperations.every((operation) => results.has(operation.toolCallId))
  ) {
    const messages =
      responsePrompt ??
      (yield* decodeResponsePrompt(response.messages).pipe(
        Effect.mapError((cause) => failure("Invalid batch evidence", cause)),
      ));

    const parts = messages.content.flatMap((message) =>
      message.role === "assistant" ? message.content : [],
    );

    for (const call of parts) {
      if (call.type !== "tool-call") continue;

      const result = call.providerExecuted
        ? parts.find((part) => part.type === "tool-result" && part.id === call.id)
        : results.get(ToolCallId.make(call.id));

      if (
        result === undefined ||
        !("isFailure" in result) ||
        ("budgetRejected" in result && result.budgetRejected === true)
      )
        continue;
      accounting.consecutiveToolFailures = result.isFailure
        ? accounting.consecutiveToolFailures + 1
        : 0;
    }
    accounting.accountedToolTurn = response.turn;
    results.clear();
    if (position !== "settling" && position !== "settled") position = "awaiting-model";
  }
  const last = facts.at(-1);

  if (last === undefined) return yield* failure("Progress has no owning fact");
  if (previous?.continuation.position === "settled") position = "settled";
  else if (previous?.continuation.position === "settling" && position !== "settled")
    position = "settling";
  const factBytes = facts.reduce((bytes, fact) => bytes + canonicalRecordBytes(fact), 0);

  const turn = facts.reduce(
    (current, fact) => ("turn" in fact.payload ? Math.max(current, fact.payload.turn) : current),
    previous?.continuation.turn ?? 1,
  );

  const turnBase =
    (previous?.continuation.turn === turn ? previous.continuation.turnBytes : initialBytes) +
    factBytes;

  const usageCharge = factUsageCharge(facts);
  const terminalUsageBytes = (previous?.continuation.terminalUsageBytes ?? 0) + usageCharge;

  const closing = facts.filter(isTerminalBudgetFact);

  return {
    accounting,
    savedContext,
    latestResponse,
    terminal,
    position,
    response,
    results,
    childResults,
    completionBytes,
    last,
    factBytes,
    turn,
    turnBase,
    usageCharge,
    terminalUsageBytes,
    terminalBytes:
      (previous?.continuation.terminalBytes ?? 0) +
      closing.reduce((bytes, fact) => bytes + canonicalRecordBytes(fact), 0),
    terminalRecords: (previous?.continuation.terminalRecords ?? 0) + closing.length,
    closing: closing.length > 0,
  };
});

/**
 * Offline integrity guardrail. Every continuation is reproducible from its immutable facts;
 * this scan is explicit verification, never normal recovery or an execution authority.
 * Import skips range projection after checking original boundaries and retained references;
 * cold recovery must compare the saved Prompt digest before using that context.
 */
export const verifyRunContinuations = Effect.fnUntraced(function* (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  options?: { readonly verifyContext?: boolean },
) {
  const byId = new Map(records.map((entry) => [entry.record.recordId, entry]));
  const states = new Map<RunId, ProgressState>();
  const preparations = new Map<RunId, Array<RecordEnvelope>>();
  const equivalent = Schema.toEquivalence(RunContinuation);
  let start = 0;

  while (start < records.length) {
    const first = records[start]!;
    let end = start + 1;

    while (end < records.length && records[end]!.batchId === first.batchId) end++;
    const batch = records.slice(start, end);
    const seen = new Set<RunId>();
    let progressStarted = false;

    for (const entry of batch) {
      const cursor = entry.record.payload;

      if (cursor._tag !== "RunContinuation") {
        if (progressStarted) return yield* failure("Execution facts follow their continuation");
        continue;
      }
      progressStarted = true;
      if (seen.has(cursor.runId)) return yield* failure("Conflicting progress in one batch");
      seen.add(cursor.runId);
      const previous = states.get(cursor.runId);
      const initial = previous === undefined ? (preparations.get(cursor.runId) ?? []) : [];

      const facts = batch.filter(
        ({ record }) =>
          record.payload._tag !== "RunContinuation" &&
          executionRunIds(record).includes(cursor.runId),
      );

      const original =
        previous === undefined
          ? [...initial, ...facts.map(({ record }) => record)].find(
              ({ payload }) =>
                payload._tag === "UserInputRecorded" &&
                payload.kind === "user" &&
                payload.runId === cursor.runId &&
                payload.submissionId === cursor.submissionId,
            )
          : undefined;

      if (
        cursor.runId !== runIdForSubmission(cursor.submissionId) ||
        (previous === undefined && original === undefined)
      )
        return yield* failure("Continuation has no exact original admitted input");
      if (
        initial.some((fact) => !isPreContinuationFact(fact)) ||
        initial.length > MAX_RUN_RECOVERY_SUFFIX_RECORDS
      )
        return yield* failure("Run preparation exceeds its bounded canonical prefix");

      const initialBytes = initial.reduce(
        (bytes, record) => bytes + canonicalRecordBytes(record),
        0,
      );

      if (initialBytes > MAX_RUN_RECOVERY_SUFFIX_BYTES)
        return yield* failure("Run preparation exceeds its byte bound");
      for (const entry of facts) {
        const savedContext = entry.record.payload;

        if (savedContext._tag !== "RunContextRecorded") continue;

        const originalEntry = byId.get(
          previous?.continuation.originalInput.recordId ?? original!.recordId,
        );

        if (originalEntry === undefined || entry.sequence <= originalEntry.sequence)
          return yield* failure("Context has no exact original input boundary");
        yield* validateContextBoundary(savedContext, originalEntry).pipe(
          Effect.mapError((cause) => failure("Context has an invalid admission boundary", cause)),
        );
        if (options?.verifyContext !== false)
          yield* readRunContext(
            savedContext,
            originalEntry,
            (yield* reference(entry.record)).digest,
          ).pipe(
            Effect.mapError((cause) => failure("Context differs from its original history", cause)),
          );
      }

      const next = yield* advanceFacts(
        previous,
        facts.map(({ record }) => record),
        cursor.submissionId,
        initialBytes,
      );

      const frontier = facts.at(-1);

      if (
        frontier === undefined ||
        frontier.sequence >= entry.sequence ||
        frontier.batchId !== entry.batchId
      )
        return yield* failure("Continuation frontier was not atomically published");
      const cursorBytes = canonicalRecordBytes(entry.record);

      if (cursorBytes > MAX_RUN_CONTINUATION_BYTES)
        return yield* failure("Continuation exceeds its encoded bound");

      const expected = yield* RunContinuation.makeEffect({
        version: 1,
        runId: cursor.runId,
        submissionId: cursor.submissionId,
        revision: (previous?.continuation.revision ?? 0) + 1,
        recordCount: (previous?.continuation.recordCount ?? initial.length) + facts.length,
        recordBytes: (previous?.continuation.recordBytes ?? initialBytes) + next.factBytes,
        turn: next.turn,
        turnBytes: next.turnBase + cursorBytes,
        terminalBytes: next.terminalBytes + (next.closing ? cursorBytes : 0),
        terminalRecords: next.terminalRecords,
        terminalUsageBytes: next.terminalUsageBytes,
        originalInput: previous?.continuation.originalInput ?? (yield* reference(original!)),
        ...(next.savedContext === undefined ? {} : { savedContext: next.savedContext }),
        ...(next.latestResponse === undefined ? {} : { latestResponse: next.latestResponse }),
        ...(next.terminal === undefined ? {} : { terminal: next.terminal }),
        lastFact: yield* reference(frontier.record),
        position: next.position,
        accounting: next.accounting,
      }).pipe(Effect.mapError((cause) => failure("Invalid recomputed Run continuation", cause)));

      if (!equivalent(expected, cursor))
        return yield* failure("Continuation differs from its recomputable canonical facts");
      states.set(cursor.runId, {
        continuation: expected,
        record: entry.record,
        ...(next.response === undefined ? {} : { response: next.response }),
        results: next.results,
        childResults: next.childResults,
        ...(next.completionBytes === undefined ? {} : { completionBytes: next.completionBytes }),
      });
      preparations.delete(cursor.runId);
    }
    for (const { record } of batch) {
      if (record.payload._tag === "RunContinuation") continue;
      for (const runId of executionRunIds(record)) {
        if (seen.has(runId)) continue;
        if (states.has(runId))
          return yield* failure("Execution fact is missing its atomic continuation");
        const pending = preparations.get(runId) ?? [];

        pending.push(record);
        preparations.set(runId, pending);
      }
    }
    start = end;
  }
  for (const [runId, pending] of preparations) {
    const accepted = pending.some(
      ({ payload }) =>
        payload._tag === "UserInputRecorded" &&
        payload.kind === "user" &&
        payload.submissionId !== undefined &&
        runIdForSubmission(payload.submissionId) === runId,
    );

    if (
      accepted &&
      (pending.some((record) => !isPreContinuationFact(record)) ||
        pending.length > MAX_RUN_RECOVERY_SUFFIX_RECORDS ||
        pending.reduce((bytes, record) => bytes + canonicalRecordBytes(record), 0) >
          MAX_RUN_RECOVERY_SUFFIX_BYTES)
    )
      return yield* failure("Accepted Run execution is missing its canonical continuation");
  }
});

/**
 * Prepare progress for the existing atomic append/publication. Accept the returned state only
 * after that append commits; a failed or stale producer cannot publish speculative progress.
 * One writer retains bounded metadata for its active Run, not its historical results or IDs.
 */
export const makeProgressWriter = Effect.fnUntraced(function* (
  threadId: ThreadId,
  deploymentId: DeploymentId,
) {
  const reader = yield* ThreadReader;
  const crypto = yield* Crypto.Crypto;
  const gate = yield* Semaphore.make(1);
  const cached = new Map<RunId, ProgressState>();

  const reservations = new Map<
    RecordId,
    {
      readonly runId: RunId;
      readonly turn: number;
      readonly bytes: number;
      readonly records: number;
    }
  >();

  const stagedUsage = new Map<RunId, number>();

  const provide = <A, E>(effect: Effect.Effect<A, E, ThreadReader | Crypto.Crypto>) =>
    effect.pipe(
      Effect.provideService(ThreadReader, reader),
      Effect.provideService(Crypto.Crypto, crypto),
    );

  const loadState = Effect.fnUntraced(function* (runId: RunId, after: CanonicalSequence) {
    const retained = cached.get(runId);

    if (retained !== undefined) return retained;
    const loaded = yield* provide(readContinuation(threadId, runId, after));

    if (Option.isNone(loaded)) return undefined;
    const continuation = loaded.value.continuation;
    let response: ModelResponseRecorded | undefined;
    let completionBytes: number | undefined;
    const results = new Map<ToolCallId, ToolCallSettled>();

    const childResults = new Map<ToolCallId, ChildResultCapacity>();

    if (continuation.latestResponse !== undefined) {
      const evidence = yield* provide(resolveEvidence(threadId, continuation.latestResponse));

      if (
        evidence.record.payload._tag !== "ModelResponseRecorded" ||
        evidence.record.payload.runId !== runId
      )
        return yield* failure("Continuation operation reference is invalid");
      response = evidence.record.payload;
      if (continuation.accounting.accountedToolTurn < response.turn) {
        for (const operation of response.toolOperations) {
          const found = yield* provide(
            getRecord({
              threadId,
              recordId: toolCallSettledRecordId(runId, response.turn, operation.toolCallId),
            }),
          );

          if (Option.isSome(found)) {
            const result = found.value.record.payload;

            if (
              result._tag !== "ToolCallSettled" ||
              result.runId !== runId ||
              result.toolCallId !== operation.toolCallId ||
              result.toolName !== operation.toolName
            )
              return yield* failure("Continuation result identity is invalid");
            results.set(operation.toolCallId, result);
          } else if (operation.executionKind === "delegation") {
            const requested = yield* provide(
              getRecord({
                threadId,
                recordId: subagentRequestedRecordId(runId, operation.toolCallId),
              }),
            );

            if (
              Option.isSome(requested) &&
              requested.value.record.payload._tag === "SubagentRequested"
            ) {
              if (
                requested.value.record.payload.runId !== runId ||
                requested.value.record.payload.toolCallId !== operation.toolCallId
              )
                return yield* failure("Continuation child request identity is invalid");

              const started = yield* provide(
                getRecord({
                  threadId,
                  recordId: subagentStartedRecordId(runId, operation.toolCallId),
                }),
              );

              if (
                Option.isSome(started) &&
                (started.value.record.payload._tag !== "SubagentStarted" ||
                  started.value.record.payload.runId !== runId ||
                  started.value.record.payload.toolCallId !== operation.toolCallId ||
                  started.value.record.payload.childThreadId !==
                    requested.value.record.payload.childThreadId)
              )
                return yield* failure("Continuation child start identity is invalid");

              const startedBytes = Option.isSome(started)
                ? canonicalRecordBytes(started.value.record) + MAX_RUN_CONTINUATION_BYTES
                : 0;

              childResults.set(operation.toolCallId, {
                resultBytes: Math.min(
                  MAX_PERSISTED_JSON_BYTES,
                  requested.value.record.payload.budget?.allocation.resultBytes ??
                    MAX_PERSISTED_JSON_BYTES,
                ),
                remainingBytes: Math.max(0, RUN_TERMINAL_RESERVE_BYTES - startedBytes),
                remainingRecords: Option.isSome(started) ? 3 : 4,
              });
            }
          }
        }
      }
    }
    if (continuation.position === "settling" && continuation.terminal !== undefined) {
      const terminal = yield* provide(resolveEvidence(threadId, continuation.terminal));
      const payload = terminal.record.payload;

      if (payload._tag === "RunCompleted")
        completionBytes =
          utf8ByteLength(JSON.stringify(payload.output)) +
          (payload.runDisposition === undefined
            ? 0
            : utf8ByteLength(JSON.stringify(payload.runDisposition)));
    }

    return {
      continuation,
      record: loaded.value.record,
      ...(response === undefined ? {} : { response }),
      results,
      childResults,
      ...(completionBytes === undefined ? {} : { completionBytes }),
    };
  });

  const checkFutureShapes = (state: ProgressState): Effect.Effect<void, ThreadStoreError> => {
    const progress = state.continuation;

    if (progress.position === "settled") return Effect.void;
    const size = canonicalRecordBytes(state.record);
    const textBytes = (value: string) => utf8ByteLength(JSON.stringify(value));
    const refBytes = (value: EvidenceReference) => utf8ByteLength(JSON.stringify(value));

    const ref = (recordId: RecordId) =>
      EvidenceReference.make({ recordId, digest: progress.lastFact.digest });

    const fits = (batchId: string, lastFact: EvidenceReference, terminal?: EvidenceReference) =>
      // Only these identities change. The fixed allowance covers scalar and timestamp growth.
      size +
        textBytes(JSON.stringify(["continuation@1", progress.runId, batchId])) -
        textBytes(state.record.recordId) +
        textBytes(deploymentId) -
        textBytes(state.record.deploymentId) +
        refBytes(lastFact) -
        refBytes(progress.lastFact) +
        (terminal === undefined
          ? 0
          : refBytes(terminal) -
            (progress.terminal === undefined ? 0 : refBytes(progress.terminal)) +
            12) +
        1024 <=
      MAX_RUN_CONTINUATION_BYTES;

    if (
      !fits(
        submissionSettlementBatchId(progress.submissionId),
        ref(submissionSettlementRecordId(progress.submissionId)),
        ref(runCompletedRecordId(progress.runId)),
      )
    )
      return Effect.fail(capacityFailure("Run has no room for a terminal continuation envelope"));

    if (
      state.response !== undefined &&
      progress.accounting.accountedToolTurn < state.response.turn
    ) {
      for (const operation of state.response.toolOperations) {
        if (state.results.has(operation.toolCallId)) continue;
        if (
          !fits(
            toolCallResultBatchId(progress.runId, state.response.turn, operation.toolCallId),
            ref(toolCallSettledRecordId(progress.runId, state.response.turn, operation.toolCallId)),
          )
        )
          return Effect.fail(
            capacityFailure("Tool dispatch has no room for its result continuation envelope"),
          );
      }
    }

    return Effect.void;
  };

  const checkCapacity = (
    state: CapacityState,
    consumed: ReadonlySet<RecordId> = new Set(),
    extra?: { readonly turn: number; readonly bytes: number; readonly records: number },
    committedUsageBytes = 0,
  ): Effect.Effect<void, ThreadStoreError> => {
    const progress = state.continuation;
    const pending = resultCapacity(state);

    const terminal =
      progress.position === "settled"
        ? 0
        : Math.max(
            0,
            RUN_TERMINAL_RESERVE_BYTES +
              2 * (state.completionBytes ?? 0) +
              2 *
                (progress.terminalUsageBytes +
                  Math.max(0, (stagedUsage.get(progress.runId) ?? 0) - committedUsageBytes)) -
              progress.terminalBytes,
          );

    let turnBytes =
      (extra !== undefined && extra.turn !== progress.turn ? 0 : progress.turnBytes) +
      pending.bytes +
      terminal +
      (extra?.bytes ?? 0);

    let recordBytes = progress.recordBytes + pending.bytes + terminal + (extra?.bytes ?? 0);

    let recordCount =
      progress.recordCount +
      pending.records +
      (progress.position === "settled"
        ? 0
        : Math.max(0, RUN_TERMINAL_RESERVE_RECORDS - progress.terminalRecords)) +
      (extra?.records ?? 0);

    for (const [recordId, reservation] of reservations) {
      if (reservation.runId !== progress.runId || consumed.has(recordId)) continue;
      if (reservation.turn === (extra?.turn ?? progress.turn)) turnBytes += reservation.bytes;
      recordBytes += reservation.bytes;
      recordCount += reservation.records;
    }
    if (
      turnBytes > MAX_TURN_CANONICAL_BYTES ||
      recordBytes > MAX_RUN_EVIDENCE_BYTES ||
      recordCount > MAX_RUN_EVIDENCE_RECORDS
    )
      return Effect.fail(
        failure(
          "Run has no remaining canonical dispatch capacity",
          AgentPersistenceCapacityError.make({
            message: "Run has no room for the next result and a bounded terminal settlement",
          }),
        ),
      );

    return Effect.void;
  };

  const prepare = Effect.fnUntraced(function* (batch: CanonicalBatch, after: CanonicalSequence) {
    if (batch.records.some((record) => record.payload._tag === "RunContinuation"))
      return yield* failure("A caller cannot supply interpreter progress to the progress writer");

    const ownedRecords = yield* Effect.try({
      try: () => batch.records.map((record) => captureRecord(record).canonical),
      catch: (cause) => failure("Cannot capture canonical progress facts", cause),
    });

    const groups = new Map<RunId, Array<RecordEnvelope>>();

    for (const record of ownedRecords) {
      for (const runId of executionRunIds(record)) {
        const facts = groups.get(runId) ?? [];

        facts.push(record);
        groups.set(runId, facts);
      }
    }
    const continuations: Array<RecordEnvelope> = [];
    const accepted = new Map<RunId, ProgressState>();

    for (const [runId, facts] of groups) {
      const previous = yield* loadState(runId, after);

      let original = facts.find(
        (record) =>
          record.payload._tag === "UserInputRecorded" &&
          record.payload.kind === "user" &&
          record.payload.runId === runId &&
          record.payload.submissionId !== undefined,
      );

      let originalCount = 0;
      let initialBytes = 0;

      if (previous === undefined && original === undefined) {
        const input = yield* provide(getRunInput({ threadId, runId }));

        if (
          Option.isNone(input) ||
          input.value.record.payload._tag !== "UserInputRecorded" ||
          input.value.record.payload.submissionId === undefined
        )
          continue;

        original = input.value.record;
      }

      const submissionId =
        previous?.continuation.submissionId ??
        (original?.payload._tag === "UserInputRecorded"
          ? original.payload.submissionId
          : undefined);

      if (submissionId === undefined)
        return yield* failure("Run progress has no original admission identity");
      if (runId !== runIdForSubmission(submissionId))
        return yield* failure("Run progress conflicts with its original admission");
      if (previous === undefined && original === undefined)
        return yield* failure("Run has no accepted original input");

      if (previous === undefined) {
        const selected = yield* provide(
          Stream.runCollect(
            runEvidence(threadId, submissionId, after).pipe(
              Stream.take(MAX_RUN_RECOVERY_SUFFIX_RECORDS + 1),
            ),
          ),
        );

        const existing = selected.filter(({ record }) => executionRunIds(record).includes(runId));

        if (
          !validateSuffix(existing, CanonicalSequence.make(0)) ||
          existing.some(({ record }) => !isPreContinuationFact(record))
        )
          return yield* failure(
            "Run execution has no canonical continuation or exceeds its preparation bound",
          );
        originalCount = existing.length;
        initialBytes = existing.reduce(
          (bytes, entry) => bytes + canonicalRecordBytes(entry.record),
          0,
        );
      }

      const originalInput =
        previous !== undefined
          ? previous.continuation.originalInput
          : original === undefined
            ? yield* failure("Run progress has no original input")
            : yield* provide(reference(original));

      const next = yield* provide(advanceFacts(previous, facts, submissionId, initialBytes));

      const {
        accounting,
        savedContext,
        latestResponse,
        terminal,
        position,
        response,
        results,
        childResults,
        completionBytes,
        last,
        factBytes,
        turn,
        turnBase,
        usageCharge,
        terminalUsageBytes,
      } = next;

      let continuation = yield* RunContinuation.makeEffect({
        version: 1,
        runId,
        submissionId,
        revision: (previous?.continuation.revision ?? 0) + 1,
        recordCount: (previous?.continuation.recordCount ?? originalCount) + facts.length,
        recordBytes: (previous?.continuation.recordBytes ?? initialBytes) + factBytes,
        turn,
        turnBytes: turnBase,
        terminalUsageBytes,
        terminalBytes: next.terminalBytes,
        terminalRecords: next.terminalRecords,
        originalInput,
        ...(savedContext === undefined ? {} : { savedContext }),
        ...(latestResponse === undefined ? {} : { latestResponse }),
        ...(terminal === undefined ? {} : { terminal }),
        lastFact: yield* provide(reference(last)),
        position,
        accounting,
      }).pipe(
        Effect.mapError(() =>
          capacityFailure("Canonical continuation exceeds its protocol bounds"),
        ),
      );

      const header = {
        recordId: RecordId.make(JSON.stringify(["continuation@1", runId, batch.batchId])),
        family: "thread" as const,
        schemaVersion: 1 as const,
        createdAt: last.createdAt,
        deploymentId: last.deploymentId,
      };

      // The validated continuation fields are JSON; only the envelope DateTime needs encoding.
      // Key order does not change byte width. Measure without capturing a provisional record,
      // then compare against the final Schema encoding before accepting its accounting.
      const provisional = {
        ...header,
        createdAt: DateTime.formatIso(header.createdAt),
        payload: continuation,
      } satisfies typeof RecordEnvelope.Encoded;

      const initialCursorBytes = utf8ByteLength(JSON.stringify(provisional));

      const initialWidth =
        String(continuation.turnBytes).length + String(continuation.terminalBytes).length;

      let turnBytes = continuation.turnBytes;
      let terminalBytes = continuation.terminalBytes;

      for (let attempts = 0; ; attempts++) {
        const cursorBytes =
          initialCursorBytes +
          String(turnBytes).length +
          String(terminalBytes).length -
          initialWidth;

        const nextTurnBytes = turnBase + cursorBytes;
        const nextTerminalBytes = next.terminalBytes + (next.closing ? cursorBytes : 0);

        if (nextTurnBytes === turnBytes && nextTerminalBytes === terminalBytes) break;
        if (attempts >= 4) return yield* failure("Continuation byte accounting did not converge");
        turnBytes = nextTurnBytes;
        terminalBytes = nextTerminalBytes;
      }

      const checkedBytes = yield* ContinuationBytes.makeEffect({
        turnBytes,
        terminalBytes,
      }).pipe(
        Effect.mapError(() =>
          capacityFailure("Turn exceeds its incremental canonical byte budget"),
        ),
      );

      // Every other field was checked above; byte accounting changes only these counters.
      continuation = new RunContinuation(
        { ...continuation, ...checkedBytes },
        { disableChecks: true },
      );

      const record = captureRecord(
        new RecordEnvelope({ ...header, payload: continuation }, { disableChecks: true }),
      ).canonical;

      if (canonicalRecordBytes(record) !== turnBytes - turnBase)
        return yield* failure("Continuation byte accounting differs from its Schema encoding");
      if (canonicalRecordBytes(record) > MAX_RUN_CONTINUATION_BYTES)
        return yield* capacityFailure(
          "Encoded continuation exceeds 8192 bytes including its envelope",
        );
      continuations.push(record);

      const state = {
        continuation,
        record,
        ...(response === undefined ? {} : { response }),
        results,
        childResults,
        ...(completionBytes === undefined ? {} : { completionBytes }),
      };

      if (
        !facts.every((fact) => isTerminalBudgetFact(fact) && fact.payload._tag !== "RunCompleted")
      )
        yield* checkCapacity(
          state,
          new Set(batch.records.map(({ recordId }) => recordId)),
          undefined,
          usageCharge,
        );
      yield* checkFutureShapes(state);
      accepted.set(runId, state);
    }

    if (ownedRecords.length + continuations.length > 256)
      return yield* failure("Progress and facts exceed the atomic batch bound");

    const prepared = new CanonicalBatch(
      {
        ...batch,
        records: [ownedRecords[0]!, ...ownedRecords.slice(1), ...continuations],
      },
      { disableChecks: true },
    );

    return {
      batch: prepared,
      accept: () => {
        for (const record of batch.records) reservations.delete(record.recordId);
        for (const [runId, state] of accepted) cached.set(runId, state);
        // Administrative writers can address many old Runs; retention is still bounded.
        while (cached.size > 8) {
          const first = cached.keys().next().value;

          if (first !== undefined) cached.delete(first);
        }
      },
    };
  });

  const tail = Effect.gen(function* () {
    const writer = yield* CurrentRunWriter;

    if (writer.threadId !== threadId)
      return yield* failure("Progress writer belongs to another Thread");

    return (yield* writer.tail).sequence;
  });

  const commitCaptured = Effect.fnUntraced(function* (batch: CanonicalBatch) {
    const writer = yield* CurrentRunWriter;

    for (let retries = 0; ; retries++) {
      const prepared = yield* prepare(batch, yield* tail).pipe(
        Effect.catchTag("ThreadNotMaterialized", (cause) =>
          failure("Selected Run disappeared during progress publication", cause),
        ),
      );

      const result = yield* writer.append(prepared.batch).pipe(
        Effect.catchTag("AppendConflict", (conflict) => {
          if (conflict.reason !== "tail" || retries >= 8) return Effect.fail(conflict);
          cached.clear();

          return Effect.succeed(undefined);
        }),
      );

      if (result === undefined) continue;

      if (result.replayed) cached.clear();
      else prepared.accept();

      return result;
    }
  }, gate.withPermits(1));

  const publishCaptured = Effect.fnUntraced(function* (batch: CanonicalBatch) {
    const publisher = yield* CurrentRunSettlement;

    if (publisher.threadId !== threadId)
      return yield* failure("Settlement publisher belongs to another Thread");

    for (let retries = 0; ; retries++) {
      const prepared = yield* prepare(batch, (yield* publisher.tail).sequence).pipe(
        Effect.catchTag("ThreadNotMaterialized", (cause) =>
          failure("Selected Run disappeared during progress publication", cause),
        ),
      );

      const result = yield* publisher.publishSettlement(prepared.batch).pipe(
        Effect.catchTag("AppendConflict", (conflict) => {
          if (conflict.reason !== "tail" || retries >= 8) return Effect.fail(conflict);
          cached.clear();

          return Effect.succeed(undefined);
        }),
      );

      if (result === undefined) continue;

      if (result.replayed) cached.clear();
      else prepared.accept();

      return result;
    }
  }, gate.withPermits(1));

  const commit = (batch: CanonicalBatch) =>
    captureFacts(batch).pipe(Effect.flatMap(commitCaptured));

  const publish = (batch: CanonicalBatch) =>
    captureFacts(batch).pipe(Effect.flatMap(publishCaptured));

  const reserve = (runId: RunId, probe: CanonicalBatch, bytes: number) =>
    gate.withPermits(1)(
      Effect.gen(function* () {
        const after = yield* tail;

        yield* prepare(yield* captureFacts(probe), after);
        const recordId = probe.records[0].recordId;
        const state = yield* loadState(runId, after);

        if (state === undefined) return yield* failure("Dispatch has no canonical Run progress");
        if (reservations.has(recordId))
          return yield* failure("Dispatch capacity is already reserved");
        yield* checkCapacity(state, new Set(), {
          turn: state.continuation.turn,
          bytes,
          records: 1,
        });
        reservations.set(recordId, { runId, turn: state.continuation.turn, bytes, records: 1 });

        return gate.withPermits(1)(
          Effect.sync(() => {
            reservations.delete(recordId);
          }),
        );
      }),
    );

  const defer = (batch: CanonicalBatch) =>
    gate.withPermits(1)(
      Effect.gen(function* () {
        const after = yield* tail;

        const owned = yield* captureFacts(batch);

        yield* prepare(owned, after);

        const response = owned.records.find(
          (record) => record.payload._tag === "ModelResponseRecorded",
        );

        if (response === undefined || response.payload._tag !== "ModelResponseRecorded")
          return yield* failure("Deferred dispatch has no response");
        const progress = yield* loadState(response.payload.runId, after);

        if (progress === undefined) return yield* failure("Deferred dispatch has no Run progress");

        const pending = resultCapacity({
          ...progress,
          continuation: {
            ...progress.continuation,
            accounting: { ...progress.continuation.accounting, accountedToolTurn: 0 },
          },
          response: response.payload,
          results: new Map(),
          childResults: new Map(),
        });

        const bytes =
          owned.records.reduce((total, record) => total + canonicalRecordBytes(record), 0) +
          MAX_RUN_CONTINUATION_BYTES +
          pending.bytes;

        const records = owned.records.length + pending.records;

        yield* checkCapacity(progress, new Set(), { turn: response.payload.turn, bytes, records });
        reservations.set(response.recordId, {
          runId: response.payload.runId,
          turn: response.payload.turn,
          bytes,
          records,
        });

        return owned;
      }),
    );

  const check = (runId: RunId) =>
    gate.withPermits(1)(
      Effect.gen(function* () {
        const state = yield* loadState(runId, yield* tail);

        if (state === undefined) return yield* failure("Execution has no canonical Run progress");
        yield* checkFutureShapes(state);
        if (state.continuation.position !== "settling" && state.continuation.position !== "settled")
          yield* checkCapacity(state);
      }),
    );

  return {
    commit,
    publish,
    reserve,
    defer,
    check,
    stageUsage: (runId: RunId, bytes: number) => {
      if (bytes === 0) stagedUsage.delete(runId);
      else stagedUsage.set(runId, bytes);
    },
  };
});
