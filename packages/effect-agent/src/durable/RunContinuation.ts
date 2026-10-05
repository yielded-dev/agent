import { Context, Crypto, Effect, Option, Schema, Semaphore, Stream } from "effect";
import { Prompt } from "effect/ai";

import { AgentPersistenceCapacityError } from "../core/AgentError.ts";
import { RunId, SubmissionId, ThreadId, ToolCallId } from "../core/Identifiers.ts";
import { utf8ByteLength } from "../core/internal/utf8.ts";
import { IdempotencyKey } from "../core/Receipt.ts";
import { summarizeModelUsage } from "../core/Usage.ts";
import type { ModelCallUsage } from "../core/Usage.ts";
import { digestJson } from "./Digest.ts";
import { ExportedRecord } from "./RecordFormat.ts";
import {
  type CanonicalRecordEnvelope,
  type ContinuationAccounting,
  type DeploymentId,
  CanonicalBatch,
  CanonicalSequence,
  EvidenceReference,
  MAX_RUN_CONTINUATION_BYTES,
  MAX_RUN_EVIDENCE_BYTES,
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

export const WorkOwner = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Admission"), submissionId: SubmissionId }),
  Schema.Struct({ _tag: Schema.Literal("Run"), runId: RunId }),
  Schema.Struct({ _tag: Schema.Literal("Operation"), runId: RunId, toolCallId: ToolCallId }),
  Schema.Struct({ _tag: Schema.Literal("Delivery"), messageId: IdempotencyKey }),
  Schema.Struct({ _tag: Schema.Literal("Handoff"), evidence: EvidenceReference }),
]);

export const ThreadWorkRequest = Schema.Struct({
  threadId: ThreadId,
  cursor: Schema.optionalKey(Schema.NonEmptyString),
  limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 128 })),
});

export const ThreadWorkPage = Schema.Struct({
  entries: Schema.Array(
    Schema.Struct({
      owner: WorkOwner,
      state: Schema.Literals(["ready", "waiting", "unknown", "handoff"]),
      notBeforeMillis: Schema.optionalKey(Schema.Natural),
    }),
  ).check(Schema.isMaxLength(128)),
  cursor: Schema.optionalKey(Schema.NonEmptyString),
});

export class WorkDiscoveryUnavailable extends Schema.TaggedError<WorkDiscoveryUnavailable>()(
  "WorkDiscoveryUnavailable",
  {
    threadId: ThreadId,
    reason: Schema.Literals(["missing-index", "incomplete-rebuild", "unsupported"]),
  },
) {}

/**
 * Thread-wide inventory port. Enumeration grants no authority; missing/incomplete indexes
 * are distinguishable from an empty inventory. Stage 2 supplies bounded reconstruction.
 */
export class ThreadWorkDiscovery extends Context.Service<
  ThreadWorkDiscovery,
  {
    readonly page: (
      request: typeof ThreadWorkRequest.Type,
    ) => Effect.Effect<typeof ThreadWorkPage.Type, WorkDiscoveryUnavailable | ThreadStoreError>;
  }
>()("@effect-agent/thread/ThreadWorkDiscovery") {}

const failure = (message: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation: "RunContinuation",
    message,
    ...(cause === undefined ? {} : { cause }),
  });

const capacityFailure = (message: string) =>
  failure(message, AgentPersistenceCapacityError.make({ message }));

const encodeRecord = Schema.encodeSync(RecordEnvelope);

const recordWire = (record: RecordEnvelope) =>
  record instanceof ExportedRecord ? record.wire : encodeRecord(record);

/** Exact UTF-8 record JSON; batch duplication and physical index bytes are separate costs. */
export const canonicalRecordBytes = (record: RecordEnvelope): number =>
  utf8ByteLength(JSON.stringify(recordWire(record)));

/** Grouped usage is no larger than its original identities/components plus bounded counters. */
export const terminalUsageCharge = (calls: ReadonlyArray<ModelCallUsage>): number =>
  calls.reduce((bytes, call) => bytes + utf8ByteLength(JSON.stringify(call)) + 512, 0);

const factUsageCharge = (facts: ReadonlyArray<RecordEnvelope>): number =>
  facts.reduce(
    (bytes, { payload }) =>
      bytes +
      (payload._tag === "ModelResponseRecorded" || payload._tag === "ModelCallAborted"
        ? terminalUsageCharge(payload.modelUsage ?? [])
        : 0),
    0,
  );

export const reference = Effect.fnUntraced(function* (record: RecordEnvelope) {
  const wire = yield* Effect.try({
    try: () => recordWire(record),
    catch: (cause) => failure("Cannot encode canonical evidence", cause),
  });

  const digest = yield* digestJson(wire).pipe(
    Effect.mapError((cause) => failure("Cannot fingerprint canonical evidence", cause)),
  );

  return EvidenceReference.make({ recordId: record.recordId, digest });
});

/** Resolve exact immutable content. A failed reference leaves the original work owed. */
export const resolveEvidence = Effect.fnUntraced(function* (
  threadId: ThreadId,
  ref: EvidenceReference,
) {
  const found = yield* getRecord({ threadId, recordId: ref.recordId });

  if (Option.isNone(found)) return yield* failure("Required canonical evidence is unavailable");
  if ((yield* reference(found.value.record)).digest !== ref.digest)
    return yield* failure("Required canonical evidence has invalid integrity");

  return found.value;
});

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
                    if (count > MAX_RUN_EVIDENCE_RECORDS || bytes > MAX_RUN_EVIDENCE_BYTES)
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

/** Called inside the adapter's existing mutation, before publishing any fact or index. */
export const validateProgressAppend = Effect.fnUntraced(function* <E, R>(
  batch: CanonicalBatch,
  previous: (runId: RunId) => Effect.Effect<RunContinuation | undefined, E, R>,
  initial: (next: RunContinuation) => Effect.Effect<ReadonlyArray<RecordEnvelope>, E, R>,
): Effect.fn.Return<void, E | ThreadStoreError, R> {
  const seen = new Set<RunId>();
  let progressStarted = false;

  for (const record of batch.records) {
    const next = record.payload;

    if (next._tag !== "RunContinuation") {
      if (progressStarted)
        return yield* failure("Execution facts cannot follow their continuation");
      continue;
    }
    progressStarted = true;
    if (canonicalRecordBytes(record) > MAX_RUN_CONTINUATION_BYTES)
      return yield* failure("Encoded continuation exceeds its record byte bound");
    if (seen.has(next.runId))
      return yield* failure("One atomic batch cannot publish conflicting Run progress");
    seen.add(next.runId);

    const facts = batch.records.filter(
      (fact) =>
        fact.payload._tag !== "RunContinuation" && canonicalRunIds(fact).includes(next.runId),
    );

    const frontier = facts.at(-1);
    const prior = yield* previous(next.runId);
    const retained = prior === undefined ? yield* initial(next) : [];
    const retainedBytes = retained.reduce((bytes, fact) => bytes + canonicalRecordBytes(fact), 0);
    const factBytes = facts.reduce((bytes, fact) => bytes + canonicalRecordBytes(fact), 0);
    const turnBase = prior?.turn === next.turn ? prior.turnBytes : retainedBytes;

    if (
      frontier === undefined ||
      retained.length > MAX_RUN_RECOVERY_SUFFIX_RECORDS ||
      retainedBytes > MAX_RUN_RECOVERY_SUFFIX_BYTES ||
      retained.some(
        (fact) => !isPreContinuationFact(fact) || !canonicalRunIds(fact).includes(next.runId),
      ) ||
      frontier.recordId !== next.lastFact.recordId ||
      next.revision !== (prior?.revision ?? 0) + 1 ||
      next.recordBytes < (prior?.recordBytes ?? 0) ||
      next.turn < (prior?.turn ?? 0) ||
      (prior !== undefined && next.turn === prior.turn && next.turnBytes <= prior.turnBytes) ||
      next.recordBytes !== (prior?.recordBytes ?? retainedBytes) + factBytes ||
      next.terminalUsageBytes !== (prior?.terminalUsageBytes ?? 0) + factUsageCharge(facts) ||
      next.turnBytes !== turnBase + factBytes + canonicalRecordBytes(record) ||
      next.recordCount !== (prior?.recordCount ?? retained.length) + facts.length ||
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
      else if (payload._tag === "RunFailed")
        completionBytes = utf8ByteLength(JSON.stringify(payload.failure));
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
    const wire = encodeRecord(state.record);

    const ref = (recordId: RecordId) =>
      EvidenceReference.make({
        recordId,
        digest: progress.lastFact.digest,
      });

    const fits = (batchId: string, lastFact: EvidenceReference, terminal?: EvidenceReference) =>
      // All references are exact prospective identities. The scalar allowance covers counter,
      // position and timestamp growth without promising room for arbitrary future payloads.
      utf8ByteLength(
        JSON.stringify({
          ...wire,
          recordId: JSON.stringify(["continuation@1", progress.runId, batchId]),
          deploymentId,
          payload: {
            ...progress,
            lastFact,
            ...(terminal === undefined ? {} : { terminal, position: "settled" }),
          },
        }),
      ) +
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
        : RUN_TERMINAL_RESERVE_BYTES +
          (state.completionBytes ?? 0) +
          2 *
            (progress.terminalUsageBytes +
              Math.max(0, (stagedUsage.get(progress.runId) ?? 0) - committedUsageBytes));

    let turnBytes =
      (extra !== undefined && extra.turn !== progress.turn ? 0 : progress.turnBytes) +
      pending.bytes +
      terminal +
      (extra?.bytes ?? 0);

    let recordBytes = progress.recordBytes + pending.bytes + terminal + (extra?.bytes ?? 0);

    let recordCount =
      progress.recordCount +
      pending.records +
      (progress.position === "settled" ? 0 : RUN_TERMINAL_RESERVE_RECORDS) +
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
    const groups = new Map<RunId, Array<RecordEnvelope>>();

    for (const record of batch.records) {
      for (const runId of canonicalRunIds(record)) {
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
        const existing = yield* provide(
          Stream.runCollect(
            runEvidence(threadId, submissionId, after).pipe(
              Stream.take(MAX_RUN_RECOVERY_SUFFIX_RECORDS + 1),
            ),
          ),
        );

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

      const accounting = { ...(previous?.continuation.accounting ?? emptyAccounting()) };
      let savedContext = previous?.continuation.savedContext;
      let latestResponse = previous?.continuation.latestResponse;
      let terminal = previous?.continuation.terminal;
      let position = previous?.continuation.position ?? "starting";
      let response = previous?.response;
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
            savedContext = yield* provide(reference(fact));
            position = "awaiting-model";
            break;
          case "ModelResponseRecorded": {
            if (savedContext === undefined)
              return yield* failure("A model response has no saved original Run context");
            if (payload.turn !== accounting.committedTurns + 1)
              return yield* failure("Canonical Turn progress must advance once");

            const messages = yield* Schema.decodeUnknownEffect(Prompt.Prompt)(
              payload.messages,
            ).pipe(Effect.mapError((cause) => failure("Invalid model evidence", cause)));

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
            latestResponse = yield* provide(reference(fact));
            response = payload;
            results = new Map();
            childResults = new Map();
            position =
              payload.toolOperations.length === 0 ? "awaiting-model" : "processing-operations";
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
                    reserved.remainingBytes -
                      canonicalRecordBytes(fact) -
                      MAX_RUN_CONTINUATION_BYTES,
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
                : utf8ByteLength(JSON.stringify(payload.failure));
            terminal = yield* provide(reference(fact));
            position = "settling";
            break;
          case "SubmissionSettled":
            if (payload.submissionId !== submissionId) break;
            terminal = yield* provide(reference(fact));
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
        const messages = yield* Schema.decodeUnknownEffect(Prompt.Prompt)(response.messages).pipe(
          Effect.mapError((cause) => failure("Invalid batch evidence", cause)),
        );

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

      if (last === undefined) continue;
      if (previous?.continuation.position === "settled") position = "settled";
      else if (previous?.continuation.position === "settling" && position !== "settled")
        position = "settling";
      const factBytes = facts.reduce((bytes, fact) => bytes + canonicalRecordBytes(fact), 0);

      const turn = facts.reduce(
        (current, fact) =>
          "turn" in fact.payload ? Math.max(current, fact.payload.turn) : current,
        previous?.continuation.turn ?? 1,
      );

      const turnBase =
        (previous?.continuation.turn === turn ? previous.continuation.turnBytes : initialBytes) +
        factBytes;

      const usageCharge = factUsageCharge(facts);
      const terminalUsageBytes = (previous?.continuation.terminalUsageBytes ?? 0) + usageCharge;

      // Reserve the largest cursor before constructing a bounded Schema value. Every owner
      // writer preserves in-flight results and terminal room, including capability appends.
      yield* checkCapacity(
        {
          continuation: {
            runId,
            position,
            turn,
            turnBytes: turnBase + MAX_RUN_CONTINUATION_BYTES,
            recordBytes: (previous?.continuation.recordBytes ?? initialBytes) + factBytes,
            recordCount: (previous?.continuation.recordCount ?? originalCount) + facts.length,
            terminalUsageBytes,
            accounting,
          },
          ...(response === undefined ? {} : { response }),
          results,
          childResults,
          ...(completionBytes === undefined ? {} : { completionBytes }),
        },
        new Set(batch.records.map((record) => record.recordId)),
        undefined,
        usageCharge,
      );

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

      let record = RecordEnvelope.make({
        recordId: RecordId.make(JSON.stringify(["continuation@1", runId, batch.batchId])),
        family: "thread",
        schemaVersion: 1,
        createdAt: last.createdAt,
        deploymentId: last.deploymentId,
        payload: continuation,
      });

      // Charging this record changes only the digits of one scalar. Converge before append;
      // exceeding the budget publishes neither the execution facts nor their progress.
      for (let attempts = 0; ; attempts++) {
        const turnBytes = turnBase + canonicalRecordBytes(record);

        if (turnBytes === continuation.turnBytes) break;
        if (attempts >= 4) return yield* failure("Continuation byte accounting did not converge");
        continuation = yield* RunContinuation.makeEffect({ ...continuation, turnBytes }).pipe(
          Effect.mapError(() =>
            capacityFailure("Turn exceeds its incremental canonical byte budget"),
          ),
        );
        record = RecordEnvelope.make({ ...record, payload: continuation });
      }

      if (utf8ByteLength(JSON.stringify(encodeRecord(record))) > MAX_RUN_CONTINUATION_BYTES)
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

      yield* checkFutureShapes(state);
      accepted.set(runId, state);
    }

    const prepared = yield* CanonicalBatch.makeEffect({
      ...batch,
      records: [batch.records[0], ...batch.records.slice(1), ...continuations],
    }).pipe(
      Effect.mapError((cause) =>
        failure("Progress and facts exceed the atomic batch bound", cause),
      ),
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

  const commit = Effect.fnUntraced(function* (batch: CanonicalBatch) {
    const writer = yield* CurrentRunWriter;

    const prepared = yield* prepare(batch, yield* tail).pipe(
      Effect.catchTag("ThreadNotMaterialized", (cause) =>
        failure("Selected Run disappeared during progress publication", cause),
      ),
    );

    const result = yield* writer.append(prepared.batch);

    if (result.replayed) cached.clear();
    else prepared.accept();

    return result;
  }, gate.withPermits(1));

  const publish = Effect.fnUntraced(function* (batch: CanonicalBatch) {
    const publisher = yield* CurrentRunSettlement;

    if (publisher.threadId !== threadId)
      return yield* failure("Settlement publisher belongs to another Thread");

    const prepared = yield* prepare(batch, (yield* publisher.tail).sequence).pipe(
      Effect.catchTag("ThreadNotMaterialized", (cause) =>
        failure("Selected Run disappeared during progress publication", cause),
      ),
    );

    const result = yield* publisher.publishSettlement(prepared.batch);

    if (result.replayed) cached.clear();
    else prepared.accept();

    return result;
  }, gate.withPermits(1));

  const reserve = (runId: RunId, probe: CanonicalBatch, bytes: number) =>
    gate.withPermits(1)(
      Effect.gen(function* () {
        const after = yield* tail;

        yield* prepare(probe, after);
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

        yield* prepare(batch, after);

        const response = batch.records.find(
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
          batch.records.reduce((total, record) => total + canonicalRecordBytes(record), 0) +
          MAX_RUN_CONTINUATION_BYTES +
          pending.bytes;

        const records = batch.records.length + pending.records;

        yield* checkCapacity(progress, new Set(), { turn: response.payload.turn, bytes, records });
        reservations.set(response.recordId, {
          runId: response.payload.runId,
          turn: response.payload.turn,
          bytes,
          records,
        });
      }),
    );

  const check = (runId: RunId) =>
    gate.withPermits(1)(
      Effect.gen(function* () {
        const state = yield* loadState(runId, yield* tail);

        if (state === undefined) return yield* failure("Execution has no canonical Run progress");
        yield* checkFutureShapes(state);
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
