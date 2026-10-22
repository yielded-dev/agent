/**
 * Bounded active Run checkpoints, evidence validation, and progress publication through the current
 * storage owner. Current checkpoints retire at settlement while canonical facts remain append-only.
 *
 * @since 0.1.0
 */

import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import { AgentPersistenceCapacityError } from "../core/AgentError.ts";
import type { RunId, SubmissionId, ThreadId } from "../core/Identifiers.ts";
import { ToolCallId } from "../core/Identifiers.ts";
import { utf8ByteLength } from "../core/internal/utf8.ts";
import { resolveEvidence } from "./internal/evidence.ts";
import { continuationUsage } from "./internal/usage-totals.ts";

export {
  /**
   * References identify immutable facts; schema and owning readers validate their meaning.
   *
   * @category operations
   * @since 0.1.0
   */
  reference,
  /**
   * Resolve an immutable fact by Thread and record identity. A failed reference leaves the original work owed.
   *
   * @category operations
   * @since 0.1.0
   */
  resolveEvidence,
} from "./internal/evidence.ts";

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

export {
  /**
   * Grouped usage is no larger than its original identities/components plus bounded counters.
   *
   * @category operations
   * @since 0.1.0
   */
  terminalUsageCharge,
} from "./internal/record-ownership.ts";

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
import type { RunStorageSession } from "./RunStorage.ts";
import {
  submissionAbortRecordId,
  submissionInputRecordId,
  submissionSettlementRecordId,
  submissionSettlementBatchId,
} from "./SubmissionLedger.ts";
import {
  CommitBatch,
  ExecutionCheckpoint,
  ExecutionUpdate,
  getRecord,
  getRunInput,
  ThreadReader,
  ThreadStoreError,
} from "./ThreadStore.ts";

export {
  /**
   * Current interpreter progress, replaced atomically with lastFact under the Thread fence.
   * lastFact is the exact accounted frontier of the same commit. References resolve within this
   * Thread's history or its authoritative active checkpoint. Results, operation declarations,
   * approvals and delivery obligations remain canonical; evaluated context and validated completion
   * belong to active state. Neither the checkpoint nor its revision grants execution authority.
   * Retired intermediate revisions are not historical verification evidence.
   *
   * @category models
   * @since 0.1.0
   */
  RunContinuation,
  /**
   * Original evaluated Run input and fixed prior-history references used to reconstruct and
   * validate saved model context.
   *
   * @category models
   * @since 0.1.0
   */
  RunContextRecorded,
  /**
   * Fact identity; active-only references resolve inside their Run checkpoint.
   *
   * @category schemas
   * @since 0.1.0
   */
  EvidenceReference,
} from "./Records.ts";

/**
 * Writer and progress sequencer selected at the local owner's resource boundary.
 *
 * @category services
 * @since 0.1.0
 */
export class CurrentRunWriter extends Context.Service<
  CurrentRunWriter,
  Pick<RunStorageSession, "threadId" | "tail" | "append" | "appendInput" | "withProgress">
>()("@effect-agent/thread/CurrentRunWriter") {}

/**
 * Settlement publication keeps its original atomic authority and claim-scoped implementation.
 *
 * @category services
 * @since 0.1.0
 */
export class CurrentRunSettlement extends Context.Service<
  CurrentRunSettlement,
  Pick<RunStorageSession, "threadId" | "tail" | "publishSettlement" | "withProgress">
>()("@effect-agent/thread/CurrentRunSettlement") {}

/**
 * Canonical reads pinned to the append owner's current transaction or mutation gate.
 *
 * @category services
 * @since 0.1.0
 */
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
  /**
   * Index membership comes from canonical owner fields, never parsing opaque identities.
   *
   * @category operations
   * @since 0.1.0
   */
  canonicalRunIds,
  /**
   * A joined Receipt's closure is discoverable from its host, but owns no host execution charge.
   *
   * @category operations
   * @since 0.1.0
   */
  executionRunIds,
  /**
   * These immutable preparations are durable handoff intents before a destination row exists.
   * Their canonical tag/identity index is published with the preparation and survives Run
   * settlement. WorkerInputCompleted closes capacity only with factual effectsResolved evidence;
   * ToolCallSettled/Resolved close operations, and destination delivery state owns transport closure.
   *
   * @category guards
   * @since 0.1.0
   */
  isWorkHandoff,
  /**
   * A host can prepare independent work before the source admission ever starts a Run.
   *
   * @category guards
   * @since 0.1.0
   */
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

const isExecutionProgressRecord = (
  record: RecordEnvelope,
): record is RecordEnvelope & { readonly payload: RunContinuation } =>
  record.payload._tag === "RunContinuation";

const validateBatchShape = Schema.decodeUnknownSync(
  Schema.Struct({
    batchId: CanonicalBatch.fields.batchId,
    producerId: CanonicalBatch.fields.producerId,
    records: Schema.NonEmptyArray(Schema.Unknown).check(Schema.isMaxLength(256)),
  }),
);

const decodeExecutionFrontier = Schema.decodeUnknownEffect(
  Schema.Struct({
    canonicalRecords: ExecutionCheckpoint.fields.canonicalRecords,
    throughSequence: ExecutionCheckpoint.fields.throughSequence,
  }),
);

/** One private snapshot for every retry; callers cannot change facts while awaiting the gate. */
const captureFacts = (batch: CommitBatch) =>
  Effect.try({
    try: () =>
      Result.gen(function* () {
        const inputs = batch.records;

        const {
          batchId,
          producerId,
          records: slots,
        } = validateBatchShape({
          batchId: batch.batchId,
          producerId: batch.producerId,
          records: inputs,
        });

        const first = yield* captureRecord(inputs[0]!);
        const records: [RecordEnvelope, ...Array<RecordEnvelope>] = [first.canonical];

        for (let index = 1; index < slots.length; index++)
          records.push((yield* captureRecord(inputs[index]!)).canonical);

        return Object.freeze(
          CanonicalBatch.make(
            { batchId, producerId, records: Object.freeze(records) },
            { disableChecks: true },
          ),
        );
      }),
    catch: (cause) => failure("Cannot capture canonical progress facts", cause),
  }).pipe(
    Effect.flatMap(Effect.fromResult),
    Effect.mapError((cause) =>
      Schema.is(ThreadStoreError)(cause)
        ? cause
        : failure("Cannot capture canonical progress facts", cause),
    ),
  );

/**
 * Exact UTF-8 wire accounting reuses the append's privately captured Schema encoding.
 *
 * @category operations
 * @since 0.1.0
 */
export const canonicalRecordBytes = (record: RecordEnvelope): number => {
  const encoded = recordEncoding(record);

  if (Result.isFailure(encoded)) {
    // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- Public synchronous byte-accounting validation must reject malformed records before returning a number.
    throw encoded.failure.cause ?? new RangeError(encoded.failure.message);
  }

  return encoded.success.bytes;
};

export type {
  /**
   * Private capture metadata, never caller-supplied wire data or execution authority.
   *
   * @category models
   * @since 0.1.0
   */
  ProgressAppendRecord,
} from "./internal/record-encoding.ts";

/**
 * Capture before acquiring the mutation owner; summaries never cross a wire boundary.
 *
 * @category operations
 * @since 0.1.0
 */
export const prepareProgressAppend = (
  records: ReadonlyArray<RecordEnvelope>,
): ReadonlyArray<ProgressAppendRecord> => {
  const encodings = Result.all(records.map(captureRecord));

  if (Result.isFailure(encodings)) {
    // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- Public synchronous progress construction must reject malformed records before acquiring a mutation owner.
    throw encodings.failure.cause ?? new RangeError(encodings.failure.message);
  }

  return Object.freeze(encodings.success.map((record) => record.progress));
};

/**
 * These values belong to the active Run, not its permanent conversation history.
 *
 * @category guards
 * @since 0.1.0
 */
export const isOperationalRecord = ({ payload }: RecordEnvelope): boolean =>
  payload._tag === "RunStarted" ||
  payload._tag === "RunContextRecorded" ||
  payload._tag === "RunCompleted";

/**
 * Loads and validates the active Run checkpoint against the requested Run identity and captured
 * canonical tail.
 *
 * @category operations
 * @since 0.1.0
 */
export const readContinuation = Effect.fnUntraced(function* (
  threadId: ThreadId,
  runId: RunId,
  throughSequence: CanonicalSequence,
) {
  const reader = yield* ThreadReader;

  const loaded = yield* reader
    .readExecution({ threadId, runId })
    .pipe(
      Effect.catchTag("ThreadNotMaterialized", (cause) =>
        throughSequence === 0
          ? Effect.succeed(Option.none())
          : failure("Active Run history is not materialized", cause),
      ),
    );

  if (Option.isNone(loaded)) return Option.none();
  const value = loaded.value;

  if (
    value.progress.runId !== runId ||
    value.throughSequence > throughSequence ||
    canonicalRecordBytes(value.progressRecord) > MAX_RUN_CONTINUATION_BYTES ||
    value.operational.some(
      (record) => !isOperationalRecord(record) || !executionRunIds(record).includes(runId),
    ) ||
    new Set(value.operational.map((record) => record.recordId)).size !== value.operational.length
  )
    return yield* failure("Invalid active Run checkpoint");

  return Option.some({
    ...value,
    checkpoint: value,
    get progress() {
      return value.progress;
    },
    record: value.progressRecord,
    continuation: value.progress,
  });
});

/**
 * Resolve an active-only fact without presenting it as a canonical envelope or sequence.
 *
 * @category operations
 * @since 0.1.0
 */
export const resolveExecutionEvidence = Effect.fnUntraced(function* (
  threadId: ThreadId,
  checkpoint: ExecutionCheckpoint,
  ref: EvidenceReference,
) {
  const operational = checkpoint.operational.find((record) => record.recordId === ref.recordId);
  const record = operational ?? (yield* resolveEvidence(threadId, ref)).record;

  return record;
});

/**
 * Each selected page is at most eight records; total resident encoded evidence is bounded.
 *
 * @category operations
 * @since 0.1.0
 */
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

/**
 * A bounded selected-Run suffix, never a Thread-wide tail or an implicit replay fallback.
 *
 * @category operations
 * @since 0.1.0
 */
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
 *
 * @category operations
 * @since 0.1.0
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
    const terminal = own.findLast(
      ({ record }) => record.payload._tag === "SubmissionSettled" && record.payload.runId === runId,
    );

    if (terminal?.record.payload._tag === "SubmissionSettled") {
      if (
        terminal.record.payload.historyRecordCount === undefined ||
        own.filter((entry) => entry.sequence <= terminal.sequence).length !==
          terminal.record.payload.historyRecordCount
      )
        return yield* failure("Settled Run history is incomplete");

      return records;
    }
    if (own.some(({ record }) => !isPreContinuationFact(record)))
      return yield* failure("Active execution has no durable checkpoint");

    return records;
  }
  const checkpoint = progress.value;
  const cursor = checkpoint.continuation;

  if (cursor.submissionId !== submissionId)
    return yield* failure("Active checkpoint has another admitted owner");
  if (
    own.filter((entry) => entry.sequence <= checkpoint.throughSequence).length !==
      checkpoint.canonicalRecords ||
    !validateSuffix(own, checkpoint.throughSequence)
  )
    return yield* failure("Selected execution history is incomplete or exceeds its suffix bound");

  const resolve = (ref: EvidenceReference) =>
    resolveExecutionEvidence(threadId, checkpoint.checkpoint, ref);

  const input = yield* resolve(cursor.originalInput);

  if (
    input.payload._tag !== "UserInputRecorded" ||
    input.payload.submissionId !== submissionId ||
    input.payload.kind !== "user" ||
    input.payload.runId !== runId
  )
    return yield* failure("Active checkpoint has no original accepted input");
  yield* resolve(cursor.lastFact);
  if (cursor.savedContext !== undefined) {
    const context = yield* resolve(cursor.savedContext);

    if (context.payload._tag !== "RunContextRecorded" || context.payload.runId !== runId)
      return yield* failure("Active checkpoint has invalid saved context");
  }
  if (cursor.latestResponse !== undefined) {
    const response = yield* resolve(cursor.latestResponse);

    if (
      response.payload._tag !== "ModelResponseRecorded" ||
      response.payload.runId !== runId ||
      response.payload.turn !== cursor.accounting.committedTurns
    )
      return yield* failure("Active checkpoint has invalid operation evidence");
  }
  if (cursor.terminal !== undefined) {
    const terminal = yield* resolve(cursor.terminal);

    if (
      terminal.payload._tag !== "RunCompleted" &&
      terminal.payload._tag !== "RunFailed" &&
      terminal.payload._tag !== "SubmissionSettled"
    )
      return yield* failure("Active checkpoint has invalid terminal evidence");
  }

  return records;
});

/**
 * Called inside the adapter's existing mutation, before publishing any fact or index.
 *
 * @category operations
 * @since 0.1.0
 */
export const validateProgressAppend = Effect.fnUntraced(function* (
  records: ReadonlyArray<ProgressAppendRecord>,
  updates: ReadonlyArray<ExecutionUpdate> = [],
): Effect.fn.Return<void, ThreadStoreError, ProgressAppendReader> {
  const reader = yield* ProgressAppendReader;

  if (records.some((record) => record.continuation !== undefined))
    return yield* failure("Interpreter progress must be committed as active execution state");

  const operational = new Map(
    updates.flatMap((update) => update.facts.map((fact) => [fact.recordId, fact] as const)),
  );

  const factsAndProgress = [
    ...records,
    ...prepareProgressAppend([...operational.values()]),
    ...prepareProgressAppend(updates.map((update) => update.checkpoint.progressRecord)),
  ];

  const seen = new Set<RunId>();
  let progressStarted = false;

  for (const record of factsAndProgress) {
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

    const update = updates.find((value) => value.checkpoint.progress.runId === next.runId);

    if (
      update === undefined ||
      update.previousRevision + 1 !== next.revision ||
      update.facts.some((fact) => !isOperationalRecord(fact)) ||
      (update.retire && (next.position !== "settled" || next.terminal === undefined))
    )
      return yield* failure("Invalid active execution transition");

    const facts = factsAndProgress.filter(
      (fact) => fact.continuation === undefined && fact.runIds.includes(next.runId),
    );

    const frontier = facts.find((fact) => fact.recordId === next.lastFact.recordId);
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
  readonly checkpoint?: ExecutionCheckpoint;
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
  let results = new Map(previous?.results);
  let childResults = new Map(previous?.childResults);
  let completionBytes = previous?.completionBytes;
  let factBytes = 0;
  let turn = previous?.continuation.turn ?? 1;
  let usageCharge = 0;
  let closingBytes = 0;
  let closingCount = 0;

  for (const fact of facts) {
    const payload = fact.payload;
    const captured = recordEncoding(fact);

    if (Result.isFailure(captured))
      return yield* failure("Cannot account canonical progress facts", captured.failure);
    const encoded = captured.success;

    factBytes += encoded.bytes;
    if ("turn" in payload) turn = Math.max(turn, payload.turn);
    usageCharge += encoded.progress.terminalUsageBytes;
    if (encoded.progress.terminal) {
      closingBytes += encoded.bytes;
      closingCount++;
    }

    switch (payload._tag) {
      case "RunStarted":
        position = "awaiting-model";
        break;
      case "RunContextRecorded":
        if (savedContext !== undefined)
          return yield* failure("A Run's original context cannot be replaced");
        savedContext = EvidenceReference.make({ recordId: fact.recordId });
        position = "awaiting-model";
        break;
      case "ModelResponseRecorded": {
        if (savedContext === undefined)
          return yield* failure("A model response has no saved original Run context");
        if (payload.turn !== accounting.committedTurns + 1)
          return yield* failure("Canonical Turn progress must advance once");

        const messages = payload.messages;

        const calls = messages.content.flatMap((message) =>
          message.role === "assistant" && typeof message.content !== "string"
            ? message.content.filter((part) => part.type === "tool-call")
            : [],
        );

        const summary =
          payload.modelUsage === undefined
            ? undefined
            : yield* continuationUsage(payload.modelUsage).pipe(
                Effect.mapError((cause) => failure("Invalid usage evidence", cause)),
              );

        accounting.committedTurns = payload.turn;
        accounting.toolCalls += calls.length;
        accounting.modelCalls += summary?.modelCalls ?? 1;
        accounting.inputTokens += summary?.inputTokens ?? payload.inputTokens ?? 0;
        accounting.outputTokens += summary?.outputTokens ?? payload.outputTokens ?? 0;
        accounting.costMicrousd += summary?.costMicrousd ?? payload.costMicrousd ?? 0;
        accounting.lastInputTokens = summary?.inputTokens ?? payload.inputTokens ?? 0;
        accounting.lastOutputTokens = summary?.outputTokens ?? payload.outputTokens ?? 0;
        accounting.unobservedModelCalls += payload.unobservedModelCalls ?? 0;
        latestResponse = EvidenceReference.make({ recordId: fact.recordId });
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

        const summary = yield* continuationUsage(payload.modelUsage).pipe(
          Effect.mapError((cause) => failure("Invalid cancelled model usage", cause)),
        );

        accounting.modelRestarts = payload.restart;
        accounting.modelCalls += summary.modelCalls;
        accounting.inputTokens += summary.inputTokens;
        accounting.outputTokens += summary.outputTokens;
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
        terminal = EvidenceReference.make({ recordId: fact.recordId });
        position = "settling";
        break;
      case "SubmissionSettled":
        if (payload.submissionId !== submissionId) break;
        terminal = EvidenceReference.make({ recordId: fact.recordId });
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
    const parts = response.messages.content.flatMap((message) =>
      message.role === "assistant" && typeof message.content !== "string" ? message.content : [],
    );

    for (const call of parts) {
      if (call.type !== "tool-call") continue;

      const result = call.providerExecuted
        ? parts.find((part) => part.type === "tool-result" && part.id === call.id)
        : results.get(ToolCallId.make(call.id));

      if (
        result === undefined ||
        !("isFailure" in result) ||
        // Only canonical application settlements carry budget-rejection authority.
        (!call.providerExecuted && "budgetRejected" in result && result.budgetRejected === true)
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

  const turnBase =
    (previous?.continuation.turn === turn ? previous.continuation.turnBytes : initialBytes) +
    factBytes;

  const terminalUsageBytes = (previous?.continuation.terminalUsageBytes ?? 0) + usageCharge;

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
    terminalBytes: (previous?.continuation.terminalBytes ?? 0) + closingBytes,
    terminalRecords: (previous?.continuation.terminalRecords ?? 0) + closingCount,
    closing: closingCount > 0,
  };
});

/**
 * Verify retained history. Mutable checkpoint revisions are deliberately not historical proof.
 * Active original context is still checked against its exact retained historyDigest.
 *
 * @category operations
 * @since 0.1.0
 */
export const verifyRunContinuations = Effect.fnUntraced(function* (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  options?: {
    readonly verifyContext?: boolean;
    readonly executions?: ReadonlyArray<ExecutionCheckpoint>;
  },
) {
  const inputs = records.filter(
    ({ record }) =>
      record.payload._tag === "UserInputRecorded" &&
      record.payload.kind === "user" &&
      record.payload.submissionId !== undefined,
  );

  if (records.some(({ record }) => record.payload._tag === "RunContinuation"))
    return yield* failure("Mutable interpreter progress is not canonical history");
  for (const input of inputs) {
    const payload = input.record.payload;

    if (payload._tag !== "UserInputRecorded" || payload.submissionId === undefined) continue;
    const runId = runIdForSubmission(payload.submissionId);
    const own = records.filter((entry) => executionRunIds(entry.record).includes(runId));

    const terminal = own.findLast(
      ({ record }) => record.payload._tag === "SubmissionSettled" && record.payload.runId === runId,
    );

    if (terminal?.record.payload._tag === "SubmissionSettled") {
      if (
        terminal.record.payload.historyRecordCount === undefined ||
        own.filter((entry) => entry.sequence <= terminal.sequence).length !==
          terminal.record.payload.historyRecordCount
      )
        return yield* failure("Terminal history differs from its retained fact count");
      continue;
    }
    if (options?.verifyContext === false) continue;
    const checkpoint = options?.executions?.find((value) => value.progress.runId === runId);

    if (checkpoint === undefined) {
      if (own.some(({ record }) => !isPreContinuationFact(record)))
        return yield* failure("Active execution is missing its authoritative checkpoint");
      continue;
    }

    const saved = checkpoint.operational.find(
      ({ payload }) => payload._tag === "RunContextRecorded",
    );

    if (saved?.payload._tag === "RunContextRecorded") {
      yield* validateContextBoundary(saved.payload, input).pipe(
        Effect.mapError((cause) => failure("Context has an invalid admission boundary", cause)),
      );
      yield* readRunContext(saved.payload, input, saved.recordId).pipe(
        Effect.mapError((cause) => failure("Context differs from its original history", cause)),
      );
    }
  }
});

/**
 * Prepare progress for the existing atomic append/publication. Accept the returned state only
 * after that append commits; a failed or stale producer cannot publish speculative progress.
 * One writer retains bounded metadata for its active Run, not its historical results or IDs.
 *
 * @category constructors
 * @since 0.1.0
 */
export const makeProgressWriter = Effect.fnUntraced(function* (
  threadId: ThreadId,
  deploymentId: DeploymentId,
) {
  const reader = yield* ThreadReader;
  const gate = yield* Semaphore.make(1);
  const cached = new Map<RunId, ProgressState>();

  let verifiedEmpty:
    | {
        readonly runId: RunId;
        readonly through: CanonicalSequence;
        readonly writer: CurrentRunWriter["Service"];
      }
    | undefined;

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

  const provide = <A, E>(effect: Effect.Effect<A, E, ThreadReader>) =>
    Effect.provideService(effect, ThreadReader, reader);

  const loadUncachedState = Effect.fnUntraced(function* (runId: RunId, after: CanonicalSequence) {
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
      const terminal = yield* provide(
        resolveExecutionEvidence(threadId, loaded.value.checkpoint, continuation.terminal),
      );

      const payload = terminal.payload;

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
      checkpoint: loaded.value.checkpoint,
      ...(response === undefined ? {} : { response }),
      results,
      childResults,
      ...(completionBytes === undefined ? {} : { completionBytes }),
    };
  });

  const checkFutureShapes = (
    state: ProgressState,
    cursorBytes?: number,
  ): Effect.Effect<void, ThreadStoreError> => {
    const progress = state.continuation;

    if (progress.position === "settled") return Effect.void;
    const size = cursorBytes ?? canonicalRecordBytes(state.record);
    const textBytes = (value: string) => utf8ByteLength(JSON.stringify(value));
    const refBytes = (value: EvidenceReference) => utf8ByteLength(JSON.stringify(value));

    const ref = (recordId: RecordId) => EvidenceReference.make({ recordId });

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

  const prepare = Effect.fnUntraced(function* (
    batch: CanonicalBatch,
    after: CanonicalSequence,
    mode: "commit" | "preflight" = "commit",
  ) {
    if (batch.records.some((record) => record.payload._tag === "RunContinuation"))
      return yield* failure("A caller cannot supply interpreter progress to the progress writer");

    // Every private caller passes captureFacts' owned snapshot. Keep a mutable
    // list for settlement accounting without capturing that snapshot a second time.
    const ownedRecords = [...batch.records];

    const groups = new Map<RunId, Array<RecordEnvelope>>();

    for (const record of ownedRecords) {
      for (const runId of executionRunIds(record)) {
        const facts = groups.get(runId) ?? [];

        facts.push(record);
        groups.set(runId, facts);
      }
    }
    const executions: Array<ExecutionUpdate> = [];
    const accepted = new Map<RunId, ProgressState>();
    let executionCount = 0;

    for (const [runId, facts] of groups) {
      const empty =
        verifiedEmpty?.runId === runId &&
        verifiedEmpty.through === after &&
        // Settlement preparation need not have a Run writer; absence disables reuse.
        verifiedEmpty.writer ===
          Option.getOrUndefined(yield* Effect.serviceOption(CurrentRunWriter));

      const previous = empty
        ? undefined
        : (cached.get(runId) ?? (yield* loadUncachedState(runId, after)));

      let original = facts.find(
        (record) =>
          record.payload._tag === "UserInputRecorded" &&
          record.payload.kind === "user" &&
          record.payload.runId === runId &&
          record.payload.submissionId !== undefined,
      );

      let originalCount = 0;
      let initialBytes = 0;
      let originalSequence = previous?.checkpoint?.originalSequence;

      if (previous === undefined && original === undefined) {
        const input = yield* provide(getRunInput({ threadId, runId }));

        if (
          Option.isNone(input) ||
          input.value.record.payload._tag !== "UserInputRecorded" ||
          input.value.record.payload.submissionId === undefined
        )
          continue;

        original = input.value.record;
        originalSequence = input.value.sequence;
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

      if (previous === undefined && !empty) {
        const settled = yield* provide(
          getRecord({
            threadId,
            recordId: submissionSettlementRecordId(submissionId),
          }),
        );

        if (
          Option.isSome(settled) &&
          settled.value.sequence <= after &&
          settled.value.record.payload._tag === "SubmissionSettled" &&
          settled.value.record.payload.runId === runId &&
          settled.value.record.payload.historyRecordCount !== undefined &&
          facts.every(
            ({ payload }) =>
              payload._tag === "WorkerReportPrepared" ||
              payload._tag === "SubmissionSettled" ||
              payload._tag === "ToolCallSettled" ||
              payload._tag === "ToolCallResolved" ||
              payload._tag === "SubagentJoined",
          )
        )
          // Independent obligations can record factual closure after interpreter retirement.
          continue;

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
            `Run execution has no active checkpoint or exceeds its preparation bound (${facts.map((fact) => fact.payload._tag).join(", ")})`,
          );
        originalCount = existing.length;
        initialBytes = existing.reduce(
          (bytes, entry) => bytes + canonicalRecordBytes(entry.record),
          0,
        );
      }

      const canonicalRecords =
        (previous?.checkpoint?.canonicalRecords ?? originalCount) +
        facts.filter((fact) => !isOperationalRecord(fact)).length;

      for (let index = 0; index < facts.length; index++) {
        const fact = facts[index]!;
        const payload = fact.payload;

        if (payload._tag !== "SubmissionSettled" || payload.runId !== runId) continue;

        const retained = yield* Effect.fromResult(
          captureRecord(
            RecordEnvelope.make({
              ...fact,
              payload: { ...payload, historyRecordCount: canonicalRecords },
            }),
          ),
        ).pipe(
          Effect.map((record) => record.canonical),
          Effect.mapError((cause) => failure("Cannot capture canonical progress facts", cause)),
        );

        facts[index] = retained;
        ownedRecords[ownedRecords.indexOf(fact)] = retained;
      }

      const originalInput =
        previous !== undefined
          ? previous.continuation.originalInput
          : original === undefined
            ? yield* failure("Run progress has no original input")
            : EvidenceReference.make({ recordId: original.recordId });

      const next = yield* advanceFacts(previous, facts, submissionId, initialBytes);

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

      // Facts and prior state are already checked. Validate the complete candidate at
      // record capture, after byte accounting; preflight has its own boundary below.
      let continuation = RunContinuation.make(
        {
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
          lastFact: EvidenceReference.make({ recordId: last.recordId }),
          position,
          accounting,
        },
        { disableChecks: true },
      );

      const header = {
        recordId: RecordId.make(JSON.stringify(["continuation@1", runId, batch.batchId])),
        family: "thread" as const,
        schemaVersion: 1 as const,
        createdAt: last.createdAt,
        deploymentId: last.deploymentId,
      };

      // Continuation fields are JSON; only the envelope DateTime needs encoding.
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

      // Byte accounting changes only these counters; the full candidate is checked below.
      continuation = RunContinuation.make(
        { ...continuation, turnBytes, terminalBytes },
        { disableChecks: true },
      );

      const proposedRecord = RecordEnvelope.make(
        { ...header, payload: continuation },
        { disableChecks: true },
      );

      const cursorBytes = turnBytes - turnBase;

      if (cursorBytes > MAX_RUN_CONTINUATION_BYTES)
        return yield* capacityFailure(
          "Encoded continuation exceeds 8192 bytes including its envelope",
        );
      executionCount++;
      if (
        mode === "preflight" &&
        previous?.checkpoint !== undefined &&
        facts.every(({ payload }) => payload._tag === "ModelResponseRecorded")
      ) {
        // Preflight does not capture a record. Check its complete payload before
        // dispatch; a commit receives the same proof from record capture below.
        continuation = yield* RunContinuation.makeEffect({ ...continuation }).pipe(
          Effect.mapError(() =>
            capacityFailure("Canonical continuation exceeds its protocol bounds"),
          ),
        );
        yield* decodeExecutionFrontier({
          canonicalRecords,
          throughSequence: after + ownedRecords.filter((fact) => !isOperationalRecord(fact)).length,
        }).pipe(Effect.mapError((cause) => failure("Active execution exceeds its bounds", cause)));

        const state: ProgressState = {
          continuation,
          record: proposedRecord,
          ...(response === undefined ? {} : { response }),
          results,
          childResults,
          ...(completionBytes === undefined ? {} : { completionBytes }),
        };

        yield* checkCapacity(
          state,
          new Set(batch.records.map(({ recordId }) => recordId)),
          undefined,
          usageCharge,
        );
        yield* checkFutureShapes(state, cursorBytes);
        continue;
      }

      const record = yield* Effect.fromResult(captureRecord(proposedRecord)).pipe(
        Effect.map((record) => record.canonical),
        Effect.mapError((cause) =>
          Schema.isSchemaError(cause.cause)
            ? capacityFailure("Canonical continuation exceeds its protocol bounds")
            : failure("Cannot capture canonical progress facts", cause),
        ),
      );

      if (!isExecutionProgressRecord(record))
        return yield* failure("Captured execution has no Run continuation");
      if (canonicalRecordBytes(record) !== cursorBytes)
        return yield* failure("Continuation byte accounting differs from its Schema encoding");

      const operational = new Map(
        (previous?.checkpoint?.operational ?? []).map((fact) => [fact.recordId, fact]),
      );

      const changed = facts.filter(isOperationalRecord);

      for (const fact of changed) {
        const prior = operational.get(fact.recordId);

        if (
          prior !== undefined &&
          (yield* Effect.fromResult(recordEncoding(prior)).pipe(
            Effect.mapError((cause) => failure("Cannot encode active Run fact", cause)),
          )).json !==
            (yield* Effect.fromResult(recordEncoding(fact)).pipe(
              Effect.mapError((cause) => failure("Cannot encode active Run fact", cause)),
            )).json
        )
          return yield* failure("An active Run fact cannot change after becoming durable");
        operational.set(fact.recordId, fact);
      }

      // The records above already belong to this writer. PreparedAppend captures the
      // complete update at the storage boundary, preserving their private encodings.
      const checkpoint = ExecutionCheckpoint.make(
        {
          version: 2,
          progressRecord: record,
          operational: [...operational.values()],
          canonicalRecords,
          originalSequence:
            originalSequence ??
            CanonicalSequence.make(
              after +
                ownedRecords
                  .filter((fact) => !isOperationalRecord(fact))
                  .findIndex((fact) => fact.recordId === originalInput.recordId) +
                1,
            ),
          throughSequence: CanonicalSequence.make(
            after + ownedRecords.filter((fact) => !isOperationalRecord(fact)).length,
          ),
          batchId: batch.batchId,
        },
        { disableChecks: true },
      );

      const state = {
        continuation: record.payload,
        record,
        checkpoint,
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
      executions.push(
        ExecutionUpdate.make(
          {
            previousRevision: previous?.continuation.revision ?? 0,
            checkpoint,
            facts: changed,
            // Pending operations and handoffs retain their own recovery rows and original facts.
            retire: position === "settled",
          },
          { disableChecks: true },
        ),
      );
      accepted.set(runId, state);
    }

    if (ownedRecords.length + executionCount > 256)
      return yield* failure("Progress and facts exceed the atomic batch bound");

    const prepared = CommitBatch.make(
      {
        ...batch,
        records: ownedRecords.filter((fact) => !isOperationalRecord(fact)),
      },
      { disableChecks: true },
    );

    return {
      batch: prepared,
      executions,
      accept: () => {
        for (const record of batch.records) reservations.delete(record.recordId);
        for (const [runId, state] of accepted) {
          if (executions.find((update) => update.checkpoint.progress.runId === runId)?.retire)
            cached.delete(runId);
          else cached.set(runId, state);
        }
        // Administrative writers can address many old Runs; retention is still bounded.
        while (cached.size > 8) {
          const first = cached.keys().next().value;

          if (first !== undefined) cached.delete(first);
        }
      },
    };
  });

  const tail = Effect.flatMap(CurrentRunWriter, (writer) =>
    writer.threadId !== threadId
      ? Effect.fail(failure("Progress writer belongs to another Thread"))
      : Effect.map(writer.tail, (position) => position.sequence),
  );

  const commitCaptured = Effect.fnUntraced(
    function* (batch: CanonicalBatch) {
      const writer = yield* CurrentRunWriter;

      for (let retries = 0; ; retries++) {
        const prepared = yield* prepare(batch, yield* tail).pipe(
          Effect.catchTag("ThreadNotMaterialized", (cause) =>
            failure("Selected Run disappeared during progress publication", cause),
          ),
        );

        const result = yield* writer.append(prepared.batch, prepared.executions).pipe(
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
    },
    (effect) =>
      Effect.flatMap(CurrentRunWriter, (writer) =>
        writer.withProgress(gate.withPermits(1)(effect)),
      ),
  );

  const publishCaptured = Effect.fnUntraced(
    function* (batch: CanonicalBatch) {
      const publisher = yield* CurrentRunSettlement;

      if (publisher.threadId !== threadId)
        return yield* failure("Settlement publisher belongs to another Thread");

      for (let retries = 0; ; retries++) {
        const prepared = yield* prepare(batch, (yield* publisher.tail).sequence).pipe(
          Effect.catchTag("ThreadNotMaterialized", (cause) =>
            failure("Selected Run disappeared during progress publication", cause),
          ),
        );

        const first = prepared.batch.records[0];

        if (first === undefined)
          return yield* failure("Settlement must publish a retained terminal fact");

        // prepare retains captured records within the validated batch bound.
        // The explicit first element proves this retained batch is still non-empty.
        const terminalBatch = CanonicalBatch.make(
          {
            ...prepared.batch,
            records: [first, ...prepared.batch.records.slice(1)],
          },
          { disableChecks: true },
        );

        const result = yield* publisher.publishSettlement(terminalBatch, prepared.executions).pipe(
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
    },
    (effect) =>
      Effect.flatMap(CurrentRunSettlement, (writer) =>
        writer.withProgress(gate.withPermits(1)(effect)),
      ),
  );

  const commit = (batch: CommitBatch) => captureFacts(batch).pipe(Effect.flatMap(commitCaptured));

  const publish = (batch: CanonicalBatch) =>
    captureFacts(batch).pipe(Effect.flatMap(publishCaptured));

  const preflight = (batch: CanonicalBatch) =>
    gate.withPermits(1)(
      Effect.gen(function* () {
        const owned = yield* captureFacts(batch);

        yield* prepare(owned, yield* tail, "preflight");
      }),
    );

  const reserve = (runId: RunId, probe: CanonicalBatch, bytes: number) =>
    gate.withPermits(1)(
      Effect.gen(function* () {
        const after = yield* tail;

        yield* prepare(yield* captureFacts(probe), after);
        const recordId = probe.records[0].recordId;
        const state = cached.get(runId) ?? (yield* loadUncachedState(runId, after));

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

        yield* prepare(owned, after, "preflight");

        const response = owned.records.find(
          (record) => record.payload._tag === "ModelResponseRecorded",
        );

        if (response === undefined || response.payload._tag !== "ModelResponseRecorded")
          return yield* failure("Deferred dispatch has no response");

        const progress =
          cached.get(response.payload.runId) ??
          (yield* loadUncachedState(response.payload.runId, after));

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

  const checkState = (state: ProgressState | undefined, recovered: boolean) => {
    if (state === undefined) return Effect.fail(failure("Execution has no canonical Run progress"));

    const capacity = () =>
      state.continuation.position === "settling" || state.continuation.position === "settled"
        ? Effect.void
        : checkCapacity(state);

    // Accepted states were shape-checked, but usage and reservations remain live inputs.
    return recovered ? Effect.andThen(checkFutureShapes(state), capacity) : capacity();
  };

  const check = (runId: RunId) =>
    gate.withPermits(1)(
      Effect.flatMap(CurrentRunWriter, (writer) => {
        if (writer.threadId !== threadId)
          return Effect.fail(failure("Progress writer belongs to another Thread"));

        const retained = cached.get(runId);

        if (retained !== undefined) return checkState(retained, false);

        return Effect.flatMap(writer.tail, (position) =>
          Effect.flatMap(loadUncachedState(runId, position.sequence), (state) =>
            checkState(state, true),
          ),
        );
      }),
    );

  return {
    commit,
    publish,
    preflight,
    reserve,
    defer,
    check,
    /**
     * Scope a verified empty execution/evidence view to one owned initialization body.
     * The caller holds progress+mutation gates; a changed writer or tail never reuses it. */
    withVerifiedEmptyRun: <A, E, R>(
      runId: RunId,
      through: CanonicalSequence,
      body: Effect.Effect<A, E, R>,
    ): Effect.Effect<A, E, R | CurrentRunWriter> =>
      Effect.flatMap(CurrentRunWriter, (writer) => {
        const view = { runId, through, writer };

        verifiedEmpty = view;

        return body.pipe(
          Effect.ensuring(
            Effect.sync(() => {
              if (verifiedEmpty === view) verifiedEmpty = undefined;
            }),
          ),
        );
      }),
    stageUsage: (runId: RunId, bytes: number) => {
      if (bytes === 0) stagedUsage.delete(runId);
      else stagedUsage.set(runId, bytes);
    },
  };
});
