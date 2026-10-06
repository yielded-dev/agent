import { Effect, Option, Schema, Stream } from "effect";
import { Prompt } from "effect/ai";

import { digestJson } from "../Digest.ts";
import {
  CanonicalSequence,
  MAX_RUN_CONTEXT_BYTES,
  MAX_RUN_CONTEXT_RECORDS,
  MAX_RUN_EVIDENCE_BYTES,
  MAX_RUN_EVIDENCE_RECORDS,
  type CanonicalRecordEnvelope,
} from "../Records.ts";
import { RunJournalError, toolCallSettledRecordId } from "../RunJournal.ts";
import { submissionSettlementRecordId } from "../SubmissionLedger.ts";
import {
  getRecord,
  getRunInput,
  PROMPT_EVIDENCE_TAGS,
  ThreadReader,
  type ThreadSelection,
} from "../ThreadStore.ts";
import { reference, resolveEvidence } from "./evidence.ts";
import { recordEncoding } from "./record-encoding.ts";
import { projectRunContext } from "./run-context.ts";

const invalid = (message: string) => RunJournalError.make({ message });
const promptTag = new Set<string>(PROMPT_EVIDENCE_TAGS);

/** One flat saved prefix plus a fixed admission suffix and exact canonical dependencies. */
export const initialContext = Effect.fnUntraced(function* (original: CanonicalRecordEnvelope) {
  const reader = yield* ThreadReader;
  const threadId = original.threadId;
  const through = CanonicalSequence.make(original.sequence - 1);
  const facts = new Map<number, CanonicalRecordEnvelope>();
  const queue: Array<CanonicalRecordEnvelope> = [];
  const readIds = new Set<string>();
  const creatorInputs = new Set<string>();
  const terminalInputs = new Set<string>();
  let residentBytes = 0;
  let readBytes = 0;

  const account = (entry: CanonicalRecordEnvelope) => {
    if (entry.threadId !== threadId || entry.sequence < 1 || entry.sequence > through) return false;
    if (readIds.has(entry.record.recordId)) return true;
    const bytes = recordEncoding(entry.record).bytes;

    if (
      readIds.size >= MAX_RUN_CONTEXT_RECORDS + MAX_RUN_EVIDENCE_RECORDS ||
      readBytes + bytes > MAX_RUN_CONTEXT_BYTES + MAX_RUN_EVIDENCE_BYTES
    )
      return false;
    readIds.add(entry.record.recordId);
    readBytes += bytes;

    return true;
  };

  const retain = (entry: CanonicalRecordEnvelope) => {
    if (!account(entry) || !promptTag.has(entry.record.payload._tag))
      return Effect.fail(
        invalid("Initial context dependency escaped its admission or read budget"),
      );
    const prior = facts.get(entry.sequence);

    if (prior !== undefined)
      return prior.record.recordId === entry.record.recordId
        ? Effect.void
        : Effect.fail(invalid("Initial context has conflicting canonical identities"));
    const bytes = recordEncoding(entry.record).bytes;

    if (facts.size >= MAX_RUN_CONTEXT_RECORDS || residentBytes + bytes > MAX_RUN_CONTEXT_BYTES)
      return Effect.fail(
        invalid("Initial model context exceeds its evidence budget; configure context compaction"),
      );
    residentBytes += bytes;
    facts.set(entry.sequence, entry);
    queue.push(entry);

    return Effect.void;
  };

  const read = Effect.fnUntraced(function* (
    selection: ThreadSelection,
    consume: (entry: CanonicalRecordEnvelope) => Effect.Effect<void, RunJournalError>,
    after = 0,
  ) {
    let cursor = CanonicalSequence.make(after);

    while (true) {
      const page = yield* reader
        .read({ threadId, selection, page: { afterSequence: cursor, limit: 8 } })
        .pipe(Stream.take(9), Stream.runCollect);

      if (page.length > 8) return yield* invalid("Initial context page exceeded its record bound");
      for (const entry of page) {
        if (entry.sequence <= cursor || !account(entry))
          return yield* invalid("Initial context selection exceeds its admission or read budget");
        cursor = entry.sequence;
        yield* consume(entry);
      }
      if (page.length < 8) return;
    }
  });

  let base: CanonicalRecordEnvelope | undefined;

  yield* read({ _tag: "LatestRunContext", throughSequence: through }, (entry) => {
    if (base !== undefined) return Effect.fail(invalid("Initial context locator is ambiguous"));
    base = entry;

    return Effect.void;
  });
  let after = 0;

  if (base !== undefined) {
    const context = base.record.payload;

    if (context._tag !== "RunContextRecorded")
      return yield* invalid("Prior context locator has another record kind");
    const input = yield* getRunInput({ threadId, runId: context.runId });

    if (
      Option.isNone(input) ||
      input.value.sequence !== context.historyThrough + 1 ||
      !account(input.value)
    )
      return yield* invalid("Prior context has no exact original input");
    const prefix: Array<CanonicalRecordEnvelope> = [];

    for (const ref of context.history) {
      const fact = yield* resolveEvidence(threadId, ref);

      if (fact.sequence !== ref.sequence)
        return yield* invalid("Prior context reference has another sequence");
      yield* retain(fact);
      prefix.push(fact);
    }
    yield* projectRunContext(context, input.value, (yield* reference(base.record)).digest, prefix);
    after = context.historyThrough;
  }
  yield* read({ _tag: "PromptEvidence", throughSequence: through }, retain, after);

  for (let index = 0; index < queue.length; index++) {
    const entry = queue[index];

    if (entry === undefined) return yield* invalid("Initial context dependency disappeared");
    const payload = entry.record.payload;

    if (payload._tag === "CompactionCreated" && !creatorInputs.has(payload.runId)) {
      creatorInputs.add(payload.runId);
      const input = yield* getRunInput({ threadId, runId: payload.runId });

      if (Option.isNone(input)) return yield* invalid("Compaction has no exact creator input");
      yield* retain(input.value);
    }
    if (payload._tag !== "ToolCallSettled") continue;
    let declaration: CanonicalRecordEnvelope | undefined;

    yield* read(
      {
        _tag: "ToolDeclaration",
        settlementRecordId: entry.record.recordId,
        throughSequence: through,
      },
      (fact) => {
        if (declaration !== undefined)
          return Effect.fail(invalid("Late Tool declaration is ambiguous"));
        declaration = fact;

        return Effect.void;
      },
    );
    if (declaration === undefined || declaration.sequence >= entry.sequence)
      return yield* invalid("Late Tool result has no exact original declaration");
    const response = declaration.record.payload;

    if (
      response._tag !== "ModelResponseRecorded" ||
      response.runId !== payload.runId ||
      toolCallSettledRecordId(response.runId, response.turn, payload.toolCallId) !==
        entry.record.recordId ||
      !response.toolOperations.some(
        (operation) =>
          operation.toolCallId === payload.toolCallId && operation.toolName === payload.toolName,
      )
    )
      return yield* invalid("Late Tool result differs from its original declaration identity");

    const prompt = yield* Schema.decodeUnknownEffect(Prompt.Prompt)(response.messages).pipe(
      Effect.mapError(() => invalid("Late Tool declaration messages are malformed")),
    );

    if (
      !prompt.content.some(
        (message) =>
          message.role === "assistant" &&
          message.content.some(
            (part) =>
              part.type === "tool-call" &&
              !part.providerExecuted &&
              part.id === payload.toolCallId &&
              part.name === payload.toolName,
          ),
      ) ||
      (yield* digestJson(response.messages).pipe(
        Effect.mapError(() => invalid("Late Tool declaration integrity is unavailable")),
      )) !== response.messagesDigest
    )
      return yield* invalid("Late Tool declaration arguments or digest are invalid");
    yield* retain(declaration);

    // Restore only the immutable terminal proof for retired incomplete pruning/rollover batches.
    if (!terminalInputs.has(payload.runId)) {
      terminalInputs.add(payload.runId);
      const input = yield* getRunInput({ threadId, runId: payload.runId });

      if (
        Option.isSome(input) &&
        input.value.record.payload._tag === "UserInputRecorded" &&
        input.value.record.payload.submissionId !== undefined
      ) {
        if (!account(input.value))
          return yield* invalid("Late Tool owner exceeds the evidence budget");

        const terminal = yield* getRecord({
          threadId,
          recordId: submissionSettlementRecordId(input.value.record.payload.submissionId),
        });

        if (Option.isSome(terminal) && terminal.value.sequence <= through) {
          if (
            terminal.value.record.payload._tag !== "SubmissionSettled" ||
            terminal.value.record.payload.runId !== payload.runId
          )
            return yield* invalid("Late Tool owner has invalid terminal evidence");
          yield* retain(terminal.value);
        }
      }
    }
  }

  return [...facts.values()].sort((left, right) => left.sequence - right.sequence).concat(original);
});
