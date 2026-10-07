import { Effect, Option, Schema, Stream } from "effect";
import { Prompt } from "effect/ai";

import { digestJson } from "../Digest.ts";
import { CanonicalSequence, type CanonicalRecordEnvelope } from "../Records.ts";
import { RunJournalError, toolCallSettledRecordId } from "../RunJournal.ts";
import { submissionSettlementRecordId } from "../SubmissionLedger.ts";
import { getRecord, getRunInput, PROMPT_EVIDENCE_TAGS, ThreadReader } from "../ThreadStore.ts";
import type { JournalRecordEnvelope } from "./journal-metadata.ts";

const invalid = (message: string) => RunJournalError.make({ message });
const promptTag = new Set<string>(PROMPT_EVIDENCE_TAGS);

/** Fresh admission trusts validated immutable history; recovery verifies its saved Prompt digest. */
export const initialContext = Effect.fnUntraced(function* (original: CanonicalRecordEnvelope) {
  const reader = yield* ThreadReader;
  const threadId = original.threadId;
  const through = CanonicalSequence.make(original.sequence - 1);
  const facts = new Map<number, JournalRecordEnvelope>();
  const queue: Array<JournalRecordEnvelope> = [];
  const creatorInputs = new Set<string>();
  const terminalInputs = new Set<string>();
  const declarations = new Set<string>();

  const retain = (entry: JournalRecordEnvelope): RunJournalError | undefined => {
    if (
      entry.threadId !== threadId ||
      entry.sequence < 1 ||
      entry.sequence > through ||
      !promptTag.has(entry.record.payload._tag)
    )
      return invalid("Initial context dependency escaped its admission boundary");
    const prior = facts.get(entry.sequence);

    if (prior !== undefined)
      return prior.record.recordId === entry.record.recordId
        ? undefined
        : invalid("Initial context has conflicting canonical identities");
    facts.set(entry.sequence, entry);
    queue.push(entry);
    const payload = entry.record.payload;

    if (
      payload._tag === "UserInputRecorded" &&
      payload.kind === "user" &&
      payload.runId !== undefined
    )
      creatorInputs.add(payload.runId);
    if (payload._tag === "ModelResponseRecorded") {
      if ("toolOperations" in payload) {
        for (const operation of payload.toolOperations)
          declarations.add(
            toolCallSettledRecordId(payload.runId, payload.turn, operation.toolCallId),
          );
      } else {
        for (const message of payload.messages.content) {
          if (message.role !== "assistant") continue;
          for (const part of message.content)
            if (part.type === "tool-call" && !part.providerExecuted)
              declarations.add(`tool-settled:${payload.runId}:${payload.turn}:${part.id}`);
        }
      }
    }

    return undefined;
  };

  const contexts = yield* reader
    .read({
      threadId,
      selection: { _tag: "LatestRunContext", throughSequence: through },
      page: { limit: 1 },
    })
    .pipe(Stream.take(2), Stream.runCollect);

  if (contexts.length > 1) return yield* invalid("Initial context locator is ambiguous");
  let after = CanonicalSequence.make(0);
  const base = contexts[0];

  if (base !== undefined) {
    const context = base.record.payload;

    if (
      base.threadId !== threadId ||
      base.sequence > through ||
      context._tag !== "RunContextRecorded" ||
      context.historyThrough >= base.sequence ||
      context.historyThrough > through
    )
      return yield* invalid("Prior context locator has an invalid admission boundary");
    after = CanonicalSequence.make(context.historyFrom - 1);
    for (const ref of context.retained) {
      const fact = yield* getRecord({ threadId, recordId: ref.recordId });

      if (Option.isNone(fact) || fact.value.sequence !== ref.sequence)
        return yield* invalid("Prior context has a missing retained fact");
      const error = retain(fact.value);

      if (error !== undefined) return yield* error;
    }
  }

  while (after < through) {
    const page = yield* reader
      .readPrompt({ threadId, afterSequence: after, throughSequence: through, limit: 256 })
      .pipe(Stream.take(257), Stream.runCollect);

    if (page.length > 256) return yield* invalid("Initial context page exceeded its record bound");
    for (const entry of page) {
      if (entry.sequence <= after)
        return yield* invalid("Initial context selection is not in canonical order");
      after = entry.sequence;
      const error = retain(entry);

      if (error !== undefined) return yield* error;
    }
    if (page.length < 256) break;
  }

  for (let index = 0; index < queue.length; index++) {
    const entry = queue[index];

    if (entry === undefined) return yield* invalid("Initial context dependency disappeared");
    const payload = entry.record.payload;

    if (payload._tag === "CompactionCreated" && !creatorInputs.has(payload.runId)) {
      creatorInputs.add(payload.runId);
      const input = yield* getRunInput({ threadId, runId: payload.runId });

      if (Option.isNone(input)) return yield* invalid("Compaction has no exact creator input");
      const error = retain(input.value);

      if (error !== undefined) return yield* error;
    }
    if (payload._tag !== "ToolCallSettled" || declarations.has(entry.record.recordId)) continue;

    const matches = yield* reader
      .read({
        threadId,
        selection: {
          _tag: "ToolDeclaration",
          settlementRecordId: entry.record.recordId,
          throughSequence: through,
        },
        page: { limit: 1 },
      })
      .pipe(Stream.take(2), Stream.runCollect);

    const declaration = matches[0];

    if (matches.length !== 1 || declaration === undefined || declaration.sequence >= entry.sequence)
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
    const error = retain(declaration);

    if (error !== undefined) return yield* error;

    // Retired incomplete batches need their original owner and immutable terminal proof.
    if (!terminalInputs.has(payload.runId)) {
      terminalInputs.add(payload.runId);
      const input = yield* getRunInput({ threadId, runId: payload.runId });

      if (Option.isNone(input)) return yield* invalid("Late Tool result has no original owner");
      const inputError = retain(input.value);

      if (inputError !== undefined) return yield* inputError;
      const owner = input.value.record.payload;

      if (owner._tag === "UserInputRecorded" && owner.submissionId !== undefined) {
        const terminal = yield* getRecord({
          threadId,
          recordId: submissionSettlementRecordId(owner.submissionId),
        });

        if (Option.isSome(terminal) && terminal.value.sequence <= through) {
          if (
            terminal.value.record.payload._tag !== "SubmissionSettled" ||
            terminal.value.record.payload.runId !== payload.runId
          )
            return yield* invalid("Late Tool owner has invalid terminal evidence");
          const terminalError = retain(terminal.value);

          if (terminalError !== undefined) return yield* terminalError;
        }
      }
    }
  }

  return [...facts.values()].sort((left, right) => left.sequence - right.sequence).concat(original);
});
