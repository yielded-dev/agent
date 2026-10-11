import { Effect, Option, Schema, Stream } from "effect";
import { Prompt } from "effect/ai";

import type { RunId, ThreadId } from "../../core/Identifiers.ts";
import { boundedValueFootprint } from "../../engine/internal/bounded-value.ts";
import { canonicalJson, digestCanonicalJson, utf8ByteLength } from "../Digest.ts";
import {
  CanonicalSequence,
  CURRENT_RECORD_FORMAT,
  type CanonicalRecordEnvelope,
  type Digest,
} from "../Records.ts";
import { CurrentRunWriter } from "../RunContinuation.ts";
import { projectRunJournalStream, RunJournalError } from "../RunJournal.ts";
import type { RunWriter } from "../RunStorage.ts";
import {
  MAX_PROMPT_CHECKPOINT_BYTES,
  MAX_PROMPT_CHECKPOINT_RUNS,
  ThreadPromptRead,
  ThreadStore,
  ThreadTailRequest,
} from "../ThreadStore.ts";
import type { JournalRecordEnvelope } from "./journal-metadata.ts";
import { promptJsonFootprint } from "./prompt-footprint.ts";
import { digestRunHistory } from "./run-context.ts";

// One Scope-owned Thread, including native values and encoded text. Compaction reconstructs
// its canonical boundary map on demand; neither cache retains a second copy of that map.
const MAX_BYTES = 16 * 1024 * 1024;
const MAX_ENTRIES = 16_384;
const MAX_SUFFIX_RECORDS = 1_024;
// Cold restoration reads a small page per snapshot; the native-value guard below
// still bounds retained suffix facts before any further page is requested.
const COLD_SUFFIX_PAGE_RECORDS = 8;
const JSON_PREFIX = '{"content":[';
const encodePrompt = Schema.encodeEffect(Schema.toCodecJson(Prompt.Prompt));
const parseJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));

// Construct these only when a persisted checkpoint is read. The encoded-side schema
// validates without running native defaults for every historical message and part.
const decodeEncodedPrompt = Schema.decodeUnknownEffect(
  Schema.suspend(() =>
    Schema.Struct({
      content: Schema.Array(Schema.toEncoded(Prompt.Message)).check(
        Schema.isMaxLength(MAX_ENTRIES),
      ),
    }),
  ),
);

const decodePrompt = Schema.decodeUnknownEffect(
  Schema.suspend(() =>
    Schema.Struct({
      content: Schema.Array(Schema.toCodecJson(Prompt.Message)).check(
        Schema.isMaxLength(MAX_ENTRIES),
      ),
    }),
  ),
);

// Inputs have crossed Effect's encoded-message Schema boundary. Native constructors
// supply only the brands and defaults; Effect still owns every message and part type.
function hydratePart(part: Prompt.UserMessagePartEncoded): Prompt.UserMessagePart;
function hydratePart(part: Prompt.ToolMessagePartEncoded): Prompt.ToolMessagePart;
function hydratePart(part: Prompt.AssistantMessagePartEncoded): Prompt.AssistantMessagePart;
function hydratePart(part: Prompt.PartEncoded): Prompt.Part {
  switch (part.type) {
    case "text":
      return Prompt.textPart(part);
    case "reasoning":
      return Prompt.reasoningPart(part);
    case "file":
      return Prompt.filePart(part);
    case "tool-call":
      // Unlike options, providerExecuted is required by Effect's constructors;
      // only its Schema decoder supplies the false default.
      return Prompt.toolCallPart({ ...part, providerExecuted: part.providerExecuted ?? false });
    case "tool-result":
      return Prompt.toolResultPart({ ...part, providerExecuted: part.providerExecuted ?? false });
    case "tool-approval-request":
      return Prompt.toolApprovalRequestPart(part);
    case "tool-approval-response":
      return Prompt.toolApprovalResponsePart(part);
  }
  throw new Error(`Unhandled Prompt part: ${part satisfies never}`);
}

const hydrateMessage = (message: Prompt.MessageEncoded): Prompt.Message => {
  switch (message.role) {
    case "system":
      return Prompt.systemMessage(message);
    case "user":
      return Prompt.userMessage({
        ...message,
        content:
          typeof message.content === "string"
            ? [Prompt.textPart({ text: message.content })]
            : message.content.map((p) => hydratePart(p)),
      });
    case "assistant":
      return Prompt.assistantMessage({
        ...message,
        content:
          typeof message.content === "string"
            ? [Prompt.textPart({ text: message.content })]
            : message.content.map((p) => hydratePart(p)),
      });
    case "tool":
      return Prompt.toolMessage({
        ...message,
        content: message.content.map((p) => hydratePart(p)),
      });
  }
  throw new Error(`Unhandled Prompt message: ${message satisfies never}`);
};

export interface PromptHistory {
  readonly prompt: Prompt.Prompt;
  encoded: string | undefined;
  bytes: number | undefined;
  digest?: Digest | undefined;
  readonly runsSinceCheckpoint?: number | undefined;
}

export interface CachedPromptHistory extends PromptHistory {
  readonly threadId: ThreadId;
  readonly through: CanonicalSequence;
}

/** Internal canonical projection; ownership comes from the ThreadStore read contract. */
export const preparePromptHistory = (prompt: Prompt.Prompt): PromptHistory | undefined =>
  prompt.content.length > MAX_ENTRIES
    ? undefined
    : { prompt, encoded: undefined, bytes: undefined };

const retainedBytes = (value: unknown, json: string, allowance: number) => {
  const encodedBytes = 2 * json.length;

  if (encodedBytes > allowance) return undefined;
  const footprint = promptJsonFootprint(value, Math.floor((allowance - encodedBytes) / 2));

  return footprint === undefined ? undefined : encodedBytes + 2 * footprint;
};

const encodeHistory = Effect.fnUntraced(function* (prompt: Prompt.Prompt, allowance: number) {
  const value = yield* encodePrompt(prompt);
  const encoded = canonicalJson(value);

  return { encoded, bytes: retainedBytes(value, encoded, allowance) };
});

// Finish parsing/decoding before suspending for suffix reads. Only the native prefix and its
// canonical string leave this Effect; it does not keep a second parsed root in the outer Run.
export const decodeCheckpointPrefix = Effect.fnUntraced(function* (
  encoded: string,
  length: number,
) {
  const value = yield* parseJson(encoded);
  const bytes = retainedBytes(value, encoded, MAX_BYTES);

  if (bytes === undefined) return undefined;

  const decoded = yield* decodeEncodedPrompt(value).pipe(
    Effect.map((prompt) => ({ content: prompt.content.map(hydrateMessage) })),
    // Keep the canonical codec as fallback for representations the encoded Schema rejects.
    Effect.catch(() => decodePrompt(value)),
  );

  return decoded.content.length === length
    ? { prompt: Prompt.fromMessages(decoded.content), encoded, bytes }
    : undefined;
});

// A concatenated content index has at most five digits under MAX_ENTRIES. Relative to the
// suffix's original one-or-more digits, reserve four extra bytes in both value estimates.
const appendedBytes = (
  prefix: number,
  suffix: { readonly bytes: number; readonly prompt: Prompt.Prompt },
) => prefix + suffix.bytes + 8 * suffix.prompt.content.length;

/** The ordinary codec remains the oracle on canonical reconstruction and explicit verify. */
export const digestPromptHistory = (prompt: Prompt.Prompt, history: PromptHistory | undefined) => {
  if (history === undefined) return digestRunHistory(prompt);
  if (
    prompt.content.length !== history.prompt.content.length ||
    prompt.content.some((message, index) => message !== history.prompt.content[index])
  ) {
    history.encoded = undefined;
    history.bytes = undefined;
    history.digest = undefined;

    return digestRunHistory(prompt);
  }

  return Effect.gen(function* () {
    let encoded = history.encoded;

    if (encoded === undefined) {
      const captured = yield* encodeHistory(prompt, MAX_BYTES);

      encoded = captured.encoded;
      if (captured.bytes !== undefined) {
        history.encoded = encoded;
        history.bytes = captured.bytes;
      }
    }
    const digest = yield* digestCanonicalJson(encoded);

    history.digest = digest;

    return digest;
  }).pipe(
    Effect.mapError((cause) =>
      RunJournalError.make({
        message: "Original model history integrity is unavailable",
        cause,
      }),
    ),
  );
};

/** Writer identity is data; effectful writer ports stay in Effect context. */
type WriterIdentity = Pick<RunWriter, "threadId">;

/** Fold only complete, sequential Runs. Raw facts and encoded/native messages are bounded. */
const readSuffix = Effect.fnUntraced(function* (
  writer: WriterIdentity,
  from: CanonicalSequence,
  through: CanonicalSequence,
  firstRun: RunId,
  allowance: number,
  firstThrough: CanonicalSequence,
  pageRecords = 1,
) {
  const store = yield* ThreadStore;
  const records: Array<JournalRecordEnvelope> = [];
  let after = from;
  let remaining = allowance;
  let active: RunId | undefined;
  let runs = 0;

  while (after < through) {
    const page = yield* store
      .readPrompt(
        ThreadPromptRead.make({
          threadId: writer.threadId,
          afterSequence: after,
          throughSequence: through,
          limit: pageRecords,
        }),
      )
      .pipe(Stream.take(pageRecords + 1), Stream.runCollect);

    if (page.length > pageRecords || records.length + page.length > MAX_SUFFIX_RECORDS)
      return undefined;
    for (const entry of page) {
      const payload = entry.record.payload;

      if (
        entry.threadId !== writer.threadId ||
        entry.sequence <= after ||
        entry.sequence > through ||
        payload._tag === "CompactionCreated" ||
        payload._tag === "ModelCompleted" ||
        payload._tag === "RunFailed"
      )
        return undefined;
      if (payload._tag === "UserInputRecorded") {
        if (
          active !== undefined ||
          payload.kind !== "user" ||
          payload.runId === undefined ||
          (runs === 0 && (payload.runId !== firstRun || entry.sequence !== from + 1))
        )
          return undefined;
        active = payload.runId;
        if (++runs > MAX_PROMPT_CHECKPOINT_RUNS) return undefined;
      } else {
        if (active === undefined || payload.runId !== active) return undefined;
        if (payload._tag === "SubmissionSettled") {
          if (payload.outcome !== "completed" || (runs === 1 && entry.sequence !== firstThrough))
            return undefined;
          active = undefined;
        }
      }

      // This native-value guard only visits a bounded new suffix. Reserve encoding/escape
      // copies before collecting it; the full encoded prefix uses the JSON-specific counter.
      const footprint = boundedValueFootprint(
        {
          ...entry,
          record: {
            ...entry.record,
            payload:
              payload._tag === "ModelResponseRecorded"
                ? { ...payload, messages: payload.messages.content }
                : { ...payload },
          },
        },
        Math.floor(remaining / 32),
      );

      if (footprint === undefined) return undefined;
      remaining -= 32 * footprint;
      records.push(entry);
      after = entry.sequence;
    }
    if (page.length === 0) break;
  }
  if (runs === 0 || active !== undefined) return undefined;
  let incomplete = false;

  const delta = yield* projectRunJournalStream(
    Stream.fromIterable(records),
    undefined,
    (boundary) => {
      if (boundary.incomplete) incomplete = true;
    },
  );

  if (incomplete || delta.prompt.content.length > MAX_ENTRIES) return undefined;
  const encoded = yield* encodeHistory(delta.prompt, allowance);

  return encoded.bytes === undefined
    ? undefined
    : { ...encoded, bytes: encoded.bytes, prompt: delta.prompt, runs };
});

const appendJson = (
  prefix: { readonly prompt: Prompt.Prompt; readonly encoded: string },
  suffix: { readonly prompt: Prompt.Prompt; readonly encoded: string },
) =>
  prefix.encoded.slice(0, -2) +
  (prefix.prompt.content.length > 0 && suffix.prompt.content.length > 0 ? "," : "") +
  suffix.encoded.slice(JSON_PREFIX.length);

/** Sequence anchors are unambiguous within one append-only Thread; import discards derivatives. */
export const makePromptHistoryCache = Effect.fnUntraced(function* () {
  let current: CachedPromptHistory | undefined;
  let closed = false;

  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      current = undefined;
      closed = true;
    }),
  );

  const take = Effect.fnUntraced(
    function* (writer: WriterIdentity) {
      const cached = current;

      current = undefined;
      if (closed) return undefined;
      const store = yield* ThreadStore;

      if (cached === undefined && store.promptCheckpoints === undefined) return undefined;
      if (cached !== undefined && cached.threadId !== writer.threadId) return undefined;
      const head = yield* store.inspectTail(ThreadTailRequest.make({ threadId: writer.threadId }));

      if (cached !== undefined) {
        return head.tailSequence === cached.through ? cached : undefined;
      }
      if (store.promptCheckpoints === undefined) return undefined;

      const loaded = yield* store.promptCheckpoints.load({
        threadId: writer.threadId,
        throughSequence: head.tailSequence,
      });

      if (Option.isNone(loaded)) return undefined;
      const checkpoint = loaded.value;

      if (
        checkpoint.threadId !== writer.threadId ||
        checkpoint.throughSequence > head.tailSequence ||
        checkpoint.historyThrough >= checkpoint.throughSequence ||
        utf8ByteLength(checkpoint.promptJson) > MAX_PROMPT_CHECKPOINT_BYTES ||
        2 * checkpoint.promptJson.length > MAX_BYTES ||
        !checkpoint.promptJson.startsWith(JSON_PREFIX) ||
        (yield* digestCanonicalJson(checkpoint.promptJson)) !== checkpoint.historyDigest
      )
        return undefined;

      const prefix = yield* decodeCheckpointPrefix(
        checkpoint.promptJson,
        checkpoint.priorHistoryLength,
      );

      if (prefix === undefined) return undefined;

      const delta = yield* readSuffix(
        writer,
        checkpoint.historyThrough,
        head.tailSequence,
        checkpoint.runId,
        MAX_BYTES - prefix.bytes,
        checkpoint.throughSequence,
        COLD_SUFFIX_PAGE_RECORDS,
      );

      if (
        delta === undefined ||
        appendedBytes(prefix.bytes, delta) > MAX_BYTES ||
        prefix.prompt.content.length + delta.prompt.content.length > MAX_ENTRIES
      )
        return undefined;

      // A cold load refreshes after this Run. Warm reuse amortizes replacements across Runs.
      return {
        threadId: writer.threadId,
        through: head.tailSequence,
        prompt: Prompt.concat(prefix.prompt, delta.prompt),
        encoded: appendJson(prefix, delta),
        bytes: appendedBytes(prefix.bytes, delta),
      } satisfies CachedPromptHistory;
    },
    Effect.orElseSucceed(() => undefined),
  );

  const complete = Effect.fnUntraced(
    function* (writer: WriterIdentity, original: CanonicalRecordEnvelope, history: PromptHistory) {
      const { encoded, bytes, digest } = history;
      const input = original.record.payload;

      if (
        closed ||
        encoded === undefined ||
        bytes === undefined ||
        digest === undefined ||
        input._tag !== "UserInputRecorded" ||
        input.runId === undefined
      )
        return;
      const store = yield* ThreadStore;
      const tail = yield* (yield* CurrentRunWriter).tail;

      const delta = yield* readSuffix(
        writer,
        CanonicalSequence.make(original.sequence - 1),
        tail.sequence,
        input.runId,
        MAX_BYTES - bytes,
        tail.sequence,
      );

      if (
        delta === undefined ||
        delta.runs !== 1 ||
        appendedBytes(bytes, delta) > MAX_BYTES ||
        history.prompt.content.length + delta.prompt.content.length > MAX_ENTRIES
      )
        return;
      const head = yield* store.inspectTail(ThreadTailRequest.make({ threadId: writer.threadId }));

      if (closed || head.tailSequence !== tail.sequence) return;
      const runsSinceCheckpoint = (history.runsSinceCheckpoint ?? MAX_PROMPT_CHECKPOINT_RUNS) + 1;
      const refresh = runsSinceCheckpoint >= MAX_PROMPT_CHECKPOINT_RUNS;

      if (refresh && store.promptCheckpoints !== undefined) {
        yield* store.promptCheckpoints
          .save({
            version: 2,
            recordFormat: CURRENT_RECORD_FORMAT,
            threadId: writer.threadId,
            runId: input.runId,
            throughSequence: tail.sequence,
            historyThrough: CanonicalSequence.make(original.sequence - 1),
            priorHistoryLength: history.prompt.content.length,
            historyDigest: digest,
            promptJson: encoded,
          })
          .pipe(Effect.catch(() => Effect.void));
      }
      if (closed) return;
      current = {
        threadId: writer.threadId,
        through: tail.sequence,
        prompt: Prompt.concat(history.prompt, delta.prompt),
        encoded: appendJson({ prompt: history.prompt, encoded }, delta),
        bytes: appendedBytes(bytes, delta),
        runsSinceCheckpoint: refresh ? 0 : runsSinceCheckpoint,
      };
    },
    Effect.catch(() => Effect.void),
  );

  return { take, complete };
});
