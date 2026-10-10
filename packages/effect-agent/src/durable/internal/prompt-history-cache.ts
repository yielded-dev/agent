import { Effect, Option, Schema, Stream } from "effect";
import { Prompt } from "effect/ai";

import type { RunId, ThreadId } from "../../core/Identifiers.ts";
import { boundedValueFootprint } from "../../engine/internal/bounded-value.ts";
import { canonicalJson, digestCanonicalJson, utf8ByteLength } from "../Digest.ts";
import {
  CanonicalSequence,
  CURRENT_RECORD_FORMAT,
  RecordId,
  type CanonicalRecordEnvelope,
  type Digest,
  type ProducerEpoch,
} from "../Records.ts";
import { projectRunJournalStream, RunJournalError } from "../RunJournal.ts";
import type { RunWriter } from "../RunStorage.ts";
import {
  getRecord,
  MAX_PROMPT_CHECKPOINT_BYTES,
  MAX_PROMPT_CHECKPOINT_RUNS,
  ThreadPromptRead,
  ThreadTailRequest,
  type ThreadStore,
} from "../ThreadStore.ts";
import type { JournalRecordEnvelope } from "./journal-metadata.ts";
import { promptJsonFootprint } from "./prompt-footprint.ts";
import { digestRunHistory } from "./run-context.ts";

// One Scope-owned Thread, including native values and encoded text. Compaction reconstructs
// its canonical boundary map on demand; neither cache retains a second copy of that map.
const MAX_BYTES = 16 * 1024 * 1024;
const MAX_ENTRIES = 16_384;
const MAX_SUFFIX_RECORDS = 1_024;
const JSON_PREFIX = '{"content":[';
const encodePrompt = Schema.encodeEffect(Schema.toCodecJson(Prompt.Prompt));
const parseJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));

const decodePrompt = Schema.decodeUnknownEffect(
  Schema.Struct({
    content: Schema.Array(Schema.toCodecJson(Prompt.Message)).check(
      Schema.isMaxLength(MAX_ENTRIES),
    ),
  }),
);

export interface PromptHistory {
  readonly prompt: Prompt.Prompt;
  encoded: string | undefined;
  bytes: number | undefined;
  digest?: Digest | undefined;
  readonly refreshedAt?: ProducerEpoch | undefined;
}

export interface CachedPromptHistory extends PromptHistory {
  readonly threadId: ThreadId;
  readonly through: CanonicalSequence;
  readonly tailDigest: Digest;
  readonly producerEpoch: ProducerEpoch;
  /** Canonical authentication of the prefix before the suffix folded into this Prompt. */
  readonly contextRecordId: RecordId;
  readonly historyDigest: Digest;
}

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

/** Fold only complete, sequential Runs. Raw facts and encoded/native messages are bounded. */
const readSuffix = Effect.fnUntraced(function* (
  store: ThreadStore["Service"],
  writer: RunWriter,
  from: CanonicalSequence,
  through: CanonicalSequence,
  firstRun: RunId,
  allowance: number,
) {
  const records: Array<JournalRecordEnvelope> = [];
  let after = from;
  let remaining = allowance;
  let active: RunId | undefined;
  let completed = false;
  let runs = 0;

  while (after < through) {
    const page = yield* store
      .readPrompt(
        ThreadPromptRead.make({
          threadId: writer.threadId,
          afterSequence: after,
          throughSequence: through,
          limit: 1,
        }),
      )
      .pipe(Stream.take(2), Stream.runCollect);

    if (page.length > 1 || records.length + page.length > MAX_SUFFIX_RECORDS) return undefined;
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
        completed = false;
        if (++runs > MAX_PROMPT_CHECKPOINT_RUNS) return undefined;
      } else {
        if (active === undefined || payload.runId !== active) return undefined;
        if (payload._tag === "RunCompleted") completed = true;
        if (payload._tag === "SubmissionSettled") {
          if (!completed) return undefined;
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

/** Memory and persisted prefixes share the same canonical anchor and suffix rules. */
export const makePromptHistoryCache = Effect.fnUntraced(function* (store: ThreadStore["Service"]) {
  let current: CachedPromptHistory | undefined;
  let closed = false;

  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      current = undefined;
      closed = true;
    }),
  );

  const take = Effect.fnUntraced(
    function* (writer: RunWriter) {
      const cached = current;

      current = undefined;
      if (closed) return undefined;
      if (cached === undefined && store.promptCheckpoints === undefined) return undefined;
      if (
        cached !== undefined &&
        (cached.threadId !== writer.threadId || writer.producerEpoch !== cached.producerEpoch + 1)
      )
        return undefined;
      const head = yield* store.inspectTail(ThreadTailRequest.make({ threadId: writer.threadId }));

      if (head.producerEpoch !== writer.producerEpoch) return undefined;
      if (cached !== undefined) {
        return head.tailSequence === cached.through && head.tailDigest === cached.tailDigest
          ? cached
          : undefined;
      }
      if (store.promptCheckpoints === undefined) return undefined;

      const loaded = yield* store.promptCheckpoints.load({
        threadId: writer.threadId,
        producerEpoch: writer.producerEpoch,
      });

      if (Option.isNone(loaded)) return undefined;
      const checkpoint = loaded.value;

      const saved = yield* getRecord({
        threadId: writer.threadId,
        recordId: checkpoint.contextRecordId,
      });

      if (Option.isNone(saved)) return undefined;
      const context = saved.value.record.payload;

      if (
        context._tag !== "RunContextRecorded" ||
        saved.value.sequence > checkpoint.throughSequence ||
        context.historyThrough >= saved.value.sequence ||
        context.historyFrom !== 1 ||
        context.retained.length !== 0 ||
        context.contextWindowId !== undefined ||
        utf8ByteLength(checkpoint.promptJson) > MAX_PROMPT_CHECKPOINT_BYTES ||
        2 * checkpoint.promptJson.length > MAX_BYTES ||
        !checkpoint.promptJson.startsWith(JSON_PREFIX) ||
        (yield* digestCanonicalJson(checkpoint.promptJson)) !== context.historyDigest
      )
        return undefined;
      const value = yield* parseJson(checkpoint.promptJson);
      const bytes = retainedBytes(value, checkpoint.promptJson, MAX_BYTES);

      if (bytes === undefined) return undefined;
      const decoded = yield* decodePrompt(value);

      if (decoded.content.length !== context.priorHistoryLength) return undefined;

      const prefix = {
        prompt: Prompt.fromMessages(decoded.content),
        encoded: checkpoint.promptJson,
        bytes,
      };

      const delta = yield* readSuffix(
        store,
        writer,
        context.historyThrough,
        head.tailSequence,
        context.runId,
        MAX_BYTES - bytes,
      );

      if (
        delta === undefined ||
        delta.runs !== writer.producerEpoch - checkpoint.producerEpoch ||
        prefix.prompt.content.length + delta.prompt.content.length > MAX_ENTRIES
      )
        return undefined;

      // A cold load refreshes after this Run. Warm claims then amortize replacements across Runs.
      return {
        threadId: writer.threadId,
        through: head.tailSequence,
        tailDigest: head.tailDigest,
        producerEpoch: writer.producerEpoch,
        contextRecordId: checkpoint.contextRecordId,
        historyDigest: context.historyDigest,
        prompt: Prompt.concat(prefix.prompt, delta.prompt),
        encoded: appendJson(prefix, delta),
        bytes: bytes + delta.bytes,
      } satisfies CachedPromptHistory;
    },
    Effect.orElseSucceed(() => undefined),
  );

  const complete = Effect.fnUntraced(
    function* (writer: RunWriter, original: CanonicalRecordEnvelope, history: PromptHistory) {
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
      const tail = yield* writer.tail;

      const delta = yield* readSuffix(
        store,
        writer,
        CanonicalSequence.make(original.sequence - 1),
        tail.sequence,
        input.runId,
        MAX_BYTES - bytes,
      );

      if (
        delta === undefined ||
        delta.runs !== 1 ||
        history.prompt.content.length + delta.prompt.content.length > MAX_ENTRIES
      )
        return;
      const head = yield* store.inspectTail(ThreadTailRequest.make({ threadId: writer.threadId }));

      if (
        closed ||
        head.producerEpoch !== writer.producerEpoch ||
        head.tailSequence !== tail.sequence ||
        head.tailDigest !== tail.digest
      )
        return;
      const contextRecordId = RecordId.make(JSON.stringify(["run-context@1", input.runId]));

      const refresh =
        history.refreshedAt === undefined ||
        writer.producerEpoch - history.refreshedAt >= MAX_PROMPT_CHECKPOINT_RUNS;

      if (refresh && store.promptCheckpoints !== undefined) {
        yield* store.promptCheckpoints
          .save({
            version: 1,
            recordFormat: CURRENT_RECORD_FORMAT,
            threadId: writer.threadId,
            producerEpoch: writer.producerEpoch,
            throughSequence: tail.sequence,
            tailDigest: tail.digest,
            contextRecordId,
            promptJson: encoded,
          })
          .pipe(Effect.catch(() => Effect.void));
      }
      current = {
        threadId: writer.threadId,
        through: tail.sequence,
        tailDigest: tail.digest,
        producerEpoch: writer.producerEpoch,
        contextRecordId,
        historyDigest: digest,
        prompt: Prompt.concat(history.prompt, delta.prompt),
        encoded: appendJson({ prompt: history.prompt, encoded }, delta),
        bytes: bytes + delta.bytes,
        refreshedAt: refresh ? writer.producerEpoch : history.refreshedAt,
      };
    },
    Effect.catch(() => Effect.void),
  );

  return { take, complete };
});
