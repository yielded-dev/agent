import { Effect, Schema, Stream } from "effect";
import { Prompt } from "effect/ai";

import type { ThreadId } from "../../core/Identifiers.ts";
import { boundedValueFootprint } from "../../engine/internal/bounded-value.ts";
import { canonicalJson, digestCanonicalJson } from "../Digest.ts";
import {
  type CanonicalSequence,
  type CanonicalRecordEnvelope,
  type Digest,
  type ProducerEpoch,
} from "../Records.ts";
import { projectRunJournalStream, RunJournalError, type JournalBoundary } from "../RunJournal.ts";
import type { RunWriter } from "../RunStorage.ts";
import { ThreadPromptRead, ThreadTailRequest, type ThreadStore } from "../ThreadStore.ts";
import type { JournalRecordEnvelope } from "./journal-metadata.ts";
import { digestRunHistory } from "./run-context.ts";

// One retained Thread, including decoded values, encoded text and boundary metadata.
// Large histories take the original path without first materializing another encoded copy.
const MAX_BYTES = 16 * 1024 * 1024;
const MAX_ENTRIES = 16_384;
const JSON_PREFIX = '{"content":[';
const encodePrompt = Schema.encodeEffect(Schema.toCodecJson(Prompt.Prompt));

export interface PromptHistory {
  readonly prompt: Prompt.Prompt;
  readonly boundaries: ReadonlyArray<JournalBoundary>;
  encoded: string | undefined;
  bytes: number | undefined;
}

export interface CachedPromptHistory extends PromptHistory {
  readonly threadId: ThreadId;
  readonly through: CanonicalSequence;
  readonly tailDigest: Digest;
  readonly producerEpoch: ProducerEpoch;
}

export const preparePromptHistory = (
  prompt: Prompt.Prompt,
  boundaries: ReadonlyArray<JournalBoundary>,
): PromptHistory | undefined =>
  prompt.content.length > MAX_ENTRIES ||
  boundaries.length > MAX_ENTRIES ||
  boundaries.some((boundary) => boundary.incomplete)
    ? undefined
    : { prompt, boundaries: [...boundaries], encoded: undefined, bytes: undefined };

const encodeHistory = Effect.fnUntraced(function* (
  prompt: Prompt.Prompt,
  boundaries: number,
  allowance: number,
) {
  const value = yield* encodePrompt(prompt);
  const encoded = canonicalJson(value);
  const overhead = 2 * encoded.length + 64 * boundaries;

  // Only concatenate the canonical array produced by the upstream Prompt codec.
  const measured =
    overhead <= allowance &&
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    Object.keys(value).length === 1 &&
    "content" in value &&
    Array.isArray(value.content) &&
    value.content.length === prompt.content.length
      ? boundedValueFootprint(value, Math.floor((allowance - overhead) / 2))
      : undefined;

  // Double the existing value estimate for decoded string/object representation as well.
  return { encoded, bytes: measured === undefined ? undefined : overhead + 2 * measured };
});

/** The ordinary digest codec remains the oracle, including on every cold recovery. */
export const digestPromptHistory = (prompt: Prompt.Prompt, history: PromptHistory | undefined) => {
  if (history === undefined) return digestRunHistory(prompt);
  if (
    prompt.content.length !== history.prompt.content.length ||
    prompt.content.some((message, index) => message !== history.prompt.content[index])
  ) {
    history.encoded = undefined;
    history.bytes = undefined;

    return digestRunHistory(prompt);
  }

  return Effect.gen(function* () {
    let encoded = history.encoded;

    if (encoded === undefined) {
      const captured = yield* encodeHistory(prompt, history.boundaries.length, MAX_BYTES);

      encoded = captured.encoded;
      if (captured.bytes !== undefined) {
        history.encoded = encoded;
        history.bytes = captured.bytes;
      }
    }

    return yield* digestCanonicalJson(encoded);
  }).pipe(
    Effect.mapError((cause) =>
      RunJournalError.make({ message: "Original model history integrity is unavailable", cause }),
    ),
  );
};

/** Disposable, Scope-owned acceleration. Neither a cached head nor a Prompt grants authority. */
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
      if (
        closed ||
        cached === undefined ||
        cached.threadId !== writer.threadId ||
        writer.producerEpoch !== cached.producerEpoch + 1
      )
        return undefined;
      const head = yield* store.inspectTail(ThreadTailRequest.make({ threadId: writer.threadId }));

      return head.producerEpoch === writer.producerEpoch &&
        head.tailSequence === cached.through &&
        head.tailDigest === cached.tailDigest
        ? cached
        : undefined;
    },
    Effect.catch(() => Effect.succeed(undefined)),
  );

  const complete = Effect.fnUntraced(
    function* (writer: RunWriter, original: CanonicalRecordEnvelope, history: PromptHistory) {
      const encoded = history.encoded;
      const bytes = history.bytes;
      const input = original.record.payload;

      if (
        closed ||
        encoded === undefined ||
        bytes === undefined ||
        input._tag !== "UserInputRecorded" ||
        input.runId === undefined
      )
        return;
      const tail = yield* writer.tail;
      let after = original.sequence;
      let remaining = MAX_BYTES - bytes;
      const records: Array<JournalRecordEnvelope> = [original];

      while (after < tail.sequence) {
        const page = yield* store
          .readPrompt(
            ThreadPromptRead.make({
              threadId: writer.threadId,
              afterSequence: after,
              throughSequence: tail.sequence,
              limit: 1,
            }),
          )
          .pipe(Stream.take(2), Stream.runCollect);

        if (page.length > 1 || records.length + page.length > MAX_ENTRIES) return;
        for (const entry of page) {
          const payload = entry.record.payload;

          if (
            entry.threadId !== writer.threadId ||
            entry.sequence <= after ||
            entry.sequence > tail.sequence ||
            payload._tag === "UserInputRecorded" ||
            payload._tag === "CompactionCreated" ||
            payload._tag === "ModelCompleted" ||
            payload.runId !== input.runId
          )
            return;

          // Reserve space for six-character JSON escapes, UTF-16 text and encoding copies.
          // Reading one fact at a time also avoids hydrating a page for an oversized Run.
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

          if (footprint === undefined) return;
          remaining -= 32 * footprint;
          after = entry.sequence;
          records.push(entry);
        }
        if (page.length === 0) break;
      }

      const boundaries: Array<JournalBoundary> = [];

      const delta = yield* projectRunJournalStream(
        Stream.fromIterable(records),
        undefined,
        (item) => boundaries.push(item),
      );

      if (
        history.prompt.content.length + delta.prompt.content.length > MAX_ENTRIES ||
        history.boundaries.length + boundaries.length > MAX_ENTRIES ||
        boundaries.some((boundary) => boundary.incomplete)
      )
        return;
      const appended = yield* encodeHistory(delta.prompt, boundaries.length, MAX_BYTES - bytes);

      if (appended.bytes === undefined) return;
      const head = yield* store.inspectTail(ThreadTailRequest.make({ threadId: writer.threadId }));

      if (
        closed ||
        head.producerEpoch !== writer.producerEpoch ||
        head.tailSequence !== tail.sequence ||
        head.tailDigest !== tail.digest
      )
        return;

      current = {
        threadId: writer.threadId,
        through: tail.sequence,
        tailDigest: tail.digest,
        producerEpoch: writer.producerEpoch,
        prompt: Prompt.concat(history.prompt, delta.prompt),
        boundaries: history.boundaries.concat(
          boundaries.map((boundary) => ({
            ...boundary,
            promptLength: history.prompt.content.length + boundary.promptLength,
          })),
        ),
        encoded:
          encoded.slice(0, -2) +
          (history.prompt.content.length > 0 && delta.prompt.content.length > 0 ? "," : "") +
          appended.encoded.slice(JSON_PREFIX.length),
        bytes: bytes + appended.bytes,
      };
    },
    Effect.catch(() => Effect.void),
  );

  return { take, complete };
});
