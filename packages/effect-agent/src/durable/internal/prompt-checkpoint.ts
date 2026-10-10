import { Effect, Option, Schema, Stream } from "effect";
import { Prompt } from "effect/ai";

import { digestCanonicalJson, utf8ByteLength } from "../Digest.ts";
import {
  CURRENT_RECORD_FORMAT,
  RecordId,
  type CanonicalRecordEnvelope,
  type CanonicalSequence,
} from "../Records.ts";
import { projectRunJournalStream } from "../RunJournal.ts";
import type { RunWriter } from "../RunStorage.ts";
import { getRecord, MAX_PROMPT_CHECKPOINT_BYTES, ThreadStore } from "../ThreadStore.ts";
import type { JournalRecordEnvelope } from "./journal-metadata.ts";
import { encodeRunHistory } from "./run-context.ts";

// Decode native messages once, rather than Prompt's encoded-array check followed by its
// native-array transformation. The canonical context digest authenticates the exact JSON.
const decodePrompt = Schema.decodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      content: Schema.Array(Schema.toCodecJson(Prompt.Message)),
    }),
  ),
);

const JSON_PREFIX = '{"content":[';
const MAX_SUFFIX_RECORDS = 1_024;

export interface CheckpointPrompt {
  readonly prompt: Prompt.Prompt;
  readonly json: string;
  readonly through: CanonicalSequence;
}

/** No authority or execution state is cached. Unfinished Runs and explicit verify still rebuild. */
export const loadPromptCheckpoint = Effect.fnUntraced(
  function* (writer: RunWriter) {
    const store = yield* ThreadStore;

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
      (yield* digestCanonicalJson(checkpoint.promptJson)) !== context.historyDigest
    )
      return undefined;

    const decoded = yield* decodePrompt(checkpoint.promptJson);

    if (decoded.content.length !== context.priorHistoryLength) return undefined;
    const records: Array<JournalRecordEnvelope> = [];
    let after = context.historyThrough;
    let original = false;
    let settled = false;

    while (after < checkpoint.throughSequence) {
      const page = yield* store
        .readPrompt({
          threadId: writer.threadId,
          afterSequence: after,
          throughSequence: checkpoint.throughSequence,
          limit: 256,
        })
        .pipe(Stream.take(257), Stream.runCollect);

      if (page.length > 256 || records.length + page.length > MAX_SUFFIX_RECORDS) return undefined;
      for (const entry of page) {
        const payload = entry.record.payload;

        if (
          entry.threadId !== writer.threadId ||
          entry.sequence <= after ||
          entry.sequence > checkpoint.throughSequence ||
          payload._tag === "CompactionCreated" ||
          payload._tag === "ModelCompleted" ||
          payload.runId !== context.runId
        )
          return undefined;
        if (payload._tag === "UserInputRecorded") {
          if (original || payload.kind !== "user" || entry.sequence !== context.historyThrough + 1)
            return undefined;
          original = true;
        }
        if (payload._tag === "SubmissionSettled") {
          if (settled) return undefined;
          settled = true;
        }
        after = entry.sequence;
        records.push(entry);
      }
      if (page.length < 256) break;
    }
    if (!original || !settled) return undefined;
    let incomplete = false;

    const delta = yield* projectRunJournalStream(
      Stream.fromIterable(records),
      undefined,
      (boundary) => {
        if (boundary.incomplete) incomplete = true;
      },
    );

    if (incomplete) return undefined;
    const encodedDelta = yield* encodeRunHistory(delta.prompt);

    if (!checkpoint.promptJson.startsWith(JSON_PREFIX) || !encodedDelta.startsWith(JSON_PREFIX))
      return undefined;

    const json =
      checkpoint.promptJson.slice(0, -2) +
      (decoded.content.length > 0 && delta.prompt.content.length > 0 ? "," : "") +
      encodedDelta.slice(JSON_PREFIX.length);

    if (utf8ByteLength(json) > MAX_PROMPT_CHECKPOINT_BYTES) return undefined;

    return {
      prompt: Prompt.fromMessages([...decoded.content, ...delta.prompt.content]),
      json,
      through: checkpoint.throughSequence,
    } satisfies CheckpointPrompt;
  },
  Effect.orElseSucceed(() => undefined),
);

export const savePromptCheckpoint = Effect.fnUntraced(
  function* (writer: RunWriter, original: CanonicalRecordEnvelope, promptJson: string) {
    const store = yield* ThreadStore;
    const input = original.record.payload;

    if (
      store.promptCheckpoints === undefined ||
      input._tag !== "UserInputRecorded" ||
      input.runId === undefined ||
      utf8ByteLength(promptJson) > MAX_PROMPT_CHECKPOINT_BYTES
    )
      return;
    const tail = yield* writer.tail;

    yield* store.promptCheckpoints.save({
      version: 1,
      recordFormat: CURRENT_RECORD_FORMAT,
      threadId: writer.threadId,
      producerEpoch: writer.producerEpoch,
      throughSequence: tail.sequence,
      tailDigest: tail.digest,
      contextRecordId: RecordId.make(JSON.stringify(["run-context@1", input.runId])),
      promptJson,
    });
  },
  // A disposable write cannot change an already canonical settlement.
  Effect.catch(() => Effect.void),
);
