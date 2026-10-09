import { type Crypto, Effect, Schema, Stream } from "effect";
import { Prompt } from "effect/ai";

import { digestJson } from "../Digest.ts";
import {
  CanonicalSequence,
  PromptRecord,
  PROMPT_EVIDENCE_TAGS,
  type CanonicalRecordEnvelope,
  type RunContextRecorded,
} from "../Records.ts";
import {
  projectRunJournalStream,
  RunJournalError,
  type RunJournalContext,
  type JournalBoundary,
} from "../RunJournal.ts";
import type { ThreadNotMaterialized, ThreadStoreError } from "../ThreadStore.ts";
import { reference } from "./evidence.ts";
import type { JournalRecordEnvelope } from "./journal-metadata.ts";
import { RunContextReader } from "./run-context-reader.ts";

const invalid = (message: string) => RunJournalError.make({ message });
const encodePrompt = Schema.encodeEffect(Schema.toCodecJson(Prompt.Prompt));

const decodePromptRecord = Schema.decodeUnknownEffect(PromptRecord);
const promptTags = new Set<string>(PROMPT_EVIDENCE_TAGS);

const projectHistoryRecord = (
  entry: CanonicalRecordEnvelope,
  ownerRunId: RunContextRecorded["runId"],
): Effect.Effect<JournalRecordEnvelope> => {
  const payload = entry.record.payload;

  if (("runId" in payload && payload.runId === ownerRunId) || !promptTags.has(payload._tag))
    return Effect.succeed(entry);

  return decodePromptRecord({ recordId: entry.record.recordId, payload }).pipe(
    Effect.map((record) => ({ threadId: entry.threadId, sequence: entry.sequence, record })),
    // Coverage and later canonical validation must retain their original error precedence.
    Effect.catchTag("SchemaError", () => Effect.succeed(entry)),
  );
};

export const digestRunHistory = (prompt: Prompt.Prompt) =>
  encodePrompt(prompt).pipe(
    Effect.flatMap(digestJson),
    Effect.mapError((cause) =>
      RunJournalError.make({ message: "Original model history integrity is unavailable", cause }),
    ),
  );

export const validateContextBoundary = (
  context: RunContextRecorded,
  original: CanonicalRecordEnvelope,
): Effect.Effect<void, RunJournalError> =>
  original.record.payload._tag !== "UserInputRecorded" ||
  original.record.payload.kind !== "user" ||
  original.record.payload.runId !== context.runId ||
  context.historyThrough + 1 !== original.sequence ||
  context.historyFrom < 1 ||
  context.historyFrom > original.sequence ||
  context.retained.some(
    (ref, index) =>
      ref.sequence <= (context.retained[index - 1]?.sequence ?? 0) ||
      ref.sequence >= context.historyFrom,
  )
    ? Effect.fail(invalid("Saved context has an invalid original admission boundary"))
    : Effect.void;

type ProjectedRunHistory = Pick<
  RunJournalContext,
  "prompt" | "historyFrom" | "contextWindowId" | "boundaries"
>;

/** Keep full canonical facts within projection so history hashing retains only its result. */
const readProjectedHistory = Effect.fnUntraced(function* (
  context: RunContextRecorded,
  original: CanonicalRecordEnvelope,
): Effect.fn.Return<
  ProjectedRunHistory,
  RunJournalError | ThreadStoreError | ThreadNotMaterialized,
  RunContextReader | Crypto.Crypto
> {
  yield* validateContextBoundary(context, original);
  const reader = yield* RunContextReader;
  const records: Array<JournalRecordEnvelope> = [];

  for (const ref of context.retained) {
    const entry = yield* reader.record(ref.recordId);

    if (
      entry === undefined ||
      entry.threadId !== original.threadId ||
      entry.record.recordId !== ref.recordId ||
      entry.sequence !== ref.sequence ||
      (yield* reference(entry.record)).digest !== ref.digest
    )
      return yield* invalid("Saved context has missing or corrupt retained evidence");
    records.push(yield* projectHistoryRecord(entry, context.runId));
  }

  let after = CanonicalSequence.make(context.historyFrom - 1);

  while (after < context.historyThrough) {
    const page = yield* reader
      .read({
        threadId: original.threadId,
        selection: { _tag: "PromptEvidence", throughSequence: context.historyThrough },
        page: { afterSequence: after, limit: 8 },
      })
      .pipe(Stream.take(9), Stream.runCollect);

    if (page.length > 8) return yield* invalid("Saved context page exceeds its record bound");
    for (const entry of page) {
      if (
        entry.threadId !== original.threadId ||
        entry.sequence <= after ||
        entry.sequence > context.historyThrough
      )
        return yield* invalid("Saved context range has invalid canonical ordering");
      after = entry.sequence;
      records.push(yield* projectHistoryRecord(entry, context.runId));
    }
    if (page.length < 8) break;
  }

  let through = 0;
  const retained = new Map(context.retained.map((ref) => [ref.sequence, ref.recordId]));

  for (const entry of records) {
    if (
      entry.threadId !== original.threadId ||
      entry.sequence <= through ||
      entry.sequence > context.historyThrough ||
      (entry.sequence < context.historyFrom &&
        retained.get(entry.sequence) !== entry.record.recordId)
    )
      return yield* invalid("Saved context has invalid canonical range evidence");
    through = entry.sequence;
    retained.delete(entry.sequence);
  }
  if (retained.size !== 0) return yield* invalid("Saved context is missing retained evidence");

  const boundaries: Array<JournalBoundary> = [];

  records.push(original);

  const history = yield* projectRunJournalStream(
    Stream.fromIterable(records),
    context.runId,
    (boundary) => boundaries.push(boundary),
  );

  return {
    prompt: history.prompt,
    historyFrom: history.historyFrom,
    boundaries,
    ...(history.contextWindowId === undefined ? {} : { contextWindowId: history.contextWindowId }),
  };
});

/** Cold recovery and explicit verify use full reads; fresh admission uses the narrow history port. */
export const readRunContext = Effect.fnUntraced(function* (
  context: RunContextRecorded,
  original: CanonicalRecordEnvelope,
  digest: string,
): Effect.fn.Return<
  RunJournalContext,
  RunJournalError | ThreadStoreError | ThreadNotMaterialized,
  RunContextReader | Crypto.Crypto
> {
  const history = yield* readProjectedHistory(context, original);
  const historyDigest = yield* digestRunHistory(history.prompt);

  if (
    historyDigest !== context.historyDigest ||
    history.historyFrom !== context.historyFrom ||
    history.prompt.content.length !== context.priorHistoryLength ||
    history.contextWindowId !== context.contextWindowId
  )
    return yield* invalid("Saved context differs from its original model history");

  const prefix = yield* Schema.decodeUnknownEffect(Prompt.Prompt)(context.runScopedInput).pipe(
    Effect.mapError(() => invalid("Saved Run instructions and input are malformed")),
  );

  return {
    runId: context.runId,
    prompt: Prompt.fromMessages([...history.prompt.content, ...prefix.content]),
    priorHistoryLength: context.priorHistoryLength,
    historyFrom: context.historyFrom,
    boundaries: history.boundaries.filter(
      (boundary) => boundary.promptLength <= context.priorHistoryLength,
    ),
    digest,
    ...(context.contextWindowId === undefined ? {} : { contextWindowId: context.contextWindowId }),
  };
});
