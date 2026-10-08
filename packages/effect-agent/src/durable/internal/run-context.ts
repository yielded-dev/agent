import { type Crypto, Effect, Schema, Stream } from "effect";
import { Prompt } from "effect/ai";

import { NativeCompaction } from "../../engine/ContextCompactor.ts";
import { digestJson } from "../Digest.ts";
import {
  CanonicalSequence,
  type CanonicalRecordEnvelope,
  type RunContextRecorded,
} from "../Records.ts";
import {
  projectRunJournalStream,
  RunJournalError,
  type RunJournalContext,
  type JournalBoundary,
} from "../RunJournal.ts";
import { reference } from "./evidence.ts";
import { RunContextReader } from "./run-context-reader.ts";

const invalid = (message: string) => RunJournalError.make({ message });
const encodePrompt = Schema.encodeEffect(Schema.toCodecJson(Prompt.Prompt));

const encodeNativeHistory = Schema.encodeEffect(
  Schema.toCodecJson(
    Schema.Struct({ prompt: Prompt.Prompt, nativeCompactions: Schema.Array(NativeCompaction) }),
  ),
);

export const digestRunHistory = (
  prompt: Prompt.Prompt,
  nativeCompactions: ReadonlyArray<NativeCompaction> = [],
) =>
  (nativeCompactions.length === 0
    ? encodePrompt(prompt)
    : encodeNativeHistory({ prompt, nativeCompactions })
  ).pipe(
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

/** Rebuild the prior Prompt and its disposable boundary mapping from full canonical facts. */
export const projectRunContext = Effect.fnUntraced(function* (
  context: RunContextRecorded,
  original: CanonicalRecordEnvelope,
  digest: string,
  records: ReadonlyArray<CanonicalRecordEnvelope>,
): Effect.fn.Return<RunJournalContext, RunJournalError, Crypto.Crypto> {
  yield* validateContextBoundary(context, original);
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

  const history = yield* projectRunJournalStream(
    Stream.fromIterable([...records, original]),
    context.runId,
    (boundary) => boundaries.push(boundary),
  );

  const historyDigest = yield* digestRunHistory(history.prompt, history.nativeCompactions);

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
    nativeCompactions: history.nativeCompactions,
    historyFrom: context.historyFrom,
    boundaries: boundaries.filter(
      (boundary) => boundary.promptLength <= context.priorHistoryLength,
    ),
    digest,
    ...(context.contextWindowId === undefined ? {} : { contextWindowId: context.contextWindowId }),
  };
});

/** Cold recovery and explicit verify use full reads; fresh admission uses the narrow history port. */
export const readRunContext = Effect.fnUntraced(function* (
  context: RunContextRecorded,
  original: CanonicalRecordEnvelope,
  digest: string,
) {
  yield* validateContextBoundary(context, original);
  const reader = yield* RunContextReader;
  const records: Array<CanonicalRecordEnvelope> = [];

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
    records.push(entry);
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
      records.push(entry);
    }
    if (page.length < 8) break;
  }

  return yield* projectRunContext(context, original, digest, records);
});
