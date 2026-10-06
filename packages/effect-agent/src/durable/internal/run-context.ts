import { Effect, Schema, Stream } from "effect";
import { Prompt } from "effect/ai";

import {
  MAX_RUN_CONTEXT_BYTES,
  type CanonicalRecordEnvelope,
  type EvidenceReference,
  type RunContextRecorded,
} from "../Records.ts";
import { projectRunJournalStream, RunJournalError, type RunJournalContext } from "../RunJournal.ts";
import { recordEncoding } from "./record-encoding.ts";

const invalid = (message: string) => RunJournalError.make({ message });

/** Resolve the original model history, never later traffic or execution authority. */
export const rebuildRunContext = Effect.fnUntraced(function* <E, R>(
  context: RunContextRecorded,
  original: CanonicalRecordEnvelope,
  digest: string,
  resolve: (ref: EvidenceReference) => Effect.Effect<CanonicalRecordEnvelope, E, R>,
): Effect.fn.Return<RunJournalContext, E | RunJournalError, R> {
  if (
    original.record.payload._tag !== "UserInputRecorded" ||
    original.record.payload.runId !== context.runId ||
    context.historyThrough + 1 !== original.sequence ||
    context.boundaries.some(
      (boundary) =>
        boundary.sequence > context.historyThrough ||
        boundary.promptLength > context.priorHistoryLength,
    )
  )
    return yield* invalid("Saved context has an invalid original admission boundary");

  const records: Array<CanonicalRecordEnvelope> = [];
  let through = 0;
  let bytes = 0;

  for (const ref of context.history) {
    const entry = yield* resolve(ref);

    if (
      entry.sequence !== ref.sequence ||
      entry.sequence <= through ||
      entry.sequence > context.historyThrough
    )
      return yield* invalid("Saved context references an invalid canonical history prefix");
    through = entry.sequence;
    bytes += recordEncoding(entry.record).bytes;
    if (bytes > MAX_RUN_CONTEXT_BYTES)
      return yield* invalid("Saved context exceeds its referenced byte bound");
    records.push(entry);
  }
  if (bytes !== context.historyBytes)
    return yield* invalid("Saved context byte accounting differs from its facts");

  const history = yield* projectRunJournalStream(
    Stream.fromIterable([...records, original]),
    context.runId,
  );

  if (
    history.prompt.content.length !== context.priorHistoryLength ||
    history.contextWindowId !== context.contextWindowId
  )
    return yield* invalid("Saved context differs from its referenced model history");

  const prefix = yield* Schema.decodeUnknownEffect(Prompt.Prompt)(context.runScopedInput).pipe(
    Effect.mapError(() => invalid("Saved Run instructions and input are malformed")),
  );

  return {
    runId: context.runId,
    prompt: Prompt.fromMessages([...history.prompt.content, ...prefix.content]),
    priorHistoryLength: context.priorHistoryLength,
    boundaries: context.boundaries,
    digest,
    ...(context.contextWindowId === undefined ? {} : { contextWindowId: context.contextWindowId }),
  };
});
