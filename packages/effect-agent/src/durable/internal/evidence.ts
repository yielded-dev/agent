import { Effect, Option, Stream } from "effect";

import type { ThreadId } from "../../core/Identifiers.ts";
import { digestCanonicalJson } from "../Digest.ts";
import {
  type ContinuationReference,
  CanonicalSequence,
  EvidenceReference,
  type RecordEnvelope,
} from "../Records.ts";
import { getRecord, ThreadRead, ThreadReader, ThreadStoreError } from "../ThreadStore.ts";
import { recordEncoding, type RecordEncoding } from "./record-encoding.ts";

const failure = (message: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation: "RunContinuation",
    message,
    ...(cause === undefined ? {} : { cause }),
  });

const evidenceDigests = new WeakMap<RecordEncoding, EvidenceReference>();

export const reference = Effect.fnUntraced(function* (record: RecordEnvelope) {
  const wire = yield* Effect.try({
    try: () => recordEncoding(record),
    catch: (cause) => failure("Cannot encode canonical evidence", cause),
  });

  const retained = evidenceDigests.get(wire);

  if (retained !== undefined) return retained;
  const recordId = wire.canonical.recordId;

  const digest = yield* digestCanonicalJson(wire.json).pipe(
    Effect.mapError((cause) => failure("Cannot fingerprint canonical evidence", cause)),
  );

  const result = EvidenceReference.make({ recordId, digest });

  evidenceDigests.set(wire, result);

  return result;
});

/** Resolve exact immutable content. A failed reference leaves the original work owed. */
export const resolveEvidence = Effect.fnUntraced(function* (
  threadId: ThreadId,
  ref: EvidenceReference,
) {
  const found = yield* getRecord({ threadId, recordId: ref.recordId });

  if (Option.isNone(found)) return yield* failure("Required canonical evidence is unavailable");
  if ((yield* reference(found.value.record)).digest !== ref.digest)
    return yield* failure("Required canonical evidence has invalid integrity");

  return found.value;
});

/** Read exactly one canonical position; a missing position must never select its successor. */
export const resolveContinuationEvidence = Effect.fnUntraced(function* (
  threadId: ThreadId,
  ref: ContinuationReference,
) {
  const reader = yield* ThreadReader;

  const records = yield* Stream.runCollect(
    reader.read(
      ThreadRead.make({
        threadId,
        afterSequence: CanonicalSequence.make(ref.sequence - 1),
        limit: 1,
      }),
    ),
  );

  const found = records[0];

  if (
    records.length !== 1 ||
    found === undefined ||
    found.threadId !== threadId ||
    found.sequence !== ref.sequence
  )
    return yield* failure("Required canonical position is unavailable");
  if ((yield* reference(found.record)).digest !== ref.digest)
    return yield* failure("Required canonical evidence has invalid integrity");

  return found;
});
