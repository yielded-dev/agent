import { Effect, Option, Result } from "effect";

import type { ThreadId } from "../../core/Identifiers.ts";
import { digestCanonicalJson } from "../Digest.ts";
import { EvidenceReference, type RecordEnvelope } from "../Records.ts";
import { getRecord, ThreadStoreError } from "../ThreadStore.ts";
import { recordEncodingResult, type RecordEncoding } from "./record-encoding.ts";

const failure = (message: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation: "RunContinuation",
    message,
    ...(cause === undefined ? {} : { cause }),
  });

const evidenceDigests = new WeakMap<RecordEncoding, EvidenceReference>();

export const reference = Effect.fnUntraced(function* (record: RecordEnvelope) {
  const captured = recordEncodingResult(record);

  if (Result.isFailure(captured))
    return yield* failure("Cannot encode canonical evidence", captured.failure);
  const wire = captured.success;

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
