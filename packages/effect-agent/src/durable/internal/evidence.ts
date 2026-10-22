import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import type { ThreadId } from "../../core/Identifiers.ts";
import { EvidenceReference, type RecordEnvelope } from "../Records.ts";
import { getRecord, ThreadStoreError } from "../ThreadStore.ts";

const failure = (message: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation: "RunContinuation",
    message,
    ...(cause === undefined ? {} : { cause }),
  });

/**
 * References identify immutable facts; schema and owning readers validate their meaning.
 *
 * @category operations
 * @since 0.1.0
 */
export const reference = (record: RecordEnvelope) =>
  Effect.succeed(EvidenceReference.make({ recordId: record.recordId }));

/**
 * Resolve an immutable fact by Thread and record identity. A failed reference leaves the original work owed.
 *
 * @category operations
 * @since 0.1.0
 */
export const resolveEvidence = Effect.fnUntraced(function* (
  threadId: ThreadId,
  ref: EvidenceReference,
) {
  const found = yield* getRecord({ threadId, recordId: ref.recordId });

  if (Option.isNone(found)) return yield* failure("Required canonical evidence is unavailable");

  return found.value;
});
