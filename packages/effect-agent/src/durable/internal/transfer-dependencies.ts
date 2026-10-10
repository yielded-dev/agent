import type { CanonicalSequence, RecordEnvelope, RecordId } from "../Records.ts";

/** Exact references outside one complete batch. Unknown positions conservatively charge all references. */
export const transferRecordDependencies = (
  records: ReadonlyArray<RecordEnvelope>,
  fromSequence?: CanonicalSequence,
): ReadonlyArray<RecordId | CanonicalSequence> => {
  const local = new Set(records.map((record) => record.recordId));
  const dependencies = new Set<RecordId | CanonicalSequence>();

  for (const { payload } of records) {
    if (payload._tag === "WorkerInputRefused")
      for (const ref of [payload.reservation, payload.stop])
        if (!local.has(ref.recordId)) dependencies.add(ref.recordId);
    if (payload._tag === "WorkHandoffCompleted" && !local.has(payload.preparationId))
      dependencies.add(payload.preparationId);
    if (payload._tag === "RunContextRecorded")
      for (const ref of payload.retained)
        if (!local.has(ref.recordId)) dependencies.add(ref.recordId);
    if (payload._tag === "RunContinuation")
      for (const ref of [
        payload.originalInput,
        payload.savedContext,
        payload.latestResponse,
        payload.terminal,
        payload.lastFact,
      ])
        if (
          ref !== undefined &&
          (fromSequence === undefined ||
            ref.sequence < fromSequence ||
            ref.sequence >= fromSequence + records.length)
        )
          dependencies.add(ref.sequence);
  }

  return [...dependencies].sort();
};
