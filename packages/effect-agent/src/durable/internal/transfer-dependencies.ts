import type { RecordEnvelope, RecordId } from "../Records.ts";

/** Exact references outside one complete canonical batch. */
export const transferRecordDependencies = (
  records: ReadonlyArray<RecordEnvelope>,
): ReadonlyArray<RecordId> => {
  const local = new Set(records.map((record) => record.recordId));
  const dependencies = new Set<RecordId>();

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
        if (ref !== undefined && !local.has(ref.recordId)) dependencies.add(ref.recordId);
  }

  return [...dependencies].sort();
};
