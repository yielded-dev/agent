import { createHash } from "node:crypto";

// Public evidence contains deterministic hashes, never opaque platform identities.
export const opaqueId = (value: string): string => createHash("sha256").update(value).digest("hex");
export const opaqueDetails = (value: Record<string, unknown>): Record<string, unknown> =>
  Object.fromEntries(Object.entries(value).map(([key, entry]) => [
    key, /^(?:threadId|submissionId|attemptId|producerId|ownershipToken)$/.test(key) && typeof entry === "string" ? opaqueId(entry) : entry,
  ]));
