import type { ThreadObjectRpc } from "../CloudflareBindings.ts";

/** Shared wire names for native and Object-local transports. */
export const hostRpcMethods = {
  submit: "submitEncoded",
  awaitSettlement: "awaitSettlementEncoded",
  awaitSettlementRecord: "awaitSettlementRecordEncoded",
  submissionStatus: "submissionStatusEncoded",
  awaitProgress: "awaitProgressEncoded",
  cancelProgress: "cancelProgressEncoded",
  observePage: "observePage",
  abort: "abortEncoded",
  resolveApproval: "resolveApprovalEncoded",
  resolveUnknown: "resolveUnknownEncoded",
} as const satisfies Record<string, keyof ThreadObjectRpc>;
