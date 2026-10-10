import { Context, Effect, Schema } from "effect";

import { SubmissionId, type ReceiptId, type RunId } from "../core/Identifiers.ts";
import {
  CanonicalSequence,
  Digest,
  RecordEnvelope,
  SubmissionSettled,
  type SubmissionSettledRecord,
} from "./Records.ts";
import { runIdForSubmission } from "./RunJournal.ts";
import {
  LedgerError,
  type OwnershipLost,
  OwnershipToken,
  type SettlementConflict,
  submissionSettlementBatchId,
  submissionSettlementId,
  submissionSettlementRecordId,
} from "./SubmissionLedger.ts";
import { FencedAppendRequest, PreparedAppend, type ThreadStoreFailure } from "./ThreadStore.ts";

/** Authority rechecked atomically on every publication attempt, including canonical replay. */
export const SettlementPublicationAuthority = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Owned"), ownershipToken: OwnershipToken }),
  Schema.Struct({ _tag: Schema.Literal("Joined"), hostSubmissionId: SubmissionId }),
  Schema.Struct({ _tag: Schema.Literal("QueuedAbort") }),
]);

export type SettlementPublicationAuthority = typeof SettlementPublicationAuthority.Type;

/** The deterministic settlement fact followed by its atomically published Run progress. */
export class SettlementPublication extends Schema.Class<SettlementPublication>(
  "@effect-agent/thread/SettlementPublication",
)({
  submissionId: SubmissionId,
  authority: SettlementPublicationAuthority,
  append: FencedAppendRequest,
}) {}

/** The actual immutable winner and CURRENT tail, including a replay of an earlier publication. */
export class SettlementPublicationResult extends Schema.Class<SettlementPublicationResult>(
  "@effect-agent/thread/SettlementPublicationResult",
)({
  record: RecordEnvelope,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  replayed: Schema.Boolean,
}) {}

export type SettlementPublicationFailure =
  | LedgerError
  | OwnershipLost
  | SettlementConflict
  | ThreadStoreFailure;

/**
 * One storage owner validates publication authority and appends the terminal fact atomically.
 * Wire preparation precedes the writer; no application callback runs inside its transaction.
 * Existing canonical intent wins an authorized replay. Owned publication always requires a
 * live token, including after a lost acknowledgement; finalization recovers an already settled
 * publication without that token. Joined and queued-abort replay retain their recorded authority.
 * Adapters may finalize in this transaction when no recoverable delivery remains; external
 * notification runs outside the transaction.
 */
export class SettlementPublisher extends Context.Service<
  SettlementPublisher,
  {
    readonly publish: (
      request: SettlementPublication,
    ) => Effect.Effect<SettlementPublicationResult, SettlementPublicationFailure>;
  }
>()("@effect-agent/thread/SettlementPublisher") {}

const PublicationHeader = SettlementPublication.mapFields((fields) => ({
  submissionId: fields.submissionId,
  authority: fields.authority,
}));

const decodePublicationHeader = Schema.decodeEffect(PublicationHeader);

/** Own the bounded request before Crypto, callbacks, or mutation authority can suspend it. */
export const validatePublication = Effect.fnUntraced(function* (input: SettlementPublication) {
  const invalid = (cause: { readonly message: string }) =>
    LedgerError.make({ operation: "publish settlement", message: cause.message, cause });

  const header = yield* decodePublicationHeader(input).pipe(Effect.mapError(invalid));
  const append = yield* PreparedAppend.capture(input.append).pipe(Effect.mapError(invalid));

  const request = Object.freeze(
    SettlementPublication.make({ ...header, authority: Object.freeze(header.authority), append }),
  );

  const record = request.append.batch.records[0];

  // Positions are checked inside append mutation after exact replay detection.
  // An authorized replay retains its original positions even when the tail has advanced.

  if (
    request.append.batch.records
      .slice(1)
      .some(
        (progress) =>
          progress.payload._tag !== "RunContinuation" ||
          (progress.payload.runId !== runIdForSubmission(request.submissionId) &&
            (record.payload._tag !== "SubmissionSettled" ||
              progress.payload.runId !== record.payload.runId)),
      ) ||
    record === undefined ||
    record.payload._tag !== "SubmissionSettled" ||
    record.payload.submissionId !== request.submissionId ||
    record.payload.settlementId !== submissionSettlementId(request.submissionId) ||
    record.recordId !== submissionSettlementRecordId(request.submissionId) ||
    request.append.batch.batchId !== submissionSettlementBatchId(request.submissionId)
  )
    return yield* LedgerError.make({
      operation: "publish settlement",
      message:
        "Publication must contain the deterministic settlement and only its owning Run progress",
    });

  return { request, record, settlement: record.payload };
});

/** The caller selects the canonical row from the expected Thread before invoking this check. */
export const validateCanonicalSettlement = Effect.fnUntraced(function* (
  input: RecordEnvelope,
  expected: {
    readonly submissionId: SubmissionId;
    readonly receiptId: ReceiptId;
    readonly runId?: RunId | undefined;
  },
): Effect.fn.Return<SubmissionSettledRecord, LedgerError> {
  const record = yield* Schema.decodeEffect(Schema.toType(RecordEnvelope))(input).pipe(
    Effect.mapError((cause) =>
      LedgerError.make({ operation: "read canonical settlement", message: cause.message, cause }),
    ),
  );

  const settlement = record.payload;

  if (
    settlement._tag !== "SubmissionSettled" ||
    settlement.submissionId !== expected.submissionId ||
    settlement.receiptId !== expected.receiptId ||
    settlement.settlementId !== submissionSettlementId(expected.submissionId) ||
    record.recordId !== submissionSettlementRecordId(expected.submissionId) ||
    ("runId" in expected && settlement.runId !== expected.runId)
  )
    return yield* LedgerError.make({
      operation: "read canonical settlement",
      message: "Canonical settlement disagrees with the admitted Submission identity",
    });

  return settlement;
});

/** Joined receipts inherit the host outcome and failed diagnostic, with the host Run identity. */
export const validateJoinedSettlement = Effect.fnUntraced(function* (
  settlement: SubmissionSettledRecord,
  host: SubmissionSettledRecord,
): Effect.fn.Return<void, LedgerError> {
  const expected = SubmissionSettled.make({
    submissionId: settlement.submissionId,
    settlementId: settlement.settlementId,
    receiptId: settlement.receiptId,
    outcome: host.outcome,
    runId: runIdForSubmission(host.submissionId),
    ...(host.outcome === "failed" ? { result: host.result } : {}),
  });

  if (!Schema.toEquivalence(SubmissionSettled)(settlement, expected))
    return yield* LedgerError.make({
      operation: "publish joined settlement",
      message: "Joined settlement disagrees with its canonical host outcome",
    });
});
