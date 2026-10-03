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
import { FencedAppendRequest, type ThreadStoreFailure } from "./ThreadStore.ts";

/** Authority rechecked atomically with the first canonical settlement publication. */
export const SettlementPublicationAuthority = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Owned"), ownershipToken: OwnershipToken }),
  Schema.Struct({ _tag: Schema.Literal("Joined"), hostSubmissionId: SubmissionId }),
  Schema.Struct({ _tag: Schema.Literal("QueuedAbort") }),
]);

export type SettlementPublicationAuthority = typeof SettlementPublicationAuthority.Type;

/** The batch must contain only this Submission's deterministic SubmissionSettled record. */
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
 * Existing canonical intent wins replay. Finalization and external notification remain separate.
 */
export class SettlementPublisher extends Context.Service<
  SettlementPublisher,
  {
    readonly publish: (
      request: SettlementPublication,
    ) => Effect.Effect<SettlementPublicationResult, SettlementPublicationFailure>;
  }
>()("@effect-agent/thread/SettlementPublisher") {}

const publicationJson = Schema.fromJsonString(SettlementPublication);

/** Own the bounded request before Crypto, callbacks, or mutation authority can suspend it. */
export const validatePublication = Effect.fnUntraced(function* (input: SettlementPublication) {
  const request = yield* Schema.encodeEffect(publicationJson)(input).pipe(
    Effect.flatMap(Schema.decodeEffect(publicationJson)),
    Effect.mapError((cause) =>
      LedgerError.make({ operation: "publish settlement", message: cause.message, cause }),
    ),
  );

  const record = request.append.batch.records[0];

  if (
    request.append.batch.records.length !== 1 ||
    record === undefined ||
    record.payload._tag !== "SubmissionSettled" ||
    record.payload.submissionId !== request.submissionId ||
    record.payload.settlementId !== submissionSettlementId(request.submissionId) ||
    record.recordId !== submissionSettlementRecordId(request.submissionId) ||
    request.append.batch.batchId !== submissionSettlementBatchId(request.submissionId)
  )
    return yield* LedgerError.make({
      operation: "publish settlement",
      message: "Publication must contain only the Submission's deterministic settlement record",
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
