import { digestJson } from "@yielded/agent/digest";
import type { SettlementId, SubmissionId } from "@yielded/agent/identifiers";
import { RecordEnvelope, type Digest, type SettlementOutcome } from "@yielded/agent/records";
import * as Ledger from "@yielded/agent/submission-ledger";
import { ThreadStore, type FencedAppendRequest } from "@yielded/agent/thread-store";
import { Effect, Predicate, Schema } from "effect";

import { BenchmarkError } from "./contracts.js";

type LegacyReservation = {
  readonly submissionId: SubmissionId;
  readonly ownershipToken: Ledger.OwnershipToken;
  readonly settlementId: SettlementId;
  readonly outcome: SettlementOutcome;
  readonly record: RecordEnvelope;
  readonly recordDigest: Digest;
};

/** The historical module owns construction and validation of its reservation value. */
const legacyModule: {
  readonly SubmissionLedger: typeof Ledger.SubmissionLedger;
  readonly SettlementReservation?: {
    readonly make: (fields: LegacyReservation) => LegacyReservation;
  };
} = Ledger;

// Resolution only detects the absent public export. An import failure in an existing
// publisher is a broken compared build, never permission to substitute older behavior.
const publisherModule = Effect.try({
  try: () => import.meta.resolve("@yielded/agent/settlement-publisher"),
  catch: (cause) => BenchmarkError.make({ message: "Cannot resolve settlement publisher", cause }),
}).pipe(
  Effect.catchIf(
    (error) =>
      Predicate.hasProperty(error.cause, "code") &&
      error.cause.code === "ERR_PACKAGE_PATH_NOT_EXPORTED",
    () => Effect.succeed(undefined),
  ),
  Effect.flatMap((resolved) =>
    resolved === undefined
      ? Effect.succeed(undefined)
      : Effect.tryPromise({
          try: () => import("@yielded/agent/settlement-publisher"),
          catch: (cause) =>
            BenchmarkError.make({ message: "Cannot import compared settlement publisher", cause }),
        }),
  ),
);

/** Seed through each revision's actual public settlement protocol, outside operation timing. */
export const publishSeedSettlement = Effect.fn("benchmark.publishSeedSettlement")(function* (
  submissionId: SubmissionId,
  ownershipToken: Ledger.OwnershipToken,
  append: FencedAppendRequest,
) {
  const publisher = yield* publisherModule;

  if (publisher !== undefined) {
    const service = yield* publisher.SettlementPublisher;

    yield* service.publish(
      publisher.SettlementPublication.make({
        submissionId,
        authority: { _tag: "Owned", ownershipToken },
        append,
      }),
    );

    return;
  }

  const ledger: {
    readonly finalizeSettlement: Ledger.SubmissionLedger["Service"]["finalizeSettlement"];
    readonly reserveSettlement?: (
      request: LegacyReservation,
    ) => Effect.Effect<
      unknown,
      Ledger.LedgerError | Ledger.OwnershipLost | Ledger.SettlementConflict
    >;
  } = yield* Ledger.SubmissionLedger;

  const record = append.batch.records[0];

  if (
    legacyModule.SettlementReservation === undefined ||
    ledger.reserveSettlement === undefined ||
    record?.payload._tag !== "SubmissionSettled"
  )
    return yield* BenchmarkError.make({
      message: "Compared revision has no supported settlement seed protocol",
    });

  yield* ledger.reserveSettlement(
    legacyModule.SettlementReservation.make({
      submissionId,
      ownershipToken,
      settlementId: record.payload.settlementId,
      outcome: record.payload.outcome,
      record,
      recordDigest: yield* digestJson(yield* Schema.encodeEffect(RecordEnvelope)(record)),
    }),
  );
  const store = yield* ThreadStore;

  yield* store.append(append);
});
