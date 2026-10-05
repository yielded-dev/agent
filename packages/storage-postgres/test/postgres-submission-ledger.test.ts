import { NodeCrypto } from "@effect/platform-node";
import { describe, it } from "@effect/vitest";
import { submissionLedgerConformanceCases } from "@yielded/agent/testing/submission-ledger-conformance";
import { Effect, Layer, String } from "effect";

import { storage as makeStorage, withTemporaryDatabase } from "./harness.ts";

describe("PostgresSubmissionLedger", () => {
  describe("shared SubmissionLedger conformance", () => {
    for (const conformanceCase of submissionLedgerConformanceCases) {
      it.effect(conformanceCase.name, () =>
        withTemporaryDatabase((url) => {
          const storage = makeStorage(
            url,
            { schema: "select" },
            {
              startupParameters: { search_path: "pg_catalog" },
              transformQueryNames: String.snakeToCamel,
              transformResultNames: String.snakeToCamel,
            },
          );

          return conformanceCase.run.pipe(
            Effect.provide(
              Layer.mergeAll(storage.submissionLedger, storage.threadStore, NodeCrypto.layer),
            ),
          );
        }),
      );
    }
  });
});
