import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { describe, it } from "@effect/vitest";
import type { StorageOwner } from "@yielded/agent/run-storage";
import type { SettlementPublisher } from "@yielded/agent/settlement-publisher";
import type { SubmissionLedger } from "@yielded/agent/submission-ledger";
import type { ThreadStore } from "@yielded/agent/thread-store";
import type * as CryptoDigest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import { ledgerReadCases } from "../../../test/fixtures/ledger-read-contracts.ts";
import { SqliteStorageFailpoint } from "../src/SqliteStorageFailpoint.ts";
import { submissionLedgerLayer } from "../src/SqliteSubmissionLedger.ts";
import { storageConfigLayer, threadStoreLayer } from "../src/SqliteThreadStore.ts";

const withFixture = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    | SubmissionLedger
    | SettlementPublisher
    | StorageOwner
    | ThreadStore
    | SqlClient.SqlClient
    | CryptoDigest.Digest
  >,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "ledger-reads-" });
      const filename = `${directory}/state.sqlite`;

      return yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;

        const deps = Layer.mergeAll(
          Layer.succeed(SqlClient.SqlClient)(sql),
          storageConfigLayer({ filename, busyTimeout: 0 }),
          NodeCrypto.layer,
          SqliteStorageFailpoint.layer,
        );

        return yield* effect.pipe(
          Effect.provide(
            Layer.merge(submissionLedgerLayer, threadStoreLayer).pipe(Layer.provideMerge(deps)),
          ),
        );
      }).pipe(Effect.provide(SqliteClient.layer({ filename })));
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

describe("SQLite ledger read contracts", () => {
  for (const test of ledgerReadCases()) it.effect(test.name, () => withFixture(test.run));
});
