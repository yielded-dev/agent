import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { makeSqlSubmissionLedger } from "@yielded/agent-storage-sql/sql-submission-ledger";
import { SubmissionLedger } from "@yielded/agent/submission-ledger";
import type { Crypto } from "effect";
import { Effect, Layer } from "effect";
import type * as SqlClientService from "effect/sql/SqlClient";

import { initializeSqliteJournal, sqliteErrors } from "./internal/sqlite-journal.ts";
import { sqlFailure } from "./internal/sqlite-ledger-errors.ts";
import { SqliteStorageConfig } from "./SqliteStorageConfig.ts";
import { SqliteStorageFailpoint } from "./SqliteStorageFailpoint.ts";
import {
  storageConfigLayer,
  storageFailpointLayer,
  type SqliteStorageInitializationError,
  type SqliteStorageOptions,
} from "./SqliteThreadStore.ts";

const makeServices = Effect.fn("SqliteSubmissionLedger.makeServices")(function* () {
  const config = yield* SqliteStorageConfig;
  const failpoint = yield* SqliteStorageFailpoint;
  const journal = yield* initializeSqliteJournal();

  return yield* makeSqlSubmissionLedger(journal, {
    errors: sqliteErrors,
    hitFailpoint: failpoint.hit,
    ownershipLeaseDuration: config.ownershipLeaseDuration,
    sqlFailure,
  });
});

/**
 * SQLite SubmissionLedger implementation sharing the journal's database file, write
 * transaction discipline, and producer-epoch fencing substrate. Configuration, failpoint,
 * SQL, and Crypto authority stay visible in the input channel.
 */
export const submissionLedgerLayer: Layer.Layer<
  SubmissionLedger,
  SqliteStorageInitializationError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = Layer.effect(SubmissionLedger, makeServices());

/**
 * A composition-root convenience Layer for the durable Submission Ledger. Point it at the
 * same database file as the ThreadStore so claims fence the same producer epochs.
 */
export const ledgerLayer = (
  options: SqliteStorageOptions,
): Layer.Layer<SubmissionLedger, SqliteStorageInitializationError> =>
  Layer.unwrap(
    Effect.map(SqliteStorageConfig, (config) =>
      submissionLedgerLayer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(SqliteStorageConfig)(config),
            storageFailpointLayer(options),
            SqliteClient.layer({ filename: options.filename }),
            NodeCrypto.layer,
          ),
        ),
      ),
    ),
  ).pipe(Layer.provide(storageConfigLayer(options)));
