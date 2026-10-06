import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { makeSqlSubmissionLedgerKernel } from "@yielded/agent-storage-sql/sql-submission-ledger";
import { SettlementPublisher } from "@yielded/agent/settlement-publisher";
import { SubmissionLedger } from "@yielded/agent/submission-ledger";
import type { Crypto } from "effect";
import { Context, Effect, Layer } from "effect";
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

const makeServices = Effect.fnUntraced(function* () {
  const config = yield* SqliteStorageConfig;
  const failpoint = yield* SqliteStorageFailpoint;
  const journal = yield* initializeSqliteJournal();

  const services = yield* makeSqlSubmissionLedgerKernel(journal, {
    errors: sqliteErrors,
    hitFailpoint: failpoint.hit,
    ownershipLeaseDuration: config.ownershipLeaseDuration,
    offsetPrefix: "effect-agent-sqlite@1:",
    sqlFailure,
  });

  return Context.make(SubmissionLedger, services.ledger).pipe(
    Context.add(SettlementPublisher, services.publisher),
  );
});

/**
 * SQLite submission transitions and canonical settlement publication share the journal's
 * database file, writer, and producer-epoch fence. Configuration, failpoint, SQL, and Crypto
 * authority stay visible in the input channel.
 */
export const submissionLedgerLayer: Layer.Layer<
  SubmissionLedger | SettlementPublisher,
  SqliteStorageInitializationError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = Layer.effectContext(makeServices());

/**
 * A composition-root Layer for submission transitions and settlement publication. Point it
 * at the ThreadStore's database file so claims fence the same producer epochs.
 */
export const ledgerLayer = (
  options: SqliteStorageOptions,
): Layer.Layer<SubmissionLedger | SettlementPublisher, SqliteStorageInitializationError> =>
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
