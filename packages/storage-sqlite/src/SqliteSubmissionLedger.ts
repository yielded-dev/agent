import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { makeSqlSubmissionLedgerKernel } from "@yielded/agent-storage-sql/sql-submission-ledger";
import { StorageOwner } from "@yielded/agent/run-storage";
import { SettlementPublisher } from "@yielded/agent/settlement-publisher";
import { SubmissionLedger } from "@yielded/agent/submission-ledger";
import * as Context from "effect/Context";
import type * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as SqlClientService from "effect/sql/SqlClient";

import { sqliteClientLayer } from "./internal/sqlite-client-layer.ts";
import { initializeSqliteJournal, sqliteErrors } from "./internal/sqlite-journal.ts";
import { sqlFailure } from "./internal/sqlite-ledger-errors.ts";
import { SqliteStorageConfig } from "./SqliteStorageConfig.ts";
import { SqliteStorageFailpoint } from "./SqliteStorageFailpoint.ts";
import {
  exclusiveHostClientLayer,
  storageConfigLayer,
  storageFailpointLayer,
  type SqliteStorageInitializationError,
  type SqliteStorageOptions,
} from "./SqliteThreadStore.ts";

const makeServices = Effect.fnUntraced(function* () {
  const failpoint = yield* SqliteStorageFailpoint;
  const journal = yield* initializeSqliteJournal;

  const services = yield* makeSqlSubmissionLedgerKernel(journal, {
    errors: sqliteErrors,
    hitFailpoint: failpoint.hit,
    offsetPrefix: "effect-agent-sqlite@1:",
    sqlFailure,
  });

  return Context.make(SubmissionLedger, services.ledger).pipe(
    Context.add(SettlementPublisher, services.publisher),
    Context.add(StorageOwner, journal.owner),
  );
});

/**
 * SQLite submission transitions and canonical settlement publication share the journal's
 * database file, scoped owner, and mutation gate. Configuration, failpoint, SQL, and Crypto
 * authority stay visible in the input channel.
 */
export const submissionLedgerLayer: Layer.Layer<
  SubmissionLedger | SettlementPublisher | StorageOwner,
  SqliteStorageInitializationError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = Layer.effectContext(makeServices()).pipe(Layer.provide(exclusiveHostClientLayer));

/**
 * A composition-root Layer for submission transitions and settlement publication. Point it
 * at the same exclusive client as ThreadStore so every mutation shares its local owner.
 */
export const ledgerLayer = (
  options: SqliteStorageOptions,
): Layer.Layer<
  SubmissionLedger | SettlementPublisher | StorageOwner,
  SqliteStorageInitializationError
> =>
  Layer.unwrap(
    Effect.map(SqliteStorageConfig, (config) =>
      submissionLedgerLayer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(SqliteStorageConfig)(config),
            storageFailpointLayer(options),
            sqliteClientLayer({ filename: options.filename, disableWAL: true }, "open SQLite"),
            NodeCrypto.layer,
          ),
        ),
      ),
    ),
  ).pipe(Layer.provide(storageConfigLayer(options)));
