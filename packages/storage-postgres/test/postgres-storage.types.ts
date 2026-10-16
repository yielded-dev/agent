import { expectTypeOf } from "@effect/vitest";
import type * as PostgresStorage from "@yielded/agent-storage-postgres/postgres-storage";
import type {
  PostgresStorageError,
  PostgresStorageInitializationError,
} from "@yielded/agent-storage-postgres/postgres-storage-error";
import type { ActivityMutationFailure, ActivityStoreError } from "@yielded/agent/activity-store";
import type { StorageOwner } from "@yielded/agent/run-storage";
import type { SettlementPublisher } from "@yielded/agent/settlement-publisher";
import type { SubmissionLedger } from "@yielded/agent/submission-ledger";
import type { ThreadImport } from "@yielded/agent/thread-import";
import type { ThreadStore, ThreadReader } from "@yielded/agent/thread-store";
import type * as Crypto from "effect/Crypto";
import type * as Layer from "effect/Layer";
import type * as SqlClient from "effect/sql/SqlClient";

type Persistence = typeof PostgresStorage.layer;
type Ledger = ReturnType<typeof PostgresStorage.submissionLedgerLayer>;

expectTypeOf<Layer.Services<Persistence>>().toEqualTypeOf<SqlClient.SqlClient | Crypto.Crypto>();
expectTypeOf<Layer.Success<Persistence>>().toEqualTypeOf<
  ThreadStore | ThreadReader | ThreadImport | SubmissionLedger | SettlementPublisher | StorageOwner
>();
expectTypeOf<Layer.Error<Persistence>>().toEqualTypeOf<PostgresStorageInitializationError>();
expectTypeOf<Layer.Services<Ledger>>().toEqualTypeOf<SqlClient.SqlClient | Crypto.Crypto>();
expectTypeOf<Layer.Success<Ledger>>().toEqualTypeOf<
  SubmissionLedger | SettlementPublisher | StorageOwner
>();
expectTypeOf<Layer.Error<Ledger>>().toEqualTypeOf<PostgresStorageInitializationError>();
expectTypeOf<
  Layer.Services<ReturnType<typeof PostgresStorage.scheduleStoreLayer>>
>().toEqualTypeOf<SqlClient.SqlClient>();
expectTypeOf<
  Layer.Services<ReturnType<typeof PostgresStorage.activityStoreLayer>>
>().toEqualTypeOf<SqlClient.SqlClient>();
expectTypeOf<Layer.Error<ReturnType<typeof PostgresStorage.activityStoreLayer>>>().toEqualTypeOf<
  PostgresStorageError | ActivityStoreError | ActivityMutationFailure
>();
expectTypeOf<
  Layer.Services<ReturnType<typeof PostgresStorage.subscriptionStoreLayer>>
>().toEqualTypeOf<SqlClient.SqlClient>();
expectTypeOf<
  Layer.Services<ReturnType<typeof PostgresStorage.messageDeliveryStoreLayer>>
>().toEqualTypeOf<SqlClient.SqlClient | Crypto.Crypto>();
