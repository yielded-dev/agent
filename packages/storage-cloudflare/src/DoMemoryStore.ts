/**
 * Bounded SQLite memory storage owned by a Cloudflare Durable Object.
 *
 * @since 0.1.0
 */
import * as SqliteClient from "@effect/sql-sqlite-do/SqliteClient";
import { MemoryStorageError, MemoryMutationFailpoint } from "@yielded/agent/memory-store";
import {
  memoryStoreLayerWithFailpoints,
  SqlMemoryLimits,
  SqlStorageOwner,
} from "@yielded/agent/sql-memory-store";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClientService from "effect/sql/SqlClient";

import { ownedState } from "./internal/owned-state.ts";

/**
 * Validated row, storage, document, receipt, and withdrawal-reserve limits.
 *
 * @category schemas
 * @since 0.1.0
 */
export class DoMemoryStorageLimits extends Schema.Class<DoMemoryStorageLimits>(
  "@effect-agent/storage-cloudflare/DoMemoryStorageLimits",
)({
  maxRowBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1_900_000 })),
  maxStorageBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 536_870_912 })),
  maxDocuments: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100_000 })),
  maxReceipts: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1_000_000 })),
  reservedWithdrawalBytes: Schema.optional(Schema.Natural),
  reservedWithdrawalReceipts: Schema.optional(Schema.Natural),
}) {}

/**
 * Default memory limits: 1.9 MB per row, 512 MiB total, 10,000 documents, and 100,000 receipts.
 *
 * @category models
 * @since 0.1.0
 */
export const defaultDoMemoryStorageLimits = DoMemoryStorageLimits.make({
  maxRowBytes: 1_900_000,
  maxStorageBytes: 536_870_912,
  maxDocuments: 10_000,
  maxReceipts: 100_000,
});

/**
 * Local memory only, without Thread tables. Pass the full storage handle: sql-only
 * handles cannot provide atomic receipts and revisions. Services over the same storage
 * share a transaction gate and bounded, write-through document and receipt views.
 * Byte limits conservatively count encoded rows, not SQLite page/index overhead.
 * Optional withdrawal reserves default to zero and stay within hard byte/receipt limits.
 * Ordinary Put cannot consume them. Deploy exclusively upgraded writers before relying
 * on reserves; the SQL accounting migration preserves older writers and their receipts.
 *
 * @category schemas
 * @since 0.1.0
 */
export const doMemoryStoreLayerWithFailpoints = (
  storage: NonNullable<SqliteClient.SqliteClientConfig["storage"]>,
  limits: DoMemoryStorageLimits = defaultDoMemoryStorageLimits,
) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const validated = yield* Schema.decodeEffect(DoMemoryStorageLimits)(limits).pipe(
        Effect.mapError(() =>
          MemoryStorageError.make({ operation: "memory storage limits", reason: "invalid-input" }),
        ),
      );

      const sql = yield* SqlClientService.SqlClient;

      const owner = yield* ownedState(sql).pipe(
        Effect.mapError(() =>
          MemoryStorageError.make({ operation: "open memory owner", reason: "unavailable" }),
        ),
      );

      return memoryStoreLayerWithFailpoints.pipe(
        Layer.provide(Layer.succeed(SqlMemoryLimits, validated)),
        Layer.provide(Layer.succeed(SqlStorageOwner, owner)),
      );
    }),
  ).pipe(Layer.provide(SqliteClient.layer({ storage })));

/**
 * Provide memory storage with fault injection disabled and default limits when omitted.
 *
 * @category layers
 * @since 0.1.0
 */
export const doMemoryStoreLayer = (
  storage: NonNullable<SqliteClient.SqliteClientConfig["storage"]>,
  limits: DoMemoryStorageLimits = defaultDoMemoryStorageLimits,
) =>
  doMemoryStoreLayerWithFailpoints(storage, limits).pipe(
    Layer.provide(MemoryMutationFailpoint.layer),
  );
