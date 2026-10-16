import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as SqlClientService from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

/**
 * An exclusive database owner's transaction gate. All cached services over that database
 * must share this identity, and every failed transaction must run its invalidators before
 * releasing readers. Hosts with independent database writers must leave this unset.
 */
export interface SqlStorageOwner {
  /** Cache identity for transaction decorators; omission uses the owner itself.
   * Shared identities must name the same database, reader gate and invalidator set. */
  readonly identity?: object;
  readonly read: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  readonly transaction: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | SqlError, R | SqlClientService.SqlClient>;
  readonly invalidators: Set<() => void>;
}

export const SqlStorageOwner = Context.Reference<SqlStorageOwner | undefined>(
  "@effect-agent/thread/SqlStorageOwner",
  { defaultValue: () => undefined },
);
