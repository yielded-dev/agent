import { makeStorageOwner, type StorageOwner } from "@yielded/agent/run-storage";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import type * as SqlClient from "effect/sql/SqlClient";

interface RetainedOwner {
  readonly ready: Deferred.Deferred<StorageOwner["Service"]>;
  readonly disposed: Deferred.Deferred<void>;
  readonly scope: Scope.Closeable;
  references: number;
  closing: boolean;
}

const owners = new WeakMap<object, Map<string, RetainedOwner>>();

/** Each live facet co-owns the root Scope; closing one facet cannot revoke another. */
export const sqlStorageOwner = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  namespace = "",
) {
  return yield* Effect.uninterruptible(
    Effect.gen(function* () {
      let namespaces = owners.get(sql.transactionService);

      if (namespaces === undefined) {
        namespaces = new Map();
        owners.set(sql.transactionService, namespaces);
      }
      const registry = namespaces;

      for (;;) {
        let retained = registry.get(namespace);

        if (retained?.closing) {
          yield* Deferred.await(retained.disposed);
          continue;
        }
        if (retained === undefined) {
          retained = {
            ready: Deferred.makeUnsafe(),
            disposed: Deferred.makeUnsafe(),
            scope: Scope.makeUnsafe(),
            references: 1,
            closing: false,
          };
          registry.set(namespace, retained);

          const root = yield* makeStorageOwner(Semaphore.makeUnsafe(1).withPermits(1)).pipe(
            Scope.provide(retained.scope),
          );

          yield* Deferred.succeed(retained.ready, root);
        } else retained.references++;
        const current = retained;

        yield* Effect.addFinalizer(() =>
          Effect.uninterruptible(
            Effect.gen(function* () {
              current.references--;
              if (current.references !== 0) return;
              current.closing = true;
              yield* Scope.close(current.scope, Exit.void).pipe(
                Effect.ensuring(
                  Effect.gen(function* () {
                    if (registry.get(namespace) === current) registry.delete(namespace);
                    yield* Deferred.succeed(current.disposed, undefined);
                  }),
                ),
              );
            }),
          ),
        );

        return yield* Deferred.await(current.ready);
      }
    }),
  );
});
