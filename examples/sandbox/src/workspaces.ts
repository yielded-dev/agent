import { desc, lt, sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/durable-sqlite";
import { migrate } from "drizzle-orm/durable-sqlite/migrator";
import { Clock, Context, Effect, Layer, Schema } from "effect";
import { DurableObject, DurableObjectState } from "effect-cf";

import migrations from "../drizzle/migrations.js";
import type { WorkspacePage } from "./contract.ts";
import { WorkspaceRegistration, WorkspaceSummary } from "./contract.ts";
import { workspaces } from "./schema.ts";

class DirectoryError extends Schema.TaggedError<DirectoryError>()("DirectoryError", {
  cause: Schema.Defect(),
}) {}

const failure = (cause: unknown) => DirectoryError.make({ cause });
const Row = Schema.Struct({ ...WorkspaceSummary.fields, position: Schema.Natural });
const pageSize = 25;

class Directory extends Context.Service<
  Directory,
  {
    readonly remember: (
      input: typeof WorkspaceRegistration.Type,
    ) => Effect.Effect<void, DirectoryError>;
    readonly list: (
      before: number | null,
    ) => Effect.Effect<typeof WorkspacePage.Type, DirectoryError>;
  }
>()("sandbox/Directory") {
  static readonly layer = Layer.effect(
    Directory,
    Effect.gen(function* () {
      const state = yield* DurableObjectState.DurableObjectState;
      const db = drizzle(state.raw.storage);

      yield* Effect.try({ try: () => migrate(db, migrations), catch: failure });

      return Directory.of({
        remember: Effect.fn("WorkspaceDirectory.remember")(function* (input) {
          const entry = yield* Schema.decodeEffect(WorkspaceRegistration)(input);
          const addedAt = yield* Clock.currentTimeMillis;

          // Reserve navigation before admission, including when its response is lost.
          // Replays retain the first title and position; this is not an activity journal.
          yield* Effect.try({
            try: () =>
              db
                .insert(workspaces)
                .values({ ...entry, id: entry.id.toLowerCase(), addedAt })
                .onConflictDoUpdate({
                  target: workspaces.id,
                  set: { title: sql`excluded.title` },
                  setWhere: sql`${workspaces.title} IS NULL AND excluded.title IS NOT NULL`,
                })
                .run(),
            catch: failure,
          });
        }, Effect.mapError(failure)),
        list: Effect.fn("WorkspaceDirectory.list")(function* (before) {
          const cursor = yield* Schema.decodeEffect(Schema.NullOr(Schema.Natural))(before);

          const rows = yield* Effect.try({
            try: () =>
              db
                .select()
                .from(workspaces)
                .where(cursor === null ? undefined : lt(workspaces.position, cursor))
                .orderBy(desc(workspaces.position))
                .limit(pageSize + 1)
                .all(),
            catch: failure,
          });

          const decoded = yield* Schema.decodeEffect(Schema.Array(Row))(rows);
          const entries = decoded.slice(0, pageSize);

          return {
            entries: entries.map(({ position: _, ...entry }) => entry),
            next: decoded.length > pageSize ? (entries.at(-1)?.position ?? null) : null,
          };
        }, Effect.mapError(failure)),
      });
    }),
  );
}

/** The Worker derives this object's name from the authenticated account, never request data. */
export class AccountWorkspaces extends DurableObject.make(Directory.layer) {
  remember(input: typeof WorkspaceRegistration.Type) {
    return this[DurableObject.RunSymbol](
      Effect.flatMap(Directory, (directory) => directory.remember(input)),
    );
  }

  list(before: number | null) {
    return this[DurableObject.RunSymbol](
      Effect.flatMap(Directory, (directory) => directory.list(before)),
    );
  }
}
