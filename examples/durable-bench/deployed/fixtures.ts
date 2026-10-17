import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { Effect, FileSystem, Schema } from "effect";

import { hash, read, workspace } from "./platform.ts";
import {
  type BulkFixture,
  Counts,
  RESTORE_BYTES,
  SqlDump,
  SqlValue,
  StorageFormat,
  type RestoreChunk,
  type Target,
} from "./worker/protocol.ts";

export class FixtureError extends Schema.TaggedError<FixtureError>()("FixtureError", {
  message: Schema.String,
}) {}

const Meta = Schema.Struct({
  version: Schema.String,
  fingerprint: Schema.String,
  tables: Counts,
  threadId: Schema.optionalKey(Schema.NonEmptyString),
});

const Definition = Schema.Struct({ name: Schema.String, sql: Schema.String, type: Schema.String });
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;

const dumpDatabase = (db: DatabaseSync, includeRows = false): SqlDump => {
  const definitions = Schema.decodeUnknownSync(Schema.Array(Definition))(
    db
      .prepare(
        "SELECT name, sql, type FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY rowid",
      )
      .all(),
  );

  const tables = definitions
    .filter((row) => row.type === "table")
    .map((table) => ({
      name: table.name,
      sql: table.sql,
      columns: Schema.decodeUnknownSync(Schema.Array(Schema.Struct({ name: Schema.String })))(
        db.prepare(`PRAGMA table_info(${quote(table.name)})`).all(),
      ).map((row) => row.name),
      rows: includeRows
        ? db
            .prepare(`SELECT * FROM ${quote(table.name)}`)
            .all()
            .map((record) =>
              Object.values(record).map((value) =>
                value instanceof Uint8Array
                  ? { blob: Buffer.from(value).toString("base64") }
                  : value,
              ),
            )
        : [],
    }));

  return Schema.decodeUnknownSync(SqlDump)({
    tables,
    indexes: definitions.filter((row) => row.type === "index").map((row) => row.sql),
    triggers: definitions.filter((row) => row.type === "trigger").map((row) => row.sql),
  });
};

/** Read saved SQLite without opening a host or rewriting any history. */
export const prepareFixtures = Effect.fnUntraced(function* (options: {
  readonly sizes: readonly number[];
  readonly targets?: readonly Target[];
  readonly directory: string;
  readonly requestBytes?: number;
}) {
  const fs = yield* FileSystem.FileSystem;
  const requestBytes = options.requestBytes ?? RESTORE_BYTES;

  const { dependencies } = yield* read(
    join(workspace, "third-party/package.json"),
    Schema.Struct({ dependencies: Schema.Record(Schema.String, Schema.String) }),
  );

  const scratch = yield* fs.makeTempDirectoryScoped({ prefix: "durable-bench-restore-" });
  const fixtures: BulkFixture[] = [];
  const chunks = new Map<string, string[]>();
  const writeEstimates = new Map<string, number>();
  const formats = new Set<string>();

  for (const target of (options.targets ?? ["yielded"]).filter(
    (target) => target !== "effect-ai-floor",
  )) {
    for (const history of options.sizes) {
      const name: string = `${target}-${history}`;
      const source = join(options.directory, name);

      if (!(yield* fs.exists(source + ".json")))
        return yield* FixtureError.make({
          message: `Missing saved ${name} fixture; explicit inputs are read-only.`,
        });
      const meta = yield* read(source + ".json", Meta);

      if (
        (target === "pi" && meta.version !== dependencies["@earendil-works/pi-durable"]) ||
        (target === "tardie" && meta.version !== dependencies.tardie)
      )
        return yield* FixtureError.make({ message: `Unexpected ${target} fixture version` });

      const classDirectory = join(
        source,
        "do",
        target === "yielded" ? "-YieldedDO" : target === "pi" ? "-PiDO" : "-ThreadDO",
      );

      const databases = (yield* fs.readDirectory(classDirectory)).filter((file) =>
        /^[a-f0-9]{64}\.sqlite$/.test(file),
      );

      if (databases.length !== 1)
        return yield* FixtureError.make({ message: `Ambiguous ${name} snapshot` });

      const db: DatabaseSync = yield* Effect.acquireRelease(
        Effect.try({
          try: () => new DatabaseSync(join(classDirectory, databases[0]!), { readOnly: true }),
          catch: () => FixtureError.make({ message: `Open ${name} snapshot` }),
        }),
        (db) => Effect.sync(() => db.close()),
      );

      let storageFormat: typeof StorageFormat.Type | undefined;

      if (target === "yielded") {
        storageFormat = yield* Effect.try({
          try: () => {
            const header = Schema.decodeUnknownSync(
              Schema.Struct({
                layout_version: Schema.Natural,
                record_format: Schema.NonEmptyString,
              }),
            )(
              db
                .prepare(
                  "SELECT layout_version, record_format FROM effect_agent_schema WHERE singleton = 1",
                )
                .get(),
            );

            Schema.decodeUnknownSync(
              Schema.Tuple([Schema.Struct({ thread_id: Schema.Literal(meta.threadId ?? "main") })]),
            )(db.prepare("SELECT thread_id FROM effect_agent_threads").all());

            Schema.decodeUnknownSync(Schema.Struct({ n: Schema.Literal(0) }))(
              db
                .prepare(
                  `SELECT (SELECT COUNT(*) FROM effect_agent_submissions WHERE state != 'settled')${[
                    "effect_agent_submission_ownership",
                    "effect_agent_executions",
                  ]
                    .filter((table) => table in meta.tables)
                    .map((table) => ` + (SELECT COUNT(*) FROM ${quote(table)})`)
                    .join("")} AS n`,
                )
                .get(),
            );

            return StorageFormat.make({
              layoutVersion: header.layout_version,
              recordFormat: header.record_format,
            });
          },
          catch: () =>
            FixtureError.make({
              message: `Snapshot ${name} must be idle and have a recognized storage header`,
            }),
        });
      }

      const exported: SqlDump = yield* Effect.try({
        try: () => dumpDatabase(db),
        catch: () => FixtureError.make({ message: `Read ${name} schema` }),
      });

      // Count occupied table/index cells, including partial and implicit indexes.
      // Multiplying every row by every partial index grossly overestimates these fixtures.
      const entries = yield* Effect.try({
        try: () =>
          Schema.decodeUnknownSync(Schema.Struct({ rows: Schema.Natural }))(
            db
              .prepare(
                `SELECT COALESCE(SUM(d.ncell), 0) AS rows FROM dbstat d JOIN sqlite_master s ON s.name = d.name WHERE s.tbl_name IN (${exported.tables.map(() => "?").join(",")})`,
              )
              .get(...exported.tables.map((table) => table.name)),
          ).rows,
        catch: () => FixtureError.make({ message: `Cannot estimate ${name} table/index writes` }),
      });

      writeEstimates.set(name, entries);

      // Describe persisted bytes, not the source code that happens to open them.
      // Each frozen arm must still pass the native open/upgrade preflight.
      formats.add(
        hash(
          JSON.stringify({
            target,
            format: storageFormat ?? meta.version,
            schema: exported,
          }),
        ).slice(0, 16),
      );

      if (
        exported.tables
          .map((table) => table.name)
          .sort()
          .join() !== Object.keys(meta.tables).sort().join()
      )
        return yield* FixtureError.make({ message: `Saved ${name} table inventory mismatch` });
      const paths: string[] = [];
      let parts: RestoreChunk["parts"][number][] = [];
      let bytes = 64;

      const flush = Effect.fnUntraced(function* () {
        if (!parts.length) return;
        const path = join(scratch, `${name}-${paths.length}.json`);
        const text = JSON.stringify({ sequence: paths.length, parts });

        if (Buffer.byteLength(text) > requestBytes)
          return yield* FixtureError.make({ message: "Restore framing exceeds byte bound" });
        yield* fs.writeFileString(path, text);
        paths.push(path);
        parts = [];
        bytes = 64;
      });

      for (const table of exported.tables) {
        let count = 0;
        let rows: (typeof table.rows)[number][] = [];
        let offset = 0;

        for (const record of db.prepare(`SELECT * FROM ${quote(table.name)}`).iterate()) {
          // SQLite values are decoded at the wire boundary below.
          const values = table.columns.map((column) => {
            const value = record[column];

            return value instanceof Uint8Array
              ? { blob: Buffer.from(value).toString("base64") }
              : value;
          });

          const decoded = Schema.decodeUnknownSync(Schema.Array(SqlValue))(values);
          const size = Buffer.byteLength(JSON.stringify(decoded)) + 1;

          if (size + 256 > requestBytes)
            return yield* FixtureError.make({ message: "Snapshot row exceeds restore byte bound" });
          if (bytes + size + 256 > requestBytes) {
            if (rows.length) parts.push({ table: table.name, offset, rows });
            yield* flush();
            rows = [];
            offset = count;
          }
          rows.push(decoded);
          count++;
          bytes += size;
        }
        if (rows.length) {
          parts.push({ table: table.name, offset, rows });
          bytes += 256;
        }
        if (count !== meta.tables[table.name])
          return yield* FixtureError.make({
            message: `Saved ${name}/${table.name} count mismatch`,
          });
      }
      yield* flush();
      let actor: SqlDump | undefined;

      if (target === "tardie") {
        const actorDirectory = join(source, "do", "-ActorDO");

        const databases = (yield* fs.readDirectory(actorDirectory)).filter((file) =>
          /^[a-f0-9]{64}\.sqlite$/.test(file),
        );

        if (databases.length !== 1)
          return yield* FixtureError.make({ message: "Ambiguous Actor snapshot" });
        actor = yield* Effect.try({
          try: () => {
            const db = new DatabaseSync(join(actorDirectory, databases[0]!), { readOnly: true });

            try {
              return dumpDatabase(db, true);
            } finally {
              db.close();
            }
          },
          catch: () => FixtureError.make({ message: "Read Actor snapshot" }),
        });
      }
      fixtures.push({
        version: 1,
        target,
        history,
        sourceVersion: meta.version,
        fingerprint: meta.fingerprint,
        threadId: meta.threadId ?? "main",
        tables: meta.tables,
        mode: "import",
        ...(storageFormat === undefined ? {} : { storageFormat }),
        thread: exported,
        ...(actor === undefined ? {} : { actor }),
      });
      if (Buffer.byteLength(JSON.stringify(fixtures.at(-1))) > requestBytes)
        return yield* FixtureError.make({
          message: `Saved ${name} restore schema envelope exceeds --restore-bytes (${requestBytes})`,
        });
      chunks.set(name, paths);
    }
  }

  if (options.targets?.includes("effect-ai-floor"))
    for (const history of options.sizes) {
      const reference = fixtures.find(
        (fixture) => fixture.target === "yielded" && fixture.history === history,
      );

      if (!reference)
        return yield* FixtureError.make({
          message: "The floor comparison needs a Yielded reference fixture",
        });
      fixtures.push({
        ...reference,
        target: "effect-ai-floor",
        tables: {},
        thread: { tables: [], indexes: [] },
      });
      chunks.set(`effect-ai-floor-${history}`, []);
    }

  return {
    fixtures,
    chunks,
    writeEstimates,
    formatKey: hash([...formats].sort().join("\n")).slice(0, 16),
  };
});
