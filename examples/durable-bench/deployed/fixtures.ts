import { homedir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { Effect, FileSystem, Schema } from "effect";

import { naturalFixture } from "./natural-fixture.ts";
import { execute, git, hash, read, repository, workspace } from "./platform.ts";
import {
  type BulkFixture,
  Counts,
  RESTORE_BYTES,
  SqlDump,
  SqlValue,
  type RestoreChunk,
  type Target,
} from "./worker/protocol.ts";

export class FixtureError extends Schema.TaggedError<FixtureError>()("FixtureError", {
  message: Schema.String,
}) {}

const Meta = Schema.Struct({
  version: Schema.String,
  formatKey: Schema.optionalKey(Schema.String),
  fingerprint: Schema.String,
  tables: Counts,
  threadId: Schema.optionalKey(Schema.NonEmptyString),
});

const Definition = Schema.Struct({ name: Schema.String, sql: Schema.String, type: Schema.String });
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;

const formatFiles = [
  "packages/storage-cloudflare/src/internal/migrations.ts",
  "packages/effect-agent/src/durable/Records.ts",
];

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

/** The format key hashes adapter migrations and the canonical record schema at the build ref. */
export const fixtureFormat = Effect.fnUntraced(function* (ref?: string) {
  const fs = yield* FileSystem.FileSystem;

  const contents = yield* Effect.forEach(formatFiles, (file) =>
    ref
      ? git(["show", `${ref}:${file}`]).pipe(Effect.map((text) => text + "\n"))
      : fs.readFileString(join(repository, file)),
  );

  return hash(contents.join("\n")).slice(0, 16);
});

/** Read saved SQLite without opening a host or rewriting any history. */
export const prepareFixtures = Effect.fnUntraced(function* (options: {
  readonly sizes: readonly number[];
  readonly targets?: readonly Target[];
  readonly directory?: string;
  readonly requestBytes?: number;
  readonly ref?: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const requestBytes = options.requestBytes ?? RESTORE_BYTES;

  const { dependencies } = yield* read(
    join(workspace, "third-party/package.json"),
    Schema.Struct({ dependencies: Schema.Record(Schema.String, Schema.String) }),
  );

  const formatKey = yield* fixtureFormat(options.ref);

  const baseDirectory =
    options.directory ?? join(homedir(), ".cache/yielded-durable-bench/fixtures", formatKey);

  const scratch = yield* fs.makeTempDirectoryScoped({ prefix: "durable-bench-restore-" });
  const fixtures: BulkFixture[] = [];
  const chunks = new Map<string, string[]>();

  for (const target of options.targets ?? ["yielded"]) {
    for (const history of options.sizes) {
      const name: string = `${target}-${history}`;
      const natural = options.directory === undefined && history === 1000 && target !== "tardie";
      const directory = natural ? baseDirectory + "-natural-id50-v1" : baseDirectory;
      const source = join(directory, name);

      if (natural && !(yield* fs.exists(source + ".json")))
        yield* naturalFixture(target, source, options.ref, formatKey);

      if (!(yield* fs.exists(source + ".json"))) {
        let local = join(scratch, "seed", name);
        // Adopt historical caches only after proving their writer has this exact format key.
        const cacheRoot = join(homedir(), ".cache/yielded-durable-bench/fixtures");

        if (options.directory === undefined && (yield* fs.exists(cacheRoot))) {
          for (const legacy of yield* fs.readDirectory(cacheRoot)) {
            const ref = /^main-([a-f0-9]+)$/.exec(legacy)?.[1];

            if (!ref || !(yield* fs.exists(join(cacheRoot, legacy, name + ".json")))) continue;

            const contents = yield* Effect.forEach(formatFiles, (file) =>
              git(["show", `${ref}:${file}`]),
            );

            const current = yield* Effect.forEach(formatFiles, (file) =>
              options.ref
                ? git(["show", `${options.ref}:${file}`])
                : fs.readFileString(join(repository, file)),
            );

            if (contents.every((content, index) => content.trim() === current[index]?.trim())) {
              local = join(cacheRoot, legacy, name);
              break;
            }
          }
        }

        if (!(yield* fs.exists(local + ".json"))) {
          if (options.ref && formatKey !== (yield* fixtureFormat()))
            return yield* new FixtureError({
              message:
                "A new format requires a natural 1,000-turn fixture; select --sizes 1000 first or supply a matching saved fixture directory.",
            });

          const seeded = yield* execute(["run", "seed", target, String(history)], workspace, {
            DURABLE_BENCH_FIXTURE_DIRECTORY: join(scratch, "seed"),
          });

          if (seeded.code !== 0) return yield* new FixtureError({ message: `Seed ${name} failed` });
        }
        yield* fs.makeDirectory(directory, { recursive: true });

        const copied = yield* execute([
          "exec",
          "cp",
          ...(process.platform === "darwin" ? ["-cR"] : ["-R"]),
          local,
          source,
        ]);

        if (copied.code !== 0) return yield* new FixtureError({ message: `Cache ${name} failed` });
        yield* fs.copyFile(local + ".json", source + ".json");
      }
      const meta = yield* read(source + ".json", Meta);

      if (meta.formatKey !== undefined && meta.formatKey !== formatKey)
        return yield* new FixtureError({
          message: `Saved ${name} belongs to another storage format`,
        });

      if (
        (target === "pi" && meta.version !== dependencies["@earendil-works/pi-durable"]) ||
        (target === "tardie" && meta.version !== dependencies.tardie)
      )
        return yield* new FixtureError({ message: `Unexpected ${target} fixture version` });

      const classDirectory = join(
        source,
        "do",
        target === "yielded" ? "-YieldedDO" : target === "pi" ? "-PiDO" : "-ThreadDO",
      );

      const databases = (yield* fs.readDirectory(classDirectory)).filter((file) =>
        /^[a-f0-9]{64}\.sqlite$/.test(file),
      );

      if (databases.length !== 1)
        return yield* new FixtureError({ message: `Ambiguous ${name} snapshot` });

      const db: DatabaseSync = yield* Effect.acquireRelease(
        Effect.try({
          try: () => new DatabaseSync(join(classDirectory, databases[0]!), { readOnly: true }),
          catch: () => new FixtureError({ message: `Open ${name} snapshot` }),
        }),
        (db) => Effect.sync(() => db.close()),
      );

      if (target === "yielded") {
        yield* Effect.try({
          try: () => {
            Schema.decodeUnknownSync(
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

            const { n } = Schema.decodeUnknownSync(Schema.Struct({ n: Schema.Literal(0) }))(
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

            return n;
          },
          catch: () =>
            new FixtureError({
              message: `Snapshot ${name} must be idle and have a recognized storage header`,
            }),
        });
      }

      const exported: SqlDump = yield* Effect.try({
        try: () => dumpDatabase(db),
        catch: () => new FixtureError({ message: `Read ${name} schema` }),
      });

      if (
        exported.tables
          .map((table) => table.name)
          .sort()
          .join() !== Object.keys(meta.tables).sort().join()
      )
        return yield* new FixtureError({ message: `Saved ${name} table inventory mismatch` });
      const paths: string[] = [];
      let parts: RestoreChunk["parts"][number][] = [];
      let bytes = 64;

      const flush = Effect.fnUntraced(function* () {
        if (!parts.length) return;
        const path = join(scratch, `${name}-${paths.length}.json`);
        const text = JSON.stringify({ sequence: paths.length, parts });

        if (Buffer.byteLength(text) > requestBytes)
          return yield* new FixtureError({ message: "Restore framing exceeds byte bound" });
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
            return yield* new FixtureError({ message: "Snapshot row exceeds restore byte bound" });
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
          return yield* new FixtureError({ message: `Saved ${name}/${table.name} count mismatch` });
      }
      yield* flush();
      let actor: SqlDump | undefined;

      if (target === "tardie") {
        const actorDirectory = join(source, "do", "-ActorDO");

        const databases = (yield* fs.readDirectory(actorDirectory)).filter((file) =>
          /^[a-f0-9]{64}\.sqlite$/.test(file),
        );

        if (databases.length !== 1)
          return yield* new FixtureError({ message: "Ambiguous Actor snapshot" });
        actor = yield* Effect.try({
          try: () => {
            const db = new DatabaseSync(join(actorDirectory, databases[0]!), { readOnly: true });

            try {
              return dumpDatabase(db, true);
            } finally {
              db.close();
            }
          },
          catch: () => new FixtureError({ message: "Read Actor snapshot" }),
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
        thread: exported,
        ...(actor === undefined ? {} : { actor }),
      });
      if (Buffer.byteLength(JSON.stringify(fixtures.at(-1))) > requestBytes)
        return yield* new FixtureError({
          message: `Saved ${name} restore schema envelope exceeds --restore-bytes (${requestBytes})`,
        });
      chunks.set(name, paths);
    }
  }

  return { fixtures, chunks, formatKey };
});
