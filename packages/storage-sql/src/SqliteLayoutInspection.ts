import { CURRENT_RECORD_FORMAT } from "@yielded/agent/records";
import { Effect, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

const Header = Schema.Struct({
  singleton: Schema.Literal(1),
  layout_version: Schema.Int.check(Schema.isGreaterThan(0)),
  record_format: Schema.NonEmptyString,
});

const Objects = Schema.Array(
  Schema.Struct({
    name: Schema.String,
    type: Schema.String,
    sql: Schema.NullOr(Schema.String),
  }),
);

export interface SqliteLayoutHeader {
  readonly layoutVersion: number;
  readonly recordFormat: string;
}

export interface SqliteLayoutInspectionOptions<C, K, S> {
  readonly version: number;
  readonly statements: ReadonlyArray<string>;
  readonly objects: ReadonlyArray<readonly [type: string, name: string, statementIndex: number]>;
  readonly headerStatement: string;
  readonly corruption: (table: string) => C;
  readonly incompatible: (actualVersion: number, message: string) => K;
  readonly storageError: (cause: SqlError) => S;
}

// SQLite preserves CREATE SQL. Ignore identifier quotes/formatting, never payload or predicate spelling.
const normalize = (statement: string) =>
  statement.replace(/'(?:''|[^'])*'|"(?:""|[^"])*"|\s+/g, (token) =>
    token.startsWith("'")
      ? token
      : token.startsWith('"')
        ? token.slice(1, -1).replaceAll('""', '"')
        : "",
  );

/**
 * Inspect an adapter's current SQLite layout. The adapter owns version discovery,
 * empty-store detection, the surrounding snapshot/transaction, and every layout statement.
 */
export const makeSqliteLayoutInspection = <C, K, S>(
  options: SqliteLayoutInspectionOptions<C, K, S>,
) => {
  const decode = <A, I>(schema: Schema.Codec<A, I>, value: unknown, table: string) =>
    Schema.decodeUnknownEffect(schema)(value, { onExcessProperty: "error" }).pipe(
      Effect.mapError(() => options.corruption(table)),
    );

  const readObjects = Effect.fnUntraced(function* () {
    const sql = (yield* SqlClient.SqlClient).withoutTransforms();

    return yield* decode(
      Objects,
      yield* sql<Record<string, unknown>>`
        SELECT name, type, sql FROM sqlite_master WHERE name GLOB 'effect_agent_*' ORDER BY name
      `.pipe(Effect.mapError(options.storageError)),
      "sqlite_master",
    );
  });

  const readHeader = Effect.fnUntraced(function* (
    objects: typeof Objects.Type,
    version: number,
  ): Effect.fn.Return<SqliteLayoutHeader, C | K | S, SqlClient.SqlClient> {
    const sql = (yield* SqlClient.SqlClient).withoutTransforms();
    const { incompatible } = options;

    if (version !== options.version)
      return yield* Effect.fail(incompatible(version, `Unsupported storage version ${version}.`));
    const schema = objects.find((row) => row.name === "effect_agent_schema");

    if (
      schema?.type !== "table" ||
      schema.sql === null ||
      schema.sql === undefined ||
      normalize(schema.sql) !== normalize(options.headerStatement)
    )
      return yield* Effect.fail(
        incompatible(version, "Missing or malformed singleton schema table."),
      );

    const [row] = yield* decode(
      Schema.Tuple([Header]),
      yield* sql<Record<string, unknown>>`SELECT * FROM effect_agent_schema`.pipe(
        Effect.mapError(options.storageError),
      ),
      "effect_agent_schema",
    );

    if (row.layout_version !== version)
      return yield* Effect.fail(
        incompatible(row.layout_version, "Unsupported or conflicting layout headers."),
      );
    const header = { layoutVersion: row.layout_version, recordFormat: row.record_format };

    if (header.recordFormat !== CURRENT_RECORD_FORMAT)
      return yield* Effect.fail(
        incompatible(header.layoutVersion, `Unsupported record format ${header.recordFormat}.`),
      );

    for (const [type, name, index] of options.objects) {
      const actual = objects.find((row) => row.name === name);

      if (
        actual?.type !== type ||
        actual.sql === null ||
        normalize(actual.sql) !== normalize(options.statements[index])
      )
        return yield* Effect.fail(
          incompatible(version, `Missing or incompatible layout object ${name}.`),
        );
    }

    return header;
  });

  /** Dedicated managed hosts allow exactly the current layout's declared triggers. */
  const readManagedTriggers = Effect.fnUntraced(function* (initialized: boolean) {
    const sql = (yield* SqlClient.SqlClient).withoutTransforms();

    const actual = yield* decode(
      Objects,
      yield* sql`SELECT name, type, sql FROM main.sqlite_master WHERE type='trigger' ORDER BY name`.pipe(
        Effect.mapError(options.storageError),
      ),
      "sqlite_master",
    );

    const expected = initialized ? options.objects.filter(([type]) => type === "trigger") : [];

    if (
      actual.length !== expected.length ||
      actual.some((row) => {
        const declared = expected.find(([, name]) => name === row.name);

        return (
          declared === undefined ||
          row.sql === null ||
          normalize(row.sql) !== normalize(options.statements[declared[2]])
        );
      })
    )
      return yield* Effect.fail(
        options.incompatible(
          initialized ? options.version : 0,
          "Managed-host database contains unknown or incompatible persistent SQL triggers.",
        ),
      );
  });

  return { decode, readObjects, readHeader, readManagedTriggers };
};
