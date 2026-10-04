import { CURRENT_RECORD_FORMAT } from "@yielded/agent/records";
import { Effect, Schema } from "effect";
import type * as SqlClient from "effect/sql/SqlClient";
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
  readonly baseline: {
    readonly version: number;
    readonly recordFormat: string;
    readonly statements: ReadonlyArray<string>;
    readonly objects: ReadonlyArray<readonly [type: string, name: string, statementIndex: number]>;
  };
  readonly steps: ReadonlyArray<{ readonly version: number }>;
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
 * Inspect an adapter's frozen SQLite layout. The adapter owns legacy version discovery,
 * empty-store detection, the surrounding snapshot/transaction, and every layout statement.
 */
export const makeSqliteLayoutInspection = <C, K, S>(
  options: SqliteLayoutInspectionOptions<C, K, S>,
) => {
  const decode = <A, I>(schema: Schema.Codec<A, I>, value: unknown, table: string) =>
    Schema.decodeUnknownEffect(schema)(value, { onExcessProperty: "error" }).pipe(
      Effect.mapError(() => options.corruption(table)),
    );

  const readObjects = Effect.fnUntraced(function* (client: SqlClient.SqlClient) {
    const sql = client.withoutTransforms();

    return yield* decode(
      Objects,
      yield* sql<Record<string, unknown>>`
        SELECT name, type, sql FROM sqlite_master WHERE name GLOB 'effect_agent_*' ORDER BY name
      `.pipe(Effect.mapError(options.storageError)),
      "sqlite_master",
    );
  });

  const readHeader = Effect.fnUntraced(function* (
    client: SqlClient.SqlClient,
    objects: typeof Objects.Type,
    version: number,
  ): Effect.fn.Return<SqliteLayoutHeader, C | K | S> {
    const sql = client.withoutTransforms();
    const { baseline, steps, incompatible } = options;

    if (!steps.some((step) => step.version === version))
      return yield* Effect.fail(incompatible(version, `Unsupported storage version ${version}.`));

    const schema = objects.find((row) => row.name === "effect_agent_schema");
    let header: SqliteLayoutHeader;

    if (schema === undefined) {
      if (version !== baseline.version)
        return yield* Effect.fail(incompatible(version, "Missing singleton schema header."));
      header = { layoutVersion: baseline.version, recordFormat: baseline.recordFormat };
    } else {
      if (
        schema.type !== "table" ||
        schema.sql === null ||
        normalize(schema.sql) !== normalize(options.headerStatement)
      )
        return yield* Effect.fail(incompatible(version, "Malformed singleton schema table."));

      const [row] = yield* decode(
        Schema.Tuple([Header]),
        yield* sql<Record<string, unknown>>`
          SELECT * FROM effect_agent_schema
        `.pipe(Effect.mapError(options.storageError)),
        "effect_agent_schema",
      );

      if (
        row.layout_version <= baseline.version ||
        !steps.some((step) => step.version === row.layout_version) ||
        row.layout_version !== version
      )
        return yield* Effect.fail(
          incompatible(row.layout_version, "Unsupported or conflicting layout headers."),
        );
      header = { layoutVersion: row.layout_version, recordFormat: row.record_format };
    }

    if (header.recordFormat !== CURRENT_RECORD_FORMAT)
      return yield* Effect.fail(
        incompatible(header.layoutVersion, `Unsupported record format ${header.recordFormat}.`),
      );

    for (const [type, name, index] of baseline.objects) {
      const actual = objects.find((row) => row.name === name);

      if (
        actual?.type !== type ||
        actual.sql === null ||
        normalize(actual.sql) !== normalize(baseline.statements[index])
      )
        return yield* Effect.fail(
          incompatible(version, `Missing or incompatible layout object ${name}.`),
        );
    }

    return header;
  });

  return { decode, readObjects, readHeader };
};
