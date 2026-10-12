import { CurrentDoStorageVersion } from "@yielded/agent-storage-cloudflare/do-storage-version";
import type { exportThread } from "@yielded/agent-storage-cloudflare/do-thread-store";
import { CURRENT_RECORD_FORMAT, RecordEnvelope } from "@yielded/agent/records";
import { ThreadExportRequest, type ThreadExportCursor } from "@yielded/agent/thread-store";
import { Context, Effect, Schema } from "effect";
import { Prompt } from "effect/ai";

import { fingerprint, type Message } from "../../src/plan.ts";
import { tables } from "../../src/serve.ts";
import { expectedSeed, type BulkFixture, type SqlDump, type Target } from "./protocol.ts";

/** Native storage is supplied only by the Durable Object request boundary. */
export class RestoreStorage extends Context.Service<RestoreStorage, DurableObjectStorage>()(
  "durable-bench/RestoreStorage",
) {}

/** Read-only export capability; the native adapter is bound by Host at the request boundary. */
export class FixtureExport extends Context.Service<
  FixtureExport,
  {
    readonly page: (request: typeof ThreadExportRequest.Type) => ReturnType<typeof exportThread>;
  }
>()("durable-bench/FixtureExport") {}

export class FixtureError extends Schema.TaggedError<FixtureError>()("FixtureError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

const fail = (message: string) => FixtureError.make({ message });
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;

export const importRows = (storage: DurableObjectStorage, dump: SqlDump): void => {
  storage.transactionSync(() => {
    for (const table of dump.tables) {
      if (table.name.startsWith("_cf_") || table.name.startsWith("sqlite_"))
        // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- Rejecting this native transactionSync callback rolls back any tables imported earlier in the same transaction.
        throw new Error("Platform tables cannot be imported");
      storage.sql.exec(table.sql.replace(/^CREATE TABLE /i, "CREATE TABLE IF NOT EXISTS "));
      if (
        storage.sql.exec<{ n: number }>(`SELECT COUNT(*) n FROM ${quote(table.name)}`).one().n !== 0
      )
        // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- This mutation-dependent guard must reject transactionSync to undo preceding imported tables.
        throw new Error(`Import destination table ${table.name} is not empty`);
      for (const row of table.rows) {
        const values = row.map((value) =>
          typeof value === "object" && value !== null
            ? Uint8Array.from(atob(value.blob), (char) => char.charCodeAt(0)).buffer
            : value,
        );

        storage.sql.exec(
          `INSERT INTO ${quote(table.name)} (${table.columns.map(quote).join(",")}) VALUES (${values.map(() => "?").join(",")})`,
          ...values,
        );
      }
    }
    for (const index of dump.indexes)
      storage.sql.exec(index.replace(/^CREATE (UNIQUE )?INDEX /i, "CREATE $1INDEX IF NOT EXISTS "));
  });
};

const args = Schema.decodeUnknownSync(Schema.Struct({ n: Schema.Int }));
const string = Schema.decodeUnknownSync(Schema.String);

const contentText = (content: string | readonly { type: string; text?: string }[]) =>
  typeof content === "string"
    ? content
    : content.flatMap((part) => (part.type === "text" ? [part.text ?? ""] : [])).join("");

const piEntry = Schema.decodeUnknownSync(
  Schema.Struct({
    model: Schema.Array(
      Schema.Struct({
        role: Schema.String,
        content: Schema.Union([
          Schema.String,
          Schema.Array(
            Schema.Struct({
              type: Schema.String,
              text: Schema.optionalKey(Schema.String),
              arguments: Schema.optionalKey(Schema.Unknown),
            }),
          ),
        ]),
      }),
    ),
  }),
);

const tardieEvent = Schema.decodeUnknownSync(
  Schema.Struct({ event: Schema.Struct({ type: Schema.String }) }),
);

const tardieUser = Schema.decodeUnknownSync(
  Schema.Struct({
    event: Schema.Struct({
      body: Schema.Struct({ type: Schema.Literal("TurnRequested"), text: Schema.String }),
    }),
  }),
);

const tardieModel = Schema.decodeUnknownSync(
  Schema.Struct({
    event: Schema.Struct({
      text: Schema.String,
      toolCalls: Schema.Array(Schema.Struct({ input: Schema.Unknown })),
    }),
  }),
);

const tardieTool = Schema.decodeUnknownSync(
  Schema.Struct({ event: Schema.Struct({ output: Schema.String }) }),
);

const appendYielded = (result: Message[], record: typeof RecordEnvelope.Type) => {
  const p = record.payload;

  if (p._tag === "UserInputRecorded") result.push({ role: "user", text: string(p.input) });
  if (p._tag === "ToolCallSettled") result.push({ role: "tool", text: string(p.result) });
  if (p._tag !== "ModelResponseRecorded") return;
  for (const message of Schema.decodeUnknownSync(Prompt.Prompt)(p.messages).content) {
    if (message.role !== "assistant") continue;

    const calls = message.content.flatMap((part) =>
      part.type === "tool-call" ? [args(part.params).n] : [],
    );

    result.push({
      role: "assistant",
      text: contentText(message.content),
      ...(calls.length ? { calls } : {}),
    });
  }
};

/** Use the revision's read-only exporter for compact layouts; never canonical-import the fixture. */
const compactTranscript = Effect.fnUntraced(function* (fixture: BulkFixture) {
  const exporter = yield* FixtureExport;

  const request = yield* Schema.decodeEffect(ThreadExportRequest)({
    threadId: fixture.threadId,
  });

  const messages: Message[] = [];
  let cursor: typeof ThreadExportCursor.Type | undefined;

  do {
    const page = yield* exporter.page({ ...request, ...(cursor === undefined ? {} : { cursor }) });

    for (const entry of page.records) appendYielded(messages, entry.record);
    cursor = page.cursor;
  } while (cursor !== undefined);
  if (messages.at(-1)?.role !== "assistant" || messages.at(-1)?.calls?.length)
    return yield* fail("Fixture is not settled");

  return messages.slice(0, -1);
});

/** Reconstruct the last seeded model request; the seed digest excludes its final answer. */
export const transcript = Effect.fnUntraced(function* (sql: SqlStorage, target: Target) {
  const result = yield* Effect.try({
    try: () => {
      const result: Message[] = [];

      if (target === "tardie") {
        for (const row of sql
          .exec<{ event: string }>("SELECT event FROM experimental_events ORDER BY seq")
          .toArray()) {
          const wire: unknown = JSON.parse(row.event);
          const { event } = tardieEvent(wire);

          if (event.type === "MessageReceived")
            result.push({ role: "user", text: tardieUser(wire).event.body.text });
          if (event.type === "ModelReturned") {
            const model = tardieModel(wire).event;
            const calls = model.toolCalls.map((call) => args(call.input).n);

            result.push({
              role: "assistant",
              text: model.text,
              ...(calls.length ? { calls } : {}),
            });
          }
          if (event.type === "ToolReturned")
            result.push({ role: "tool", text: string(JSON.parse(tardieTool(wire).event.output)) });
        }
      } else if (target === "pi") {
        for (const row of sql
          .exec<{ record: string }>("SELECT record FROM entries ORDER BY commit_seq")
          .toArray()) {
          for (const message of piEntry(JSON.parse(row.record)).model) {
            if (
              message.role !== "user" &&
              message.role !== "assistant" &&
              message.role !== "toolResult"
            )
              continue;

            const calls =
              typeof message.content === "string"
                ? []
                : message.content.flatMap((part) =>
                    part.type === "toolCall" ? [args(part.arguments).n] : [],
                  );

            result.push({
              role: message.role === "toolResult" ? "tool" : message.role,
              text: contentText(message.content),
              ...(calls.length ? { calls } : {}),
            });
          }
        }
      } else {
        for (const row of sql.exec<{ record_json: string }>(
          "SELECT record_json FROM effect_agent_canonical_records ORDER BY sequence",
        )) {
          const record = Schema.decodeUnknownSync(RecordEnvelope)(JSON.parse(row.record_json));

          appendYielded(result, record);
        }
      }

      return result;
    },
    catch: (cause) => FixtureError.make({ message: "Read fixture transcript", cause }),
  });

  if (result.at(-1)?.role !== "assistant" || result.at(-1)?.calls?.length)
    return yield* fail("Fixture is not settled");

  return result.slice(0, -1);
});

export const verifyFixture = Effect.fnUntraced(function* (fixture: BulkFixture) {
  const storage = yield* RestoreStorage;

  if (fixture.target === "yielded") {
    yield* Effect.try({
      try: () =>
        Schema.decodeUnknownSync(
          Schema.Struct({
            layout_version: Schema.Literal(CurrentDoStorageVersion),
            record_format: Schema.Literal(CURRENT_RECORD_FORMAT),
          }),
        )(
          storage.sql
            .exec(
              "SELECT layout_version, record_format FROM effect_agent_schema WHERE singleton = 1",
            )
            .one(),
        ),
      catch: (cause) =>
        FixtureError.make({ message: "Fixture storage format differs from this revision", cause }),
    });
  }

  const compact =
    fixture.target === "yielded" &&
    !fixture.thread?.tables
      .find((table) => table.name === "effect_agent_canonical_records")
      ?.columns.includes("record_json");

  const messages = compact
    ? yield* compactTranscript(fixture)
    : yield* transcript(storage.sql, fixture.target);

  const digest = yield* Effect.tryPromise({
    try: () => fingerprint(messages),
    catch: (cause) => FixtureError.make({ message: "Read fixture transcript", cause }),
  });

  if (
    digest !== fixture.fingerprint ||
    (expectedSeed[fixture.history] !== undefined && digest !== expectedSeed[fixture.history])
  )
    return yield* fail("Imported transcript fingerprint differs from local fixture");
  const counts = tables(storage.sql);

  if (Object.keys(counts).sort().join("\n") !== Object.keys(fixture.tables).sort().join("\n"))
    return yield* fail("Initialized table inventory differs from local source fixture");

  for (const [table, expected] of Object.entries(fixture.tables)) {
    if (counts[table] !== expected)
      return yield* fail(
        `Initialized ${table} count ${counts[table]} differs from source ${expected}`,
      );
  }

  return { fingerprint: digest, tables: counts };
});
