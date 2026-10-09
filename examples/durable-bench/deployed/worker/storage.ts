import { BrowserCrypto } from "@effect/platform-browser";
import * as DoThreadStore from "@yielded/agent-storage-cloudflare/do-thread-store";
import { canonicalJson, digestJson } from "@yielded/agent/digest";
import { ThreadId } from "@yielded/agent/identifiers";
import { RecordEnvelope } from "@yielded/agent/records";
import { ThreadArchive, ThreadImport } from "@yielded/agent/thread-import";
import { ThreadAdmission, ThreadExport, ThreadExportRequest } from "@yielded/agent/thread-store";
import { Effect, Schema, Stream } from "effect";
import { Prompt } from "effect/ai";

import { fingerprint, type Message } from "../../src/plan.ts";
import { tables } from "../../src/serve.ts";
import { expectedSeed, type BulkFixture, type SqlDump, type Target } from "./protocol.ts";

export class FixtureError extends Schema.TaggedError<FixtureError>()("FixtureError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

const fail = (message: string) => new FixtureError({ message });
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;

export const importRows = (storage: DurableObjectStorage, dump: SqlDump): void => {
  storage.transactionSync(() => {
    for (const table of dump.tables) {
      if (table.name.startsWith("_cf_") || table.name.startsWith("sqlite_"))
        throw fail("Platform tables cannot be imported");
      storage.sql.exec(table.sql.replace(/^CREATE TABLE /i, "CREATE TABLE IF NOT EXISTS "));
      if (
        storage.sql.exec<{ n: number }>(`SELECT COUNT(*) n FROM ${quote(table.name)}`).one().n !== 0
      )
        throw fail(`Import destination table ${table.name} is not empty`);
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

const Manifest = Schema.Struct({
  format: ThreadArchive.fields.format,
  threadId: ThreadArchive.fields.threadId,
  tailSequence: ThreadArchive.fields.tailSequence,
  tailDigest: ThreadArchive.fields.tailDigest,
  snapshot: ThreadArchive.fields.snapshot,
  externalObligations: ThreadArchive.fields.externalObligations,
});

const fixtureRecords = new Set<RecordEnvelope["payload"]["_tag"]>([
  "ThreadCreated",
  "UserInputRecorded",
  "RunStarted",
  "RunContextRecorded",
  "ModelResponseRecorded",
  "ToolCallSettled",
  "RunCompleted",
  "RunContinuation",
  "SubmissionSettled",
]);

/** Re-address only a settled readonly fixture; never alter canonical bytes or their references. */
export const addressArchive = Effect.fnUntraced(function* (
  wire: readonly Schema.Json[],
  name: string,
) {
  const pages = yield* Effect.forEach(wire, (page) =>
    Schema.decodeUnknownEffect(ThreadArchive)(page),
  );

  const first = pages[0];

  if (!first) return yield* fail("Empty canonical fixture");
  const threadId = ThreadId.make(name);
  const sourceManifest = yield* Schema.encodeEffect(Manifest)(first);

  if ((yield* digestJson(sourceManifest)) !== first.snapshotId)
    return yield* fail("Source transfer snapshot digest mismatch");
  const snapshotId = yield* digestJson({ ...sourceManifest, threadId });

  return yield* Effect.forEach(pages, (page) =>
    Effect.gen(function* () {
      if (
        page.threadId !== first.threadId ||
        page.snapshotId !== first.snapshotId ||
        canonicalJson(yield* Schema.encodeEffect(Manifest)(page)) !== canonicalJson(sourceManifest)
      )
        return yield* fail("Source transfer changes identity or snapshot");
      if (
        page.workerSeal ||
        page.deliveries.length ||
        page.externalObligations?.length ||
        page.commands.aborts.length ||
        page.commands.approvals.length ||
        page.commands.resolutions.length
      )
        return yield* fail("Fixture contains cross-Thread or operator obligations");
      for (const entry of page.records) {
        const record = yield* Schema.decodeUnknownEffect(RecordEnvelope)(entry.record);

        if (entry.threadId !== first.threadId || !fixtureRecords.has(record.payload._tag))
          return yield* fail("Fixture contains foreign identity or unsupported canonical facts");
      }
      if (
        page.admissions.some(
          (admission) =>
            admission.threadId !== first.threadId ||
            admission.parentLinkage ||
            admission.workerAdmission ||
            admission.messageAdmission ||
            admission.admissionFence ||
            admission.admissionGroup,
        )
      )
        return yield* fail("Fixture contains foreign admission authority");

      return {
        ...page,
        threadId,
        snapshotId,
        records: page.records.map((entry) => ({ ...entry, threadId })),
        admissions: page.admissions.map((admission) =>
          ThreadAdmission.make({ ...admission, threadId }),
        ),
      };
    }),
  );
});

export const importCanonical = Effect.fnUntraced(function* (
  storage: DurableObjectStorage,
  wire: readonly Schema.Json[],
  object: string,
) {
  const pages = yield* addressArchive(wire, object).pipe(Effect.provide(BrowserCrypto.layer));

  return yield* Effect.gen(function* () {
    const importer = yield* ThreadImport;

    return yield* importer.import(Stream.fromIterable(pages));
  }).pipe(Effect.provide(DoThreadStore.layer({ storage })));
});

/** Re-address native directory coordinates; checkpoint bytes and digests remain untouched. */
export const addressTardie = (dump: SqlDump, object: string) =>
  Effect.try({
    try: () => {
      const replace = (value: Schema.Json): Schema.Json => {
        if (Array.isArray(value)) {
          if (
            value.length === 3 &&
            value[0] === "bench-agent" &&
            value[1] === "main" &&
            value[2] === "supervisor"
          )
            return ["bench-agent", object, "supervisor"];

          return value.map(replace);
        }
        if (typeof value === "object" && value !== null) {
          if (
            "actor" in value &&
            "instance" in value &&
            "thread" in value &&
            value.actor === "bench-agent" &&
            value.instance === "main" &&
            value.thread === "bench"
          )
            return { ...value, instance: object };

          return Object.fromEntries(
            Object.entries(value).map(([key, item]) => [key, replace(item)]),
          );
        }

        return value;
      };

      const chunks = dump.tables.find((table) => table.name === "checkpoint_chunks");

      if (chunks?.rows.length) {
        const index = chunks.columns.indexOf("payload");

        const bytes = chunks.rows.flatMap((row) => {
          const value = row[index];

          if (typeof value !== "object" || value === null)
            throw fail("Malformed native checkpoint chunk");

          return Array.from(atob(value.blob), (char) => char.charCodeAt(0));
        });

        const value = Schema.decodeUnknownSync(Schema.Json)(
          JSON.parse(new TextDecoder().decode(new Uint8Array(bytes))),
        );

        if (canonicalJson(value) !== canonicalJson(replace(value)))
          throw fail(
            "Tardie checkpoint contains address authority and cannot be transferred unchanged",
          );
      }

      return {
        ...dump,
        tables: dump.tables.map((table) => ({
          ...table,
          rows: table.rows.map((row) =>
            row.map((value) => {
              if (typeof value !== "string" || (!value.startsWith("{") && !value.startsWith("[")))
                return value;
              const decoded = Schema.decodeUnknownSync(Schema.Json)(JSON.parse(value));
              const addressed = replace(decoded);

              return canonicalJson(decoded) === canonicalJson(addressed)
                ? value
                : JSON.stringify(addressed);
            }),
          ),
        })),
      };
    },
    catch: (cause) =>
      cause instanceof FixtureError
        ? cause
        : new FixtureError({ message: "Re-address Tardie fixture", cause }),
  });

export const exportCanonical = Effect.fnUntraced(function* (storage: DurableObjectStorage) {
  const pages: Schema.Json[] = [];
  let cursor: string | undefined;

  do {
    const page = yield* DoThreadStore.exportThread(
      { storage },
      ThreadExportRequest.make({
        threadId: ThreadId.make("main"),
        ...(cursor === undefined ? {} : { cursor }),
      }),
    );

    pages.push(
      yield* Schema.decodeUnknownEffect(Schema.Json)(
        yield* Schema.encodeEffect(ThreadExport)(page),
      ),
    );
    cursor = page.cursor;
  } while (cursor !== undefined);

  return pages;
});

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

/** Reconstruct the last seeded model request; the seed digest excludes its final answer. */
export const transcript = (sql: SqlStorage, target: Target): readonly Message[] => {
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

        result.push({ role: "assistant", text: model.text, ...(calls.length ? { calls } : {}) });
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
    for (const row of sql
      .exec<{ record_json: string }>(
        "SELECT record_json FROM effect_agent_canonical_records ORDER BY sequence",
      )
      .toArray()) {
      const record = Schema.decodeUnknownSync(RecordEnvelope)(JSON.parse(row.record_json));
      const p = record.payload;

      if (p._tag === "UserInputRecorded") result.push({ role: "user", text: string(p.input) });
      if (p._tag === "ToolCallSettled") result.push({ role: "tool", text: string(p.result) });
      if (p._tag !== "ModelResponseRecorded") continue;
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
    }
  }
  if (result.at(-1)?.role !== "assistant" || result.at(-1)?.calls?.length)
    throw fail("Fixture is not settled");

  return result.slice(0, -1);
};

export const verifyFixture = Effect.fnUntraced(function* (
  storage: DurableObjectStorage,
  fixture: BulkFixture,
) {
  const digest = yield* Effect.tryPromise({
    try: () => fingerprint(transcript(storage.sql, fixture.target)),
    catch: (cause) => new FixtureError({ message: "Read fixture transcript", cause }),
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
