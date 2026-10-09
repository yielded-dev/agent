import { Receipt } from "@yielded/agent/receipt";
import { Settlement } from "@yielded/agent/submission-ledger";
import { Schema } from "effect";

export const TARGETS = ["yielded", "pi", "tardie"] as const;
export const Target = Schema.Literals(TARGETS);
export type Target = typeof Target.Type;
export const History = Schema.Int.check(Schema.isGreaterThan(0));

export const Query = Schema.Struct({
  target: Target,
  object: Schema.NonEmptyString,
  history: History,
  sample: Schema.NonEmptyString,
  expectedBuild: Schema.NonEmptyString,
  ttftMs: Schema.Literals([0, 400]),
  chunkDelayMs: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1000 })),
  textStreaming: Schema.Boolean,
});

export type Query = typeof Query.Type;

export const ProfileTarget = Schema.Struct({
  actorId: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
  binding: Schema.Literals(["YIELDED", "PI"]),
  versionId: Schema.NonEmptyString,
  build: Schema.NonEmptyString,
});

export const readQuery = (url: URL): Query =>
  Schema.decodeUnknownSync(Query)({
    target: url.searchParams.get("target"),
    object: url.searchParams.get("object"),
    history: Number(url.searchParams.get("history")),
    sample: url.searchParams.get("sample") ?? "import",
    expectedBuild: url.searchParams.get("expectedBuild"),
    ttftMs: Number(url.searchParams.get("ttftMs") ?? 0),
    chunkDelayMs: Number(url.searchParams.get("chunkDelayMs") ?? 0),
    textStreaming:
      Schema.decodeUnknownSync(Schema.Literals(["true", "false"]))(
        url.searchParams.get("textStreaming") ?? "false",
      ) === "true",
  });

/** Acknowledgement follows public subscription acquisition; text excludes historical messages. */
export const TextObservation = Schema.Union([
  Schema.TaggedStruct("Ready", {}),
  Schema.TaggedStruct("Text", {
    // Yielded's Receipt submissionId, or the pi sample on its exclusively observed idle conversation.
    input: Schema.NonEmptyString,
    text: Schema.NonEmptyString,
  }),
]);

export type TextObservation = typeof TextObservation.Type;

export const expectedSeed: Readonly<Record<number, string>> = {
  50: "b017b487524e44a4",
  250: "dcea9f30b0917245",
};

export const Counts = Schema.Record(Schema.String, Schema.Natural);

export const SqlValue = Schema.Union([
  Schema.Null,
  Schema.String,
  Schema.Number,
  Schema.Struct({ blob: Schema.String }),
]);

export const SqlTable = Schema.Struct({
  name: Schema.NonEmptyString,
  sql: Schema.NonEmptyString,
  columns: Schema.Array(Schema.String),
  rows: Schema.Array(Schema.Array(SqlValue)),
});

export const SqlDump = Schema.Struct({
  tables: Schema.Array(SqlTable),
  indexes: Schema.Array(Schema.String),
});

export type SqlDump = typeof SqlDump.Type;

export const FixtureMode = Schema.Literals(["import", "replay"]);

export const BulkFixture = Schema.Struct({
  version: Schema.Literal(1),
  target: Target,
  history: History,
  fingerprint: Schema.NonEmptyString,
  sourceVersion: Schema.NonEmptyString,
  tables: Counts,
  mode: FixtureMode,
  fallbackReason: Schema.optionalKey(Schema.NonEmptyString),
  // Canonical pages stay encoded until the storage importer decodes their schema.
  archive: Schema.optionalKey(Schema.Array(Schema.Json)),
  thread: Schema.optionalKey(SqlDump),
  actor: Schema.optionalKey(SqlDump),
});

export type BulkFixture = typeof BulkFixture.Type;

export const Identity = Schema.Struct({
  build: Schema.NonEmptyString,
  incarnation: Schema.NonEmptyString,
  constructedMs: Schema.Number,
  firstEntry: Schema.Boolean,
  priorAlarms: Schema.Natural,
});

export type Identity = typeof Identity.Type;

export const ImportResult = Schema.Struct({
  ok: Schema.Literal(true),
  target: Target,
  history: History,
  fingerprint: Schema.String,
  tables: Counts,
  mode: FixtureMode,
  fallbackReason: Schema.optionalKey(Schema.NonEmptyString),
  directoryTables: Schema.optionalKey(Counts),
  identity: Identity,
});

export type ImportResult = typeof ImportResult.Type;

export const ColdResult = Schema.Struct({
  ok: Schema.Boolean,
  before: Identity,
  directoryBefore: Schema.optionalKey(Identity),
  threadAborted: Schema.Boolean,
  directoryAborted: Schema.optionalKey(Schema.Boolean),
});

export type ColdResult = typeof ColdResult.Type;

export const SubmitResult = Schema.Struct({
  ok: Schema.Literal(true),
  receipt: Schema.toEncoded(Receipt),
});

export type SubmitResult = typeof SubmitResult.Type;

export const AwaitResult = Schema.Struct({
  ok: Schema.Literal(true),
  settlement: Schema.toEncoded(Settlement),
});

export type AwaitResult = typeof AwaitResult.Type;

export const RunResult = Schema.Struct({
  ok: Schema.Literal(true),
  outcome: Schema.Literal("completed"),
  identity: Identity,
});

export type RunResult = typeof RunResult.Type;

export const ProviderReceipt = Schema.Struct({
  ...Query.fields,
  objectBuild: Schema.NonEmptyString,
  call: Schema.Natural,
  requestId: Schema.NonEmptyString,
  arrivalMs: Schema.Number,
  firstByteMs: Schema.Number,
  endMs: Schema.Number,
  fingerprint: Schema.String,
  rawWireFingerprint: Schema.String,
  requestBytes: Schema.Natural,
  colo: Schema.NullOr(Schema.String),
  error: Schema.Null,
});

export type ProviderReceipt = typeof ProviderReceipt.Type;

export const ProviderCall = Schema.Struct({
  call: Schema.Natural,
  fingerprint: Schema.String,
  startMs: Schema.Number,
  endMs: Schema.optionalKey(Schema.Number),
  status: Schema.optionalKey(Schema.Int),
  receipt: Schema.optionalKey(ProviderReceipt),
  error: Schema.optionalKey(Schema.String),
});

export type ProviderCall = typeof ProviderCall.Type;

export const Metrics = Schema.Struct({
  ok: Schema.Literal(true),
  query: Query,
  identity: Identity,
  directory: Schema.optionalKey(Identity),
  directoryUsed: Schema.optionalKey(Schema.Boolean),
  calls: Schema.Array(ProviderCall),
  tables: Counts,
  bytes: Schema.Natural,
});

export type Metrics = typeof Metrics.Type;

export const Failure = Schema.Struct({
  ok: Schema.Literal(false),
  error: Schema.String,
  sample: Schema.NullOr(Schema.String),
});

export type Failure = typeof Failure.Type;

export interface Env {
  BUILD: string;
  BENCH_TOKEN: string;
  PROVIDER_URL: string;
  CPU?: string | boolean;
  VERSION?: { id: string };
}

export const errorText = (cause: unknown): string =>
  cause instanceof Error ? cause.message : String(cause);

const Chat = Schema.Struct({
  messages: Schema.Array(
    Schema.Struct({
      role: Schema.Literals(["system", "developer", "user", "assistant", "tool"]),
      content: Schema.optionalKey(
        Schema.Union([
          Schema.String,
          Schema.Null,
          Schema.Array(
            Schema.Struct({ type: Schema.String, text: Schema.optionalKey(Schema.String) }),
          ),
        ]),
      ),
      tool_calls: Schema.optionalKey(
        Schema.Array(
          Schema.Struct({
            function: Schema.Struct({ arguments: Schema.String }),
          }),
        ),
      ),
    }),
  ),
});

export const decodeChat = Schema.decodeUnknownSync(Chat);
const argument = Schema.decodeUnknownSync(Schema.Struct({ n: Schema.Int }));

export const chatTranscript = (chat: typeof Chat.Type) =>
  chat.messages.flatMap((message) => {
    if (message.role === "system" || message.role === "developer") return [];

    let text =
      typeof message.content === "string"
        ? message.content
        : (message.content ?? []).map((part) => part.text ?? "").join("");

    if (message.role === "tool" && text.startsWith('"'))
      text = Schema.decodeUnknownSync(Schema.String)(JSON.parse(text));

    const calls = message.tool_calls?.map(
      (call) => argument(JSON.parse(call.function.arguments)).n,
    );

    return [{ role: message.role, text, ...(calls?.length ? { calls } : {}) }];
  });
