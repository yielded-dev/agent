import { Schema } from "effect";

import { type Message } from "../../../src/plan.ts";

export const TARGETS = ["yielded"] as const;

export const Query = Schema.Struct({
  target: Schema.Literals(TARGETS),
  variant: Schema.Literals(["baseline", "candidate"]),
  history: Schema.Literals([50, 250]),
  object: Schema.NonEmptyString,
  sample: Schema.NonEmptyString,
  ttftMs: Schema.Literals([0, 400]),
  chunkDelayMs: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1000 })),
});

export type Query = typeof Query.Type;

// A legal SSE comment carries measurement evidence beside the native provider
// stream. Cloudflare can sample invocation logs at ingestion despite head=1.
export const ProviderReceipt = Schema.Struct({
  ...Query.fields,
  call: Schema.Int,
  requestId: Schema.NonEmptyString,
  arrivalMs: Schema.Number,
  firstByteMs: Schema.Number,
  endMs: Schema.Number,
  fingerprint: Schema.String,
  rawWireFingerprint: Schema.String,
  requestBytes: Schema.Int,
  colo: Schema.NullOr(Schema.String),
  error: Schema.Null,
});
export type ProviderReceipt = typeof ProviderReceipt.Type;
export const decodeProviderReceipt = Schema.decodeUnknownSync(ProviderReceipt);

export const Seed = Schema.Struct({ from: Schema.Int, to: Schema.Int });
export const decodeSeed = Schema.decodeUnknownSync(Seed);

export const readQuery = (url: URL): Query =>
  Schema.decodeUnknownSync(Query)({
    target: url.searchParams.get("target"),
    variant: url.searchParams.get("variant") ?? "baseline",
    history: Number(url.searchParams.get("history")),
    object: url.searchParams.get("object"),
    sample: url.searchParams.get("sample"),
    ttftMs: Number(url.searchParams.get("ttftMs") ?? 0),
    chunkDelayMs: Number(url.searchParams.get("chunkDelayMs") ?? 0),
  });

export const SeedState = Schema.Struct({
  history: Schema.Int,
  through: Schema.Int,
  fingerprint: Schema.String,
});

export const decodeSeedState = Schema.decodeUnknownSync(SeedState);
export const expectedSeed = { 50: "b017b487524e44a4", 250: "dcea9f30b0917245" } as const;

export interface Env {
  TOKEN: string;
  PROVIDER_URL: string;
  PHASE: "seed" | "measure";
  BUILD_MODE: "baseline" | "ab";
  BUILD_ID: string;
  VERSION: { id: string; tag?: string; timestamp?: string };
}

export const errorText = (cause: unknown): string =>
  cause instanceof Error ? (cause.stack ?? cause.message) : String(cause);

const Content = Schema.Union([
  Schema.String,
  Schema.Null,
  Schema.Array(Schema.Struct({ type: Schema.String, text: Schema.optionalKey(Schema.String) })),
]);

const Chat = Schema.Struct({
  model: Schema.String,
  stream: Schema.Boolean,
  messages: Schema.Array(
    Schema.Struct({
      role: Schema.Literals(["system", "developer", "user", "assistant", "tool"]),
      content: Schema.optionalKey(Content),
      tool_calls: Schema.optionalKey(
        Schema.Array(
          Schema.Struct({
            id: Schema.String,
            type: Schema.Literal("function"),
            function: Schema.Struct({ name: Schema.Literal("lookup"), arguments: Schema.String }),
          }),
        ),
      ),
    }),
  ),
});

const argument = Schema.decodeUnknownSync(Schema.Struct({ n: Schema.Int }));

export const decodeChat = Schema.decodeUnknownSync(Chat);

export const chatTranscript = (chat: typeof Chat.Type): Message[] =>
  chat.messages.flatMap((message): Message[] => {
    if (message.role === "system" || message.role === "developer") return [];

    let text =
      typeof message.content === "string"
        ? message.content
        : (message.content ?? []).map((part) => part.text ?? "").join("");

    // Effect's native adapter JSON-encodes Schema.String tool results.
    if (message.role === "tool" && text.startsWith('"'))
      text = Schema.decodeUnknownSync(Schema.String)(JSON.parse(text));

    const calls = message.tool_calls?.map(
      (call) => argument(JSON.parse(call.function.arguments)).n,
    );

    return [{ role: message.role, text, ...(calls?.length ? { calls } : {}) }];
  });

export const decodeIdentity = Schema.decodeUnknownSync(
  Schema.Struct({
    objectId: Schema.String,
    incarnation: Schema.String,
    constructedMs: Schema.Number,
    version: Schema.String,
    generation: Schema.Literals(["seed", "measure"]),
  }),
);
