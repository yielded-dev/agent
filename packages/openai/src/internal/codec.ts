import { OpenAiSchema } from "@effect/ai-openai";
import {
  MAX_NATIVE_COMPACTION_BYTES,
  NativeCompaction,
  NativeCompactionContext,
} from "@yielded/agent/context-compactor";
import { Effect, Option, Predicate, Schema } from "effect";
import { AiError, type Response } from "effect/ai";

export const provider = "openai";
export const format = "openai.responses.compaction@1";
export const MAX_ITEMS = 1_024;
export const MAX_BODY_BYTES = 1024 * 1024;

export const TokenCount = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);

export const Identity = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const CacheBreakpoint = Schema.Struct({ mode: Schema.Literal("explicit") });
const Status = OpenAiSchema.MessageStatus;

const TextContent = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("input_text"),
    text: Schema.String,
    prompt_cache_breakpoint: Schema.optionalKey(CacheBreakpoint),
  }),
  Schema.Struct({
    type: Schema.Literal("output_text"),
    text: Schema.String,
    annotations: Schema.Array(OpenAiSchema.Annotation),
    logprobs: Schema.optionalKey(Schema.Array(Schema.Json)),
  }),
  Schema.Struct({ type: Schema.Literal("text"), text: Schema.String }),
  Schema.Struct({ type: Schema.Literal("summary_text"), text: Schema.String }),
  Schema.Struct({ type: Schema.Literal("reasoning_text"), text: Schema.String }),
  Schema.Struct({ type: Schema.Literal("refusal"), refusal: Schema.String }),
]);

export const Message = Schema.Struct({
  type: Schema.optionalKey(Schema.Literal("message")),
  id: Schema.optionalKey(Schema.NonEmptyString),
  role: Schema.Literals(["user", "assistant"]),
  content: Schema.Union([Schema.String, Schema.Array(TextContent)]),
  status: Schema.optionalKey(Status),
  phase: Schema.optionalKey(Schema.NullOr(Schema.Literals(["commentary", "final_answer"]))),
});

export const FunctionCall = Schema.Struct({
  type: Schema.Literal("function_call"),
  id: Schema.optionalKey(Schema.NonEmptyString),
  call_id: Schema.NonEmptyString,
  name: Schema.NonEmptyString,
  arguments: Schema.String,
  status: Schema.optionalKey(Status),
});

export const FunctionResult = Schema.Struct({
  type: Schema.Literal("function_call_output"),
  id: Schema.optionalKey(Schema.NullOr(Schema.NonEmptyString)),
  call_id: Schema.NonEmptyString,
  output: Schema.String,
  status: Schema.optionalKey(Schema.NullOr(Status)),
});

export const WireItem = Schema.Union([
  Message,
  OpenAiSchema.ReasoningItem,
  FunctionCall,
  FunctionResult,
  Schema.Struct({
    type: Schema.Literal("compaction"),
    id: Schema.optionalKey(Schema.NonEmptyString),
    encrypted_content: Schema.NonEmptyString,
  }),
]);

export type WireItem = typeof WireItem.Type;
export const WireItems = Schema.Array(WireItem).check(Schema.isMaxLength(MAX_ITEMS));

const NativeOutput = WireItems.check(
  Schema.makeFilter(
    (items) => completeItems(items) && items.some((item) => item.type === "compaction"),
    { title: "Complete native window with compaction state" },
  ),
);

export const WindowData = Schema.Struct({ output: NativeOutput, inputTokens: TokenCount });
export type WindowData = typeof WindowData.Type;

const Usage = Schema.Struct({
  input_tokens: TokenCount,
  output_tokens: TokenCount,
  total_tokens: TokenCount,
  input_tokens_details: Schema.optionalKey(
    Schema.Struct({
      cached_tokens: Schema.optionalKey(TokenCount),
      cache_write_tokens: Schema.optionalKey(TokenCount),
    }),
  ),
  output_tokens_details: Schema.optionalKey(
    Schema.Struct({ reasoning_tokens: Schema.optionalKey(TokenCount) }),
  ),
}).check(
  Schema.makeFilter(
    (usage) =>
      usage.total_tokens === usage.input_tokens + usage.output_tokens &&
      (usage.input_tokens_details?.cached_tokens ?? 0) +
        (usage.input_tokens_details?.cache_write_tokens ?? 0) <=
        usage.input_tokens &&
      (usage.output_tokens_details?.reasoning_tokens ?? 0) <= usage.output_tokens,
    { title: "Consistent native compaction token accounting" },
  ),
);

const UsageRecord = Schema.Struct({ usage: Schema.Record(Schema.String, Schema.Unknown) });
const decodeUsageRecord = Schema.decodeUnknownOption(UsageRecord);
const decodeToken = Schema.decodeUnknownOption(TokenCount);

export const decodeUsage = Schema.decodeUnknownEffect(Schema.Struct({ usage: Usage }));

export const decodeReply = Schema.decodeUnknownEffect(
  Schema.Struct({
    id: Schema.NonEmptyString,
    object: Schema.Literal("response.compaction"),
    created_at: TokenCount,
    output: Schema.Unknown,
    usage: Schema.Unknown,
  }),
  { onExcessProperty: "error" },
);

const decodeData = Schema.decodeUnknownEffect(WindowData, { onExcessProperty: "error" });
const decodeNativeOutput = Schema.decodeUnknownEffect(NativeOutput, { onExcessProperty: "error" });

export const decodeItems = Schema.decodeUnknownEffect(WireItems, { onExcessProperty: "error" });
const decodeContext = Schema.decodeUnknownEffect(NativeCompactionContext);
const encodeWindow = Schema.encodeEffect(NativeCompaction);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Json));
const textEncoder = new TextEncoder();

export function byteLength(text: string): number {
  return textEncoder.encode(text).byteLength;
}

export function jsonText(value: Schema.Json): string {
  return encodeJson(value);
}

export function invalidInput(method: string, description: string): AiError.AiError {
  return AiError.make({
    module: "OpenAiCompaction",
    method,
    reason: AiError.InvalidRequestError.make({ description }),
  });
}

export function invalidOutput(usage: typeof AiError.UsageInfo.Type | undefined): AiError.AiError {
  return AiError.make({
    module: "OpenAiCompaction",
    method: "compact",
    reason: AiError.InvalidOutputError.make({
      description:
        "Native compaction context, accounting, or token counting could not be validated",
      usage,
    }),
  });
}

export function errorUsage(raw: unknown): typeof AiError.UsageInfo.Type | undefined {
  const record = decodeUsageRecord(raw);

  if (Option.isNone(record)) return undefined;
  const usage = record.value.usage;

  return {
    promptTokens: Option.getOrUndefined(decodeToken(usage.input_tokens)),
    completionTokens: Option.getOrUndefined(decodeToken(usage.output_tokens)),
    totalTokens: Option.getOrUndefined(decodeToken(usage.total_tokens)),
  };
}

export function responseUsage(usage: typeof Usage.Type): Response.Usage {
  const cacheRead = usage.input_tokens_details?.cached_tokens;
  const cacheWrite = usage.input_tokens_details?.cache_write_tokens;
  const reasoning = usage.output_tokens_details?.reasoning_tokens;

  return {
    inputTokens: {
      total: usage.input_tokens,
      uncached:
        cacheRead === undefined ? undefined : usage.input_tokens - cacheRead - (cacheWrite ?? 0),
      cacheRead,
      cacheWrite,
    },
    outputTokens: {
      total: usage.output_tokens,
      text: reasoning === undefined ? undefined : usage.output_tokens - reasoning,
      reasoning,
    },
  };
}

export const boundedJson = Effect.fnUntraced(function* (
  value: unknown,
): Effect.fn.Return<Schema.Json, AiError.AiError> {
  const context = yield* decodeContext({ format, data: value, estimatedTokens: 0 }).pipe(
    Effect.mapError(() => invalidInput("encode", "Native context exceeds canonical JSON limits")),
  );

  return context.data;
});

export function completeItems(items: ReadonlyArray<WireItem>): boolean {
  const pending = new Set<string>();
  const callIds = new Set<string>();
  const itemIds = new Set<string>();

  for (const item of items) {
    if (Predicate.isNotNullish(item.id)) {
      if (itemIds.has(item.id)) return false;
      itemIds.add(item.id);
    }
    if (item.type === "function_call") {
      if (callIds.has(item.call_id)) return false;
      callIds.add(item.call_id);
      pending.add(item.call_id);
    } else if (item.type === "function_call_output") {
      if (!pending.delete(item.call_id)) return false;
    }
  }

  return pending.size === 0;
}

export const readOutput = Effect.fnUntraced(function* (
  value: unknown,
): Effect.fn.Return<ReadonlyArray<WireItem>, AiError.AiError> {
  const json = yield* boundedJson(value);

  const output = yield* decodeNativeOutput(json).pipe(
    Effect.mapError(() => invalidInput("validate", "Unsupported native context output")),
  );

  if (byteLength(jsonText(output)) > MAX_NATIVE_COMPACTION_BYTES) {
    return yield* invalidInput("validate", "Native context exceeds its encoded byte bound");
  }

  return output;
});

export const makeContext = Effect.fnUntraced(function* (
  output: ReadonlyArray<WireItem>,
  inputTokens: number,
): Effect.fn.Return<NativeCompactionContext, AiError.AiError> {
  const json = yield* boundedJson({ output, inputTokens });

  const data = yield* decodeData(json).pipe(
    Effect.mapError(() => invalidInput("validate", "Unsupported native context format")),
  );

  const context = { format, data, estimatedTokens: data.inputTokens };

  if (byteLength(jsonText(context)) > MAX_NATIVE_COMPACTION_BYTES) {
    return yield* invalidInput("validate", "Native context exceeds its encoded byte bound");
  }

  return context;
});

export const readWindow = Effect.fnUntraced(function* (
  window: NativeCompaction,
): Effect.fn.Return<WindowData, AiError.AiError> {
  yield* encodeWindow(window).pipe(
    Effect.mapError(() => invalidInput("validate", "Invalid native compaction envelope")),
  );
  if (window.affinity.provider !== provider || window.context.format !== format) {
    return yield* invalidInput("validate", "Native context provider or format mismatch");
  }

  const data = yield* decodeData(window.context.data).pipe(
    Effect.mapError(() => invalidInput("validate", "Unsupported native context format")),
  );

  if (data.inputTokens !== window.context.estimatedTokens) {
    return yield* invalidInput("validate", "Native context input-token count mismatch");
  }

  return data;
});
