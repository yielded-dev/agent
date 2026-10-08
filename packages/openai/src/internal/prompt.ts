import { OpenAiSchema, OpenAiTool } from "@effect/ai-openai";
import { Effect, Predicate, Schema } from "effect";
import type { AiError, Prompt } from "effect/ai";

import {
  boundedJson,
  byteLength,
  completeItems,
  decodeItems,
  invalidInput,
  jsonText,
  MAX_BODY_BYTES,
  MAX_ITEMS,
  type WireItem,
} from "./codec.ts";

const localHandlerToolNames = new Set<string>([
  OpenAiTool.ApplyPatch({}).name,
  OpenAiTool.LocalShell({}).name,
  OpenAiTool.Shell({}).name,
]);

type Message = Extract<WireItem, { readonly role: string }>;
type TextContent = Exclude<Message["content"], string>[number];
const ItemId = Schema.optionalKey(Schema.NullOr(Schema.NonEmptyString));
const Status = Schema.optionalKey(Schema.NullOr(OpenAiSchema.MessageStatus));

const TextOptions = Schema.Struct({
  openai: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({
        itemId: ItemId,
        status: Status,
        annotations: Schema.optionalKey(Schema.NullOr(Schema.Array(OpenAiSchema.Annotation))),
        promptCacheBreakpoint: Schema.optionalKey(
          Schema.NullOr(Schema.Struct({ mode: Schema.Literal("explicit") })),
        ),
      }),
    ),
  ),
});

const ReasoningOptions = Schema.Struct({
  openai: Schema.Struct({
    itemId: Schema.NonEmptyString,
    encryptedContent: Schema.optionalKey(Schema.NullOr(Schema.String)),
  }),
});

const ToolOptions = Schema.Struct({
  openai: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({
        itemId: ItemId,
        status: Status,
        approvalId: Schema.optionalKey(Schema.NullOr(Schema.String)),
        approvalRequestId: Schema.optionalKey(Schema.NullOr(Schema.String)),
      }),
    ),
  ),
});

const decodeMessageOptions = Schema.decodeUnknownEffect(Schema.Struct({}), {
  onExcessProperty: "error",
});

const decodeTextOptions = Schema.decodeUnknownEffect(TextOptions, { onExcessProperty: "error" });

const decodeReasoningOptions = Schema.decodeUnknownEffect(ReasoningOptions, {
  onExcessProperty: "error",
});

const decodeToolOptions = Schema.decodeUnknownEffect(ToolOptions, { onExcessProperty: "error" });

function unsupported(): AiError.AiError {
  return invalidInput(
    "compact",
    "The eligible Prompt contains unsupported content or provider metadata",
  );
}

/** Only the new ordinary prefix is encoded; retained native items never pass through Prompt. */
export const encodePrompt = Effect.fnUntraced(function* (
  prompt: Prompt.Prompt,
): Effect.fn.Return<ReadonlyArray<WireItem>, AiError.AiError> {
  if (prompt.content.length > MAX_ITEMS) return yield* unsupported();
  const items: Array<WireItem> = [];
  const calls = new Map<string, string>();
  const reasoningIds = new Set<string>();
  let textUnits = 0;

  for (const message of prompt.content) {
    if (message.role === "system") {
      return yield* invalidInput(
        "compact",
        "System instructions cannot be covered by native compaction",
      );
    }
    yield* decodeMessageOptions(message.options).pipe(Effect.mapError(unsupported));
    if (message.content.length > MAX_ITEMS) return yield* unsupported();
    const userContent: Array<TextContent> = [];

    for (const part of message.content) {
      if (items.length >= MAX_ITEMS) return yield* unsupported();
      if (part.type === "text" || part.type === "reasoning") {
        textUnits += part.text.length;
        if (textUnits > MAX_BODY_BYTES) return yield* unsupported();
      }
      if (
        (part.type === "tool-call" || part.type === "tool-result") &&
        localHandlerToolNames.has(part.name)
      ) {
        return yield* invalidInput(
          "compact",
          "Provider-defined local-handler tools cannot be covered by native compaction",
        );
      }
      const optionsJson = yield* boundedJson(part.options);

      switch (part.type) {
        case "text": {
          if (message.role === "tool") return yield* unsupported();

          const options = (yield* decodeTextOptions(optionsJson).pipe(Effect.mapError(unsupported)))
            .openai;

          if (message.role === "user") {
            if (
              Predicate.isNotNullish(options?.itemId) ||
              Predicate.isNotNullish(options?.status) ||
              Predicate.isNotNullish(options?.annotations)
            ) {
              return yield* unsupported();
            }
            userContent.push({
              type: "input_text",
              text: part.text,
              ...(Predicate.isNullish(options?.promptCacheBreakpoint)
                ? {}
                : { prompt_cache_breakpoint: options.promptCacheBreakpoint }),
            });
          } else {
            if (Predicate.isNotNullish(options?.promptCacheBreakpoint)) return yield* unsupported();
            items.push({
              type: "message",
              role: "assistant",
              ...(Predicate.isNullish(options?.itemId) ? {} : { id: options.itemId }),
              status: options?.status ?? "completed",
              content: [
                { type: "output_text", text: part.text, annotations: options?.annotations ?? [] },
              ],
            });
          }
          break;
        }
        case "reasoning": {
          if (message.role !== "assistant") return yield* unsupported();

          const { openai } = yield* decodeReasoningOptions(optionsJson).pipe(
            Effect.mapError(unsupported),
          );

          if (Predicate.isNotNullish(openai.encryptedContent)) {
            return yield* invalidInput(
              "compact",
              "Ordinary encrypted reasoning is unsupported by native compaction",
            );
          }
          const summary: Array<OpenAiSchema.SummaryTextContent> = [];

          if (part.text.length > 0) summary.push({ type: "summary_text", text: part.text });
          const last = items.at(-1);

          if (last?.type === "reasoning" && last.id === openai.itemId) {
            items[items.length - 1] = { ...last, summary: [...last.summary, ...summary] };
          } else {
            if (reasoningIds.has(openai.itemId)) return yield* unsupported();
            reasoningIds.add(openai.itemId);
            items.push({ type: "reasoning", id: openai.itemId, summary });
          }
          break;
        }
        case "tool-call": {
          if (message.role !== "assistant" || part.providerExecuted || calls.has(part.id)) {
            return yield* unsupported();
          }

          const options = (yield* decodeToolOptions(optionsJson).pipe(Effect.mapError(unsupported)))
            .openai;

          if (
            Predicate.isNotNullish(options?.approvalId) ||
            Predicate.isNotNullish(options?.approvalRequestId)
          )
            return yield* unsupported();
          const params = yield* boundedJson(part.params);

          calls.set(part.id, part.name);
          items.push({
            type: "function_call",
            ...(Predicate.isNullish(options?.itemId) ? {} : { id: options.itemId }),
            ...(Predicate.isNullish(options?.status) ? {} : { status: options.status }),
            call_id: part.id,
            name: part.name,
            arguments: jsonText(params),
          });
          break;
        }
        case "tool-result": {
          if (
            message.role !== "tool" ||
            part.providerExecuted ||
            calls.get(part.id) !== part.name
          ) {
            return yield* invalidInput(
              "compact",
              "Ordinary tool results require matching calls and tool messages",
            );
          }

          const options = (yield* decodeToolOptions(optionsJson).pipe(Effect.mapError(unsupported)))
            .openai;

          if (
            Predicate.isNotNullish(options?.approvalId) ||
            Predicate.isNotNullish(options?.approvalRequestId)
          )
            return yield* unsupported();
          const result = yield* boundedJson(part.result);

          calls.delete(part.id);
          items.push({
            type: "function_call_output",
            ...(options?.itemId === undefined ? {} : { id: options.itemId }),
            ...(options?.status === undefined ? {} : { status: options.status }),
            call_id: part.id,
            output: typeof result === "string" ? result : jsonText(result),
          });
          break;
        }
        case "file":
        case "tool-approval-request":
        case "tool-approval-response":
        default:
          return yield* unsupported();
      }
    }
    if (message.role === "user") items.push({ role: "user", content: userContent });
  }
  if (calls.size > 0 || !completeItems(items)) {
    return yield* invalidInput(
      "compact",
      "Native compaction requires complete ordinary tool history",
    );
  }
  const json = yield* boundedJson(items);

  if (byteLength(jsonText(json)) > MAX_BODY_BYTES) return yield* unsupported();

  return yield* decodeItems(json).pipe(Effect.mapError(unsupported));
});
