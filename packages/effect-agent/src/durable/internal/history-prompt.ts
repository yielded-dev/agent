import { Effect, Schema, SchemaParser, SchemaTransformation } from "effect";
import { Prompt } from "effect/ai";

function restorePart(part: Prompt.UserMessagePartEncoded): Prompt.UserMessagePart;
function restorePart(part: Prompt.AssistantMessagePartEncoded): Prompt.AssistantMessagePart;
function restorePart(part: Prompt.ToolMessagePartEncoded): Prompt.ToolMessagePart;
function restorePart(part: Prompt.PartEncoded): Prompt.Part {
  switch (part.type) {
    case "text":
      return Prompt.textPart(part);
    case "reasoning":
      return Prompt.reasoningPart(part);
    case "file":
      return Prompt.filePart(part);
    case "tool-call":
      return Prompt.toolCallPart({ ...part, providerExecuted: part.providerExecuted ?? false });
    case "tool-result":
      return Prompt.toolResultPart({ ...part, providerExecuted: part.providerExecuted ?? false });
    case "tool-approval-request":
      return Prompt.toolApprovalRequestPart(part);
    case "tool-approval-response":
      return Prompt.toolApprovalResponsePart(part);
  }
}

const restoreMessage = (message: Prompt.MessageEncoded): Prompt.Message => {
  switch (message.role) {
    case "system":
      return Prompt.makeMessage("system", message);
    case "user":
      return Prompt.makeMessage("user", {
        ...message,
        content:
          typeof message.content === "string"
            ? [Prompt.textPart({ text: message.content })]
            : message.content.map((part) => restorePart(part)),
      });
    case "assistant":
      return Prompt.makeMessage("assistant", {
        ...message,
        content:
          typeof message.content === "string"
            ? [Prompt.textPart({ text: message.content })]
            : message.content.map((part) => restorePart(part)),
      });
    case "tool":
      return Prompt.makeMessage("tool", {
        ...message,
        content: message.content.map((part) => restorePart(part)),
      });
  }
};

/**
 * Validate the upstream encoded history once, then restore native values with its constructors.
 * The canonical codec remains authoritative at append, import, recovery and explicit verification.
 */
export const HistoryPrompt = Schema.toEncoded(Prompt.Prompt).pipe(
  Schema.decodeTo(
    Schema.toType(Prompt.Prompt),
    SchemaTransformation.transformEffect({
      decode: (input) => Effect.succeed(Prompt.fromMessages(input.content.map(restoreMessage))),
      encode: SchemaParser.encodeEffect(Prompt.Prompt),
    }),
  ),
);
