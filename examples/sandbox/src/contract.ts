import { Schema } from "effect";

// Covers JSON escaping of an 8,000-character prompt plus its identifiers.
export const MAX_INPUT_BYTES = 64 * 1024;

export const WorkspaceId = Schema.String.check(Schema.isUUID());

export const CodingInput = Schema.Struct({
  requestId: Schema.String.check(Schema.isUUID()),
  message: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(8_000)),
});

export const MessageInput = Schema.Struct({ workspaceId: WorkspaceId, ...CodingInput.fields });

export const Accepted = Schema.Struct({ submissionId: Schema.String });

export const Entry = Schema.Struct({
  id: Schema.String,
  kind: Schema.Literals(["user", "assistant", "tool", "result", "status"]),
  text: Schema.String,
  name: Schema.optionalKey(Schema.String),
  failed: Schema.optionalKey(Schema.Boolean),
  reviewRequired: Schema.optionalKey(Schema.Boolean),
  submissionId: Schema.optionalKey(Schema.String),
  requestId: Schema.optionalKey(Schema.String),
});

export type Entry = typeof Entry.Type;

export const EventPage = Schema.Struct({
  cursor: Schema.Int,
  entries: Schema.Array(Entry),
});

export type EventPage = typeof EventPage.Type;

export const FileList = Schema.Array(
  Schema.Struct({ path: Schema.String, directory: Schema.Boolean }),
);

export const FileContent = Schema.Struct({
  path: Schema.String,
  content: Schema.String,
  truncated: Schema.Boolean,
});

export class SandboxError extends Schema.TaggedError<SandboxError>()("SandboxError", {
  message: Schema.String,
}) {}
