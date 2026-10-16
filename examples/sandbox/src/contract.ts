import { Schema } from "effect";

// Covers JSON escaping of an 8,000-character prompt plus its identifiers.
export const MAX_INPUT_BYTES = 64 * 1024;
export const LIVE_TEXT_THREAD_HEADER = "x-yielded-thread-id";

export const WorkspaceId = Schema.String.check(Schema.isUUID());

export const CodingInput = Schema.Struct({
  requestId: Schema.String.check(Schema.isUUID()),
  message: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(8_000)),
});

export const MessageInput = Schema.Struct({ workspaceId: WorkspaceId, ...CodingInput.fields });

export const Accepted = Schema.Struct({ submissionId: Schema.String });

export const CommandInput = Schema.Struct({
  requestId: WorkspaceId,
  script: Schema.NonEmptyString.check(
    Schema.isMaxLength(8_000),
    Schema.makeFilter((value: string) => !value.includes("\0"), {
      expected: "a command without NUL characters",
    }),
  ),
});

export const CommandOutput = Schema.Struct({
  stdout: Schema.String.check(Schema.isMaxLength(16 * 1024)),
  stderr: Schema.String.check(Schema.isMaxLength(16 * 1024)),
  exitCode: Schema.Int,
  timedOut: Schema.Boolean,
  truncated: Schema.Boolean,
  nextOffset: Schema.NullOr(Schema.Natural),
});

export const ShellCommand = Schema.Struct({
  ...CommandInput.fields,
  afterSequence: Schema.Natural,
  createdAt: Schema.Finite,
  status: Schema.Literals(["queued", "running", "completed", "failed", "unknown"]),
  failure: Schema.optionalKey(Schema.Literals(["workspace-expired", "not-started"])),
  output: Schema.optionalKey(CommandOutput),
});

export type ShellCommand = typeof ShellCommand.Type;

export class CommandRejected extends Schema.TaggedError<CommandRejected>()("CommandRejected", {
  message: Schema.String,
  status: Schema.Literals([400, 409, 429]),
}) {}

export const CommandAccepted = Schema.Union([
  Schema.Struct({ command: ShellCommand }),
  Schema.Struct({ error: CommandRejected }),
]);

export const Entry = Schema.Struct({
  id: Schema.String,
  kind: Schema.Literals(["user", "assistant", "reasoning", "tool", "result", "status"]),
  text: Schema.String,
  name: Schema.optionalKey(Schema.String),
  failed: Schema.optionalKey(Schema.Boolean),
  reviewRequired: Schema.optionalKey(Schema.Boolean),
  submissionId: Schema.optionalKey(Schema.String),
  requestId: Schema.optionalKey(Schema.String),
  runId: Schema.optionalKey(Schema.String),
  turnId: Schema.optionalKey(Schema.String),
  toolCallId: Schema.optionalKey(Schema.String),
  toolResultId: Schema.optionalKey(Schema.String),
  sequence: Schema.optionalKey(Schema.Natural),
});

export type Entry = typeof Entry.Type;

export interface Draft {
  readonly id: string;
  readonly kind: "assistant" | "reasoning";
  readonly text: string;
  readonly submissionId: string;
  readonly attemptId: string;
  readonly runId: string;
  readonly turnId: string;
  readonly generation: number;
}

export const EventPage = Schema.Struct({
  cursor: Schema.Int,
  entries: Schema.Array(Entry),
  // Empty model responses still replace provisional output for their Turn.
  committedTurns: Schema.optionalKey(
    Schema.Array(Schema.Struct({ runId: Schema.String, turnId: Schema.String })),
  ),
});

export type EventPage = typeof EventPage.Type;

export const WorkspaceEvent = Schema.Union([
  EventPage,
  Schema.TaggedStruct("CommandHistoryChanged", {}),
]);

export const FileList = Schema.Struct({
  entries: Schema.Array(Schema.Struct({ path: Schema.String, directory: Schema.Boolean })),
  expired: Schema.Boolean,
});

export const FileContent = Schema.Struct({
  path: Schema.String,
  content: Schema.String,
  truncated: Schema.Boolean,
});

export class SandboxError extends Schema.TaggedError<SandboxError>()("SandboxError", {
  message: Schema.String,
}) {}
