import { ContextWindow } from "@yielded/agent/context-window";
import { DurableStep, ToolExecutionClass } from "@yielded/agent/durable-step";
import { ThreadWorkDiscovery, type ThreadWorkRequest } from "@yielded/agent/thread-work";
import type { SandboxCommand } from "@yielded/sandbox/cloudflare";
import { Clock, Effect, Schema } from "effect";
import { Tool, Toolkit } from "effect/ai";

import { Workspace } from "./workspace.ts";

export const CODING_ROOT = "/workspace";
const OUTPUT_BYTES = 16 * 1024;
const ARGUMENT_BYTES = 64 * 1024;
const Path = Schema.NonEmptyString.check(Schema.isMaxLength(4096));
const Text = Schema.String.check(Schema.isMaxLength(ARGUMENT_BYTES));
const OutputText = Schema.String.check(Schema.isMaxLength(OUTPUT_BYTES));

/** Infrastructure uncertainty fails the Run; it must not invite a new mutation from the model. */
export class CodingToolError extends Schema.TaggedError<CodingToolError>()("CodingToolError", {
  reason: Schema.Literals(["input", "workspace", "execution", "persistence", "protocol"]),
  message: Schema.String.check(Schema.isMaxLength(512)),
  outcome: Schema.Literals(["not-started", "unknown", "completed"]),
}) {
  // Live diagnostics only: not part of the public/persisted Schema.
  declare readonly cause?: unknown;
}

const failure = (
  reason: CodingToolError["reason"],
  message: string,
  outcome: CodingToolError["outcome"],
  cause?: unknown,
) => {
  const error = CodingToolError.make({ reason, message, outcome });

  if (cause !== undefined) Object.defineProperty(error, "cause", { value: cause });

  return error;
};

/** Output is a combined UTF-8 byte budget, independently of the native capture budget. */
export const CodingToolResult = Schema.Struct({
  stdout: OutputText,
  stderr: OutputText,
  exitCode: Schema.Int,
  timedOut: Schema.Boolean,
  truncated: Schema.Boolean,
  nextOffset: Schema.NullOr(Schema.Natural),
});

type CodingToolResult = typeof CodingToolResult.Type;

const Allocation = Schema.Struct({
  processId: Schema.NonEmptyString,
  instanceId: Schema.NonEmptyString,
  deadlineAt: Schema.Finite,
  workspaceIdentity: Schema.Struct({ path: Path, id: Schema.NonEmptyString }),
});

const FileReply = Schema.Struct({
  stdout: OutputText,
  stderr: OutputText,
  exitCode: Schema.Int,
  truncated: Schema.Boolean,
  nextOffset: Schema.NullOr(Schema.Natural),
});

const bounded = (text: string, bytes: number) => {
  const encoded = new TextEncoder().encode(text);

  return encoded.length <= bytes
    ? text
    : new TextDecoder().decode(encoded.subarray(0, bytes), { stream: true });
};

const boundedResult = (result: CodingToolResult): CodingToolResult => {
  const stderr = bounded(result.stderr, OUTPUT_BYTES / 2);
  const stdout = bounded(result.stdout, OUTPUT_BYTES - new TextEncoder().encode(stderr).length);

  return {
    ...result,
    stdout,
    stderr,
    truncated: result.truncated || stdout !== result.stdout || stderr !== result.stderr,
  };
};

/**
 * The host provides an exact-bound Workspace for the authorized materialization
 * and serializes tools. Steps use the native ticket, not re-execution, for recovery.
 * expectedInstanceId/recovery are EnsureOptions; ExecOptions instead fences with the issued ticket.
 */
const run = Effect.fn("CodingTools.run")(
  function* (command: SandboxCommand, structured: boolean, timeoutMs = 30_000) {
    const { threadId, runId } = yield* (yield* ContextWindow).status;
    const work = yield* ThreadWorkDiscovery;
    let cursor: ThreadWorkRequest["cursor"];

    // Failed Runs can retain operations without ToolCallUnknown; only their owner may recover them.
    do {
      const page = yield* work
        .page({ threadId, limit: 64, ...(cursor === undefined ? {} : { cursor }) })
        .pipe(
          Effect.mapError((cause) =>
            failure(
              "workspace",
              "Workspace operation history could not be verified; no new command was started.",
              "not-started",
              cause,
            ),
          ),
        );

      if (page.entries.some(({ owner }) => owner._tag === "Operation" && owner.runId !== runId))
        return yield* failure(
          "workspace",
          "A previous Run has an unresolved workspace operation. Recover its original receipt or start a new workspace before running more commands.",
          "not-started",
        );
      cursor = page.cursor;
    } while (cursor !== undefined);

    const workspace = yield* Workspace;
    const sandbox = yield* workspace.open;
    const steps = yield* DurableStep;

    const allocation = yield* steps.do(
      "allocate",
      Allocation,
      Effect.gen(function* () {
        const workspaceIdentity = workspace.identity;

        const { instanceId } = yield* sandbox.ensure();
        const processId = yield* sandbox.allocateProcess();

        if (!processId.startsWith(`process:${instanceId}:`))
          return yield* failure(
            "workspace",
            "Workspace changed while allocating a process; no command was dispatched.",
            "not-started",
          );

        return {
          processId,
          instanceId,
          workspaceIdentity,
          deadlineAt: (yield* Clock.currentTimeMillis) + timeoutMs,
        };
      }),
    );

    const result = yield* steps.do(
      "terminal",
      CodingToolResult,
      Effect.gen(function* () {
        yield* sandbox.ensure({
          expectedInstanceId: allocation.instanceId,
          recovery: "exactWorkspaceRequired",
        });

        const process = yield* sandbox.exec(command, {
          processId: allocation.processId,
          cwd: CODING_ROOT,
          deadlineAt: allocation.deadlineAt,
          workspaceIdentity: allocation.workspaceIdentity,
        });

        // Native deadlines kill the process group. This timeout only bounds observation, with cleanup grace.
        const remaining = Math.max(
          1,
          allocation.deadlineAt - (yield* Clock.currentTimeMillis) + 10_000,
        );

        const output = yield* process.outputText({
          timeout: remaining,
          maxBytes: 128 * 1024,
          acknowledge: false,
        });

        if (structured && output.exitCode === 0 && !output.timedOut) {
          if (output.truncated)
            return yield* failure(
              "protocol",
              "The filesystem helper returned an oversized response; retain its receipt.",
              "unknown",
            );

          const reply = yield* Schema.decodeEffect(Schema.fromJsonString(FileReply))(
            output.stdout,
          ).pipe(
            Effect.mapError((cause) =>
              failure(
                "protocol",
                "The filesystem helper returned an invalid response; retain its receipt.",
                "unknown",
                cause,
              ),
            ),
          );

          return boundedResult({ ...reply, timedOut: false });
        }

        return boundedResult({ ...output, nextOffset: null });
      }).pipe(
        // A lost response is not permission to dispatch again with a different identity.
        // Interrupting this attempt fences even an allocated, not-yet-dispatched ticket.
        Effect.onInterrupt(() => sandbox.signalProcess(allocation.processId, 9)),
      ),
    );

    // A crash here reuses terminal output and only repeats acknowledgement, never execution.
    yield* steps
      .do(
        "acknowledge",
        Schema.Boolean,
        sandbox.acknowledgeProcess(allocation.processId).pipe(Effect.as(true)),
      )
      .pipe(
        Effect.mapError((cause) =>
          failure(
            "persistence",
            "The result was recorded, but native acknowledgement failed.",
            "completed",
            cause,
          ),
        ),
      );

    return result;
  },
  Effect.catchTags({
    SandboxOperationError: (cause) =>
      Effect.fail(
        failure(
          "execution",
          "Native execution could not be confirmed. Recover the original receipt; do not submit the mutation again.",
          "unknown",
          cause,
        ),
      ),
    DurableStepError: (cause) =>
      Effect.fail(
        failure(
          "persistence",
          "The durable step could not be confirmed. Recover this tool call before continuing.",
          "unknown",
          cause,
        ),
      ),
  }),
);

// Runs only inside the native sandbox. Model values cross as JSON argv, never interpolated code.
// Filesystem tools reject symlinks and protect the kernel's state; bash retains the host's full grant.
const FILE_SCRIPT = String.raw`
import * as fs from 'node:fs/promises';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
const input = JSON.parse(process.argv.at(-1));
const root = '/workspace', budget = 16384;
const result = { stdout: '', stderr: '', exitCode: 0, truncated: false, nextOffset: null };
const deny = code => { const error = new Error(code); error.code = code; throw error; };
const readBytes = async (file, limit, position = 0) => {
  const buffer = Buffer.alloc(limit);
  let used = 0;
  while (used < limit) {
    const { bytesRead } = await file.read(buffer, used, limit - used, position + used);
    if (bytesRead === 0) break;
    used += bytesRead;
  }
  return buffer.subarray(0, used);
};
const target = path.resolve(root, input.path ?? '.');
const relative = path.relative(root, target);
try {
  if (relative === '..' || relative.startsWith('../') || path.isAbsolute(relative)) deny('PATH_OUTSIDE_WORKSPACE');
  const pieces = relative.split('/').filter(Boolean);
  if (pieces[0]?.startsWith('.sandbox-agent')) deny('RESERVED_PATH');
  let current = root;
  if ((await fs.lstat(root)).isSymbolicLink()) deny('SYMLINK');
  for (let index = 0; index < pieces.length; index++) {
    current = path.join(current, pieces[index]);
    try { if ((await fs.lstat(current)).isSymbolicLink()) deny('SYMLINK'); }
    catch (error) {
      if (!(error.code === 'ENOENT' && index === pieces.length - 1 && input.operation === 'write_file')) throw error;
    }
  }
  if (input.operation === 'read_file') {
    const file = await fs.open(target, 'r');
    try {
      if (!(await file.stat()).isFile()) deny('NOT_A_FILE');
      const limit = input.maxBytes ?? budget;
      const offset = input.offsetBytes ?? 0;
      const buffer = await readBytes(file, limit + 1, offset);
      const bytes = buffer.subarray(0, limit);
      if (bytes.includes(0)) deny('BINARY_FILE');
      result.truncated = buffer.length > limit;
      result.stdout = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(bytes, { stream: result.truncated });
      if (result.truncated) result.nextOffset = offset + Buffer.byteLength(result.stdout);
    } finally { await file.close(); }
  } else if (input.operation === 'write_file' || input.operation === 'edit_file') {
    let content = input.content;
    let previous;
    try { previous = await fs.lstat(target); } catch (error) { if (error.code !== 'ENOENT') throw error; }
    if (previous && !previous.isFile()) deny('NOT_A_FILE');
    if (input.operation === 'edit_file') {
      if (!previous) deny('ENOENT');
      if (previous.size > 1048576) deny('FILE_TOO_LARGE');
      const file = await fs.open(target, 'r');
      let original;
      try {
        const bytes = await readBytes(file, 1048577);
        if (bytes.length > 1048576) deny('FILE_TOO_LARGE');
        original = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(bytes);
      } finally { await file.close(); }
      const first = original.indexOf(input.oldText);
      if (first < 0) deny('TEXT_NOT_FOUND');
      if (original.indexOf(input.oldText, first + 1) >= 0) deny('AMBIGUOUS_TEXT');
      content = original.slice(0, first) + input.newText + original.slice(first + input.oldText.length);
    }
    const temporary = path.join(path.dirname(target), '.coding-' + randomUUID());
    try {
      await fs.writeFile(temporary, content, { flag: 'wx', mode: previous ? previous.mode & 0o777 : 0o644 });
      await fs.rename(temporary, target);
    } finally { await fs.rm(temporary, { force: true }); }
    result.stdout = input.operation === 'edit_file' ? 'Replaced one exact match.' : 'File written.';
  } else if (input.operation === 'list_files') {
    let count = 0;
    const maximum = input.maxEntries ?? 200;
    const walk = async directory => {
      const entries = await fs.opendir(directory);
      for await (const entry of entries) {
        if (entry.name === '.git' || entry.name.startsWith('.sandbox-agent')) continue;
        const absolute = path.join(directory, entry.name);
        const line = path.relative(root, absolute) + (entry.isDirectory() ? '/' : entry.isSymbolicLink() ? '@' : '') + '\n';
        if (count >= maximum || Buffer.byteLength(result.stdout) + Buffer.byteLength(line) > budget) { result.truncated = true; return; }
        result.stdout += line;
        count++;
        if (input.recursive && entry.isDirectory()) {
          await walk(absolute);
          if (result.truncated) return;
        }
      }
    };
    await walk(target);
  } else if (input.operation === 'search') {
    const child = spawn('rg', ['--fixed-strings', '--line-number', '--with-filename', '--no-heading', '--color', 'never', '--glob', '!.git/**', '--glob', '!.sandbox-agent*', '--', input.query, target], { cwd: root, stdio: ['ignore', 'pipe', 'pipe'] });
    let used = 0;
    const decoders = { stdout: new TextDecoder(), stderr: new TextDecoder() };
    const collect = (field, bytes) => {
      const remaining = Math.max(0, budget - used);
      const accepted = bytes.subarray(0, remaining);
      result[field] += decoders[field].decode(accepted, { stream: true });
      used += accepted.length;
      if (accepted.length < bytes.length) { result.truncated = true; child.kill('SIGKILL'); }
    };
    child.stdout.on('data', bytes => collect('stdout', bytes));
    child.stderr.on('data', bytes => collect('stderr', bytes));
    result.exitCode = await new Promise((resolve, reject) => {
      child.once('error', reject);
      child.once('close', code => resolve(result.truncated || code === 1 ? 0 : code ?? 1));
    });
    // Do not flush incomplete UTF-8 after a byte-limit cut.
    if (!result.truncated) { result.stdout += decoders.stdout.decode(); result.stderr += decoders.stderr.decode(); }
  } else deny('UNKNOWN_OPERATION');
} catch (error) {
  const safe = new Set(['ENOENT', 'EACCES', 'ENOTDIR', 'EISDIR', 'PATH_OUTSIDE_WORKSPACE', 'RESERVED_PATH', 'SYMLINK', 'NOT_A_FILE', 'BINARY_FILE', 'FILE_TOO_LARGE', 'TEXT_NOT_FOUND', 'AMBIGUOUS_TEXT']);
  result.exitCode = 1;
  result.stderr = safe.has(error.code) ? error.code : 'FILESYSTEM_OPERATION_FAILED';
}
process.stdout.write(JSON.stringify(result));
`;

const file = Effect.fn("CodingTools.file")(function* (
  operation: string,
  parameters: Schema.JsonObject,
) {
  const argument = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.JsonObject))({
    ...parameters,
    operation,
  }).pipe(
    Effect.mapError((cause) =>
      failure("input", "The filesystem request could not be encoded.", "not-started", cause),
    ),
  );

  if (new TextEncoder().encode(argument).length > ARGUMENT_BYTES)
    return yield* failure(
      "input",
      "The encoded filesystem request exceeds 64 KiB; use a smaller edit.",
      "not-started",
    );

  return yield* run(["bun", "-e", FILE_SCRIPT, argument], true);
});

const options = {
  success: CodingToolResult,
  failure: CodingToolError,
  // Infrastructure errors propagate and stop the Run; confirmed command failures are result data.
  failureMode: "error" as const,
  dependencies: [DurableStep, ContextWindow, ThreadWorkDiscovery, Workspace],
};

// Replays dispatch only the persisted native ticket with its original deadline/workspace; terminal output is durable before repeatable acknowledgement.
export const codingTools = Toolkit.make(
  Tool.make("read_file", {
    ...options,
    description:
      "Read UTF-8 text inside /workspace. Byte offsets start at zero; follow nextOffset when truncated. Symlinks and binary files are rejected.",
    parameters: Schema.Struct({
      path: Path,
      offsetBytes: Schema.optionalKey(Schema.Natural),
      maxBytes: Schema.optionalKey(
        Schema.Int.check(Schema.isBetween({ minimum: 4, maximum: OUTPUT_BYTES })),
      ),
    }),
  }).annotate(ToolExecutionClass, "idempotent"),
  Tool.make("write_file", {
    ...options,
    description:
      "Atomically create or replace a UTF-8 file inside /workspace. Parent directory must exist. Symlinks are rejected. Encoded request limit: 64 KiB.",
    parameters: Schema.Struct({ path: Path, content: Text }),
  }).annotate(ToolExecutionClass, "idempotent"),
  Tool.make("edit_file", {
    ...options,
    description:
      "Replace exactly one occurrence of oldText in a UTF-8 file inside /workspace. Missing or ambiguous matches fail without changes. Maximum existing file: 1 MiB.",
    parameters: Schema.Struct({
      path: Path,
      oldText: Schema.NonEmptyString.check(Schema.isMaxLength(ARGUMENT_BYTES)),
      newText: Text,
    }),
  }).annotate(ToolExecutionClass, "idempotent"),
  Tool.make("list_files", {
    ...options,
    description:
      "List paths relative to /workspace with / for directories and @ for symlinks. Does not follow symlinks. Omits .git and kernel state. Results are bounded, not sorted.",
    parameters: Schema.Struct({
      path: Schema.optionalKey(Path),
      recursive: Schema.optionalKey(Schema.Boolean),
      maxEntries: Schema.optionalKey(
        Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 })),
      ),
    }),
  }).annotate(ToolExecutionClass, "idempotent"),
  Tool.make("search", {
    ...options,
    description:
      "Search literal text with ripgrep inside /workspace; returns filename:line:text. Honors ignore files, skips hidden files and symlinks. Empty matches succeed. Output is bounded.",
    parameters: Schema.Struct({
      path: Schema.optionalKey(Path),
      query: Schema.NonEmptyString.check(
        Schema.isMaxLength(4096),
        Schema.makeFilter(
          (value: string) =>
            !value.includes("\0") && !value.includes("\r") && !value.includes("\n"),
          { expected: "text without NUL or newline characters" },
        ),
      ),
    }),
  }).annotate(ToolExecutionClass, "idempotent"),
  Tool.make("bash", {
    ...options,
    description:
      "Run bash in /workspace with the host's container permissions. Output is bounded to 16 KiB. Nonzero exit and timeout are terminal results. No interactive stdin; do not background long-lived services.",
    parameters: Schema.Struct({
      script: Schema.NonEmptyString.check(
        Schema.isMaxLength(16 * 1024),
        Schema.makeFilter((value: string) => !value.includes("\0"), {
          expected: "a script without NUL characters",
        }),
      ),
      timeoutMs: Schema.optionalKey(
        Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 120_000 })),
      ),
    }),
  }).annotate(ToolExecutionClass, "idempotent"),
);

export const codingToolsLive = codingTools.toLayer({
  read_file: (parameters) => file("read_file", parameters),
  write_file: (parameters) => file("write_file", parameters),
  edit_file: (parameters) => file("edit_file", parameters),
  list_files: (parameters) => file("list_files", parameters),
  search: (parameters) => file("search", parameters),
  bash: ({ script, timeoutMs }) =>
    run(["bash", "--noprofile", "--norc", "-c", script], false, timeoutMs),
});
