import { createHash, randomBytes } from "node:crypto";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { Effect, FileSystem, Schema, Stream } from "effect";
import { ChildProcess } from "effect/process";

export class BenchError extends Schema.TaggedError<BenchError>()("BenchError", {
  message: Schema.String,
}) {}

export const directory = dirname(fileURLToPath(import.meta.url));
export const workspace = resolve(directory, "..");
export const repository = resolve(workspace, "../..");
export const privateDirectory = "/private/tmp/cold-storage-state";

export const hash = (value: string | Uint8Array) =>
  createHash("sha256").update(value).digest("hex");

export const nonce = () =>
  Array.from(randomBytes(8), (byte) => byte.toString(16).padStart(2, "0")).join("");

export const redact = (text: string, secrets: readonly string[] = []) => {
  for (const secret of secrets) if (secret) text = text.replaceAll(secret, "[redacted]");

  return text
    .replace(/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi, "[redacted-email]")
    .replace(/\b[0-9a-f]{32,64}\b/gi, "[redacted-id]")
    .replace(/\breference\s*=\s*[a-z0-9]+/gi, "reference = [redacted-id]")
    .replace(/\b[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\b/gi, "[redacted-id]");
};

/** The CLI's scoped subprocess boundary; captured output never goes straight to the terminal. */
export const execute = Effect.fnUntraced(function* (
  args: readonly string[],
  cwd = workspace,
  env: Record<string, string | undefined> = {},
  input?: Uint8Array,
) {
  const child = yield* ChildProcess.make("vp", args, {
    cwd,
    env,
    extendEnv: true,
    stdout: "pipe",
    stderr: "pipe",
    ...(input ? { stdin: Stream.succeed(input) } : { stdin: "ignore" }),
  });

  const output = yield* child.all.pipe(Stream.decodeText(), Stream.mkString);
  const code = yield* child.exitCode;

  return { code: Number(code), output };
}, Effect.scoped);

export const git = Effect.fnUntraced(function* (args: readonly string[]) {
  const result = yield* execute(["exec", "git", ...args], repository);

  if (result.code !== 0)
    return yield* new BenchError({ message: "Git command failed; check the requested reference." });

  return result.output.trim();
});

export const save = Effect.fnUntraced(function* (file: string, value: unknown) {
  const fs = yield* FileSystem.FileSystem;

  yield* fs.makeDirectory(dirname(file), { recursive: true, mode: 0o700 });
  yield* fs.writeFileString(file + ".tmp", JSON.stringify(value, null, 2) + "\n", { mode: 0o600 });
  yield* fs.rename(file + ".tmp", file);
});

export const read = <S extends Schema.Top & { readonly DecodingServices: never }>(
  file: string,
  schema: S,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const raw = yield* fs.readFileString(file);

    return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(raw).pipe(
      Effect.mapError(
        () =>
          new BenchError({
            message: `Invalid ${file.endsWith("state.json") ? "private deployment state" : "benchmark data"}.`,
          }),
      ),
    );
  });
