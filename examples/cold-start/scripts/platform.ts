import { createHash, randomUUID } from "node:crypto";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";

import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as Schema from "effect/Schema";

export class BenchError extends Schema.TaggedError<BenchError>()("BenchError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export const workspace = resolve(dirname(fileURLToPath(import.meta.url)), "..");

/** Small native adapters for capabilities not supplied by the platform services. */
export const hash = (value: string | Uint8Array) =>
  createHash("sha256").update(value).digest("hex");

export const nonce = Effect.sync(() => randomUUID().replaceAll("-", ""));

export const gunzip = (value: Uint8Array) =>
  Effect.try({
    try: () => gunzipSync(value),
    catch: () => BenchError.make({ message: "Profiler response is not valid gzip" }),
  });

export const check = (condition: boolean, message: string) =>
  condition ? Effect.void : Effect.fail(BenchError.make({ message }));

export const save = Effect.fnUntraced(function* (file: string, value: unknown) {
  const fs = yield* FileSystem.FileSystem;

  yield* fs.makeDirectory(dirname(file), { recursive: true, mode: 0o700 });
  yield* fs.writeFileString(file + ".tmp", JSON.stringify(value, null, 2) + "\n", { mode: 0o600 });
  yield* fs.rename(file + ".tmp", file);
});

export const read = Effect.fnUntraced(function* <
  S extends Schema.Top & { readonly DecodingServices: never },
>(file: string, schema: S) {
  const fs = yield* FileSystem.FileSystem;

  return yield* Schema.decodeEffect(Schema.fromJsonString(schema))(
    yield* fs.readFileString(file),
  ).pipe(Effect.mapError(() => BenchError.make({ message: `Invalid evidence: ${file}` })));
});

export const revision = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  return (yield* spawner.string(
    ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: workspace }),
  )).trim();
});
