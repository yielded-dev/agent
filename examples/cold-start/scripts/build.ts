import { builtinModules } from "node:module";

import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import { build } from "esbuild";

class BuildError extends Schema.TaggedError<BuildError>()("BuildError", {
  cause: Schema.Defect(),
}) {}

const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;

  yield* fs.makeDirectory("dist", { recursive: true });
  for (const [name, entry] of Object.entries({
    yielded: "src/yielded-entry.ts",
    pi: "third-party/src/entry.ts",
    front: "src/front.ts",
    provider: "src/provider.ts",
    driver: "src/driver.ts",
  })) {
    yield* Effect.tryPromise({
      try: () =>
        build({
          entryPoints: [entry],
          outfile: `dist/${name}.mjs`,
          bundle: true,
          format: "esm",
          platform: "neutral",
          target: "es2024",
          conditions: ["workerd", "worker", "browser", "import"],
          mainFields: ["module", "main"],
          external: ["cloudflare:*", "node:*", ...builtinModules.filter((name) => name !== "ws")],
          minifyWhitespace: true,
          sourcemap: "linked",
          metafile: true,
        }).then((result) => JSON.stringify(result.metafile)),
      catch: (cause) => BuildError.make({ cause }),
    }).pipe(Effect.flatMap((metadata) => fs.writeFileString(`dist/${name}.meta.json`, metadata)));
  }
});

NodeRuntime.runMain(program.pipe(Effect.provide(NodeServices.layer)));
