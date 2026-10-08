import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { builtinModules } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";

import { NodeRuntime } from "@effect/platform-node";
import { Effect, Schema } from "effect";
import { build, version as esbuildVersion } from "esbuild";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../../../..");
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
const BuildError = Schema.TaggedError()("ProdPathBuildError", { message: Schema.String });

export const program = Effect.tryPromise({
  try: async () => {
    const privateDirectory =
      process.env.PROD_PATH_PRIVATE ??
      (existsSync(join(here, "resources.json"))
        ? JSON.parse(readFileSync(join(here, "resources.json"), "utf8")).privateDirectory
        : undefined);

    if (!privateDirectory || !privateDirectory.startsWith("/private/tmp/prod-path-"))
      throw new Error("Run experiment init first (private build/state directory)");
    const selected = process.argv.slice(2).filter((arg) => arg !== "--");
    const entries = { provider: "network/provider.ts", network: "network/worker.ts" };
    const identitiesPath = join(here, "build-identities");

    mkdirSync(identitiesPath, { recursive: true });
    const allFile = join(identitiesPath, "all.json");
    const identities = existsSync(allFile) ? JSON.parse(readFileSync(allFile, "utf8")) : [];

    const commit = execFileSync("vp", ["exec", "git", "rev-parse", "HEAD"], {
      cwd: root,
      encoding: "utf8",
    }).trim();

    for (const [name, relative] of Object.entries(entries)) {
      if (selected.length && !selected.includes(name)) continue;
      if (!existsSync(join(here, relative))) continue;
      const output = join(privateDirectory, "bundles", name);
      const minify = name === "network-min";

      mkdirSync(output, { recursive: true });

      const compiled = await build({
        entryPoints: [join(here, relative)],
        outfile: join(output, "worker.mjs"),
        bundle: true,
        format: "esm",
        platform: "neutral",
        target: "es2024",
        minify,
        conditions: ["workerd", "worker", "browser", "import"],
        mainFields: ["module", "main"],
        external: ["cloudflare:*", "node:*", ...builtinModules],
        logLevel: "error",
        metafile: true,
        plugins: [
          {
            name: "prod-path-third-party-boundary",
            setup(bundler) {
              bundler.onResolve({ filter: /^[^./]/ }, async (args) => {
                if (
                  args.pluginData?.prodPathResolved ||
                  ![join(here, "network/pi.ts"), join(here, "network/tardie.ts")].includes(
                    args.importer,
                  )
                )
                  return;
                if (args.path.startsWith("cloudflare:") || args.path.startsWith("node:")) return;

                return bundler.resolve(args.path, {
                  resolveDir: join(root, "examples/durable-bench/third-party/src"),
                  kind: args.kind,
                  pluginData: { prodPathResolved: true },
                });
              });
            },
          },
        ],
      });

      const bundle = readFileSync(join(output, "worker.mjs"));

      const inputs = Object.keys(compiled.metafile.inputs)
        .sort()
        .map((path) => ({
          path: resolve(path).replace(root + "/", ""),
          sha256: hash(readFileSync(resolve(path))),
        }));

      const fixtureFiles = [
        "src/plan.ts",
        "src/yielded.ts",
        "third-party/src/pi.ts",
        "third-party/src/tardie.ts",
      ].map((path) => ({
        path,
        sha256: hash(readFileSync(join(root, "examples/durable-bench", path))),
      }));

      const identity = {
        name,
        repositoryCommit: commit,
        dirty:
          execFileSync("vp", ["exec", "git", "status", "--porcelain"], {
            cwd: root,
            encoding: "utf8",
          }).trim().length > 0,
        bundleSha256: hash(bundle),
        bundleBytes: bundle.length,
        gzipBytes: gzipSync(bundle).length,
        fixtureSha256: hash(JSON.stringify(fixtureFiles)),
        fixtureFiles,
        inputsSha256: hash(JSON.stringify(inputs)),
        esbuildVersion,
        minify,
        target: "es2024",
        output,
        effectVersion: JSON.parse(
          readFileSync(join(root, "node_modules/effect/package.json"), "utf8"),
        ).version,
      };

      writeFileSync(
        join(identitiesPath, `${name}-${identity.bundleSha256}.mjs.gz`),
        gzipSync(bundle),
      );

      const sources = inputs
        .filter(
          (item) => !item.path.startsWith("node_modules/") && !item.path.includes("/node_modules/"),
        )
        .map((item) => ({ ...item, source: readFileSync(join(root, item.path), "utf8") }));

      writeFileSync(
        join(identitiesPath, `${name}-${identity.bundleSha256}-sources.json.gz`),
        gzipSync(JSON.stringify(sources)),
      );
      writeFileSync(
        join(identitiesPath, `${name}-${identity.bundleSha256}-inputs.json.gz`),
        gzipSync(JSON.stringify(inputs, null, 2) + "\n"),
      );
      const previous = identities.findIndex((item) => item.name === name);

      if (previous >= 0) identities.splice(previous, 1);
      identities.push(identity);
      writeFileSync(allFile, JSON.stringify(identities, null, 2) + "\n");
      console.log(JSON.stringify(identity));
    }
  },
  catch: (cause) => new BuildError({ message: String(cause) }),
});

if (import.meta.url === `file://${process.argv[1]}`) NodeRuntime.runMain(program);
