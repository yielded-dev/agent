import { createHash } from "node:crypto";
import { gzipSync } from "node:zlib";

import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Console, Effect, FileSystem, Path, Schema } from "effect";
import { Command, Flag } from "effect/cli";

import { REFERENCE_ALGORITHM, REFERENCE_CHECKSUM, REFERENCE_ITERATIONS } from "./contracts.ts";
import { referenceSource, wrapper } from "./wrapper.ts";

const roles = ["pinned", "base", "head", "control", "pi", "tardie"] as const;
const extraFields = Schema.Record(Schema.String, Schema.Unknown);

const FileIdentity = Schema.StructWithRest(
  Schema.Struct({
    sha256: Schema.String,
    rawBytes: Schema.Int,
    gzipBytes: Schema.Int,
  }),
  [extraFields],
);

// Preserve every other manifest field, including fields added by the parent.
const Manifest = Schema.StructWithRest(
  Schema.Struct({
    target: Schema.Literals(["yielded", "pi", "tardie"]),
    label: Schema.Literals(roles),
    bench: FileIdentity,
    wrapper: FileIdentity,
    sizes: Schema.Array(Schema.Int.check(Schema.isGreaterThan(0))),
    samples: Schema.Int.check(Schema.isGreaterThan(0)),
    locationHint: Schema.Literal("wnam"),
  }),
  [extraFields],
);

const ManifestJson = Schema.fromJsonString(Manifest);

class RewrapError extends Schema.TaggedError<RewrapError>()("RewrapError", {
  message: Schema.String,
}) {}

const sha256 = (bytes: Uint8Array | string): string =>
  createHash("sha256").update(bytes).digest("hex");

const identity = (bytes: Uint8Array) => ({
  sha256: sha256(bytes),
  rawBytes: bytes.byteLength,
  gzipBytes: gzipSync(bytes, { level: 9 }).byteLength,
});

const verify = (file: string, bytes: Uint8Array | string, expected: string) =>
  Effect.fail(new RewrapError({ message: `SHA-256 mismatch: ${file}` })).pipe(
    Effect.when(Effect.sync(() => sha256(bytes) !== expected)),
  );

export const command = Command.make(
  "cf-bench-rewrap",
  {
    bundlesDir: Flag.String("bundles-dir").pipe(Flag.withSchema(Schema.NonEmptyString)),
    outputDir: Flag.String("output-dir").pipe(Flag.withSchema(Schema.NonEmptyString)),
    archiveName: Flag.Literals("archive-name", [
      "wrapper-v1",
      "wrapper-v2",
      "wrapper-v3",
      "wrapper-v4",
      "wrapper-v5",
      "wrapper-v6",
    ]).pipe(Flag.withDefault("wrapper-v1")),
  },
  Effect.fnUntraced(function* ({ bundlesDir, outputDir, archiveName }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const bundles = path.resolve(bundlesDir);
    const builds = path.resolve(outputDir, "builds");
    const archive = path.join(builds, archiveName);

    const evidence = path.join(
      builds,
      archiveName === "wrapper-v1" ? "rewrap.json" : `rewrap-${archiveName.slice(8)}.json`,
    );

    if (yield* fs.exists(archive))
      return yield* new RewrapError({ message: `Refusing to overwrite archive: ${archive}` });
    if (yield* fs.exists(evidence))
      return yield* new RewrapError({ message: `Refusing to overwrite evidence: ${evidence}` });

    // Validate the entire set before creating archives or changing any bundle.
    const plans = yield* Effect.forEach(
      roles,
      Effect.fnUntraced(function* (role) {
        const directory = path.join(bundles, role);
        const benchFile = path.join(directory, "bench.mjs");
        const workerFile = path.join(directory, "worker.mjs");
        const manifestFile = path.join(directory, "manifest.json");
        const oldManifest = yield* fs.readFileString(manifestFile);
        const manifest = yield* Schema.decodeEffect(ManifestJson)(oldManifest);
        const target = role === "pi" || role === "tardie" ? role : "yielded";

        if (manifest.label !== role || manifest.target !== target) {
          return yield* new RewrapError({
            message: `Manifest target/label does not match bundle directory: ${directory}`,
          });
        }
        if (new Set(manifest.sizes).size !== manifest.sizes.length || manifest.sizes.length === 0) {
          return yield* new RewrapError({ message: `Invalid sizes in ${manifestFile}` });
        }
        const oldWorker = yield* fs.readFile(workerFile);

        yield* verify(benchFile, yield* fs.readFile(benchFile), manifest.bench.sha256);
        yield* verify(workerFile, oldWorker, manifest.wrapper.sha256);

        const newWorker = new TextEncoder().encode(
          wrapper(target, manifest.sizes, manifest.samples),
        );

        const next = {
          ...manifest,
          wrapper: { ...manifest.wrapper, ...identity(newWorker) },
          reference: {
            algorithm: REFERENCE_ALGORITHM,
            iterations: REFERENCE_ITERATIONS,
            checksum: REFERENCE_CHECKSUM,
            sourceSha256: sha256(referenceSource),
          },
        };

        const newManifest = (yield* Schema.encodeEffect(ManifestJson)(next)) + "\n";

        return {
          role,
          directory,
          benchFile,
          workerFile,
          manifestFile,
          oldWorker,
          oldManifest,
          manifest,
          newWorker,
          newManifest,
          next,
        };
      }),
    );

    // Refuse an existing archive, and preserve exact old bytes for every role before the first update.
    yield* fs.makeDirectory(builds, { recursive: true });
    yield* fs.makeDirectory(archive, { mode: 0o700 });
    for (const plan of plans) {
      const directory = path.join(archive, plan.role);

      yield* fs.makeDirectory(directory, { mode: 0o700 });
      yield* fs.writeFile(path.join(directory, "worker.mjs"), plan.oldWorker, {
        flag: "wx",
        mode: 0o600,
      });
      yield* fs.writeFileString(path.join(directory, "manifest.json"), plan.oldManifest, {
        flag: "wx",
        mode: 0o600,
      });
    }

    // No prepare(), builds, install changes, or bench.mjs writes occur in this command.
    for (const plan of plans) {
      yield* verify(plan.benchFile, yield* fs.readFile(plan.benchFile), plan.manifest.bench.sha256);
      yield* verify(
        plan.workerFile,
        yield* fs.readFile(plan.workerFile),
        plan.manifest.wrapper.sha256,
      );
      yield* verify(
        plan.manifestFile,
        yield* fs.readFileString(plan.manifestFile),
        sha256(plan.oldManifest),
      );
      yield* fs.writeFile(plan.workerFile, plan.newWorker);
      yield* fs.writeFileString(plan.manifestFile, plan.newManifest);
      yield* verify(plan.benchFile, yield* fs.readFile(plan.benchFile), plan.manifest.bench.sha256);
      yield* verify(plan.workerFile, yield* fs.readFile(plan.workerFile), plan.next.wrapper.sha256);
      yield* verify(
        plan.manifestFile,
        yield* fs.readFileString(plan.manifestFile),
        sha256(plan.newManifest),
      );
    }

    const Evidence = Schema.Struct({
      archive: Schema.String,
      bundles: Schema.Array(
        Schema.Struct({
          role: Schema.Literals(roles),
          directory: Schema.String,
          benchSha256: Schema.String,
          previousWrapper: FileIdentity,
          wrapper: FileIdentity,
          previousManifestSha256: Schema.String,
          manifestSha256: Schema.String,
        }),
      ),
    });

    const report = yield* Schema.encodeEffect(Schema.fromJsonString(Evidence))({
      archive,
      bundles: plans.map((plan) => ({
        role: plan.role,
        directory: plan.directory,
        benchSha256: plan.manifest.bench.sha256,
        previousWrapper: plan.manifest.wrapper,
        wrapper: plan.next.wrapper,
        previousManifestSha256: sha256(plan.oldManifest),
        manifestSha256: sha256(plan.newManifest),
      })),
    });

    yield* fs.writeFileString(evidence, report + "\n", { flag: "wx", mode: 0o600 });
    yield* Console.log(evidence);
  }),
).pipe(
  Command.withDescription(
    "Archive wrapper-v1 and replace only worker.mjs plus wrapper manifest identity in all six bundles.",
  ),
);

if (import.meta.main) {
  command.pipe(
    Command.run({ version: "1.0.0" }),
    Effect.provide(BunServices.layer),
    BunRuntime.runMain,
  );
}
