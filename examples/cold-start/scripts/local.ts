import { Console, Effect, FileSystem, Path, Predicate, Schema, Stream } from "effect";
import { Command, Flag } from "effect/cli";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import { environment, executable, sourceMap } from "./local-child.ts";
import { BenchError, check, hash, read, save, workspace } from "./platform.ts";

const frameworks = ["yielded", "pi"] as const;
const Framework = Schema.Literals(frameworks);

type Framework = typeof Framework.Type;
type Mode = "timing" | "coverage" | "profile";

const Timing = Schema.Struct({
  arm: Framework,
  parseLinkMs: Schema.Finite,
  evaluationMs: Schema.Finite,
});

const Coverage = Schema.Struct({
  ...Timing.fields,
  evidence: Schema.Struct({
    result: Schema.Array(
      Schema.Struct({
        scriptId: Schema.String,
        url: Schema.String,
        functions: Schema.Array(
          Schema.Struct({
            functionName: Schema.String,
            ranges: Schema.NonEmptyArray(
              Schema.Struct({
                startOffset: Schema.Int,
                endOffset: Schema.Int,
                count: Schema.Int,
              }),
            ),
            isBlockCoverage: Schema.Boolean,
          }),
        ),
      }),
    ),
    timestamp: Schema.Finite,
  }),
});

const Profile = Schema.Struct({
  ...Timing.fields,
  evidence: Schema.Struct({
    nodes: Schema.Array(
      Schema.Struct({
        id: Schema.Int,
        callFrame: Schema.Struct({
          functionName: Schema.String,
          scriptId: Schema.String,
          url: Schema.String,
          lineNumber: Schema.Int,
          columnNumber: Schema.Int,
        }),
        hitCount: Schema.optionalKey(Schema.Int),
        children: Schema.optionalKey(Schema.Array(Schema.Int)),
        deoptReason: Schema.optionalKey(Schema.String),
        positionTicks: Schema.optionalKey(
          Schema.Array(Schema.Struct({ line: Schema.Int, ticks: Schema.Int })),
        ),
      }),
    ),
    startTime: Schema.Finite,
    endTime: Schema.Finite,
    samples: Schema.Array(Schema.Int),
    timeDeltas: Schema.Array(Schema.Finite),
  }),
});

const SourceMapPayload = Schema.Struct({
  version: Schema.Int,
  sources: Schema.Array(Schema.String),
  sourcesContent: Schema.Array(Schema.String),
  names: Schema.Array(Schema.String),
  mappings: Schema.String,
  file: Schema.optionalKey(Schema.String),
  sourceRoot: Schema.optionalKey(Schema.String),
});

const ArmSummary = Schema.Struct({
  sha256: Schema.String,
  evaluationMeanMs: Schema.Finite,
  timings: Schema.Array(Schema.Finite),
  countRuns: Schema.Array(
    Schema.Struct({ classes: Schema.Int, structs: Schema.Int, wrappers: Schema.Int }),
  ),
  sampledSelfMs: Schema.Record(Schema.String, Schema.Finite),
});

const Summary = Schema.Struct({
  node: Schema.String,
  v8: Schema.String,
  cpu: Schema.String,
  arms: Schema.Record(Framework, ArmSummary),
});

const launch = Effect.fn("local.launch")(function* <
  S extends Schema.Top & { readonly DecodingServices: never; readonly Type: typeof Timing.Type },
>(out: string, reuse: boolean, arm: Framework, mode: Mode, sample: number, schema: S) {
  const path = yield* Path.Path;
  const file = path.join(out, `${arm}-${mode}-${sample}.json`);

  if (reuse) {
    const value = yield* read(file, schema);

    yield* check(value.arm === arm, "Saved capture belongs to another framework");

    return value;
  }
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const child = yield* spawner.spawn(
    ChildProcess.make(
      executable(),
      [
        "--experimental-strip-types",
        "--experimental-vm-modules",
        "--no-warnings",
        path.join(workspace, "scripts/local-child.ts"),
        arm,
        mode,
      ],
      { cwd: workspace, stdin: "ignore", stdout: "pipe", stderr: "pipe", extendEnv: true },
    ),
  );

  const [stdout, stderr, code] = yield* Effect.all(
    [
      child.stdout.pipe(Stream.decodeText(), Stream.mkString),
      child.stderr.pipe(Stream.decodeText(), Stream.mkString),
      child.exitCode,
    ],
    { concurrency: "unbounded" },
  );

  yield* check(Number(code) === 0, `Local ${arm} ${mode} child failed: ${stderr.trim()}`);

  const value = yield* Schema.decodeEffect(Schema.fromJsonString(schema))(stdout).pipe(
    Effect.mapError((cause) => BenchError.make({ message: "Invalid local child capture", cause })),
  );

  yield* check(value.arm === arm, "Child captured another framework");
  yield* save(file, value);

  return value;
}, Effect.scoped);

/** Fresh child captures, or exact replay of saved captures after checking bundle hashes. */
export const local = Effect.fn("local")(function* (directory: string, reuse = false) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const out = path.resolve(directory);
  const previous = reuse ? yield* read(path.join(out, "summary.json"), Summary) : undefined;

  if (previous) {
    for (const arm of frameworks) {
      const bytes = yield* fs.readFile(path.join(workspace, `dist/${arm}.mjs`));

      yield* check(
        previous.arms[arm].sha256 === hash(bytes),
        "Saved capture belongs to another bundle",
      );
    }
  }
  const runs: Record<Framework, (typeof Timing.Type)[]> = { yielded: [], pi: [] };

  for (let sample = 0; sample < 12; sample++) {
    for (const arm of sample % 2 ? (["pi", "yielded"] as const) : frameworks)
      runs[arm].push(yield* launch(out, reuse, arm, "timing", sample, Timing));
  }
  const summaries: Partial<Record<Framework, typeof ArmSummary.Type>> = {};

  for (const arm of frameworks) {
    const code = yield* fs.readFileString(path.join(workspace, `dist/${arm}.mjs`));
    const payload = yield* read(path.join(workspace, `dist/${arm}.mjs.map`), SourceMapPayload);

    const map = yield* Effect.try({
      try: () => sourceMap(payload),
      catch: (cause) => BenchError.make({ message: "Invalid bundle source map", cause }),
    });

    const starts = [0];

    for (let i = 0; i < code.length; i++) if (code[i] === "\n") starts.push(i + 1);

    const mapped = (offset: number) => {
      let lo = 0,
        hi = starts.length - 1;

      while (lo < hi) {
        const mid = Math.ceil((lo + hi) / 2);

        if (starts[mid] <= offset) lo = mid;
        else hi = mid - 1;
      }

      return map.findEntry(lo, offset - starts[lo]);
    };

    const countRuns = [];

    for (let sample = 0; sample < 3; sample++) {
      const capture = yield* launch(out, reuse, arm, "coverage", sample, Coverage);
      const coverage = capture.evidence.result.find((entry) => entry.url === `target://${arm}`);

      if (!coverage)
        return yield* BenchError.make({ message: "Coverage omitted the target bundle" });

      const functions = coverage.functions
        .filter((f) => f.ranges[0].count > 0)
        .map((f) => ({
          ...mapped(f.ranges[0].startOffset),
          name: f.functionName,
          count: f.ranges[0].count,
        }));

      yield* save(path.join(out, `${arm}-functions-${sample}.json`), functions);

      const count = (name: RegExp, file: RegExp) =>
        functions
          .filter(
            (f) =>
              name.test(f.name) && file.test("originalSource" in f ? (f.originalSource ?? "") : ""),
          )
          .reduce((sum, f) => sum + f.count, 0);

      countRuns.push({
        classes: count(/^makeClass\d*$/, /\/Schema\.[jt]s$/),
        structs: count(/^Struct\d*$/, /\/Schema\.[jt]s$/),
        wrappers: count(/^make\d*$/, /\/internal\/schema\/make\.[jt]s$/),
      });
    }
    const groups: Record<string, number> = {};

    for (let sample = 0; sample < 7; sample++) {
      const { evidence: profile } = yield* launch(out, reuse, arm, "profile", sample, Profile);

      yield* check(
        profile.samples.length === profile.timeDeltas.length,
        "Profile sample timing mismatch",
      );
      const nodes = new Map(profile.nodes.map((node) => [node.id, node]));

      for (let i = 0; i < profile.samples.length; i++) {
        const node = nodes.get(profile.samples[i]);

        if (!node) return yield* BenchError.make({ message: "Profile references a missing node" });
        const frame = node.callFrame;

        const location =
          frame.url === `target://${arm}`
            ? map.findEntry(frame.lineNumber, frame.columnNumber)
            : {};

        const source =
          "originalSource" in location && Predicate.isString(location.originalSource)
            ? location.originalSource
            : "";

        const group = /\/(Schema|SchemaAST)\.[jt]s$|\/internal\/schema\//.test(source)
          ? "Schema/AST"
          : frame.url === `target://${arm}`
            ? "other bundle code"
            : "runtime/profiler/GC";

        groups[group] = (groups[group] ?? 0) + profile.timeDeltas[i] / 1000 / 7;
      }
    }
    summaries[arm] = {
      sha256: hash(code),
      evaluationMeanMs: runs[arm].reduce((n, r) => n + r.evaluationMs, 0) / runs[arm].length,
      timings: runs[arm].map((r) => r.evaluationMs),
      countRuns,
      sampledSelfMs: groups,
    };
  }
  // Replay preserves the machine provenance of the captures, even on another host.
  const metadata = previous ?? (yield* Effect.sync(environment));

  const summary = yield* Schema.decodeUnknownEffect(Summary)({
    node: metadata.node,
    v8: metadata.v8,
    cpu: metadata.cpu,
    arms: summaries,
  }).pipe(
    Effect.mapError((cause) => BenchError.make({ message: "Incomplete local summary", cause })),
  );

  yield* save(path.join(out, "summary.json"), summary);

  return summary;
});

export const command = Command.make(
  "local",
  {
    out: Flag.String("out").pipe(Flag.withDescription("Private capture directory")),
    reuse: Flag.Boolean("reuse").pipe(
      Flag.withDescription("Replay saved captures after verifying bundle hashes"),
      Flag.withDefault(false),
    ),
  },
  Effect.fn(function* ({ out, reuse }) {
    yield* Console.log(JSON.stringify(yield* local(out, reuse), null, 2));
  }),
).pipe(
  Command.withDescription("Measure fresh module evaluation CPU, coverage, and sampled self time."),
);
