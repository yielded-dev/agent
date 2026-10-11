import { join, resolve } from "node:path";

import { Console, Effect, FileSystem, Schema } from "effect";
import { Command, Flag } from "effect/cli";

import {
  history,
  MEASURED_TOOLS,
  next,
  payload,
  turn,
  type Message,
  type Turn,
} from "../src/plan.ts";
import { ensureVendor } from "./build.ts";
import { countInstrumentation } from "./count-instrument.ts";
import { CountReport } from "./count-runtime.ts";
import { NATURAL_THREAD, withNaturalHistory } from "./natural-fixture.ts";
import { BenchError, git, hash, repository, save } from "./platform.ts";
import { AwaitResult, Counts, Metrics, RunResult, SubmitResult } from "./worker/protocol.ts";

const CountTarget = Schema.Literals(["yielded", "pi"]);
const CountMetrics = Schema.Struct({ ...Metrics.fields, counts: CountReport });
const Inventory = Schema.Struct({ tables: Counts, bytes: Schema.Natural });

export const CountProbe = Schema.Struct({
  version: Schema.Literal(1),
  target: CountTarget,
  threadId: Schema.NonEmptyString,
  workload: Schema.Literal("natural-id50-v1"),
  codecUnit: Schema.Literal("Schema API calls, not nested parser entries"),
  source: Schema.Struct({
    ref: Schema.String,
    frameworkRevision: Schema.String,
    harnessRevision: Schema.String,
    targetBundleSha256: Schema.String,
    providerBundleSha256: Schema.String,
    files: Schema.Record(Schema.String, Schema.String),
  }),
  seed: Schema.Struct({
    turns: Schema.Literal(1000),
    expectedFingerprint: Schema.String,
    observedFingerprint: Schema.String,
    referenceTranscriptFingerprint: Schema.String,
    inventory: Inventory,
  }),
  window: Schema.Struct({
    beforeHistoryTurns: Schema.Literal(1000),
    afterHistoryTurns: Schema.Literal(1001),
    modelCalls: Schema.Natural,
    chatDecodeCalls: Schema.Natural,
    expectedFingerprints: Schema.Array(Schema.String),
    observedFingerprints: Schema.Array(Schema.String),
    referenceTranscriptFingerprint: Schema.String,
    inventory: Inventory,
    counts: CountReport,
  }),
});

const fingerprint = (messages: readonly Message[]) =>
  hash(
    JSON.stringify(messages.map((message) => [message.role, message.text, message.calls ?? []])),
  ).slice(0, 16);

// Fold the shared scripted workload independently of the target's stored/model messages.
const appendReference = (messages: Message[], input: Turn, capture: boolean): string[] => {
  const fingerprints: string[] = [];

  messages.push({ role: "user", text: input.text });
  for (;;) {
    if (capture) fingerprints.push(fingerprint(messages));
    const step = next(messages);

    if ("answer" in step) {
      messages.push({ role: "assistant", text: step.answer });

      return fingerprints;
    }
    messages.push(
      { role: "assistant", text: "", calls: [step.call] },
      { role: "tool", text: payload(step.call) },
    );
  }
};

const sourceFiles = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;

  const paths = yield* git([
    "ls-files",
    "--cached",
    "--others",
    "--exclude-standard",
    "-z",
    "--",
    "examples/durable-bench",
    "package.json",
    "bun.lock",
  ]);

  const files: Record<string, string> = {};

  for (const path of [...new Set(paths.split("\0"))].sort()) {
    if (!/\.(?:[cm]?[jt]s|json|lock)$/.test(path)) continue;
    files[path] = hash(yield* fs.readFile(join(repository, path)));
  }

  return files;
});

/** One warm eight-tool turn after exactly 1000 natural turns; no local latency measurements. */
export const count = Effect.fnUntraced(function* (options: {
  readonly ref: string;
  readonly target: typeof CountTarget.Type;
  readonly output: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const output = resolve(options.output);

  if (yield* fs.exists(output))
    return yield* new BenchError({ message: "Count output already exists; choose a new path." });

  yield* ensureVendor;
  const files = yield* sourceFiles;
  const messages: Message[] = [];
  let expectedSeedFingerprint = "";

  for (const input of history(0, 1000)) {
    const fingerprints = appendReference(messages, input, input.id === "h999");

    if (input.id === "h999") expectedSeedFingerprint = fingerprints.at(-1) ?? "";
  }
  const seedTranscript = fingerprint(messages);
  const sample = "count-0";
  const expectedFingerprints = appendReference(messages, turn(sample, MEASURED_TOOLS), true);
  const completedTranscript = fingerprint(messages);

  const report = yield* withNaturalHistory(
    {
      target: options.target,
      ref: options.ref,
      plugins: [countInstrumentation()],
      keepContext: true,
    },
    Effect.fnUntraced(function* ({ compiled, provider, invoke, seed }) {
      if (
        seed.fingerprint !== expectedSeedFingerprint ||
        seed.metrics.calls.at(-1)?.fingerprint !== expectedSeedFingerprint ||
        seed.metrics.query.sample !== "h999" ||
        seed.metrics.query.history !== 1000 ||
        seed.metrics.identity.build !== compiled.sha256
      )
        return yield* new BenchError({
          message: "Natural 1000-turn seed fingerprint or identity mismatch.",
        });

      if (options.target === "yielded") {
        const submitted = yield* invoke("/submit", SubmitResult, {}, sample);
        const settled = yield* invoke("/await", AwaitResult, submitted.receipt, sample);

        if (settled.settlement.outcome !== "completed")
          return yield* new BenchError({ message: "Count turn did not complete." });
      } else {
        yield* invoke("/run", RunResult, {}, sample);
      }

      // Pi /run waits for native idle. Its close handler would discard the warm Harness.
      // Yielded's quiescent metrics additionally drain alarms without discarding its runtime.
      const metrics = yield* invoke(
        options.target === "yielded" ? "/metrics?quiescent=true" : "/metrics",
        CountMetrics,
        undefined,
        sample,
      );

      if (
        metrics.query.target !== options.target ||
        metrics.query.object !== NATURAL_THREAD ||
        metrics.query.sample !== sample ||
        metrics.query.history !== 1000 ||
        metrics.query.expectedBuild !== compiled.sha256 ||
        metrics.identity.build !== compiled.sha256 ||
        metrics.identity.isolate !== seed.metrics.identity.isolate ||
        metrics.identity.incarnation !== seed.metrics.identity.incarnation ||
        metrics.counts.sample !== sample
      )
        return yield* new BenchError({
          message: "Count window lost its warm seed context or build identity.",
        });

      if (
        metrics.calls.length !== MEASURED_TOOLS + 1 ||
        metrics.calls.length !== expectedFingerprints.length ||
        metrics.calls.some(
          (call, index) =>
            call.call !== index ||
            call.fingerprint !== expectedFingerprints[index] ||
            call.status !== 200 ||
            call.error !== undefined ||
            call.endMs === undefined ||
            call.receipt?.fingerprint !== call.fingerprint ||
            call.receipt.expectedBuild !== compiled.sha256 ||
            call.receipt.objectBuild !== compiled.sha256 ||
            call.receipt.sample !== sample,
        )
      )
        return yield* new BenchError({
          message: "Count turn model calls differ from the independent transcript.",
        });

      const chatDecodeCalls = metrics.counts.codecs
        .filter(
          (row) =>
            row.scope === "bench" &&
            row.source === "examples/durable-bench/deployed/worker/protocol.ts" &&
            row.api === "Schema.decodeUnknownSync" &&
            row.schema === "Chat",
        )
        .reduce((total, row) => total + row.calls, 0);

      if (chatDecodeCalls !== metrics.calls.length)
        return yield* new BenchError({
          message: "Chat Schema API calls do not match observed model calls.",
        });

      return CountProbe.make({
        version: 1,
        target: options.target,
        threadId: NATURAL_THREAD,
        workload: "natural-id50-v1",
        codecUnit: "Schema API calls, not nested parser entries",
        source: {
          ref: options.ref,
          frameworkRevision: compiled.revision,
          harnessRevision: provider.revision,
          targetBundleSha256: compiled.sha256,
          providerBundleSha256: provider.sha256,
          files,
        },
        seed: {
          turns: 1000,
          expectedFingerprint: expectedSeedFingerprint,
          observedFingerprint: seed.fingerprint,
          referenceTranscriptFingerprint: seedTranscript,
          inventory: { tables: seed.metrics.tables, bytes: seed.metrics.bytes },
        },
        window: {
          beforeHistoryTurns: 1000,
          afterHistoryTurns: 1001,
          modelCalls: metrics.calls.length,
          chatDecodeCalls,
          expectedFingerprints,
          observedFingerprints: metrics.calls.map((call) => call.fingerprint),
          referenceTranscriptFingerprint: completedTranscript,
          inventory: { tables: metrics.tables, bytes: metrics.bytes },
          counts: metrics.counts,
        },
      });
    }),
  );

  yield* save(output, yield* Schema.encodeEffect(CountProbe)(report));
  yield* Console.log("Saved natural-1000 " + options.target + " operation counts to " + output);
});

export const command = Command.make(
  "count",
  {
    ref: Flag.String("ref").pipe(
      Flag.withDescription("Framework ref; harness and dependencies come from this checkout"),
    ),
    target: Flag.Literals("target", ["yielded", "pi"]),
    output: Flag.String("output").pipe(Flag.withDescription("New JSON report path")),
  },
  count,
).pipe(
  Command.withDescription(
    "Count SQL and Schema API calls for one warm turn after 1000 natural turns, locally.",
  ),
);
