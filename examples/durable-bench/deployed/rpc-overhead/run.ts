import { builtinModules } from "node:module";
import { join } from "node:path";

import { Console, Effect, FileSystem, Option, Schema, Semaphore } from "effect";
import { Command, Flag } from "effect/cli";
import { build } from "esbuild";

import { Cloudflare, request } from "../cloudflare.ts";
import {
  BenchError,
  directory,
  execute,
  git,
  hash,
  nonce,
  read,
  redact,
  repository,
  save,
  workspace,
} from "../platform.ts";
import { cpu } from "./cpu.ts";
import type { Batch } from "./model.ts";
import { PushResult, Result, ThreadCpuControl, ThreadCpuSampleResult, Variant } from "./model.ts";

const privateDirectory = "/private/tmp/rpc-overhead-private";
const stateFile = join(privateDirectory, "state.json");
const outputDirectory = join(workspace, "results/rpc-overhead");

const State = Schema.Struct({
  name: Schema.String.check(Schema.isPattern(/^rpc-overhead-[a-f0-9]+$/)),
  account: Schema.String,
  token: Schema.String,
  build: Schema.String,
  bundle: Schema.String,
  revision: Schema.String,
});

type State = typeof State.Type;

const alchemy = Effect.fnUntraced(function* (state: State, action: "deploy" | "destroy") {
  const cloud = yield* Cloudflare;

  if (cloud.accountId !== state.account)
    return yield* new BenchError({ message: "Private state account mismatch" });

  const result = yield* execute(
    [
      "exec",
      "alchemy",
      action,
      join(directory, "rpc-overhead/stack.ts"),
      "--stage",
      state.name,
      "--yes",
    ],
    privateDirectory,
    {
      CI: "true",
      NO_COLOR: "1",
      ALCHEMY_HOME: join(privateDirectory, "auth"),
      CLOUDFLARE_ACCOUNT_ID: cloud.accountId,
      CLOUDFLARE_API_TOKEN: cloud.apiToken,
      RPC_OVERHEAD_NAME: state.name,
      RPC_OVERHEAD_TOKEN: state.token,
      RPC_OVERHEAD_BUILD: state.build,
      RPC_OVERHEAD_BUNDLE: state.bundle,
    },
  );

  if (result.code !== 0)
    return yield* new BenchError({
      message: `Alchemy ${action} failed: ${redact(result.output, [cloud.accountId, cloud.apiToken, cloud.accountName, cloud.subdomain, state.token]).slice(-2500)}`,
    });
});

const loadState = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;

  if (!(yield* fs.exists(stateFile)))
    return yield* new BenchError({ message: "Deploy first; no owned state exists" });
  const state = yield* read(stateFile, State);
  const cloud = yield* Cloudflare;

  if (state.account !== cloud.accountId)
    return yield* new BenchError({ message: "Private state account mismatch" });

  return state;
});

const deploy = Effect.gen(function* () {
  const cloud = yield* Cloudflare;
  const fs = yield* FileSystem.FileSystem;
  const previous = (yield* fs.exists(stateFile)) ? yield* loadState : undefined;

  if (!previous && !(yield* cloud.resources("rpc-overhead")).verified)
    return yield* new BenchError({
      message: "Unowned rpc-overhead resources exist; refusing adoption",
    });
  yield* fs.makeDirectory(privateDirectory, { recursive: true, mode: 0o700 });
  yield* fs.chmod(privateDirectory, 0o700);
  if (!(yield* fs.exists(join(privateDirectory, "node_modules"))))
    yield* fs.symlink(join(repository, "node_modules"), join(privateDirectory, "node_modules"));
  yield* fs.writeFileString(
    join(privateDirectory, "package.json"),
    '{"private":true,"type":"module"}\n',
    { mode: 0o600 },
  );
  const bundle = join(privateDirectory, "worker.mjs");

  yield* Effect.tryPromise({
    try: () =>
      build({
        entryPoints: [join(directory, "rpc-overhead/worker.ts")],
        outfile: bundle,
        bundle: true,
        format: "esm",
        platform: "neutral",
        target: "es2024",
        conditions: ["workerd", "worker", "browser", "import"],
        mainFields: ["module", "main"],
        external: ["cloudflare:*", "node:*", ...builtinModules],
        logLevel: "silent",
      }),
    catch: (cause) =>
      new BenchError({
        message: `Build failed: ${redact(cause instanceof Error ? cause.message : "unknown")}`,
      }),
  });

  const state: State = {
    name: previous?.name ?? `rpc-overhead-${nonce().slice(0, 8)}`,
    account: cloud.accountId,
    token: previous?.token ?? nonce() + nonce(),
    build: hash(yield* fs.readFile(bundle)),
    bundle,
    revision: yield* git(["rev-parse", "HEAD"]),
  };

  yield* save(stateFile, state);
  yield* Console.error("Deploying owned rpc-overhead Worker and SQLite Object namespaces…");
  yield* alchemy(state, "deploy");
  const url = `https://${state.name}.${cloud.subdomain}.workers.dev`;
  const Health = Schema.Struct({ ok: Schema.Boolean, build: Schema.String });
  let consecutive = 0;

  for (let i = 0; i < 45; i++) {
    const ready = yield* request(url + "/health?probe=" + nonce(), state.token, Health).pipe(
      Effect.result,
    );

    consecutive =
      ready._tag === "Success" && ready.success.value.build === state.build ? consecutive + 1 : 0;
    if (consecutive === 3) {
      yield* Console.error(
        "Deployment health and personal account verified. Object build guards run before measurement.",
      );

      return;
    }
    yield* Effect.sleep("2 seconds");
  }

  return yield* new BenchError({
    message: "Worker build not ready; deployment retained for inspection/cleanup",
  });
});

const destroy = Effect.gen(function* () {
  const cloud = yield* Cloudflare;
  const fs = yield* FileSystem.FileSystem;
  const state = yield* loadState;

  yield* alchemy(state, "destroy");
  const remaining = yield* cloud.resources("rpc-overhead");

  yield* save(join(outputDirectory, "cleanup.json"), {
    verified: remaining.verified,
    workersRemaining: remaining.workers.length,
    namespacesRemaining: remaining.namespaces.length,
  });
  if (!remaining.verified)
    return yield* new BenchError({
      message: "Cleanup unverified; preserve private state and retry destroy",
    });
  yield* fs.remove(privateDirectory, { recursive: true });
  yield* Console.error("Cleanup verified: zero rpc-overhead Workers or Object namespaces remain.");
});

const run = Effect.fnUntraced(function* (options: {
  objects: number;
  calls: number;
  warmup: number;
  concurrency: number;
  variants: readonly Variant[];
  round: string;
}) {
  const state = yield* loadState;
  const cloud = yield* Cloudflare;
  const start = Date.now();
  const rows: Result[] = [];
  const failures: { object: number; variant: Variant; size: number; error: string }[] = [];
  const writing = yield* Semaphore.make(1);
  const url = `https://${state.name}.${cloud.subdomain}.workers.dev`;

  const snapshot = () =>
    save(join(outputDirectory, `${options.round}.json`), {
      revision: state.revision,
      build: state.build,
      compatibilityDate: "2026-08-18",
      placement: "aws:us-west-1",
      objectHint: "wnam",
      options,
      start,
      end: Date.now(),
      rows,
      failures,
    });

  yield* Effect.forEach(
    Array.from({ length: options.objects }, (_, i) => i),
    (object) =>
      Effect.gen(function* () {
        // Rotate/reverse the order by Object: common location, bounded concurrency, no overlap within an Object.
        const variants = options.variants.map(
          (_, i) => options.variants[(i + object) % options.variants.length]!,
        );

        if (object % 2) variants.reverse();
        for (const size of object % 2 ? ([20_000, 200] as const) : ([200, 20_000] as const)) {
          for (const variant of variants) {
            if (variant.startsWith("thread-") && size !== 200) continue;
            const batch: Batch = { ...options, object, size, variant, build: state.build };

            const result = yield* request(url + "/batch", state.token, Result, batch).pipe(
              Effect.result,
            );

            if (result._tag === "Success")
              rows.push({ ...result.success.value, placement: result.success.placement });
            else
              failures.push({
                object,
                variant,
                size,
                error: redact(String(result.failure), [
                  state.token,
                  cloud.accountId,
                  cloud.accountName,
                  cloud.subdomain,
                ]),
              });
            yield* snapshot().pipe(writing.withPermit);
          }
        }
        yield* Console.error(
          `Object ${object + 1}/${options.objects} complete; ${rows.length} batches, ${failures.length} failures.`,
        );
      }),
    { concurrency: options.concurrency },
  );
  yield* snapshot();
  for (const variant of options.variants)
    for (const size of [200, 20_000]) {
      const samples = rows
        .filter((r) => r.batch.variant === variant && r.batch.size === size)
        .flatMap((r) => r.latencyMs)
        .sort((a, b) => a - b);

      if (samples.length)
        yield* Console.log(
          `${variant} ${size}B: n=${samples.length}, p50=${samples[Math.floor(samples.length * 0.5)]}ms, p90=${samples[Math.floor(samples.length * 0.9)]}ms`,
        );
    }
  if (failures.length)
    return yield* new BenchError({ message: "Benchmark retained failures in the result artifact" });
});

const push = Effect.fnUntraced(function* (options: {
  objects: number;
  concurrency: number;
  round: string;
}) {
  const state = yield* loadState;
  const cloud = yield* Cloudflare;
  const start = Date.now();
  const rows: PushResult[] = [];
  const failures: { object: number; error: string }[] = [];
  const writing = yield* Semaphore.make(1);

  const snapshot = () =>
    save(join(outputDirectory, `${options.round}.json`), {
      revision: state.revision,
      build: state.build,
      options,
      start,
      end: Date.now(),
      rows,
      failures,
    });

  yield* Effect.forEach(
    Array.from({ length: options.objects }, (_, object) => object),
    (object) =>
      Effect.gen(function* () {
        const result = yield* request(
          `https://${state.name}.${cloud.subdomain}.workers.dev/push`,
          state.token,
          PushResult,
          {
            ...options,
            object,
            build: state.build,
            framesPerBurst: 4,
          },
          "6 minutes",
        ).pipe(Effect.result);

        if (result._tag === "Success") rows.push(result.success.value);
        else
          failures.push({
            object,
            error: redact(String(result.failure), [
              state.token,
              cloud.accountId,
              cloud.accountName,
              cloud.subdomain,
            ]),
          });
        yield* snapshot().pipe(writing.withPermit);
        yield* Console.error(
          `Push Object ${object + 1}/${options.objects}: ${result._tag === "Success" && result.success.value.ok ? "passed" : "failed; preserved evidence"}.`,
        );
      }),
    { concurrency: options.concurrency },
  );
  yield* snapshot();
  const recreated = rows.filter((r) => r.websocket?.recreationObserved).length;

  yield* Console.log(
    `Push: ${rows.length} results, ${recreated} observed constructor recreations on the same connection.`,
  );
  if (failures.length || rows.some((r) => !r.ok))
    return yield* new BenchError({
      message: "Push experiment retained failures in the result artifact",
    });
});

const threadCpu = Effect.fnUntraced(function* (options: {
  objects: number;
  calls: number;
  concurrency: number;
  round: string;
  variants: readonly Variant[];
}) {
  if (options.variants.some((variant) => !variant.startsWith("thread-")))
    return yield* new BenchError({ message: "thread-cpu requires Thread variants" });
  const state = yield* loadState;
  const cloud = yield* Cloudflare;
  const start = Date.now();
  const rows: Result[] = [];

  const samples: {
    object: number;
    variant: Variant;
    index: number;
    latencyMs: number;
    clientWarm: boolean;
    placement: string | null;
  }[] = [];

  const failures: { object: number; variant: Variant; error: string }[] = [];
  const writing = yield* Semaphore.make(1);
  const url = `https://${state.name}.${cloud.subdomain}.workers.dev/thread-cpu`;

  const snapshot = () =>
    save(join(outputDirectory, `${options.round}.json`), {
      mode: "thread-cpu",
      revision: state.revision,
      build: state.build,
      options,
      start,
      end: Date.now(),
      rows,
      samples,
      failures,
    });

  const recordFailure = (object: number, variant: Variant, cause: unknown) =>
    failures.push({
      object,
      variant,
      error: redact(String(cause), [
        state.token,
        cloud.accountId,
        cloud.accountName,
        cloud.subdomain,
      ]),
    });

  yield* Effect.forEach(
    Array.from({ length: options.objects }, (_, i) => i),
    (object) =>
      Effect.gen(function* () {
        for (let offset = 0; offset < options.variants.length; offset++) {
          const variant = options.variants[(offset + object) % options.variants.length]!;

          const batch: Batch = {
            ...options,
            object,
            variant,
            size: 200,
            build: state.build,
            warmup: 0,
          };

          yield* Effect.gen(function* () {
            const prepared = yield* request(url + "/prepare", state.token, ThreadCpuControl, batch);
            const control = yield* Schema.encodeEffect(ThreadCpuControl)(prepared.value);
            const latencyMs: number[] = [];
            let colo: string | null = null;
            let placement: string | null = null;

            const measured = yield* Effect.gen(function* () {
              for (let index = 0; index < batch.calls; index++) {
                const response = yield* request(
                  url + "/sample",
                  state.token,
                  ThreadCpuSampleResult,
                  { control, index },
                );

                const sample = response.value;

                latencyMs.push(sample.latencyMs);
                colo = response.colo;
                placement = response.placement;
                samples.push({
                  object,
                  variant,
                  index,
                  latencyMs: sample.latencyMs,
                  clientWarm: sample.clientWarm,
                  placement,
                });
                if (sample.receipt !== null) {
                  const encoded = yield* Schema.encodeEffect(ThreadCpuSampleResult)(sample);

                  yield* request(
                    url + "/drain",
                    state.token,
                    Schema.Struct({ ok: Schema.Literal(true) }),
                    { control, receipt: encoded.receipt },
                  );
                }
              }
            }).pipe(Effect.result);

            // Finish always attempts to release metadata; a partial batch fails its count guard.
            const finished = yield* request(
              url + "/finish",
              state.token,
              Schema.Struct({ sameInstance: Schema.Literal(true) }),
              { control },
            ).pipe(Effect.result);

            if (measured._tag === "Failure") return yield* measured.failure;
            if (finished._tag === "Failure") return yield* finished.failure;
            rows.push({
              ok: true,
              batch,
              latencyMs,
              setupMs: 0,
              sameInstance: true,
              colo,
              placement,
              clientSetup:
                "one call per driver invocation; prepare, settlement drain and finish are separate invocations; cold clients marked separately",
            });
          }).pipe(
            Effect.catch((cause) =>
              Effect.sync(() => {
                recordFailure(object, variant, cause);
              }),
            ),
          );
          yield* snapshot().pipe(writing.withPermit);
        }
        yield* Console.error(
          `CPU Object ${object + 1}/${options.objects} complete; ${samples.length} calls, ${failures.length} failures.`,
        );
      }),
    { concurrency: options.concurrency },
  );
  yield* snapshot();
  yield* Console.log(
    `Single-call CPU cohort: ${samples.length} calls, ${samples.filter((sample) => sample.clientWarm).length} with cached clients.`,
  );
  if (failures.length)
    return yield* new BenchError({ message: "CPU cohort retained failures in its artifact" });
});

export const command = Command.make(
  "rpc-overhead",
  {
    action: Flag.Literals("action", ["deploy", "run", "push", "thread-cpu", "cpu", "destroy"]).pipe(
      Flag.withDefault("run"),
    ),
    objects: Flag.Int("objects").pipe(Flag.withDefault(16)),
    calls: Flag.Int("calls").pipe(Flag.withDefault(100)),
    warmup: Flag.Int("warmup").pipe(Flag.withDefault(10)),
    concurrency: Flag.Int("concurrency").pipe(Flag.withDefault(4)),
    variants: Flag.String("variants").pipe(
      Flag.withDefault(
        "native,fetch,schema-sync,schema-runtime,effect-json,effect-ndjson,effect-ws",
      ),
    ),
    round: Flag.String("round").pipe(
      Flag.withSchema(Schema.String.check(Schema.isPattern(/^[a-z0-9-]+$/))),
      Flag.optional,
    ),
  },
  Effect.fnUntraced(function* (flags) {
    if (flags.action === "deploy") return yield* deploy;
    if (flags.action === "destroy") return yield* destroy;
    if (flags.action === "cpu") {
      const state = yield* loadState;

      if (Option.isNone(flags.round))
        return yield* new BenchError({ message: "CPU export requires --round" });
      const round = flags.round.value;

      const period = yield* read(
        join(outputDirectory, `${round}.json`),
        Schema.Struct({
          start: Schema.Finite,
          end: Schema.Finite,
          rows: Schema.Array(Schema.Unknown),
          mode: Schema.optionalKey(Schema.String),
        }),
      );

      const result = yield* cpu(state.name, period.start - 2000, period.end + 2000);

      const batches = period.rows.flatMap((row) =>
        Option.match(Schema.decodeUnknownOption(Result)(row), {
          onNone: () => [],
          onSome: (value) => [value],
        }),
      );

      const coverage = batches.map(({ batch }) => ({
        object: batch.object,
        variant: batch.variant,
        size: batch.size,
        expectedObject: batch.calls,
        observedObject: result.rows.filter(
          (r) =>
            r.marker?.round === round &&
            r.marker.role === "object" &&
            r.marker.object === batch.object &&
            r.marker.variant === batch.variant &&
            r.marker.size === batch.size &&
            r.marker.phase === "measure",
        ).length,
        expectedDriver: period.mode === "thread-cpu" ? batch.calls : 1,
        observedDriver: result.rows.filter(
          (r) =>
            r.marker?.round === round &&
            r.marker.role === "driver" &&
            r.marker.object === batch.object &&
            r.marker.variant === batch.variant &&
            r.marker.size === batch.size,
        ).length,
      }));

      yield* save(join(outputDirectory, `${round}-cpu.json`), { ...result, coverage });
      yield* Console.log(
        `CPU: ${result.rows.length} invocations, ${result.rows.filter((r) => r.marker !== null).length} attributed, ${result.unmatchedMarkers} unmatched markers.`,
      );

      return;
    }

    const variants = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(Variant))(
      flags.variants.split(","),
    );

    if (flags.objects < 1 || flags.objects > 64 || flags.concurrency < 1 || flags.concurrency > 16)
      return yield* new BenchError({ message: "Objects must be 1–64 and concurrency 1–16" });
    if (flags.action === "push")
      return yield* push({
        ...flags,
        round: Option.getOrElse(flags.round, () => `push-${Date.now()}`),
      });
    if (flags.action === "thread-cpu")
      return yield* threadCpu({
        ...flags,
        variants,
        round: Option.getOrElse(flags.round, () => `thread-cpu-${Date.now()}`),
      });
    yield* run({
      ...flags,
      variants,
      round: Option.getOrElse(flags.round, () => `round-${Date.now()}`),
    });
  }),
);
