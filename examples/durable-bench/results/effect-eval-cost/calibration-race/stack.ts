import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect } from "effect";

// Adapted from tooling/context-continuity-eval/src/replay-cpu.stack.ts.
export default Alchemy.Stack("effect-eval-cost-race", {
  providers: Cloudflare.providers(), state: Alchemy.localState(),
}, Effect.gen(function* () {
  const name = yield* Config.NonEmptyString("EVAL_COST_WORKER");
  const kind = yield* Config.String("EVAL_COST_KIND").pipe(Config.withDefault("micro"));
  const binding = Cloudflare.DurableObject("effect-eval-cost-race-object", {
    className: kind === "micro" ? "Calibration" : "MeasuredYieldedDO",
  });
  const worker = yield* Cloudflare.Worker("effect-eval-cost-race-worker", {
    name, main: yield* Config.NonEmptyString("EVAL_COST_BUNDLE"), bundle: false, rules: [],
    compatibility: { date: "2026-08-18", flags: ["nodejs_compat"] },
    limits: { cpuMs: 300_000 },
    workersDev: { enabled: true, previewsEnabled: false },
    observability: {
      enabled: true, headSamplingRate: 1,
      logs: { enabled: true, invocationLogs: true }, traces: { enabled: false },
    },
    env: {
      TOKEN: Config.Redacted("EVAL_COST_TOKEN"), VERSION: Cloudflare.VersionMetadata(),
      ...(kind === "micro" ? { BENCH: binding } : { THREADS: binding }),
    },
  });
  return { workerName: worker.workerName, url: worker.url, namespaces: worker.durableObjectNamespaces };
}));
