import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect } from "effect";

export default Alchemy.Stack("effect-eval-cost-turn", {
  providers: Cloudflare.providers(), state: Alchemy.localState(),
}, Effect.gen(function* () {
  const binding = Cloudflare.DurableObject("effect-eval-cost-turn-object", { className: "MeasuredYieldedDO" });
  const worker = yield* Cloudflare.Worker("effect-eval-cost-turn-worker", {
    name: yield* Config.NonEmptyString("EVAL_COST_WORKER"), main: yield* Config.NonEmptyString("EVAL_COST_BUNDLE"), bundle: false, rules: [],
    compatibility: { date: "2026-08-18", flags: ["nodejs_compat"] }, limits: { cpuMs: 300_000 },
    workersDev: { enabled: true, previewsEnabled: false },
    observability: { enabled: true, headSamplingRate: 1, logs: { enabled: true, invocationLogs: true }, traces: { enabled: false } },
    env: { TOKEN: Config.Redacted("EVAL_COST_TOKEN"), VERSION: Cloudflare.VersionMetadata(), PHASE: yield* Config.NonEmptyString("EVAL_COST_PHASE"), THREADS: binding },
  });
  return { workerName: worker.workerName, url: worker.url, namespaces: worker.durableObjectNamespaces };
}));
