import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect } from "effect";

// One private Alchemy stage per physical Worker. Upload prebuilt bytes unchanged.
export default Alchemy.Stack("cf-latency", {
  providers: Cloudflare.providers(), state: Alchemy.localState(),
}, Effect.gen(function* () {
  const kind = yield* Config.NonEmptyString("CF_LATENCY_KIND");
  const env: Record<string, unknown> = {
    TOKEN: Config.Redacted("CF_LATENCY_TOKEN"),
    VERSION: Cloudflare.VersionMetadata(),
    PHASE: yield* Config.String("CF_LATENCY_PHASE").pipe(Config.withDefault("seed")),
    PROVIDER_URL: yield* Config.String("CF_LATENCY_PROVIDER").pipe(Config.withDefault("")),
  };
  if (kind === "probe") env.PROBES = Cloudflare.DurableObject("cf-latency-probes", { className: "ProbeDO" });
  if (kind === "network") {
    env.YIELDED = Cloudflare.DurableObject("cf-latency-yielded", { className: "NetworkYieldedDO" });
    env.PI = Cloudflare.DurableObject("cf-latency-pi", { className: "NetworkPiDO" });
    env.THREADS = Cloudflare.DurableObject("cf-latency-tardie-threads", { className: "NetworkThreadDO" });
    env.ACTORS = Cloudflare.DurableObject("cf-latency-tardie-actors", { className: "NetworkActorDO" });
  }
  const worker = yield* Cloudflare.Worker("cf-latency-worker", {
    name: yield* Config.NonEmptyString("CF_LATENCY_WORKER"),
    main: yield* Config.NonEmptyString("CF_LATENCY_BUNDLE"), bundle: false, rules: [],
    compatibility: { date: "2026-08-18", flags: ["nodejs_compat"] },
    limits: { cpuMs: 300_000 }, workersDev: { enabled: true, previewsEnabled: false },
    observability: { enabled: true, headSamplingRate: 1, logs: { enabled: true, invocationLogs: true }, traces: { enabled: false } },
    env,
  });
  return { workerName: worker.workerName, url: worker.url, namespaces: worker.durableObjectNamespaces };
}));
