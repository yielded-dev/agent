import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect } from "effect";

// Reused cf-latency deployment boundary: private local state, exact prebuilt bytes.
export default Alchemy.Stack("wake-defer", {
  providers: Cloudflare.providers(), state: Alchemy.localState(),
}, Effect.gen(function* () {
  const kind = yield* Config.NonEmptyString("WAKE_DEFER_KIND");
  const name = yield* Config.NonEmptyString("WAKE_DEFER_WORKER");
  if (!name.startsWith("wake-defer-")) return yield* Effect.die("wake-defer resource prefix required");
  const env: Record<string, unknown> = {
    TOKEN: Config.Redacted("WAKE_DEFER_TOKEN"),
    VERSION: Cloudflare.VersionMetadata(),
    PHASE: yield* Config.String("WAKE_DEFER_PHASE").pipe(Config.withDefault("seed")),
    PROVIDER_URL: yield* Config.String("WAKE_DEFER_PROVIDER").pipe(Config.withDefault("")),
    TARGET_URL: yield* Config.String("WAKE_DEFER_TARGET").pipe(Config.withDefault("")),
    BUILD_MODE: yield* Config.NonEmptyString("WAKE_DEFER_BUILD_MODE"),
    BUILD_ID: yield* Config.NonEmptyString("WAKE_DEFER_BUILD_ID"),
  };
  if (kind === "network") env.YIELDED = Cloudflare.DurableObject("wake-defer-yielded", { className: "NetworkYieldedDO" });
  const worker = yield* Cloudflare.Worker("wake-defer-worker", {
    name, main: yield* Config.NonEmptyString("WAKE_DEFER_BUNDLE"), bundle: false, rules: [],
    compatibility: { date: "2026-08-18", flags: ["nodejs_compat", "global_fetch_strictly_public"] },
    ...(kind === "driver" ? { placement: { mode: "targeted" as const, region: "aws:us-west-1" } } : {}),
    limits: { cpuMs: 300_000 }, workersDev: { enabled: true, previewsEnabled: false },
    observability: { enabled: true, headSamplingRate: 1, logs: { enabled: true, invocationLogs: true }, traces: { enabled: false } },
    env,
  });
  return { workerName: worker.workerName, url: worker.url };
}));
