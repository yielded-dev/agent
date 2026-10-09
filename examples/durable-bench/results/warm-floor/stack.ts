import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect } from "effect";

// One private Alchemy stage per physical Worker. Upload prebuilt bytes unchanged.
export default Alchemy.Stack(
  "warm-floor",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const kind = yield* Config.NonEmptyString("WARM_FLOOR_KIND");

    const env: Record<string, unknown> = {
      TOKEN: Config.Redacted("WARM_FLOOR_TOKEN"),
      VERSION: Cloudflare.VersionMetadata(),
      PHASE: yield* Config.String("WARM_FLOOR_PHASE").pipe(Config.withDefault("seed")),
      PROVIDER_URL: yield* Config.String("WARM_FLOOR_PROVIDER").pipe(Config.withDefault("")),
      // Force an upload for repeated startup controls without changing module bytes.
      UPLOAD_REVISION: yield* Config.String("WARM_FLOOR_UPLOAD_REVISION").pipe(
        Config.withDefault(""),
      ),
    };

    if (kind === "network") {
      env.YIELDED = Cloudflare.DurableObject("warm-floor-yielded", {
        className: "NetworkYieldedDO",
      });
      env.PI = Cloudflare.DurableObject("warm-floor-pi", { className: "NetworkPiDO" });
    }

    const worker = yield* Cloudflare.Worker("warm-floor-worker", {
      name: yield* Config.NonEmptyString("WARM_FLOOR_WORKER"),
      main: yield* Config.NonEmptyString("WARM_FLOOR_BUNDLE"),
      bundle: false,
      rules: [],
      compatibility: {
        date: "2026-08-18",
        flags: ["nodejs_compat", "global_fetch_strictly_public"],
      },
      limits: { cpuMs: 300_000 },
      workersDev: { enabled: true, previewsEnabled: false },
      ...(kind === "network" ? { placement: { region: "aws:us-west-1" } } : {}),
      observability: {
        enabled: true,
        headSamplingRate: 1,
        logs: { enabled: true, invocationLogs: true },
        traces: { enabled: false },
      },
      env,
    });

    return {
      workerName: worker.workerName,
      url: worker.url,
      namespaces: worker.durableObjectNamespaces,
    };
  }),
);
