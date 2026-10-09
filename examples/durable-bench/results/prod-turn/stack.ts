import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect } from "effect";

// One private Alchemy stage per physical Worker. Upload prebuilt bytes unchanged.
export default Alchemy.Stack(
  "prod-turn",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const kind = yield* Config.NonEmptyString("PROD_TURN_KIND");

    const env: Record<string, unknown> = {
      TOKEN: Config.Redacted("PROD_TURN_TOKEN"),
      VERSION: Cloudflare.VersionMetadata(),
      PHASE: yield* Config.String("PROD_TURN_PHASE").pipe(Config.withDefault("seed")),
      PROVIDER_URL: yield* Config.String("PROD_TURN_PROVIDER").pipe(Config.withDefault("")),
      // Force an upload for repeated startup controls without changing module bytes.
      UPLOAD_REVISION: yield* Config.String("PROD_TURN_UPLOAD_REVISION").pipe(
        Config.withDefault(""),
      ),
    };

    if (kind === "network") {
      env.YIELDED = Cloudflare.DurableObject("prod-turn-yielded", {
        className: "NetworkYieldedDO",
      });
      env.PI = Cloudflare.DurableObject("prod-turn-pi", { className: "NetworkPiDO" });
    }

    const worker = yield* Cloudflare.Worker("prod-turn-worker", {
      name: yield* Config.NonEmptyString("PROD_TURN_WORKER"),
      main: yield* Config.NonEmptyString("PROD_TURN_BUNDLE"),
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
