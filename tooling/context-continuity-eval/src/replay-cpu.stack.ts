import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect } from "effect";

import { ReplayCpuStage } from "./replay-cpu-contracts.ts";

/** Run each baseline/candidate/control stage from the same private state directory. */
export default Alchemy.Stack(
  "sync-do-append",
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const stage = yield* Config.schema(ReplayCpuStage, "REPLAY_CPU_RUN");

    const worker = yield* Cloudflare.Worker("sync-do-append-worker", {
      name: `sync-do-append-${stage}`,
      main: yield* Config.NonEmptyString("REPLAY_CPU_BUNDLE"),
      bundle: false,
      // The uploaded module must be byte-identical to the measured build, with no sibling files.
      rules: [],
      compatibility: { date: "2026-08-01", flags: ["nodejs_compat"] },
      workersDev: { enabled: true, previewsEnabled: false },
      limits: { cpuMs: 300_000 },
      observability: {
        enabled: true,
        headSamplingRate: 1,
        logs: { enabled: true, invocationLogs: true },
        traces: { enabled: false },
      },
      env: {
        REPLAY_CPU_RUN: stage,
        REPLAY_CPU_TOKEN: Config.Redacted("REPLAY_CPU_TOKEN"),
        REPLAY_CPU_VERSION: Cloudflare.VersionMetadata(),
        REPLAY_CPU_THREADS: Cloudflare.DurableObject("sync-do-append-threads", {
          className: "ReplayCpuThread",
        }),
      },
    });

    return {
      workerName: worker.workerName,
      url: worker.url,
      namespaces: worker.durableObjectNamespaces,
    };
  }),
);
