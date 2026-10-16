import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { makeWorkerRuntime } from "./worker-host.ts";

/** Canonical worker operations constructed once by the durable coordinator. */
export class WorkerRuntime extends Context.Service<
  WorkerRuntime,
  Effect.Success<ReturnType<typeof makeWorkerRuntime>>
>()("@effect-agent/thread/internal/WorkerRuntime") {}
