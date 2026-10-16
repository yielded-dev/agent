import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";

import { memoryActivityWorker } from "./memory-activity-worker.ts";

NodeRuntime.runMain(memoryActivityWorker.pipe(Effect.provide(NodeServices.layer)), {
  disableErrorReporting: false,
});
