import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { workflowCrashWorker } from "./workflow-worker.ts";

NodeRuntime.runMain(
  workflowCrashWorker.pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  { disableErrorReporting: true },
);
