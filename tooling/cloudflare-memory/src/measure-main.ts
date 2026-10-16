import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Command from "effect/cli/Command";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { command, measurementHttpLayer } from "./measure.ts";

NodeRuntime.runMain(
  Command.run(command, { version: "0.0.0" }).pipe(
    Effect.scoped,
    Effect.provide(Layer.mergeAll(measurementHttpLayer, NodeServices.layer)),
  ),
);
