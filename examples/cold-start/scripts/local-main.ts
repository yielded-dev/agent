import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Command from "effect/cli/Command";
import * as Effect from "effect/Effect";

import { command } from "./local.ts";

NodeRuntime.runMain(
  Command.run(command, { version: "1.0.0" }).pipe(Effect.provide(NodeServices.layer)),
);
