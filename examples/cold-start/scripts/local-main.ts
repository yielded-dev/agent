import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect } from "effect";
import { Command } from "effect/cli";

import { command } from "./local.ts";

NodeRuntime.runMain(
  Command.run(command, { version: "1.0.0" }).pipe(Effect.provide(NodeServices.layer)),
);
