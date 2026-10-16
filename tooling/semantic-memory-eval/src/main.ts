import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Cause from "effect/Cause";
import * as Command from "effect/cli/Command";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";

import { command } from "./command.ts";

const program = Command.run(command, { version: "0.0.0" }).pipe(
  Effect.tapCause((cause) => Console.error(Cause.pretty(cause))),
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
