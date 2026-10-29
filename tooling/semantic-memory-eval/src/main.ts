import { NodeRuntime, NodeServices } from "@effect/platform-node";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { Cause, Console, Effect } from "effect";
import { Command } from "effect/cli";

import { command } from "./command.ts";

const program = Command.run(command, { version: "0.0.0" }).pipe(
  Effect.tapCause((cause) => Console.error(Cause.pretty(cause))),
  Effect.scoped,
  Effect.provide([NodeServices.layer, NodeCrypto.layerDigest]),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
