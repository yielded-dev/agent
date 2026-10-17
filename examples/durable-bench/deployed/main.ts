import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Effect } from "effect";
import { Command } from "effect/cli";

import { command } from "./command.ts";

BunRuntime.runMain(
  Command.run(command, { version: "1.0.0" }).pipe(Effect.provide(BunServices.layer)),
);
