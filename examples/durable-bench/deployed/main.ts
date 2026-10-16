import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Effect } from "effect";
import { Command } from "effect/cli";

import { Cloudflare } from "./cloudflare.ts";
import { command } from "./command.ts";

BunRuntime.runMain(
  Command.run(command.pipe(Command.provide(Cloudflare.layer)), { version: "1.0.0" }).pipe(
    Effect.provide(BunServices.layer),
  ),
);
