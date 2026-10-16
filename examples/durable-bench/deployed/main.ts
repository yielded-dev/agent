import * as BunRuntime from "@effect/platform-bun/BunRuntime";
import * as BunServices from "@effect/platform-bun/BunServices";
import * as Command from "effect/cli/Command";
import * as Effect from "effect/Effect";

import { Cloudflare } from "./cloudflare.ts";
import { command } from "./command.ts";

BunRuntime.runMain(
  Command.run(command.pipe(Command.provide(Cloudflare.layer)), { version: "1.0.0" }).pipe(
    Effect.provide(BunServices.layer),
  ),
);
