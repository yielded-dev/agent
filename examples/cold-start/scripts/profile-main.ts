import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect } from "effect";
import { Command } from "effect/cli";
import { FetchHttpClient } from "effect/http";

import { Cloudflare } from "./cloudflare.ts";
import { command } from "./profile.ts";

NodeRuntime.runMain(
  Command.run(command.pipe(Command.provide(Cloudflare.layer)), { version: "1.0.0" }).pipe(
    Effect.provide([FetchHttpClient.layer, NodeServices.layer]),
  ),
);
