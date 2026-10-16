import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Command from "effect/cli/Command";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";

import { Cloudflare } from "./cloudflare.ts";
import { command } from "./profile.ts";

NodeRuntime.runMain(
  Command.run(command.pipe(Command.provide(Cloudflare.layer)), { version: "1.0.0" }).pipe(
    Effect.provide([FetchHttpClient.layer, NodeServices.layer]),
  ),
);
