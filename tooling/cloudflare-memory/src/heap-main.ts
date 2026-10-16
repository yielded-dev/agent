import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Command from "effect/cli/Command";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Socket from "effect/socket/Socket";

import { command } from "./heap.ts";

NodeRuntime.runMain(
  Command.run(command, { version: "0.0.0" }).pipe(
    Effect.scoped,
    Effect.provide([
      NodeServices.layer,
      FetchHttpClient.layer,
      Socket.layerWebSocketConstructorGlobal,
    ]),
  ),
);
