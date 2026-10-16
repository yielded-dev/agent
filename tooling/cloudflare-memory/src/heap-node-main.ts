import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Argument from "effect/cli/Argument";
import * as Command from "effect/cli/Command";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { NodeSample } from "./heap-contracts.ts";
import { measureNode } from "./heap-node.ts";

const command = Command.make(
  "heap-node",
  {
    bundle: Argument.String("bundle").pipe(
      Argument.withDescription("Exact Wrangler JavaScript bundle to evaluate."),
    ),
  },
  ({ bundle }) =>
    measureNode(bundle).pipe(
      Effect.flatMap(Schema.encodeEffect(Schema.fromJsonString(NodeSample))),
      Effect.flatMap(Console.log),
    ),
);

NodeRuntime.runMain(
  Command.run(command, { version: "0.0.0" }).pipe(Effect.provide(NodeServices.layer)),
);
