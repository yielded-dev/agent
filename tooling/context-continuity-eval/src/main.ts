import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Command from "effect/cli/Command";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { command } from "./command.ts";
import { EvaluationError } from "./contracts.ts";

const program = Command.run(command, { version: "1.0.0" }).pipe(
  Effect.tapError((error) =>
    Console.error(
      Schema.is(EvaluationError)(error)
        ? `${error.stage}: ${error.message}`
        : `Context evaluation failed (${error._tag}). Check --help and the required environment configuration.`,
    ),
  ),
  Effect.scoped,
  Effect.provide(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
