import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Command from "effect/cli/Command";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";

import { command } from "./command.ts";
import { CURRENT_RUNNER_VERSION } from "./contracts.ts";

const localConfig = ConfigProvider.layerAdd(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const envFile = yield* path.fromFileUrl(new URL("../../../.env.local", import.meta.url));

    return (yield* fs.exists(envFile))
      ? yield* ConfigProvider.fromDotEnv({ path: envFile })
      : ConfigProvider.fromUnknown({});
  }),
);

const program = Command.run(command, { version: CURRENT_RUNNER_VERSION }).pipe(
  Effect.tapError((error) => Console.error(String(error))),
  Effect.scoped,
  Effect.provide(localConfig.pipe(Layer.provideMerge(NodeServices.layer))),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
