import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";

import { reviewActionProgram, withActionInputs } from "./action.ts";

const configProvider = withActionInputs(ConfigProvider.fromEnv());

NodeRuntime.runMain(
  reviewActionProgram.pipe(
    Effect.scoped,
    Effect.provideService(ConfigProvider.ConfigProvider, configProvider),
    Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer)),
  ),
  { disableErrorReporting: true },
);
