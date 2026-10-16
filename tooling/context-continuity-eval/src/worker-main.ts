import process from "node:process";

import * as OpenAiClient from "@effect/ai-openai/OpenAiClient";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { EvaluationError } from "./contracts.ts";
import { runEvaluation } from "./evaluate.ts";
import { WorkerOptions } from "./process-host.ts";

const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const file = yield* Config.String("CONTEXT_EVAL_WORKER_OPTIONS");

  const options = yield* Schema.decodeEffect(Schema.fromJsonString(WorkerOptions))(
    yield* fs.readFileString(file),
  );

  const report = yield* runEvaluation({ ...options, processId: process.pid });

  if (report.status !== "passed")
    return yield* EvaluationError.make({ stage: "gate", message: "Worker evaluation failed" });
}).pipe(
  Effect.scoped,
  Effect.provide(
    Layer.mergeAll(
      OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") }).pipe(
        Layer.provide(FetchHttpClient.layer),
      ),
      NodeServices.layer,
      NodeCrypto.layer,
    ),
  ),
);

NodeRuntime.runMain(program);
