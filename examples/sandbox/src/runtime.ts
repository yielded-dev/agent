import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import type { CreateResponse } from "@effect/ai-openai/Generated";
import { Agent } from "@yielded/agent";
import { hostedModelAccess } from "@yielded/agent-example-auth/server";
import { ThreadMaintenance } from "@yielded/agent-platform-cloudflare/alarm";
import { ThreadObjectIdentity } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import * as ThreadObject from "@yielded/agent-platform-cloudflare/thread-object";
import { DurableAgentRuntime } from "@yielded/agent/durable-agent-runtime";
import { ThreadWorkDiscovery } from "@yielded/agent/thread-work";
import { Effect, Layer, Schema } from "effect";
import { DurableObject, WorkerEnvironment } from "effect-cf";
import { Model } from "effect/ai";
import { FetchHttpClient, HttpClient, HttpClientError, HttpClientRequest } from "effect/http";

import { codingAgent, definitions } from "./agent.ts";
import { SandboxError } from "./contract.ts";
import { codingToolsLive } from "./tools.ts";
import { Workspace } from "./workspace.ts";

const Address = Schema.String.check(Schema.isPattern(/^([0-9a-f-]{36})--([0-9a-f-]{36})$/i));

export const ownerOf = (threadId: string) => Schema.decodeSync(Address)(threadId).split("--")[0];

const provider = Layer.effect(
  OpenAiClient.OpenAiClient,
  Effect.gen(function* () {
    const env = yield* WorkerEnvironment;
    const identity = yield* ThreadObjectIdentity;

    return yield* OpenAiClient.make({
      transformClient: (client) =>
        HttpClient.mapRequestEffect(client, (request) =>
          hostedModelAccess(ownerOf(identity.threadId), env).pipe(
            Effect.flatMap((allowed) =>
              allowed && env.OPENAI_API_KEY
                ? Effect.succeed(HttpClientRequest.bearerToken(request, env.OPENAI_API_KEY))
                : Effect.fail(
                    SandboxError.make({
                      message: "Hosted model access is not available for this account.",
                    }),
                  ),
            ),
            Effect.mapError(
              (error) =>
                new HttpClientError.HttpClientError({
                  reason: new HttpClientError.TransportError({
                    request,
                    description: error.message,
                  }),
                }),
            ),
          ),
        ),
    });
  }),
).pipe(Layer.provide(FetchHttpClient.layer));

// The provider passes through this Responses option; its Config type omits it.
const modelConfig: typeof OpenAiLanguageModel.Config.Service &
  Pick<CreateResponse, "parallel_tool_calls"> = {
  reasoning: { effort: "low" },
  max_output_tokens: 16_384,
  parallel_tool_calls: false,
};

const model = Model.make(
  "openai",
  "gpt-6.1-sol",
  OpenAiLanguageModel.layer({
    model: "gpt-6.1-sol",
    config: modelConfig,
  }).pipe(Layer.provide(provider)),
);

const registered = DurableAgentRuntime.layerRegistered([
  { agent: Agent.withModel(codingAgent, model), definitions },
]).pipe(Layer.provide(codingToolsLive));

// Share the owner's existing storage with tools; alarms use the registered runtime.
const owner = ThreadWorkDiscovery.layer.pipe(Layer.provideMerge(ThreadObject.layer([])));

const application = Layer.fresh(ThreadMaintenance.layer).pipe(
  Layer.provideMerge(registered),
  Layer.provideMerge(owner),
  Layer.provideMerge(Workspace.layer),
);

const pathInWorkspace = (path: string) => {
  const components = path.split("/").filter((part) => part !== "" && part !== ".");

  return path.startsWith("/") || components.includes("..") || path.includes("\0")
    ? Effect.fail(SandboxError.make({ message: "Choose a relative workspace path." }))
    : Effect.succeed(`/workspace/${components.join("/")}`);
};

export class CodingThread extends ThreadObject.make(application, {
  namespaceBinding: "THREADS",
  deploymentId: "sandbox-v1",
  producerPrefix: "sandbox",
  maxQueueDepthPerLane: 2,
  maxInputBytes: 16 * 1024,
}) {
  files(path: string) {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const sandbox = yield* (yield* Workspace).existing;

        if (sandbox === null) return [];
        const directory = yield* pathInWorkspace(path);
        const description = yield* sandbox.describe;

        if (!description.running) return [];
        const entries = yield* sandbox.readDirectory(directory);

        return entries.slice(0, 200).map((entry) => ({
          path: `${path ? `${path}/` : ""}${entry.name}`,
          directory: entry.type === "directory",
        }));
      }),
    );
  }
  file(path: string) {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const sandbox = yield* (yield* Workspace).existing;

        if (sandbox === null)
          return yield* SandboxError.make({ message: "This workspace has not been created." });
        const response = yield* sandbox.readFile(yield* pathInWorkspace(path));

        if (!response.body) return { path, content: "", truncated: false };
        const reader = response.body.getReader();

        return yield* Effect.acquireUseRelease(
          Effect.succeed(reader),
          (reader) =>
            Effect.gen(function* () {
              const chunks: Uint8Array[] = [];
              let length = 0;
              let truncated = false;

              while (true) {
                const next = yield* Effect.tryPromise(() => reader.read());

                if (next.done) break;
                const remaining = 64 * 1024 - length;

                chunks.push(next.value.subarray(0, remaining));
                length += Math.min(remaining, next.value.length);
                if (next.value.length > remaining || length === 64 * 1024) {
                  truncated = true;
                  break;
                }
              }
              const bytes = new Uint8Array(length);
              let offset = 0;

              for (const chunk of chunks) {
                bytes.set(chunk, offset);
                offset += chunk.length;
              }

              return { path, content: new TextDecoder().decode(bytes), truncated };
            }),
          (reader) =>
            Effect.promise(() => reader.cancel()).pipe(
              Effect.ensuring(Effect.sync(() => reader.releaseLock())),
            ),
        );
      }),
    );
  }
}
