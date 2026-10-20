import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { hostedModelAccess } from "@yielded/agent-example-auth/server";
import { ThreadMaintenance } from "@yielded/agent-platform-cloudflare/alarm";
import { ThreadObjectIdentity } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import * as R2ToolResultStorage from "@yielded/agent-platform-cloudflare/r2-tool-result-storage";
import * as ThreadObject from "@yielded/agent-platform-cloudflare/thread-object";
import { ContextCompactor } from "@yielded/agent/context-compactor";
import * as ContextTools from "@yielded/agent/context-tools";
import { ModelCallContext } from "@yielded/agent/context-window";
import { DurableAgentRuntime } from "@yielded/agent/durable-agent-runtime";
import { SubmissionLedger, SubmissionLookupById } from "@yielded/agent/submission-ledger";
import * as ThreadContextHistory from "@yielded/agent/thread-context-history";
import { ThreadWorkDiscovery } from "@yielded/agent/thread-work";
import * as ToolResults from "@yielded/agent/tool-results";
import { Cause, Context, Effect, Layer, Option, Schema, Stream } from "effect";
import { DurableObject, Rpc, WorkerEnvironment } from "effect-cf";
import { AiError, LanguageModel, Model } from "effect/ai";
import { FetchHttpClient, HttpClient, HttpClientError, HttpClientRequest } from "effect/http";

import { codingAgent, definitions, modelContext, modelOptions } from "./agent.ts";
import { commandAdmission, commandMaintenance, ShellCommands } from "./commands.ts";
import {
  CodingInput,
  CommandInput,
  defaultModelSettings,
  MAX_INPUT_BYTES,
  type ModelSettings,
  SandboxError,
  SavedFiles,
  ShellCommand,
  WorkspaceEvent,
  WorkspaceId,
} from "./contract.ts";
import { WorkspacePreviews } from "./previews.ts";
import { codingReconciler, recoverWorkspace, workspaceRecoveryRequired } from "./recovery.ts";
import { snapshotLane, WorkspaceSnapshots } from "./snapshots.ts";
import { codingToolsLive } from "./tools.ts";
import { webLive } from "./web.ts";
import { Workspace } from "./workspace.ts";

const Address = Schema.String.check(Schema.isPattern(/^([0-9a-f-]{36})--([0-9a-f-]{36})$/i));

export const ownerOf = (threadId: string) => Schema.decodeSync(Address)(threadId).split("--")[0];

class CommandEventsLifetime extends Rpc.RpcTarget {
  readonly controller = new AbortController();

  [Symbol.dispose]() {
    this.controller.abort();
  }
}

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

class RunSettings extends Context.Service<
  RunSettings,
  { readonly read: Effect.Effect<ModelSettings | undefined, AiError.AiError> }
>()("sandbox-example/RunSettings") {}

const model = Layer.unwrap(
  Effect.gen(function* () {
    const selected = yield* (yield* RunSettings).read.pipe(Effect.result);

    if (selected._tag === "Failure")
      return Model.make(
        "openai",
        "unavailable",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.fail(selected.failure),
            streamText: () => Stream.fail(selected.failure),
          }),
        ),
      );

    const { model, ...config } = modelOptions(selected.success);

    return Layer.merge(
      OpenAiLanguageModel.model(model, config),
      Layer.succeed(ModelCallContext.current, modelContext(selected.success)),
    );
  }),
).pipe(Layer.provide(provider));

const registered = DurableAgentRuntime.layerRegistered([
  {
    agent: codingAgent,
    model,
    definitions: definitions(defaultModelSettings),
    attemptLayer: ({ submissionId, threadId }) =>
      Layer.effect(
        RunSettings,
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const snapshots = yield* WorkspaceSnapshots;
          const workspace = yield* Workspace;
          const scope = yield* Effect.scope;

          const read = yield* Effect.cached(
            Effect.gen(function* () {
              yield* snapshots.beforeExecution;
              const found = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));

              if (Option.isNone(found) || found.value.threadId !== threadId)
                return yield* SandboxError.make({ message: "The accepted input is unavailable." });

              const input = yield* Schema.decodeUnknownEffect(CodingInput)(
                found.value.inputPayload,
              );

              // Prewarm only an admitted attempt; workspace tools join creation when needed.
              yield* workspace.open.pipe(
                Effect.onError((cause) =>
                  Cause.hasInterruptsOnly(cause)
                    ? Effect.void
                    : Effect.logWarning("Background workspace startup failed."),
                ),
                Effect.forkIn(scope),
              );

              return input.settings;
            }).pipe(
              Effect.mapError((cause) =>
                AiError.make({
                  module: "Sandbox",
                  method: "modelSettings",
                  reason: Object.assign(
                    AiError.UnknownError.make({
                      description: "The admitted model settings could not be read.",
                    }),
                    { cause },
                  ),
                }),
              ),
            ),
          );

          return { read };
        }),
      ),
  },
]).pipe(
  Layer.provide(codingToolsLive),
  Layer.provide(webLive),
  Layer.provide(codingReconciler),
  Layer.provide(ContextTools.layer),
  Layer.provide(ToolResults.layer),
  Layer.provide(ThreadContextHistory.layer()),
  Layer.provide(ContextCompactor.layer),
);

// Share the owner's existing storage with tools; alarms use the registered runtime.
const owner = ThreadWorkDiscovery.layer.pipe(
  Layer.provideMerge(
    ThreadObject.layer([], {
      hostLanesForMutation: (mutation) => (mutation._tag === "Admission" ? [] : [snapshotLane]),
    }).pipe(Layer.provide(commandAdmission)),
  ),
);

const application = Layer.fresh(ThreadMaintenance.layer).pipe(
  Layer.provideMerge(commandMaintenance),
  Layer.provideMerge(ShellCommands.layer),
  Layer.provideMerge(registered),
  Layer.provideMerge(WorkspaceSnapshots.layer),
  Layer.provideMerge(WorkspacePreviews.layer),
  Layer.provideMerge(owner),
  Layer.provideMerge(Workspace.layer),
  Layer.provide(
    Layer.unwrap(
      Effect.gen(function* () {
        const env = yield* WorkerEnvironment;
        const { threadId } = yield* ThreadObjectIdentity;

        return R2ToolResultStorage.layer(env.BACKUPS, threadId);
      }),
    ),
  ),
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
  maxQueueDepthPerLane: 1,
  maxInputBytes: MAX_INPUT_BYTES,
}) {
  preview() {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        return { grant: yield* (yield* WorkspacePreviews).open };
      }).pipe(
        Effect.catchTag("PreviewRejected", (error) => Effect.succeed({ error: error.message })),
      ),
    );
  }
  fileAction(action: "save" | "restore") {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const snapshots = yield* WorkspaceSnapshots;

        yield* action === "save" ? snapshots.requestSave : snapshots.requestRestore;

        return { saved: yield* snapshots.status };
      }).pipe(
        Effect.catchTag("SnapshotRejected", (error) => Effect.succeed({ error: error.message })),
      ),
    );
  }
  recoveryRequired() {
    return this[DurableObject.RunSymbol](workspaceRecoveryRequired());
  }
  recoverWorkspace() {
    return this[DurableObject.RunSymbol](recoverWorkspace());
  }
  command(input: unknown) {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const decoded = yield* Schema.decodeUnknownEffect(CommandInput)(input);
        const command = yield* (yield* ShellCommands).accept(decoded);

        return { command };
      }).pipe(Effect.catchTag("CommandRejected", (error) => Effect.succeed({ error }))),
    );
  }
  commands() {
    return this[DurableObject.RunSymbol](
      Effect.flatMap(ShellCommands, (commands) => commands.list),
    );
  }
  commandEvents() {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const commands = yield* ShellCommands;
        const snapshots = yield* WorkspaceSnapshots;
        const workspace = yield* Workspace;

        const lifetime = new CommandEventsLifetime();

        const stream = commands.watch.pipe(
          Stream.mapEffect((commands) =>
            snapshots.status.pipe(Effect.map((saved) => ({ commands, saved }))),
          ),
          Stream.changesWith(
            Schema.toEquivalence(
              Schema.Struct({ commands: Schema.Array(ShellCommand), saved: SavedFiles }),
            ),
          ),
          Stream.map(({ saved }) => ({
            _tag: "CommandHistoryChanged" as const,
            saved,
          })),
          Stream.merge(
            workspace.starting.pipe(
              Stream.map(
                (starting) =>
                  ({
                    _tag: "CommandHistoryChanged",
                    starting,
                  }) as const,
              ),
            ),
          ),
          Stream.mapEffect((event) =>
            Schema.encodeEffect(Schema.fromJsonString(WorkspaceEvent))(event),
          ),
          Stream.map((changed) => `${changed}\n`),
          Stream.encodeText,
          Stream.toReadableStreamWith(yield* Effect.context(), {
            signal: lifetime.controller.signal,
          }),
        );

        // Idle RPC byte streams defer source cancellation until another write.
        return { stream, lifetime };
      }),
    );
  }
  commandResult(id: string) {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const requestId = yield* Schema.decodeEffect(WorkspaceId)(id);

        return yield* (yield* ShellCommands).wait(requestId);
      }),
    );
  }
  stopCommand(id: string) {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const requestId = yield* Schema.decodeEffect(WorkspaceId)(id);

        return yield* (yield* ShellCommands).stop(requestId);
      }),
    );
  }
  files(path: string) {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const { sandbox, expired } = yield* (yield* Workspace).inspect;
        const saved = yield* (yield* WorkspaceSnapshots).status;

        if (sandbox === null || expired) return { entries: [], expired, saved };
        const directory = yield* pathInWorkspace(path);
        const entries = yield* sandbox.readDirectory(directory);

        return {
          entries: entries
            .filter((entry) => !entry.name.startsWith(".sandbox-agent"))
            .slice(0, 200)
            .map((entry) => ({
              path: `${path ? `${path}/` : ""}${entry.name}`,
              directory: entry.type === "directory",
            })),
          expired: false,
          saved,
        };
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
