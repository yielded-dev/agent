import { BrowserCrypto } from "@effect/platform-browser";
import {
  authenticateRequest,
  hostedModelAccess,
  type AuthEnvironment,
} from "@yielded/agent-example-auth/server";
import { CloudflareThreadClient } from "@yielded/agent-platform-cloudflare/cloudflare-thread-client";
import { digestDefinitions } from "@yielded/agent/digest";
import { ThreadId, SubmissionId, ToolCallId } from "@yielded/agent/identifiers";
import { CanonicalSequence, type CanonicalRecordEnvelope } from "@yielded/agent/records";
import { toolCallSettledRecordId } from "@yielded/agent/run-journal";
import { AbortCommand, Principal, IdempotencyKey } from "@yielded/agent/submission-ledger";
import { makeSandboxAgentContainer, makeSandboxAgentOutbound } from "@yielded/sandbox/cloudflare";
import { Cause, Deferred, Effect, Layer, Schedule, Schema, Stream } from "effect";
import { Rpc, Worker, WorkerEnvironment } from "effect-cf";
import { Prompt } from "effect/ai";
import * as HttpServerResponse from "effect/http/HttpServerResponse";

import { codingAgent, definitions } from "./agent.ts";
import {
  CodingInput,
  CODING_ADMISSION_GROUP,
  CommandAccepted,
  CommandInput,
  EventPage,
  LIVE_TEXT_THREAD_HEADER,
  MAX_INPUT_BYTES,
  MessageInput,
  WorkspaceId,
  type Entry,
} from "./contract.ts";
import { workspaceIdFromPath } from "./location.ts";
import { CodingThread } from "./runtime.ts";

export { CodingThread };

const containerOptions = {
  namespaceBinding: "SANDBOXES",
  idleMillis: 1_800_000,
  allowedHosts: () => ["registry.npmjs.org"],
};

export class CodingSandbox extends makeSandboxAgentContainer(containerOptions) {}
export class SandboxOutbound extends makeSandboxAgentOutbound(containerOptions) {}

declare global {
  namespace Cloudflare {
    interface Env extends AuthEnvironment {
      THREADS: DurableObjectNamespace<CodingThread>;
      SANDBOXES: DurableObjectNamespace<CodingSandbox>;
      ASSETS: Fetcher;
      RUN_LIMIT: RateLimit;
      OPENAI_API_KEY: string;
      AUTH_YIELDED_CLIENT_SECRET: string;
    }
  }
}

const json = (value: unknown, status = 200) =>
  Response.json(value, { status, headers: { "cache-control": "no-store" } });

const boundedBody = Effect.fn("Sandbox.body")(function* (request: Request) {
  if (!request.body) return "";
  const reader = request.body.getReader();

  return yield* Effect.acquireUseRelease(
    Effect.succeed(reader),
    (reader) =>
      Effect.gen(function* () {
        const decoder = new TextDecoder();
        let text = "";
        let bytes = 0;

        while (true) {
          const next = yield* Effect.tryPromise(() => reader.read());

          if (next.done) return text + decoder.decode();
          bytes += next.value.byteLength;
          if (bytes > MAX_INPUT_BYTES) return yield* Effect.fail("body-too-large" as const);
          text += decoder.decode(next.value, { stream: true });
        }
      }),
    (reader) =>
      Effect.promise(() => reader.cancel()).pipe(
        Effect.ensuring(Effect.sync(() => reader.releaseLock())),
      ),
  );
});

const FailedWorkspaceOperation = Schema.Struct({ errorTag: Schema.Literal("CodingToolError") });

/** Project canonical records; the UI owns no second execution journal. */
const entriesForRecord = ({ sequence, record }: CanonicalRecordEnvelope): Entry[] => {
  const id = String(sequence);
  const p = record.payload;

  switch (p._tag) {
    case "UserInputRecorded":
      if (!Schema.is(CodingInput)(p.input)) return [];

      return [
        {
          id,
          kind: "user",
          text: p.input.message,
          ...(p.input.settings === undefined ? {} : { settings: p.input.settings }),
          requestId: p.input.requestId,
          submissionId: p.submissionId,
          ...(p.runId === undefined ? {} : { runId: p.runId }),
        },
      ];
    case "RunStarted":
      return [{ id, kind: "status", text: "Running", runId: p.runId }];
    case "ModelResponseRecorded": {
      const prompt = Schema.decodeSync(Prompt.Prompt)(p.messages);

      return prompt.content.flatMap((message, messageIndex) =>
        message.role !== "assistant"
          ? []
          : message.content.flatMap((part, index): Entry[] => {
              if (part.type === "text" || part.type === "reasoning")
                return part.text.length === 0
                  ? []
                  : [
                      {
                        id: `${id}-${messageIndex}-${index}`,
                        kind: part.type === "text" ? "assistant" : "reasoning",
                        text: part.text,
                        runId: p.runId,
                        turnId: p.turnId,
                      },
                    ];
              if (part.type === "tool-call")
                return [
                  {
                    id: `${id}-${messageIndex}-${index}`,
                    kind: "tool",
                    name: part.name,
                    text: JSON.stringify(part.params, null, 2),
                    toolCallId: part.id,
                    toolResultId: toolCallSettledRecordId(
                      p.runId,
                      p.turn,
                      ToolCallId.make(part.id),
                    ),
                    runId: p.runId,
                    turnId: p.turnId,
                  },
                ];

              return [];
            }),
      );
    }
    case "ToolCallSettled":
      return [
        {
          id,
          kind: "result",
          name: p.toolName,
          text: JSON.stringify(p.result, null, 2),
          failed: p.isFailure,
          toolCallId: p.toolCallId,
          toolResultId: record.recordId,
          runId: p.runId,
        },
      ];
    case "ToolCallUnknown":
      return [
        {
          id,
          kind: "status",
          text: "Unknown",
          failed: true,
          runId: p.runId,
          toolCallId: p.toolCallId,
        },
      ];
    case "SubmissionSettled":
      return [
        {
          id,
          kind: "status",
          text:
            p.outcome === "completed"
              ? "Completed"
              : p.outcome === "aborted"
                ? "Stopped"
                : "Failed",
          submissionId: p.submissionId,
          ...(p.runId === undefined ? {} : { runId: p.runId }),
          failed: p.outcome === "failed",
          reviewRequired: p.outcome === "failed" && Schema.is(FailedWorkspaceOperation)(p.result),
        },
      ];
    default:
      return [];
  }
};

const entriesFor = (record: CanonicalRecordEnvelope): Entry[] =>
  entriesForRecord(record).map((entry) => ({ ...entry, sequence: record.sequence }));

const handle = Effect.fn("Sandbox.fetch")(function* () {
  const request = yield* Worker.NativeRequest;
  const env = yield* WorkerEnvironment;
  const url = new URL(request.url);

  if (url.pathname === "/sandbox") return Response.redirect(`${url.origin}/sandbox/`, 308);
  if (!url.pathname.startsWith("/sandbox/")) return new Response("Not found", { status: 404 });
  if (!url.pathname.startsWith("/sandbox/api/")) {
    const document = url.pathname === "/sandbox/" || workspaceIdFromPath(url.pathname) !== null;

    if (!document && !url.pathname.startsWith("/sandbox/assets/"))
      return new Response("Workspace not found", { status: 404 });
    const assetUrl = new URL(request.url);

    if (document) assetUrl.pathname = "/sandbox/";
    const asset = yield* Effect.promise(() => env.ASSETS.fetch(new Request(assetUrl, request)));
    const headers = new Headers(asset.headers);

    headers.set("x-content-type-options", "nosniff");
    headers.set("referrer-policy", "same-origin");
    headers.set(
      "content-security-policy",
      "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'",
    );

    return new Response(asset.body, { status: asset.status, headers });
  }
  const session = yield* authenticateRequest(request, env);

  if (request.method !== "GET" && request.headers.get("origin") !== url.origin)
    return json({ message: "Origin not allowed." }, 403);
  const clientLayer = CloudflareThreadClient.layerFromBinding({ namespace: env.THREADS });

  const address = (id: string) =>
    ThreadId.make(`${session.accountId.toLowerCase()}--${id.toLowerCase()}`);

  if (request.method === "POST" && url.pathname === "/sandbox/api/commands") {
    const input = yield* Schema.decodeEffect(
      Schema.fromJsonString(Schema.Struct({ workspaceId: WorkspaceId, ...CommandInput.fields })),
    )(yield* boundedBody(request));

    const limit = yield* Effect.promise(() => env.RUN_LIMIT.limit({ key: session.accountId }));

    if (!limit.success)
      return json({ message: "Please wait a moment before running another command." }, 429);

    const reply = yield* Effect.tryPromise(
      async () => await env.THREADS.getByName(address(input.workspaceId)).command(input),
    );

    const decoded = yield* Schema.decodeEffect(CommandAccepted)(reply);

    return "error" in decoded
      ? json({ message: decoded.error.message }, decoded.error.status)
      : json(decoded.command, 202);
  }
  if (request.method === "POST" && url.pathname === "/sandbox/api/stop-command") {
    const input = yield* Schema.decodeEffect(
      Schema.fromJsonString(Schema.Struct({ workspaceId: WorkspaceId, requestId: WorkspaceId })),
    )(yield* boundedBody(request));

    const stopped = yield* Effect.tryPromise(() =>
      env.THREADS.getByName(address(input.workspaceId)).stopCommand(input.requestId),
    );

    return stopped ? json({}) : json({ message: "The command has not been accepted yet." }, 409);
  }

  if (request.method === "POST" && url.pathname === "/sandbox/api/messages") {
    const input = yield* Schema.decodeEffect(Schema.fromJsonString(MessageInput))(
      yield* boundedBody(request),
    );

    const limit = yield* Effect.promise(() => env.RUN_LIMIT.limit({ key: session.accountId }));

    if (!limit.success)
      return json({ message: "Please wait a moment before sending another request." }, 429);
    if (!(yield* hostedModelAccess(session.accountId, env)))
      return json({ message: "Hosted model access is not available for this account." }, 403);

    const receipt = yield* Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.submit(
        { definition: codingAgent },
        {
          requestId: input.requestId,
          message: input.message,
          ...(input.settings === undefined ? {} : { settings: input.settings }),
        },
        {
          threadId: address(input.workspaceId),
          principal: Principal.make(session.accountId),
          idempotencyKey: IdempotencyKey.make(input.requestId),
          ...(input.settings === undefined ? {} : { admissionGroup: CODING_ADMISSION_GROUP }),
          definitions: yield* digestDefinitions(definitions(input.settings)).pipe(
            Effect.provide(BrowserCrypto.layer),
          ),
        },
      );
    }).pipe(Effect.provide(clientLayer));

    return json({ submissionId: receipt.submissionId }, 202);
  }
  if (request.method === "POST" && url.pathname === "/sandbox/api/stop") {
    const input = yield* Schema.decodeEffect(
      Schema.fromJsonString(
        Schema.Struct({ workspaceId: WorkspaceId, submissionId: SubmissionId }),
      ),
    )(yield* boundedBody(request));

    yield* Effect.flatMap(CloudflareThreadClient, (client) =>
      client.abort(
        address(input.workspaceId),
        AbortCommand.make({
          submissionId: input.submissionId,
          author: session.accountId,
          reason: "Stopped by the workspace owner.",
        }),
      ),
    ).pipe(Effect.provide(clientLayer));

    return json({});
  }
  if (request.method === "GET") {
    const workspaceId = yield* Schema.decodeEffect(WorkspaceId)(
      url.searchParams.get("workspaceId") ?? "",
    );

    const threadId = address(workspaceId);

    if (url.pathname === "/sandbox/api/commands")
      return json(yield* Effect.tryPromise(() => env.THREADS.getByName(threadId).commands()));
    if (url.pathname === "/sandbox/api/command-result") {
      const requestId = yield* Schema.decodeEffect(WorkspaceId)(
        url.searchParams.get("requestId") ?? "",
      );

      return json(
        yield* Effect.tryPromise(() => env.THREADS.getByName(threadId).commandResult(requestId)),
      );
    }

    if (url.pathname === "/sandbox/api/text") {
      const text = Stream.unwrap(
        Effect.map(CloudflareThreadClient, (client) => client.watchTextBytes(threadId)),
      ).pipe(Stream.provide(clientLayer));

      return HttpServerResponse.stream(text, {
        headers: {
          "content-type": "application/x-ndjson",
          "cache-control": "no-store",
          [LIVE_TEXT_THREAD_HEADER]: threadId,
        },
      });
    }
    if (url.pathname === "/sandbox/api/events") {
      const after = yield* Schema.decodeEffect(CanonicalSequence)(
        Number(url.searchParams.get("after") ?? "0"),
      );

      const bodyClosed = yield* Deferred.make<void>();

      const commandChanges = Stream.unwrap(
        Effect.gen(function* () {
          type CommandEvents = Awaited<
            ReturnType<DurableObjectStub<CodingThread>["commandEvents"]>
          >;
          let opening: Promise<CommandEvents> | undefined;
          let received: CommandEvents | undefined;
          let closed = false;

          const cancel = () => {
            const observation = received;

            received = undefined;

            try {
              observation?.lifetime[Symbol.dispose]();
            } catch {
              // Disposing the opening RPC may already have revoked this capability.
            }
            const body = observation?.stream;

            return body && !body.locked ? body.cancel().catch(() => undefined) : Promise.resolve();
          };

          // Own the original RPC before awaiting it, including a body returned after cancellation.
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              closed = true;
            }).pipe(
              Effect.andThen(Rpc.dispose(opening).pipe(Effect.ignoreCause)),
              Effect.andThen(
                Effect.promise(cancel).pipe(
                  Effect.interruptible,
                  Effect.timeout("1 second"),
                  Effect.ignore,
                ),
              ),
            ),
          );

          const observation = yield* Effect.tryPromise(() => {
            opening = env.THREADS.getByName(threadId).commandEvents();

            return opening.then((value) => {
              received = value;

              return closed ? cancel().then(() => value) : value;
            });
          }).pipe(Effect.interruptible);

          return Stream.fromReadableStream({
            evaluate: () => observation.stream,
            releaseLockOnEnd: true,
            onError: (cause) => new Cause.UnknownError(cause),
          });
        }),
      ).pipe(
        Stream.decodeText(),
        Stream.splitLines,
        Stream.map((line) => `${line}\n`),
        Stream.encodeText,
        // Command hints can lag a deployment without closing canonical history.
        Stream.catch(() => Stream.empty),
        Stream.repeat(Schedule.spaced("2 seconds")),
      );

      const pages = Stream.unwrap(
        Effect.gen(function* () {
          const client = yield* CloudflareThreadClient;

          return Stream.unfold({ cursor: after, wait: false }, ({ cursor, wait }) =>
            Effect.gen(function* () {
              if (wait) yield* client.awaitProgress(threadId, cursor);

              const records = yield* client
                .readPage(threadId, { afterSequence: cursor, limit: 64 })
                .pipe(
                  Effect.catchTag("ThreadNotMaterialized", (error) =>
                    cursor === 0 ? Effect.succeed([]) : Effect.fail(error),
                  ),
                );

              const next = records.at(-1)?.sequence ?? cursor;

              const page: EventPage = {
                cursor: next,
                entries: records.flatMap(entriesFor),
                committedTurns: records.flatMap(({ record: { payload } }) =>
                  payload._tag === "ModelResponseRecorded"
                    ? [{ runId: payload.runId, turnId: payload.turnId }]
                    : [],
                ),
              };

              return [page, { cursor: next, wait: records.length < 64 }] as const;
            }),
          );
        }),
      ).pipe(
        Stream.provide(clientLayer),
        Stream.map((page) =>
          new TextEncoder().encode(
            `${Schema.encodeSync(Schema.fromJsonString(EventPage))(page)}\n`,
          ),
        ),
        Stream.merge(commandChanges, { haltStrategy: "either" }),
        // Both sources emit complete NDJSON lines; idle writes expose disconnects.
        Stream.merge(
          Stream.tick("15 seconds").pipe(
            Stream.drop(1),
            Stream.map(() => new Uint8Array([10])),
          ),
          { haltStrategy: "left" },
        ),
        // The Worker response bridge does not forward the native request signal.
        Stream.interruptWhen(
          Effect.callback<void>((resume) => {
            const onAbort = () => resume(Effect.void);

            if (request.signal.aborted) onAbort();
            else request.signal.addEventListener("abort", onAbort, { once: true });

            return Effect.sync(() => request.signal.removeEventListener("abort", onAbort));
          }),
        ),
        Stream.onExit(() => Deferred.succeed(bodyClosed, undefined)),
      );

      const context = yield* Worker.WorkerContext;

      // Retain the request while body cleanup cancels the outstanding progress RPC.
      yield* context.waitUntil(Deferred.await(bodyClosed));

      return HttpServerResponse.stream(pages, {
        headers: { "content-type": "application/x-ndjson", "cache-control": "no-store" },
      });
    }
    const path = url.searchParams.get("path") ?? "";

    if (path.length > 1024) return json({ message: "Path too long." }, 400);
    if (url.pathname === "/sandbox/api/files")
      return json(yield* Effect.tryPromise(() => env.THREADS.getByName(threadId).files(path)));
    if (url.pathname === "/sandbox/api/file")
      return json(yield* Effect.tryPromise(() => env.THREADS.getByName(threadId).file(path)));
  }

  return new Response("Not found", { status: 404 });
});

export default Worker.make(Layer.empty, {
  fetch: handle().pipe(
    Effect.catchTags({
      AccountError: Effect.fnUntraced(function* (error) {
        if (error.code === "unavailable")
          yield* Effect.logError("Sandbox authentication unavailable");

        return json({ message: error.message }, error.code === "unauthorized" ? 401 : 503);
      }),
      SchemaError: () => Effect.succeed(json({ message: "Invalid request." }, 400)),
      AdmissionConflict: () =>
        Effect.succeed(
          json({ message: "This message ID already belongs to different input." }, 409),
        ),
      AdmissionLimitExceeded: (error) =>
        Effect.succeed(
          json(
            { message: "The request exceeds this workspace's limits." },
            error.limit === "input-bytes" ? 413 : 429,
          ),
        ),
      AdmissionPolicyError: (error) =>
        Effect.succeed(
          json(
            {
              message:
                error.code === "sandbox-reload"
                  ? "Reload this page to choose model settings before sending a new message."
                  : error.code === "admission-group"
                    ? "Wait for the current message to finish before sending another."
                    : error.code === "sandbox-shell-active"
                      ? "Wait for the current command to finish before messaging the agent."
                      : "This workspace is not accepting new work.",
            },
            error.reason === "unavailable" ? 503 : 409,
          ),
        ),
    }),
    Effect.catch(
      Effect.fnUntraced(function* (error) {
        if (error === "body-too-large") return json({ message: "Request body too large." }, 413);
        yield* Effect.logError("Sandbox request failed", { errorTag: error._tag });

        return json(
          {
            message:
              "The request could not be completed. Reconnect to check its state before retrying.",
          },
          503,
        );
      }),
    ),
  ),
});
