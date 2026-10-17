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
import {
  makeSandboxAgentContainer,
  makeSandboxAgentOutbound,
  NativeSandboxNamespace,
} from "@yielded/sandbox/cloudflare";
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
  FileActionReply,
  LIVE_TEXT_THREAD_HEADER,
  MAX_INPUT_BYTES,
  MessageInput,
  PreviewReply,
  WorkspaceId,
  WorkspacePage,
  WorkspaceSummary,
  type Entry,
  type ShellCommand,
} from "./contract.ts";
import { workspaceIdFromPath } from "./location.ts";
import { WorkspacePreviewProxy } from "./preview-proxy.ts";
import { CodingThread } from "./runtime.ts";
import { AccountWorkspaces } from "./workspaces.ts";

export { CodingThread, AccountWorkspaces };
export { DirectoryBackupGateway } from "@yielded/sandbox/cloudflare";

const containerOptions = {
  namespaceBinding: "SANDBOXES",
  backupBucketBinding: "BACKUPS",
  idleMillis: 1_800_000,
  allowedHosts: () => ["registry.npmjs.org"],
};

export class CodingSandbox extends makeSandboxAgentContainer(containerOptions) {}
export class SandboxOutbound extends makeSandboxAgentOutbound(containerOptions) {}

declare global {
  namespace Cloudflare {
    interface Env extends AuthEnvironment {
      THREADS: DurableObjectNamespace<CodingThread>;
      WORKSPACES: DurableObjectNamespace<AccountWorkspaces>;
      SANDBOXES: DurableObjectNamespace<CodingSandbox>;
      ASSETS: Fetcher;
      BACKUPS: R2Bucket;
      PREVIEW_DOMAIN: string;
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

const RunFailure = Schema.Struct({ errorTag: Schema.String });

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
    case "SubmissionSettled": {
      const failure = Schema.is(RunFailure)(p.result) ? p.result.errorTag : undefined;

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
          reviewRequired: p.outcome === "failed" && failure === "CodingToolError",
          ...(p.outcome === "failed" &&
          (failure === "ContextBudgetError" || failure === "ContextOverflowError")
            ? {
                detail:
                  "The conversation exceeded this turn’s context limit. Send a new message to continue.",
              }
            : {}),
        },
      ];
    }
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
  const preview = yield* (yield* WorkspacePreviewProxy).handle(request);

  if (preview !== null) return preview;

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
    headers.set("referrer-policy", "strict-origin-when-cross-origin");
    headers.set(
      "content-security-policy",
      `default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src 'self'; frame-src https://*.${env.PREVIEW_DOMAIN}; frame-ancestors 'none'; base-uri 'self'; form-action 'self' https://*.${env.PREVIEW_DOMAIN}`,
    );

    return new Response(asset.body, { status: asset.status, headers });
  }
  const session = yield* authenticateRequest(request, env);

  if (request.method !== "GET" && request.headers.get("origin") !== url.origin)
    return json({ message: "Origin not allowed." }, 403);
  const clientLayer = CloudflareThreadClient.layerFromBinding({ namespace: env.THREADS });

  const address = (id: string) =>
    ThreadId.make(`${session.accountId.toLowerCase()}--${id.toLowerCase()}`);

  const directory = env.WORKSPACES.getByName(session.accountId.toLowerCase());
  const titleFor = (text: string) => text.replace(/\s+/g, " ").trim().slice(0, 120) || null;

  const remember = Effect.fnUntraced(function* (id: string, title: string | null) {
    return yield* Schema.decodeEffect(WorkspaceSummary)(
      yield* Effect.tryPromise(() => directory.remember({ id, title })),
    );
  });

  const firstPage = Effect.fnUntraced(function* (id: string) {
    const client = yield* CloudflareThreadClient;

    return yield* client
      .readPage(address(id), { afterSequence: CanonicalSequence.make(0), limit: 64 })
      .pipe(Effect.catchTag("ThreadNotMaterialized", () => Effect.succeed([])));
  });

  // Legacy titles use recorded user messages and saved commands.
  const rememberHistory = Effect.fnUntraced(function* (
    id: string,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    commands?: ReadonlyArray<ShellCommand>,
    fallback?: string | null,
  ) {
    const command = (commands ??
      (yield* Effect.tryPromise(() => env.THREADS.getByName(address(id)).commands())))[0];

    if (records.length === 0 && command === undefined && fallback === undefined) return;

    const firstInput = (page: ReadonlyArray<CanonicalRecordEnvelope>) =>
      page.flatMap(({ sequence, record: { payload } }) =>
        payload._tag === "UserInputRecorded" && Schema.is(CodingInput)(payload.input)
          ? [{ sequence, text: payload.input.message }]
          : [],
      )[0];

    let page = records;
    let input = firstInput(page);

    if (input === undefined && page.length === 64) {
      // Recovery records can precede the first input. Only untitled entries need the scan.
      if ((yield* remember(id, null)).title !== null) return;
      const client = yield* CloudflareThreadClient;

      while (input === undefined && page.length === 64) {
        const last = page.at(-1);

        if (last === undefined || (command && last.sequence >= command.afterSequence)) break;
        page = yield* client.readPage(address(id), { afterSequence: last.sequence, limit: 64 });
        input = firstInput(page);
      }
    }

    // Commands follow their recorded canonical tail, before the next agent input.
    const text =
      command && (input === undefined || command.afterSequence < input.sequence)
        ? `!${command.script}`
        : input?.text;

    yield* remember(id, text === undefined ? (fallback ?? null) : titleFor(text));
  }, Effect.provide(clientLayer));

  const rememberRequest = Effect.fnUntraced(function* (id: string, title: string | null) {
    const entry = yield* remember(id, null);

    // The existing registration call returns indexed titles without reading history.
    if (entry.title !== null) return;

    const records = yield* firstPage(id).pipe(Effect.provide(clientLayer));

    yield* rememberHistory(id, records, undefined, title);
  });

  if (request.method === "POST" && url.pathname === "/sandbox/api/workspaces") {
    const input = yield* Schema.decodeEffect(
      Schema.fromJsonString(Schema.Struct({ workspaceId: WorkspaceId })),
    )(yield* boundedBody(request));

    const limit = yield* Effect.promise(() => env.RUN_LIMIT.limit({ key: session.accountId }));

    if (!limit.success)
      return json({ message: "Please wait before creating another workspace." }, 429);
    yield* remember(input.workspaceId, null);

    return json({});
  }
  if (request.method === "GET" && url.pathname === "/sandbox/api/workspaces") {
    const before = yield* Schema.decodeEffect(Schema.NullOr(Schema.Natural))(
      url.searchParams.has("before") ? Number(url.searchParams.get("before")) : null,
    );

    const previous = url.searchParams.get("previous");

    if (previous !== null) {
      const id = yield* Schema.decodeEffect(WorkspaceId)(previous);

      const records = yield* firstPage(id).pipe(Effect.provide(clientLayer));

      yield* rememberHistory(id, records);
    }

    return json(
      yield* Schema.decodeEffect(WorkspacePage)(
        yield* Effect.tryPromise(() => directory.list(before)),
      ),
    );
  }

  if (request.method === "POST" && url.pathname === "/sandbox/api/preview") {
    const input = yield* Schema.decodeEffect(
      Schema.fromJsonString(Schema.Struct({ workspaceId: WorkspaceId })),
    )(yield* boundedBody(request));

    const limit = yield* Effect.promise(() => env.RUN_LIMIT.limit({ key: session.accountId }));

    if (!limit.success)
      return json({ message: "Please wait before opening another preview." }, 429);

    const reply = yield* Schema.decodeEffect(PreviewReply)(
      yield* Effect.tryPromise(
        async () => await env.THREADS.getByName(address(input.workspaceId)).preview(),
      ),
    );

    if ("error" in reply) return json({ message: reply.error }, 409);
    const proxy = yield* WorkspacePreviewProxy;

    return json({ ...reply.grant, url: yield* proxy.handoffUrl(reply.grant.token) });
  }
  if (
    request.method === "POST" &&
    ["/sandbox/api/save", "/sandbox/api/restore"].includes(url.pathname)
  ) {
    const input = yield* Schema.decodeEffect(
      Schema.fromJsonString(Schema.Struct({ workspaceId: WorkspaceId })),
    )(yield* boundedBody(request));

    const limit = yield* Effect.promise(() => env.RUN_LIMIT.limit({ key: session.accountId }));

    if (!limit.success) return json({ message: "Please wait before trying again." }, 429);

    const reply = yield* Schema.decodeEffect(FileActionReply)(
      yield* Effect.tryPromise(
        async () =>
          await env.THREADS.getByName(address(input.workspaceId)).fileAction(
            url.pathname.endsWith("/save") ? "save" : "restore",
          ),
      ),
    );

    return "error" in reply ? json({ message: reply.error }, 409) : json(reply.saved, 202);
  }
  if (request.method === "POST" && url.pathname === "/sandbox/api/recover") {
    const input = yield* Schema.decodeEffect(
      Schema.fromJsonString(Schema.Struct({ workspaceId: WorkspaceId })),
    )(yield* boundedBody(request));

    const limit = yield* Effect.promise(() => env.RUN_LIMIT.limit({ key: session.accountId }));

    if (!limit.success)
      return json({ message: "Please wait before checking recovery again." }, 429);

    return json(
      yield* Effect.tryPromise(() =>
        env.THREADS.getByName(address(input.workspaceId)).recoverWorkspace(),
      ),
    );
  }
  if (request.method === "POST" && url.pathname === "/sandbox/api/commands") {
    const input = yield* Schema.decodeEffect(
      Schema.fromJsonString(Schema.Struct({ workspaceId: WorkspaceId, ...CommandInput.fields })),
    )(yield* boundedBody(request));

    const limit = yield* Effect.promise(() => env.RUN_LIMIT.limit({ key: session.accountId }));

    if (!limit.success)
      return json({ message: "Please wait a moment before running another command." }, 429);

    yield* rememberRequest(input.workspaceId, titleFor(`!${input.script}`));

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

    yield* rememberRequest(input.workspaceId, titleFor(input.message));

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

    if (url.pathname === "/sandbox/api/commands") {
      const commands = yield* Effect.tryPromise(() => env.THREADS.getByName(threadId).commands());

      if (commands[0]) {
        const records = yield* firstPage(workspaceId).pipe(Effect.provide(clientLayer));

        yield* rememberHistory(workspaceId, records, commands);
      }

      return json(commands);
    }
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

          let needsRecoveryCheck = after > 0;
          let recoveryUnavailable = false;

          return Stream.unfold({ cursor: after, wait: false }, ({ cursor, wait }) =>
            Effect.gen(function* () {
              // Deliver history first, then let the client's reconnect delay retry authority.
              if (recoveryUnavailable) return;
              if (wait) yield* client.awaitProgress(threadId, cursor);

              const records = yield* client
                .readPage(threadId, { afterSequence: cursor, limit: 64 })
                .pipe(
                  Effect.catchTag("ThreadNotMaterialized", (error) =>
                    cursor === 0 ? Effect.succeed([]) : Effect.fail(error),
                  ),
                );

              if (cursor === 0) yield* rememberHistory(workspaceId, records);
              const next = records.at(-1)?.sequence ?? cursor;
              const entries = records.flatMap(entriesFor);

              needsRecoveryCheck ||=
                entries.some(
                  (entry) =>
                    entry.reviewRequired ||
                    (entry.kind === "status" &&
                      ["Unknown", "Failed", "Stopped"].includes(entry.text)),
                ) || records.some(({ record: { payload } }) => payload._tag === "ToolCallResolved");

              const reviewRequired =
                needsRecoveryCheck && records.length < 64
                  ? yield* Effect.tryPromise(() =>
                      env.THREADS.getByName(threadId).recoveryRequired(),
                    ).pipe(
                      Effect.catch(() =>
                        Effect.sync(() => {
                          recoveryUnavailable = true;

                          return undefined;
                        }),
                      ),
                    )
                  : undefined;

              if (reviewRequired !== undefined) needsRecoveryCheck = reviewRequired;

              const page: EventPage = {
                cursor: next,
                caughtUp: records.length < 64 && !recoveryUnavailable,
                entries,
                ...(reviewRequired === undefined ? {} : { reviewRequired }),
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

const previewProxy = Layer.unwrap(
  Effect.gen(function* () {
    const env = yield* WorkerEnvironment;

    return WorkspacePreviewProxy.layer({
      applicationOrigin: env.AUTH_RESOURCE ?? "",
      previewDomain: env.PREVIEW_DOMAIN,
    }).pipe(Layer.provide(NativeSandboxNamespace.layer({ binding: "SANDBOXES" })));
  }),
);

export default Worker.make(previewProxy, {
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
                      : error.code === "sandbox-files-active"
                        ? "Wait for workspace files to finish saving or restoring."
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
