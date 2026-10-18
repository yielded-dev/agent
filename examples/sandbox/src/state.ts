import { fetchWithSession } from "@yielded/agent-example-auth/client";
import { decodeLiveText } from "@yielded/agent-platform-cloudflare/live-text";
import { ThreadId } from "@yielded/agent/identifiers";
import { DateTime, Effect, Layer, Schema, Stream } from "effect";
import { AsyncResult, Atom, Reactivity } from "effect/reactivity";

import {
  Accepted,
  CommandInput,
  defaultModelSettings,
  DIAGNOSTIC_PAGE_SIZE,
  DiagnosticsPage,
  FileContent,
  FileList,
  LIVE_TEXT_THREAD_HEADER,
  MessageInput,
  ModelSettings,
  PreviewSession,
  RecoveryResult,
  SandboxError,
  SavedFiles,
  ShellCommand,
  WorkspaceEvent,
  WorkspaceId,
  WorkspacePage,
  type Draft,
  type Entry,
} from "./contract";
import { workspaceIdFromPath } from "./location";

const runtime = Atom.runtime(Layer.empty);
const problem = (message: string) => SandboxError.make({ message });
const storageKey = (subject: string) => `yielded:sandbox:${subject}`;

const storedWorkspace = (subject: string) => {
  try {
    const id = localStorage.getItem(storageKey(subject));

    return Schema.is(WorkspaceId)(id) ? id.toLowerCase() : null;
  } catch {
    return null;
  }
};

type Location =
  | { readonly page: "list" }
  | { readonly page: "workspace"; readonly id: string }
  | { readonly page: "not-found" };

/** The authenticated account's Atom registry owns navigation and its listener. */
export const location = Atom.writable(
  (get) => {
    const read = (): Location => {
      const pathname = window.location.pathname;

      if (pathname === "/sandbox" || pathname === "/sandbox/") return { page: "list" };
      const id = workspaceIdFromPath(pathname);

      if (id === null) return { page: "not-found" };
      if (pathname !== `/sandbox/${id}`)
        window.history.replaceState(window.history.state, "", `/sandbox/${id}`);

      return { page: "workspace", id };
    };

    const update = () => get.setSelf(read());

    window.addEventListener("popstate", update);
    get.addFinalizer(() => window.removeEventListener("popstate", update));

    return read();
  },
  (get, id: string | null) => {
    const next = id === null ? null : Schema.decodeSync(WorkspaceId)(id).toLowerCase();

    window.history.pushState(
      window.history.state,
      "",
      next === null ? "/sandbox/" : `/sandbox/${next}`,
    );
    get.setSelf(next === null ? { page: "list" } : { page: "workspace", id: next });
  },
).pipe(Atom.keepAlive);

class RequestRejected extends Schema.TaggedError<RequestRejected>()("RequestRejected", {
  status: Schema.Int,
  message: Schema.String,
}) {}

const request = Effect.fnUntraced(function* (path: string, input?: unknown) {
  const controller = yield* Effect.acquireRelease(
    Effect.sync(() => new AbortController()),
    (controller) => Effect.sync(() => controller.abort()),
  );

  const response = yield* fetchWithSession(`/sandbox/api/${path}`, {
    signal: controller.signal,
    credentials: "same-origin",
    ...(input === undefined
      ? {}
      : {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(input),
        }),
  });

  if (!response.ok) {
    const detail = yield* Effect.tryPromise(() => response.json()).pipe(
      Effect.flatMap(
        Schema.decodeUnknownEffect(
          Schema.Struct({ message: Schema.String.check(Schema.isMaxLength(512)) }),
        ),
      ),
      Effect.map((value) => value.message),
      Effect.orElseSucceed(() => null),
    );

    return yield* RequestRejected.make({
      status: response.status,
      message:
        response.status === 401
          ? "Your session expired. Sign in again."
          : (detail ??
            `Request failed (${response.status}). Check your account access and try again.`),
    });
  }

  return response;
});

const json = Effect.fnUntraced(function* <A>(
  path: string,
  schema: Schema.Codec<A, unknown>,
  input?: unknown,
) {
  const response = yield* request(path, input);

  const text = yield* Effect.tryPromise({
    try: () => response.text(),
    catch: () => problem("The response was interrupted."),
  });

  return yield* Schema.decodeEffect(Schema.fromJsonString(schema))(text);
}, Effect.scoped);

export const workspaceList = Atom.family((subject: string) => {
  const cursors = Atom.make<ReadonlyArray<number>>([]);

  const page = runtime
    .atom((get) => {
      const before = get(cursors).at(-1);

      return Effect.gen(function* () {
        const params = new URLSearchParams();
        const previous = storedWorkspace(subject);

        if (before !== undefined) params.set("before", String(before));
        if (previous !== null) params.set("previous", previous);
        const result = yield* json(`workspaces?${params}`, WorkspacePage);

        // A successful list can precede recovery of an admitted workspace's history.
        if (previous !== null && result.entries.some((entry) => entry.id === previous))
          yield* Effect.try(() => {
            if (storedWorkspace(subject) === previous) localStorage.removeItem(storageKey(subject));
          }).pipe(Effect.ignore);

        return result;
      });
    })
    .pipe(runtime.factory.withReactivity(["sandbox-workspaces"]), Atom.setIdleTTL(0));

  return {
    page,
    cursors,
    next: Atom.fnSync((_, get) => {
      const current = get(page);

      if (AsyncResult.isSuccess(current) && current.value.next !== null)
        get.set(cursors, [...get(cursors), current.value.next]);
    }),
    previous: Atom.fnSync((_, get) => get.set(cursors, get(cursors).slice(0, -1))),
    refresh: Atom.fnSync((_, get) => {
      get.set(cursors, []);
      get.refresh(page);
    }),
  };
});

const pendingWorkspaceId = Atom.make<string | null>(null).pipe(Atom.keepAlive);

const createWorkspace = runtime.fn<void>()(
  Effect.fnUntraced(function* (_, get) {
    const currentLocation = get(location);

    if (currentLocation.page === "workspace") {
      const current = get(workspace(currentLocation.id).state);

      if (current.active || current.pending || current.admissionRequestId) return;
    }
    const id = get(pendingWorkspaceId) ?? crypto.randomUUID();

    get.set(pendingWorkspaceId, id);
    yield* json("workspaces", Schema.Struct({}), { workspaceId: id });
    get.set(pendingWorkspaceId, null);
    get.set(location, id);
  }),
  { reactivityKeys: ["sandbox-workspaces"] },
);

export const newWorkspace = Atom.writable(
  (get) => get(createWorkspace),
  (get, _: void) => {
    if (!get.get(createWorkspace).waiting) get.set(createWorkspace, undefined);
  },
);

const query = (id: string, path: string) =>
  new URLSearchParams({ workspaceId: id, path }).toString();

interface Pending {
  readonly requestId: string;
  readonly text: string;
  readonly submissionId?: string;
  readonly command?: boolean;
  readonly settings?: ModelSettings;
}
interface Workspace {
  readonly historyReady: boolean;
  readonly acceptedSettings: ModelSettings;
  readonly entries: ReadonlyArray<Entry>;
  readonly commands: ReadonlyArray<ShellCommand>;
  readonly drafts: ReadonlyArray<Draft>;
  readonly runId: string | null;
  readonly committedTurns: ReadonlyArray<string>;
  readonly cursor: number;
  readonly active: string | null;
  readonly admissionRequestId: string | null;
  readonly pending: Pending | null;
  readonly connection: string;
  readonly error: string | null;
  readonly uncertain: boolean;
  readonly reviewRequired: boolean;
}

export const workspace = Atom.family((id: string) => {
  const recoveryKey = `sandbox-recovery:${id}`;
  const filesKey = `sandbox-files:${id}`;

  const state = Atom.make<Workspace>({
    historyReady: false,
    acceptedSettings: defaultModelSettings,
    entries: [],
    commands: [],
    drafts: [],
    runId: null,
    committedTurns: [],
    cursor: 0,
    active: null,
    admissionRequestId: null,
    pending: null,
    connection: "Connecting…",
    error: null,
    uncertain: false,
    reviewRequired: false,
  }).pipe(Atom.setIdleTTL("5 minutes"));

  const diagnosticAfter = Atom.make<number | null>(null);

  const diagnostics = runtime
    .atom((get) => {
      const after =
        get(diagnosticAfter) ?? Math.max(0, get.once(state).cursor - DIAGNOSTIC_PAGE_SIZE);

      return json(
        `diagnostics?${new URLSearchParams({ workspaceId: id, after: String(after) })}`,
        DiagnosticsPage,
      );
    })
    .pipe(Atom.setIdleTTL(0));

  const refreshDiagnostics = Atom.fnSync((_, get) => {
    get.set(diagnosticAfter, null);
    get.refresh(diagnostics);
  });

  const copyDiagnostics = runtime.fn<void>()(
    Effect.fnUntraced(function* (_, get) {
      const page = yield* get.result(diagnostics);

      const text = yield* Schema.encodeEffect(
        Schema.fromJsonString(
          Schema.Struct({
            workspaceId: WorkspaceId,
            ...DiagnosticsPage.fields,
          }),
        ),
      )({ workspaceId: id, ...page });

      yield* Effect.tryPromise({
        try: () => navigator.clipboard.writeText(text),
        catch: () => problem("Could not copy diagnostics. Try again."),
      });
    }),
  );

  const draft = Atom.make("").pipe(Atom.setIdleTTL("5 minutes"));
  const settingsDraft = Atom.make<ModelSettings | null>(null);

  const settings = Atom.make((get) => get(settingsDraft) ?? get(state).acceptedSettings);

  const changeSettings = Atom.fnSync(
    (change: Partial<Record<keyof ModelSettings, string>>, get) => {
      if (get(fileOperation) !== "idle") return;
      get.set(
        settingsDraft,
        Schema.decodeUnknownSync(ModelSettings)({ ...get(settings), ...change }),
      );
    },
  );

  const directory = Atom.make("");
  const selectedFile = Atom.make<string | null>(null);
  const revision = Atom.make(0);
  const commandRevision = Atom.make(0);
  const savedFromServer = Atom.make<typeof SavedFiles.Type | undefined>(undefined);

  const files = runtime
    .atom((get) => {
      get(revision);
      const path = get(directory);
      const statusAtRequest = get.once(savedFromServer);

      return Effect.gen(function* () {
        const result = yield* json(`files?${query(id, path)}`, FileList);

        // Recover missed status events without overwriting one received during this read.
        if (get.once(savedFromServer) === statusAtRequest) get.set(savedFromServer, result.saved);

        return result;
      });
    })
    .pipe(runtime.factory.withReactivity([filesKey]));

  const saved = Atom.make(
    (get) => get(savedFromServer) ?? AsyncResult.getOrElse(get(files), () => null)?.saved,
  );

  const expired = Atom.make(
    (get) => AsyncResult.getOrElse(get(files), () => null)?.expired ?? false,
  );

  const file = runtime
    .atom((get) => {
      get(revision);
      const path = get(selectedFile);

      return path === null ? Effect.succeed(null) : json(`file?${query(id, path)}`, FileContent);
    })
    .pipe(runtime.factory.withReactivity([filesKey]));

  const preparePreview = runtime.fn<void>()(
    Effect.fnUntraced(function* (_, get) {
      if (get(expired) || get(fileOperation) !== "idle")
        return yield* problem("Wait for a live workspace before preparing its preview.");

      return yield* json("preview", PreviewSession, { workspaceId: id });
    }),
  );

  const preview = Atom.make((get) => {
    const result = get(preparePreview);

    if (!AsyncResult.isSuccess(result) || result.waiting) return null;
    const remaining = result.value.expiresAt - DateTime.toEpochMillis(DateTime.nowUnsafe());

    if (remaining <= 0) return null;
    const timer = setTimeout(() => get.refreshSelf(), remaining);

    get.addFinalizer(() => clearTimeout(timer));

    return result.value;
  });

  const fileView = Atom.make<"source" | "preview">("source");

  const changeFileView = Atom.fnSync((view: "source" | "preview", get) => {
    get.set(fileView, view);
    if (view === "preview" && get(preview) === null && !get(preparePreview).waiting)
      get.set(preparePreview, undefined);
  });

  const restore = runtime.fn<void>()(
    Effect.fnUntraced(function* (_, get) {
      get.set(preparePreview, Atom.Reset);

      // Restoration must reload the directory to clear its expired state.
      const result = yield* Reactivity.mutation(json("restore", SavedFiles, { workspaceId: id }), [
        filesKey,
      ]);

      yield* get.result(files, { suspendOnWaiting: true });

      return result;
    }),
  );

  const save = runtime.fn<void>()(
    Effect.fnUntraced(function* (_, get) {
      const statusRevision = get(commandRevision);
      const result = yield* json("save", SavedFiles, { workspaceId: id });

      // Keep admission blocked by its reply unless an event has supplied newer status.
      if (get(commandRevision) === statusRevision) get.set(savedFromServer, result);

      return result;
    }),
    { reactivityKeys: [filesKey] },
  );

  const fileOperation = Atom.make((get) => {
    if (get(restore).waiting) return "restoring";
    if (get(save).waiting) return "saving";

    return get(saved)?.phase ?? "idle";
  });

  const refresh = Atom.fnSync((_, get) => get.set(revision, get(revision) + 1));

  const commandEvents = runtime
    .atom((get) => {
      get(commandRevision);

      const apply = (commands: ReadonlyArray<ShellCommand>) => {
        const current = get.once(state);

        const active = commands.find(
          (command) => command.status === "queued" || command.status === "running",
        );

        const unknown = commands.some((command) => command.status === "unknown");

        const admitting =
          current.pending?.command && current.admissionRequestId === current.pending.requestId;

        const missing =
          current.pending?.command &&
          !admitting &&
          !commands.some((command) => command.requestId === current.pending?.requestId);

        const pending = current.pending?.command
          ? admitting || missing
            ? current.pending
            : active
              ? { requestId: active.requestId, text: `!${active.script}`, command: true }
              : null
          : (current.pending ??
            (active
              ? { requestId: active.requestId, text: `!${active.script}`, command: true }
              : null));

        get.set(state, {
          ...current,
          commands,
          pending,
          uncertain: unknown || missing || (current.pending?.command ? false : current.uncertain),
          reviewRequired: unknown || current.reviewRequired,
          error: unknown
            ? "The command's outcome could not be confirmed. Its original receipt is retained; start a new workspace before making more changes."
            : missing
              ? "Checking the command's original request. It will not be sent with a new ID."
              : current.pending?.command
                ? null
                : current.error,
        });
      };

      return Effect.gen(function* () {
        // A failed command read must not prevent an independent file refresh.
        get.set(revision, get.once(revision) + 1);
        let commands = yield* json(`commands?${query(id, "")}`, Schema.Array(ShellCommand));
        const current = get.once(state);

        if (
          current.pending?.command &&
          !current.admissionRequestId &&
          !commands.some((command) => command.requestId === current.pending?.requestId)
        ) {
          // An empty read can precede a delayed admission. Only replay its exact identity.
          const accepted = yield* json("commands", ShellCommand, {
            workspaceId: id,
            requestId: current.pending.requestId,
            script: current.pending.text.slice(1).trim(),
          });

          commands = [...commands, accepted];
        }
        apply(commands);
      }).pipe(
        Effect.tapError(() =>
          Effect.sync(() => {
            const current = get.once(state);

            if (current.pending?.command)
              get.set(state, {
                ...current,
                uncertain: true,
                error:
                  "Could not confirm the command's outcome. Reconnect to its original request.",
              });
          }),
        ),
      );
    })
    .pipe(Atom.setIdleTTL(0));

  const reconnectCommand = Atom.fnSync((_, get) =>
    get.set(commandRevision, get(commandRevision) + 1),
  );

  const events = runtime
    .atom((get) => {
      const registry = get.registry;

      // Clear synchronously when this observer is disposed, before another mount can start.
      // Its context cannot write after disposal; the retained Workspace still can.
      get.addFinalizer(() => {
        if (registry.getNodes().has(state))
          registry.update(state, (current) => ({ ...current, drafts: [] }));
      });

      const clearDrafts = Effect.sync(() => {
        if (!registry.getNodes().has(state)) return;
        const current = get.once(state);

        if (current.drafts.length > 0) get.set(state, { ...current, drafts: [] });
      });

      const pages = Effect.gen(function* () {
        get.set(state, { ...get.once(state), historyReady: false });

        const response = yield* request(
          "events?" +
            new URLSearchParams({ workspaceId: id, after: String(get.once(state).cursor) }),
        );

        const body = response.body;

        if (!body) return yield* problem("The event connection has no body.");
        let lineSize = 0;

        yield* Stream.fromReadableStream({
          evaluate: () => body,
          onError: () => problem("Event connection interrupted."),
        }).pipe(
          Stream.decodeText(),
          // Bound incomplete lines too, before splitLines can retain an arbitrary buffer.
          Stream.mapEffect((chunk) =>
            Effect.gen(function* () {
              for (const part of chunk.split(/(?<=\n)/)) {
                lineSize += part.length;
                if (lineSize > 2_000_000) return yield* problem("The event page is too large.");
                if (part.endsWith("\n")) lineSize = 0;
              }

              return chunk;
            }),
          ),
          Stream.splitLines,
          Stream.filter((line) => line.trim().length > 0),
          Stream.mapEffect((line) =>
            Schema.decodeEffect(Schema.fromJsonString(WorkspaceEvent))(line),
          ),
          Stream.runForEach((page) =>
            Effect.sync(() => {
              if ("_tag" in page) {
                get.set(savedFromServer, page.saved);
                get.set(commandRevision, get.once(commandRevision) + 1);

                return;
              }
              const current = get.once(state);

              if (page.cursor < current.cursor) return;
              const entries = new Map(current.entries.map((entry) => [entry.id, entry]));
              let active = current.active;
              let pending = current.pending;
              let uncertain = current.uncertain;
              let error = current.error;
              let reviewRequired = current.reviewRequired;
              let runId = current.runId;
              let drafts = current.drafts;
              let committedTurns = current.committedTurns;
              let acceptedSettings = current.acceptedSettings;

              if (page.cursor > current.cursor) {
                for (const entry of page.entries) {
                  entries.set(entry.id, entry);
                  if (
                    entry.runId &&
                    (entry.kind === "user" ||
                      (entry.kind === "status" && entry.text === "Running")) &&
                    entry.runId !== runId
                  ) {
                    drafts = drafts.filter((draft) => draft.runId !== runId);
                    runId = entry.runId;
                    committedTurns = [];
                  }
                  if (
                    entry.reviewRequired === true ||
                    (entry.kind === "status" && entry.text === "Unknown")
                  )
                    reviewRequired = true;
                  if (entry.kind === "user") {
                    acceptedSettings = entry.settings ?? defaultModelSettings;
                    if (entry.submissionId) active = entry.submissionId;
                    if (pending && entry.requestId === pending.requestId) {
                      pending = null;
                      uncertain = false;
                      error = null;
                    }
                  }
                  if (entry.kind === "status" && entry.submissionId) {
                    if (entry.text === "Running") active = entry.submissionId;
                    else if (["Completed", "Failed", "Stopped"].includes(entry.text)) {
                      drafts = drafts.filter((draft) => draft.submissionId !== entry.submissionId);
                      if (active === entry.submissionId) active = null;
                    }
                  }
                }
                committedTurns = [
                  ...new Set([
                    ...committedTurns,
                    ...(page.committedTurns ?? [])
                      .filter((turn) => turn.runId === runId)
                      .map((turn) => turn.turnId),
                  ]),
                ];
                get.set(revision, get.once(revision) + 1);
              }
              if (page.reviewRequired !== undefined) {
                // Only the owner can clear recovery; command receipts remain independently binding.
                reviewRequired =
                  page.reviewRequired ||
                  current.commands.some((command) => command.status === "unknown");
                if (current.reviewRequired && !reviewRequired) error = null;
              }
              // Review gates writes; it does not settle an active or pending submission.
              get.set(state, {
                ...current,
                entries: [...entries.values()].slice(-500),
                acceptedSettings,
                historyReady: page.caughtUp === true,
                drafts: drafts.filter(
                  (draft) => draft.runId !== runId || !committedTurns.includes(draft.turnId),
                ),
                runId,
                committedTurns,
                cursor: page.cursor,
                active,
                pending,
                uncertain,
                reviewRequired,
                error: reviewRequired
                  ? "A workspace operation needs review. Review the transcript and files, or start a new workspace. Further changes are blocked here."
                  : error,
                connection: page.caughtUp === true ? "Live" : "Loading history…",
              });
            }),
          ),
        );

        return yield* problem("Event connection interrupted.");
      });

      const previews = Effect.gen(function* () {
        const response = yield* request("text?" + new URLSearchParams({ workspaceId: id }));

        const threadId = yield* Schema.decodeUnknownEffect(ThreadId)(
          response.headers.get(LIVE_TEXT_THREAD_HEADER),
        );

        const body = response.body;

        if (!body) return yield* problem("The text connection has no body.");
        yield* decodeLiveText(
          threadId,
          Stream.fromReadableStream({
            evaluate: () => body,
            onError: () => problem("Text connection interrupted."),
          }),
        ).pipe(
          Stream.runForEach((frame) =>
            Effect.sync(() => {
              const current = get.once(state);

              if (frame._tag === "Reset") {
                get.set(state, { ...current, drafts: [] });

                return;
              }
              const event = frame.event;

              if (event._tag !== "Text") {
                get.set(state, {
                  ...current,
                  drafts: current.drafts.filter(
                    (draft) =>
                      draft.submissionId !== event.submissionId ||
                      draft.attemptId !== event.attemptId ||
                      (event._tag === "Discard" &&
                        (draft.runId !== event.runId ||
                          draft.turnId !== event.turnId ||
                          draft.generation !== event.generation)),
                  ),
                });

                return;
              }
              // A later snapshot can still contain a canonically committed model call.
              if (event.runId === current.runId && current.committedTurns.includes(event.turnId))
                return;
              const part = event.part;
              const kind = part.type.startsWith("reasoning-") ? "reasoning" : "assistant";

              const draftId = JSON.stringify([
                event.threadId,
                event.submissionId,
                event.attemptId,
                event.runId,
                event.turnId,
                event.generation,
                kind,
                part.id,
              ]);

              if (part.type === "text-start" || part.type === "reasoning-start") {
                // Retained canonical evidence rejects late snapshots from older or settled Runs.
                if (
                  current.entries.some(
                    (entry) =>
                      (entry.runId === event.runId && event.runId !== current.runId) ||
                      (entry.kind === "status" &&
                        entry.submissionId === event.submissionId &&
                        ["Completed", "Failed", "Stopped"].includes(entry.text)),
                  )
                )
                  return;

                const draft: Draft = {
                  id: draftId,
                  kind,
                  text: "",
                  submissionId: event.submissionId,
                  attemptId: event.attemptId,
                  runId: event.runId,
                  turnId: event.turnId,
                  generation: event.generation,
                };

                get.set(state, { ...current, drafts: [...current.drafts, draft] });
              } else if (part.type === "text-delta" || part.type === "reasoning-delta") {
                get.set(state, {
                  ...current,
                  drafts: current.drafts.map((draft) =>
                    draft.id === draftId ? { ...draft, text: draft.text + part.delta } : draft,
                  ),
                });
              }
            }),
          ),
        );
      });

      return Effect.all(
        [
          Effect.gen(function* () {
            while (true) {
              yield* pages.pipe(Effect.scoped, Effect.ignore);
              get.set(state, {
                ...get.once(state),
                historyReady: false,
                drafts: [],
                connection: "Reconnecting…",
              });
              yield* Effect.sleep("2 seconds");
            }
          }),
          Effect.gen(function* () {
            while (true) {
              yield* previews.pipe(Effect.ensuring(clearDrafts), Effect.scoped, Effect.ignore);
              yield* Effect.sleep("2 seconds");
            }
          }),
        ],
        { concurrency: 2, discard: true },
      ).pipe(Effect.ensuring(clearDrafts));
    })
    .pipe(runtime.factory.withReactivity([recoveryKey]), Atom.setIdleTTL(0));

  const recover = runtime.fn<void>()(() => json("recover", RecoveryResult, { workspaceId: id }), {
    reactivityKeys: [recoveryKey],
  });

  const admission = Atom.fn<void>()(
    Effect.fnUntraced(function* (_, get) {
      const current = get(state);
      const retry = current.uncertain && !current.pending?.command ? current.pending : null;
      const message = retry?.text ?? get(draft).trim();

      if (
        !message ||
        get(expired) ||
        get(fileOperation) !== "idle" ||
        current.active ||
        current.reviewRequired ||
        current.admissionRequestId ||
        (!retry && (!current.historyReady || current.pending || current.uncertain))
      )
        return;

      const input = yield* Schema.decodeEffect(MessageInput)(
        retry
          ? {
              workspaceId: id,
              requestId: retry.requestId,
              message,
              ...(retry.settings === undefined ? {} : { settings: retry.settings }),
            }
          : {
              workspaceId: id,
              requestId: crypto.randomUUID(),
              message,
              settings: get(settings),
            },
      );

      const pending: Pending = {
        requestId: input.requestId,
        text: message,
        ...(input.settings === undefined ? {} : { settings: input.settings }),
      };

      if (message.startsWith("!")) {
        const command = yield* Schema.decodeEffect(CommandInput)({
          requestId: input.requestId,
          script: message.slice(1).trim(),
        });

        get.set(state, {
          ...current,
          pending: { ...pending, command: true },
          admissionRequestId: input.requestId,
          error: null,
        });
        get.set(draft, "");
        yield* json("commands", ShellCommand, { ...command, workspaceId: id }).pipe(
          Effect.catch((error) =>
            Effect.sync(() => {
              const latest = get(state);
              const rejected = Schema.is(RequestRejected)(error) && error.status < 500;

              if (rejected && !get(draft)) get.set(draft, message);
              get.set(state, {
                ...latest,
                pending: rejected ? null : latest.pending,
                uncertain: !rejected,
                error: rejected ? error.message : "Checking the command's original receipt…",
              });
            }),
          ),
          Effect.ensuring(
            Effect.sync(() => {
              const latest = get(state);

              if (latest.admissionRequestId === input.requestId)
                get.set(state, { ...latest, admissionRequestId: null });
              get.set(commandRevision, get(commandRevision) + 1);
            }),
          ),
        );

        return;
      }

      get.set(state, {
        ...current,
        pending,
        admissionRequestId: input.requestId,
        uncertain: false,
        error: null,
      });
      if (!retry) get.set(draft, "");
      yield* json("messages", Accepted, input).pipe(
        Effect.tap((accepted) =>
          Effect.sync(() => {
            const latest = get(state);

            // Preserve canonical reconciliation after its entry leaves the displayed history.
            if (
              latest.reviewRequired ||
              latest.admissionRequestId !== input.requestId ||
              latest.pending?.requestId !== input.requestId
            )
              return;

            const durable = latest.entries.some(
              (entry) => entry.kind === "user" && entry.requestId === input.requestId,
            );

            const terminal = latest.entries.some(
              (entry) =>
                entry.kind === "status" &&
                entry.submissionId === accepted.submissionId &&
                ["Completed", "Failed", "Stopped"].includes(entry.text),
            );

            get.set(state, {
              ...latest,
              pending: durable ? null : { ...pending, submissionId: accepted.submissionId },
              active: terminal ? null : accepted.submissionId,
              uncertain: false,
              error: null,
              // The accepted submission's Run is known only when its canonical input arrives.
              runId: durable || terminal ? latest.runId : null,
              committedTurns: durable || terminal ? latest.committedTurns : [],
            });
          }),
        ),
        Effect.catch((error) =>
          Effect.sync(() => {
            const latest = get(state);

            if (
              latest.reviewRequired ||
              latest.admissionRequestId !== input.requestId ||
              latest.pending?.requestId !== input.requestId
            )
              return;
            // A later refusal cannot prove that the original request was never accepted.
            const rejected = !retry && Schema.is(RequestRejected)(error) && error.status < 500;

            const durable = latest.entries.some(
              (entry) => entry.kind === "user" && entry.requestId === input.requestId,
            );

            if (rejected && !durable && !get(draft)) get.set(draft, message);
            get.set(state, {
              ...latest,
              pending: rejected || durable ? null : pending,
              uncertain: !rejected && !durable,
              error: durable
                ? null
                : rejected
                  ? error.message
                  : "Delivery could not be confirmed. Retry to continue with this message.",
            });
          }),
        ),
        Effect.ensuring(
          Effect.sync(() => {
            const latest = get(state);

            if (latest.admissionRequestId === input.requestId)
              get.set(state, { ...latest, admissionRequestId: null });
          }),
        ),
      );
    }),
  );

  // Reject duplicate dispatch before Atom.fn can invalidate the in-flight context.
  const send = Atom.writable(
    (get) => get(admission),
    (get, value: void) => {
      const current = get.get(state);

      const retry = current.uncertain && current.pending && !current.pending.command;

      if (
        get.get(fileOperation) !== "idle" ||
        current.active ||
        current.reviewRequired ||
        current.admissionRequestId ||
        (!retry && (!current.historyReady || current.pending || current.uncertain))
      )
        return;
      get.set(admission, value);
    },
  );

  const stop = Atom.fn<void>()(
    Effect.fnUntraced(function* (_, get) {
      const current = get(state);

      if (current.reviewRequired) return;
      const pending = current.pending;

      if (pending?.command) {
        if (
          current.admissionRequestId ||
          !current.commands.some((command) => command.requestId === pending.requestId)
        )
          return;
        yield* json("stop-command", Schema.Struct({}), {
          workspaceId: id,
          requestId: pending.requestId,
        }).pipe(
          Effect.catch(() =>
            Effect.sync(() => {
              const latest = get(state);

              get.set(state, {
                ...latest,
                error: "Stop could not be confirmed. The command keeps its original time limit.",
              });
            }),
          ),
        );

        return;
      }
      const submissionId = get(state).active;

      if (!submissionId || get(state).uncertain) return;
      get.set(state, { ...get(state), error: null });
      yield* json("stop", Schema.Struct({}), { workspaceId: id, submissionId }).pipe(
        Effect.catch(() =>
          Effect.sync(() => {
            const latest = get(state);

            if (latest.reviewRequired) return;
            get.set(state, {
              ...latest,
              error: "Stop could not be confirmed. Check the live status before trying again.",
            });
          }),
        ),
      );
    }),
  );

  const visibleState = Atom.make((get) => {
    const current = get(state);

    return {
      ...current,
      // Keep early parts until their canonical admission arrives; old Runs stay hidden.
      drafts: current.drafts.filter(
        (draft) =>
          draft.runId === current.runId &&
          draft.submissionId === current.active &&
          !current.committedTurns.includes(draft.turnId),
      ),
    };
  });

  return {
    state: visibleState,
    draft,
    settings,
    changeSettings,
    diagnostics,
    diagnosticAfter,
    refreshDiagnostics,
    copyDiagnostics,
    directory,
    selectedFile,
    files,
    saved,
    expired,
    file,
    preparePreview,
    preview,
    fileView,
    changeFileView,
    restore,
    save,
    fileOperation,
    events,
    commandEvents,
    reconnectCommand,
    recover,
    send,
    stop,
    refresh,
  };
});
