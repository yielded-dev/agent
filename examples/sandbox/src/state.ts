import { fetchWithSession } from "@yielded/agent-example-auth/client";
import { Effect, Layer, Schema, Stream } from "effect";
import { Atom } from "effect/reactivity";

import {
  Accepted,
  EventPage,
  FileContent,
  FileList,
  MessageInput,
  SandboxError,
  WorkspaceId,
  type Entry,
} from "./contract";

const runtime = Atom.runtime(Layer.empty);
const problem = (message: string) => SandboxError.make({ message });
const storageKey = (subject: string) => `yielded:sandbox:${subject}`;

const saveWorkspace = (subject: string, id: string) => {
  try {
    localStorage.setItem(storageKey(subject), id);
  } catch {
    /* Storage is optional. */
  }

  return id;
};

const storedWorkspace = (subject: string) => {
  try {
    const id = localStorage.getItem(storageKey(subject));

    if (Schema.is(WorkspaceId)(id)) return id;
  } catch {
    /* Keep the workspace in memory when storage is unavailable. */
  }

  return saveWorkspace(subject, crypto.randomUUID());
};

export const workspaceId = Atom.family((subject: string) =>
  Atom.writable(
    () => storedWorkspace(subject),
    (get, id: string) => get.setSelf(saveWorkspace(subject, id)),
  ).pipe(Atom.keepAlive),
);

export const newWorkspace = Atom.fnSync((subject: string, get) => {
  const current = get(workspace(get(workspaceId(subject))).state);

  if (current.active || current.pending || current.admissionRequestId) return;
  get.set(workspaceId(subject), crypto.randomUUID());
});

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

  if (!response.ok)
    return yield* RequestRejected.make({
      status: response.status,
      message:
        response.status === 401
          ? "Your session expired. Sign in again."
          : `Request failed (${response.status}). Check your account access and try again.`,
    });

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

const query = (id: string, path: string) =>
  new URLSearchParams({ workspaceId: id, path }).toString();

interface Pending {
  readonly requestId: string;
  readonly text: string;
  readonly submissionId?: string;
}
interface Workspace {
  readonly entries: ReadonlyArray<Entry>;
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
  const state = Atom.make<Workspace>({
    entries: [],
    cursor: 0,
    active: null,
    admissionRequestId: null,
    pending: null,
    connection: "Connecting…",
    error: null,
    uncertain: false,
    reviewRequired: false,
  }).pipe(Atom.setIdleTTL("5 minutes"));

  const draft = Atom.make("").pipe(Atom.setIdleTTL("5 minutes"));
  const directory = Atom.make("");
  const selectedFile = Atom.make<string | null>(null);
  const revision = Atom.make(0);

  const files = runtime.atom((get) => {
    get(revision);

    return json(`files?${query(id, get(directory))}`, FileList);
  });

  const file = runtime.atom((get) => {
    get(revision);
    const path = get(selectedFile);

    return path === null ? Effect.succeed(null) : json(`file?${query(id, path)}`, FileContent);
  });

  const refresh = Atom.fnSync((_, get) => get.set(revision, get(revision) + 1));

  const events = runtime
    .atom((get) =>
      Effect.gen(function* () {
        while (true) {
          yield* Effect.gen(function* () {
            const response = yield* request(
              `events?${new URLSearchParams({ workspaceId: id, after: String(get.once(state).cursor) })}`,
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
                Schema.decodeEffect(Schema.fromJsonString(EventPage))(line),
              ),
              Stream.runForEach((page) =>
                Effect.sync(() => {
                  const current = get.once(state);

                  if (page.cursor < current.cursor) return;
                  const entries = new Map(current.entries.map((entry) => [entry.id, entry]));
                  let active = current.active;
                  let pending = current.pending;
                  let uncertain = current.uncertain;
                  let error = current.error;
                  let reviewRequired = current.reviewRequired;

                  if (page.cursor > current.cursor) {
                    for (const entry of page.entries) {
                      entries.set(entry.id, entry);
                      if (
                        entry.reviewRequired === true ||
                        (entry.kind === "status" && entry.text === "Unknown")
                      )
                        reviewRequired = true;
                      if (entry.kind === "user") {
                        if (entry.submissionId) active = entry.submissionId;
                        if (pending && entry.requestId === pending.requestId) {
                          pending = null;
                          uncertain = false;
                          error = null;
                        }
                      }
                      if (entry.kind === "status" && entry.submissionId) {
                        if (entry.text === "Running") active = entry.submissionId;
                        else if (
                          ["Completed", "Failed", "Stopped"].includes(entry.text) &&
                          active === entry.submissionId
                        )
                          active = null;
                      }
                    }
                    get.set(revision, get.once(revision) + 1);
                  }
                  get.set(state, {
                    ...current,
                    entries: [...entries.values()].slice(-500),
                    cursor: page.cursor,
                    active: reviewRequired ? null : active,
                    pending: reviewRequired ? null : pending,
                    uncertain: reviewRequired || uncertain,
                    reviewRequired,
                    error: reviewRequired
                      ? "A workspace operation needs review. Review the transcript and files, or start a new workspace. Further changes are blocked here."
                      : error,
                    connection: "Live",
                  });
                }),
              ),
            );
          }).pipe(Effect.scoped, Effect.ignore);
          get.set(state, { ...get.once(state), connection: "Reconnecting…" });
          yield* Effect.sleep("2 seconds");
        }
      }),
    )
    .pipe(Atom.setIdleTTL(0));

  const admission = Atom.fn<void>()(
    Effect.fnUntraced(function* (_, get) {
      const message = get(draft).trim();
      const current = get(state);

      if (
        !message ||
        current.pending ||
        current.active ||
        current.uncertain ||
        current.admissionRequestId
      )
        return;

      const input = yield* Schema.decodeEffect(MessageInput)({
        workspaceId: id,
        requestId: crypto.randomUUID(),
        message,
      });

      const pending: Pending = { requestId: input.requestId, text: message };

      get.set(state, { ...current, pending, admissionRequestId: input.requestId, error: null });
      get.set(draft, "");
      yield* json("messages", Accepted, input).pipe(
        Effect.tap((accepted) =>
          Effect.sync(() => {
            const latest = get(state);

            if (latest.reviewRequired || latest.admissionRequestId !== input.requestId) return;

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
            });
          }),
        ),
        Effect.catch((error) =>
          Effect.sync(() => {
            const latest = get(state);

            if (latest.reviewRequired || latest.admissionRequestId !== input.requestId) return;
            const rejected = Schema.is(RequestRejected)(error) && error.status < 500;

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
                  : "Delivery could not be confirmed. Watching workspace events; the message will not be resent.",
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

      if (current.pending || current.active || current.uncertain || current.admissionRequestId)
        return;
      get.set(admission, value);
    },
  );

  const stop = Atom.fn<void>()(
    Effect.fnUntraced(function* (_, get) {
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

  return { state, draft, directory, selectedFile, files, file, events, send, stop, refresh };
});
