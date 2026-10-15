import { InMemory } from "@yielded/agent";
import {
  CredentialAccessError,
  CredentialBinding,
  CredentialForm,
  CredentialFillError,
  FillCredentialRequest,
  type CredentialAccessRequest,
} from "@yielded/agent-platform-cloudflare/browser-credentials";
import {
  BrowserSessionReference,
  BrowserSessions,
} from "@yielded/agent-platform-cloudflare/browser-session";
import { ThreadId } from "@yielded/agent/identifiers";
import type { ThreadHistory } from "@yielded/agent/thread-history";
import { Cause, Clock, Effect, Exit, Fiber, type Layer, Option, Redacted, Schema } from "effect";

import { Browser, makeBrowser } from "./browser.ts";
import {
  LabError,
  browserCommandTimeoutMillis,
  requiresReasoning,
  supportsUltrafast,
  defaultChallenge,
  racePrompt,
  coffeeDemo,
  Report,
  PrivateEntry,
  type ModelApi,
  type ModelId,
  type RunInput,
  type Snapshot,
  type ViewMode,
} from "./contract.ts";
import type { connectKitesurf } from "./kitesurf.ts";
import { executeTask } from "./runner.ts";
import { makeTrace, traceOtlp, Trace } from "./telemetry.ts";
import { TraceStore } from "./trace-store.ts";

/** Validated provider keys for one run: the visitor's, or the lab's for a funded account. */
export interface VisitorKeys {
  readonly openai?: string;
}

const RetainedEntry = Schema.Struct({
  ...PrivateEntry.fields,
  form: CredentialForm,
  binding: CredentialBinding,
});

type RetainedEntry = typeof RetainedEntry.Type;

const entrySummary = (entry: RetainedEntry): PrivateEntry => ({
  id: entry.id,
  runId: entry.runId,
  kind: entry.form.kind,
  target: entry.target,
  expiresAt: entry.expiresAt,
  status: entry.status,
  ...(entry.outcome === undefined ? {} : { outcome: entry.outcome }),
});

export const Control = Schema.Struct({
  version: Schema.Literal(6),
  privateEntry: Schema.NullOr(RetainedEntry),
  view: Schema.NullOr(
    Schema.Struct({
      url: Schema.String,
      mode: Schema.Literals(["watch", "control"]),
      expiresAt: Schema.Finite,
    }),
  ),
  chatId: Schema.NullOr(Schema.String),
  reference: Schema.NullOr(Schema.toCodecJson(BrowserSessionReference)),
  active: Schema.NullOr(Schema.String),
  lastRun: Schema.NullOr(Schema.String),
  admitted: Schema.Array(Schema.String).check(Schema.isMaxLength(100)),
  report: Schema.NullOr(Report),
});

export type Control = typeof Control.Type;

export const emptyControl: Control = {
  version: 6,
  privateEntry: null,
  view: null,
  chatId: null,
  reference: null,
  active: null,
  lastRun: null,
  admitted: [],
  report: null,
};

export interface OwnerStore {
  readonly read: () => Control;
  /** Implementations must atomically persist the whole control record. */
  readonly write: (state: Control, transition: string) => void;
  readonly alarm: (at: number | null) => Effect.Effect<void, LabError>;
}

/** The host alone owns references, run admission and cleanup. Lost actions are never replayed. */
export const makeOwner = (
  store: OwnerStore,
  config: {
    model: string;
    apiKey: string;
    /** Completed messages are retained by the host; each request owns its storage Scope. */
    history: Layer.Layer<ThreadHistory, LabError>;
    apiUrl?: string;
    apiType?: typeof ModelApi.Type;
    browserConfigured?: boolean;
    models?: ReadonlyArray<{
      model: ModelId;
      label: string;
      apiKey: string;
      apiUrl?: string;
      apiType?: typeof ModelApi.Type;
    }>;
    /** Public labs use visitor keys or same-origin funded credentials. */
    public?: boolean;
    build?: { readonly id: string; readonly tag: string; readonly timestamp: string };
    kitesurf?: (
      retainClose: Parameters<typeof connectKitesurf>[1],
    ) => ReturnType<typeof connectKitesurf>;
  },
) => {
  let trace: Trace["Service"] | undefined;
  let running: Fiber.Fiber<void, LabError> | undefined;
  let changingBrowser = false;
  let image: string | null = null;
  let notice: string | null = null;
  let closeKitesurf: Effect.Effect<void, LabError> | undefined;

  const patch = (change: Partial<Control>, transition: string) =>
    store.write({ ...store.read(), ...change }, transition);

  const provider = <A, E extends { readonly _tag: string }, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError((error) =>
        LabError.make({
          code: "browser",
          message: `Browser operation failed (${error._tag}). Close the browser before trying a fresh run.`,
        }),
      ),
    );

  const snapshot = Effect.fnUntraced(function* (): Effect.fn.Return<typeof Snapshot.Type> {
    const state = store.read();
    const now = yield* Clock.currentTimeMillis;

    return {
      ready:
        state.report?.cleanup !== "failed" &&
        ((state.reference !== null && state.reference.expiresAt > now) ||
          closeKitesurf !== undefined),
      busy: state.active !== null || changingBrowser,
      canClose:
        running === undefined &&
        !changingBrowser &&
        (state.reference !== null || state.active !== null || state.report?.cleanup === "failed"),
      model: config.model,
      browserConfigured: config.browserConfigured ?? true,
      agentConfigured: config.apiKey.length > 0,
      models: (config.models ?? []).map(({ model, label, apiKey }) => ({
        id: model,
        label,
        configured: apiKey.length > 0,
      })),
      public: config.public ?? false,
      chatId: state.chatId,
      viewMode: state.view?.mode ?? "watch",
      privateEntry:
        state.privateEntry === null
          ? null
          : {
              ...entrySummary(state.privateEntry),
              status:
                state.privateEntry.status === "pending" && state.privateEntry.expiresAt <= now
                  ? "expired"
                  : state.privateEntry.status,
            },
      report: trace?.snapshot() ?? state.report,
      liveViewUrl:
        state.reference !== null &&
        state.reference.expiresAt > now &&
        state.view !== null &&
        state.view.expiresAt > now
          ? state.view.url
          : null,
      image,
      notice:
        state.active !== null && running === undefined
          ? "The previous request is unresolved. Close the browser to recover; it will not be replayed."
          : notice,
    };
  });

  const closeReference = Effect.fnUntraced(function* () {
    patch({ chatId: null, view: null, privateEntry: null }, "chat-ended");
    if (closeKitesurf !== undefined) {
      yield* closeKitesurf;
      closeKitesurf = undefined;
    }
    const reference = store.read().reference;

    if (reference !== null) {
      const sessions = yield* BrowserSessions;

      yield* provider(sessions.close(reference.sessionId));
      patch({ reference: null }, "closed");
    }
    yield* store.alarm(null);
  });

  const setView = Effect.fnUntraced(function* (mode: typeof ViewMode.Type) {
    const sessions = yield* BrowserSessions;
    const reference = store.read().reference;

    if (reference === null)
      return yield* LabError.make({
        code: "expired",
        message: "This browser has closed. Start a new chat.",
      });
    const session = yield* provider(sessions.attach(reference));
    const now = yield* Clock.currentTimeMillis;
    const expiresInMs = Math.max(60_000, Math.min(3_600_000, session.reference.expiresAt - now));

    const view = yield* provider(
      mode === "control"
        ? session.getLiveView(Effect.void, { mode: "tab", expiresInMs })
        : session.getReadOnlyLiveView(Effect.void, { mode: "tab", expiresInMs }),
    );

    patch(
      {
        view: {
          mode,
          url: Redacted.value(view.devtoolsFrontendUrl),
          expiresAt: now + expiresInMs,
        },
      },
      "view-changed",
    );
  }, Effect.scoped);

  const view = Effect.fnUntraced(function* (mode: typeof ViewMode.Type) {
    const state = store.read();

    if (
      state.active !== null ||
      running !== undefined ||
      changingBrowser ||
      state.privateEntry?.status === "pending" ||
      state.report?.cleanup === "failed"
    )
      return yield* LabError.make({
        code: "busy",
        message: "Wait for the agent to finish before taking over.",
      });
    if (state.chatId === null || state.reference === null)
      return yield* LabError.make({
        code: "expired",
        message: "This browser has closed. Start a new chat.",
      });
    changingBrowser = true;
    yield* Effect.gen(function* () {
      const traces = yield* TraceStore;

      yield* setView(mode);
      if (state.report !== null)
        yield* traces.control(state.report, mode).pipe(
          Effect.catch((error) =>
            Effect.sync(() => {
              notice = error.message;
            }),
          ),
        );
    }).pipe(
      Effect.scoped,
      Effect.ensuring(
        Effect.sync(() => {
          changingBrowser = false;
        }),
      ),
    );
  });

  const saveEntry = Effect.fnUntraced(function* (entry: RetainedEntry, transition: string) {
    const summary = entrySummary(entry);
    const report = store.read().report;
    const updated = report === null ? null : { ...report, privateEntry: summary };

    patch({ privateEntry: entry, report: updated }, transition);
    trace = undefined;
    if (updated !== null) {
      const traces = yield* TraceStore;

      yield* traces.entry(updated, summary).pipe(
        Effect.catch((error) =>
          Effect.sync(() => {
            notice = error.message;
          }),
        ),
      );
    }

    return summary;
  });

  // The transient resolver is supplied by the HTTP composition root. This owner never receives values.
  const authorizeEntry = Effect.fnUntraced(function* (
    id: string,
    request: CredentialAccessRequest,
  ) {
    const state = store.read();
    const entry = state.privateEntry;

    if (
      !changingBrowser ||
      state.active !== null ||
      entry?.id !== id ||
      entry.status !== "uncertain" ||
      entry.expiresAt <= (yield* Clock.currentTimeMillis) ||
      state.reference === null ||
      state.reference.expiresAt <= (yield* Clock.currentTimeMillis) ||
      request.credential !== id ||
      request.kind !== entry.form.kind ||
      request.target.topOrigin !== entry.target.topOrigin ||
      request.target.frameOrigin !== entry.target.frameOrigin ||
      request.target.recipientOrigin !== entry.target.recipientOrigin ||
      request.roles.length !== entry.form.fields.length ||
      request.roles.some((role, index) => role !== entry.form.fields[index]?.role)
    )
      return yield* CredentialAccessError.make({ reason: "denied" });
  });

  const cancelPrivateEntry = Effect.fnUntraced(function* (id: string) {
    const state = store.read();
    const entry = state.privateEntry;

    if (changingBrowser || state.active !== null || running !== undefined)
      return yield* LabError.make({
        code: "busy",
        message: "Wait for the current browser operation.",
      });
    if (entry?.id !== id)
      return yield* LabError.make({
        code: "expired",
        message: "This private-entry request is no longer available.",
      });

    return entry.status === "pending"
      ? yield* saveEntry({ ...entry, status: "cancelled" }, "entry-cancelled")
      : entrySummary(entry);
  });

  const privateEntry = Effect.fnUntraced(function* (id: string) {
    const state = store.read();
    const entry = state.privateEntry;

    if (changingBrowser || state.active !== null || running !== undefined)
      return yield* LabError.make({
        code: "busy",
        message: "Wait for the current browser operation.",
      });
    if (entry?.id !== id)
      return yield* LabError.make({
        code: "expired",
        message: "This private-entry request is no longer available.",
      });
    // Repeated submissions return the retained outcome without resolving or filling again.
    if (entry.status !== "pending") return entrySummary(entry);
    const now = yield* Clock.currentTimeMillis;

    if (entry.expiresAt <= now || state.reference === null || state.reference.expiresAt <= now)
      return yield* saveEntry({ ...entry, status: "expired" }, "entry-expired");
    if (state.view?.mode === "control")
      return yield* LabError.make({
        code: "busy",
        message: "Return control before filling private information.",
      });

    const reference = state.reference;

    changingBrowser = true;

    return yield* Effect.gen(function* () {
      // Write uncertainty before external dispatch. A restart must never replay these values.
      yield* saveEntry({ ...entry, status: "uncertain" }, "entry-claimed");
      const sessions = yield* BrowserSessions;

      const result = yield* sessions.attach(reference).pipe(
        Effect.flatMap((session) =>
          session.fillCredential(
            FillCredentialRequest.make({
              ...entry.form,
              credential: entry.id,
              binding: entry.binding,
            }),
          ),
        ),
        Effect.result,
      );

      const outcome =
        result._tag === "Success"
          ? result.success
          : result.failure._tag === "CredentialFillError"
            ? result.failure
            : CredentialFillError.make({
                reason: "provider",
                dispatch: result.failure.dispatch,
                filled: 0,
                cleanup: result.failure.cleanup,
              });

      if ("cleanup" in outcome && outcome.cleanup !== "not-requested") {
        const report = store.read().report;

        if (outcome.cleanup === "confirmed") {
          patch(
            {
              reference: null,
              view: null,
              report: report === null ? null : { ...report, cleanup: "closed" },
            },
            "entry-browser-closed",
          );
          yield* store.alarm(null);
        } else {
          patch(
            { report: report === null ? null : { ...report, cleanup: "failed" } },
            "entry-cleanup-failed",
          );
          notice = "Browser cleanup could not be confirmed. Close the browser before continuing.";
        }
      }

      return yield* saveEntry(
        {
          ...entry,
          outcome,
          status:
            result._tag === "Success"
              ? "filled"
              : outcome.dispatch === "not-dispatched"
                ? "failed"
                : "uncertain",
        },
        "entry-settled",
      );
    }).pipe(
      Effect.scoped,
      Effect.ensuring(
        Effect.sync(() => {
          changingBrowser = false;
        }),
      ),
    );
  });

  const allocate = Effect.fnUntraced(function* () {
    const sessions = yield* BrowserSessions;
    const now = yield* Clock.currentTimeMillis;

    yield* store.alarm(now + 3_600_000);

    return yield* provider(
      sessions.create(
        {
          maxElapsedMillis: 3_600_000,
          keepAliveMillis: 600_000,
          commandTimeoutMillis: browserCommandTimeoutMillis,
        },
        (reference) => Effect.sync(() => patch({ reference }, "retained")),
      ),
    );
  });

  const stop = Effect.fnUntraced(function* () {
    if (running !== undefined) yield* Fiber.interrupt(running);
  });

  const close = Effect.fnUntraced(function* () {
    if (running !== undefined || changingBrowser)
      return yield* LabError.make({
        code: "busy",
        message: "Stop the active run before closing its browser.",
      });
    const traces = yield* TraceStore;
    const state = store.read();

    const latest =
      state.report === null
        ? null
        : ((yield* traces.report(state.report.input.id)) ?? state.report);

    yield* closeReference();

    patch(
      {
        active: null,
        report:
          latest?.status === "running"
            ? {
                ...latest,
                status: "cancelled",
                cleanup: "closed",
                message: "Lost request closed. No actions were replayed.",
              }
            : latest === null
              ? null
              : { ...latest, cleanup: "closed" },
      },
      "released",
    );
    const closedReport = store.read().report;

    if (closedReport !== null) {
      traces.sink(closedReport.input.id).checkpoint(closedReport);
      yield* traces.archive(closedReport.input.id).pipe(
        Effect.catch((error) =>
          Effect.sync(() => {
            notice = error.message;
          }),
        ),
      );
    }
    trace = undefined;
    image = null;
  });

  /** Visitor keys apply to one run only; they never enter owner state, reports or telemetry. */
  const run = Effect.fnUntraced(function* (requested: RunInput, keys: VisitorKeys = {}) {
    const openai = !(requested.model ?? config.model).startsWith("@cf/");

    if (!openai && (requested.reasoning !== undefined || requested.serviceTier !== undefined))
      return yield* LabError.make({
        code: "invalid",
        message: "Reasoning and service tier are available only for OpenAI models.",
      });
    if (
      requested.serviceTier === "ultrafast" &&
      !supportsUltrafast(requested.model ?? config.model)
    )
      return yield* LabError.make({
        code: "invalid",
        message: "Ultrafast is available only for GPT-6 Astra and GPT-6.1 Sol.",
      });
    if (requested.scenario === "chat" && !requested.prompt.trim())
      return yield* LabError.make({
        code: "invalid",
        message: "Write a message to the agent.",
      });
    if (
      requested.chatId !== undefined &&
      (requested.scenario !== "chat" || requested.url !== undefined)
    )
      return yield* LabError.make({
        code: "invalid",
        message:
          "Follow-up messages use the current browser. Start a new chat to choose a starting URL.",
      });
    if (requested.scenario === "chat" && requested.engine === "kitesurf")
      return yield* LabError.make({
        code: "invalid",
        message: "Chat uses Chromium to keep the browser between messages.",
      });
    const reasoning = requested.reasoning ?? "none";

    const input: RunInput = {
      ...requested,
      engine: requested.engine ?? "chromium",
      ...(openai
        ? {
            reasoning:
              reasoning === "none" && requiresReasoning(requested.model ?? config.model)
                ? "low"
                : reasoning,
            serviceTier: requested.serviceTier ?? "fast",
          }
        : {}),
      prompt:
        requested.scenario === "wikipedia"
          ? racePrompt(requested.wikipedia ?? defaultChallenge)
          : requested.scenario === "coffee"
            ? coffeeDemo.prompt
            : requested.prompt,
    };

    const state = store.read();

    if (
      changingBrowser ||
      state.active !== null ||
      state.admitted.includes(input.id) ||
      input.id === state.lastRun
    )
      return yield* LabError.make({
        code: "busy",
        message: "This run is active or was already admitted. It will not be replayed.",
      });
    if (state.privateEntry?.status === "pending")
      return yield* LabError.make({
        code: "busy",
        message: "Complete or skip private entry before continuing.",
      });
    if (state.view?.mode === "control")
      return yield* LabError.make({
        code: "busy",
        message: "Return control to the agent before sending a message.",
      });
    if (state.admitted.length >= 100)
      return yield* LabError.make({
        code: "invalid",
        message:
          "This tab has reached its 100-run limit. Close the browser, export your results, and open a fresh tab to start a new session.",
      });
    if (state.report?.cleanup === "failed")
      return yield* LabError.make({
        code: "busy",
        message: "Close the browser to resolve the previous cleanup failure before another run.",
      });
    const continuing = input.chatId !== undefined;

    if (
      continuing &&
      (state.chatId !== input.chatId ||
        state.reference === null ||
        state.reference.expiresAt <= (yield* Clock.currentTimeMillis))
    )
      return yield* LabError.make({
        code: "expired",
        message: "This chat has ended. Start a new chat; previous actions will not be replayed.",
      });

    if (input.engine === "kitesurf" && config.kitesurf === undefined)
      return yield* LabError.make({
        code: "configuration",
        message: "Kitesurf is not configured in this host.",
      });

    const selectedModel =
      input.model === undefined
        ? config
        : config.models?.find((candidate) => candidate.model === input.model);

    if (selectedModel === undefined)
      return yield* LabError.make({
        code: "configuration",
        message: "This model is not available in the lab.",
      });
    const cloudflareModel = selectedModel.model.startsWith("@cf/");
    const modelKey = cloudflareModel ? selectedModel.apiKey : (keys.openai ?? selectedModel.apiKey);

    if (!modelKey)
      return yield* LabError.make({
        code: "configuration",
        message: cloudflareModel
          ? "This model is not available in the lab."
          : "Add your OpenAI key to run the agent.",
      });
    const traces = yield* TraceStore;

    trace = yield* makeTrace(input, selectedModel.model, [modelKey]);
    const current = trace;

    current.event("runtime.build", config.build ?? { id: "development" });
    let retainChat = input.scenario === "chat";

    image = null;
    notice = null;
    patch(
      {
        active: input.id,
        lastRun: input.id,
        admitted: [...state.admitted, input.id],
        report: current.snapshot(),
        privateEntry: null,
      },
      "admitted",
    );

    const attempt = Effect.fnUntraced(function* (ordinal: number) {
      const sessions = yield* BrowserSessions;

      current.update({
        message:
          ordinal === 1
            ? "Preparing browser and starting page…"
            : "Retrying browser preparation · attempt 2 of 2…",
      });
      if (!continuing)
        yield* current.measure("cleanup", "Close previous browser", closeReference());

      const session = yield* Effect.gen(function* () {
        if (input.engine === "kitesurf") {
          const open = config.kitesurf;

          if (open === undefined)
            return yield* LabError.make({
              code: "configuration",
              message: "Kitesurf is unavailable.",
            });
          yield* store.alarm((yield* Clock.currentTimeMillis) + 900_000);

          return yield* current.measure(
            "setup",
            `Connect Kitesurf · preparation ${ordinal}/2`,
            open((close) => {
              closeKitesurf = close;
            }),
          );
        }
        if (!continuing)
          yield* current.measure("setup", `Launch browser · preparation ${ordinal}/2`, allocate());
        else
          yield* store.alarm(store.read().reference?.expiresAt ?? (yield* Clock.currentTimeMillis));
        const reference = store.read().reference;

        if (reference === null)
          return yield* LabError.make({
            code: "expired",
            message: "The browser is no longer available.",
          });

        return yield* current.measure(
          "setup",
          "Attach to browser",
          provider(sessions.attach(reference)),
        );
      });

      const browser = yield* makeBrowser(session, input.screenshots, (value) => {
        image = value;
      });

      const identity = yield* current.measure(
        "setup",
        "Verify browser engine",
        "identity" in session
          ? Effect.succeed(session.identity)
          : browser.native(async (page) => {
              const cdp = await page.createCDPSession();

              try {
                return await cdp.send("Browser.getVersion");
              } finally {
                await cdp.detach();
              }
            }),
      );

      current.update({
        browserVersion: identity.product,
        browserRevision: identity.revision,
        browserUserAgent: identity.userAgent,
      });
      if ((input.engine === "kitesurf") !== (identity.revision === "@kitesurf"))
        return yield* LabError.make({
          code: "configuration",
          message:
            "The provider returned a different browser engine. The requested engine will not be replaced.",
        });

      if (
        input.liveView &&
        "getReadOnlyLiveView" in session &&
        (store.read().view?.expiresAt ?? 0) <= (yield* Clock.currentTimeMillis)
      )
        yield* current.measure("setup", "Connect read-only Live View", setView("watch")).pipe(
          Effect.catch(() =>
            Effect.sync(() => {
              notice = "Live View is unavailable. Enable screenshots to see action snapshots.";
            }),
          ),
        );
      else if (input.liveView && !("getReadOnlyLiveView" in session))
        notice =
          "Kitesurf has no persistent Live View. Enable screenshots to see action snapshots.";
      if (input.scenario === "chat" && !continuing) patch({ chatId: input.id }, "chat-started");

      const result = yield* executeTask(
        input,
        selectedModel.model,
        modelKey,
        selectedModel.apiUrl,
        selectedModel.apiType,
        input.scenario === "chat" ? ThreadId.make(input.chatId ?? input.id) : undefined,
      ).pipe(
        Effect.provide(input.scenario === "chat" ? config.history : InMemory.layer),
        Effect.provideService(Browser, browser),
      );

      const entryForm = result.handoff?.privateEntry;

      if (entryForm !== null && entryForm !== undefined && "prepareCredential" in session) {
        const prepared = yield* session
          .prepareCredential(CredentialForm.make(entryForm))
          .pipe(Effect.result);

        if (prepared._tag === "Success") {
          const entry: RetainedEntry = {
            id: crypto.randomUUID(),
            runId: input.id,
            form: entryForm,
            binding: prepared.success,
            target: prepared.success.target,
            status: "pending",
            expiresAt: Math.min(
              session.reference.expiresAt,
              (yield* Clock.currentTimeMillis) + 600_000,
            ),
          };

          patch({ privateEntry: entry }, "entry-requested");
          current.update({ privateEntry: entrySummary(entry) });
          current.event("private-entry.requested", entrySummary(entry));
        } else {
          if (prepared.failure._tag === "BrowserSessionError")
            return yield* provider(Effect.fail(prepared.failure));
          notice =
            "This form does not support private entry. Take over to enter your details on the website.";
          current.event("private-entry.unavailable", { reason: prepared.failure.reason });
        }
      }
      current.update({ finishedAt: current.now() });
      if (input.screenshots || input.scenario !== "chat")
        yield* browser.capture(true).pipe(
          Effect.catch(() =>
            Effect.sync(() => {
              retainChat = false;
              if (input.scenario === "chat")
                notice = "The final screenshot was unavailable. This chat has ended.";
            }),
          ),
        );
    });

    const work = Effect.gen(function* () {
      for (const ordinal of [1, 2]) {
        const result = yield* attempt(ordinal).pipe(
          Effect.provideService(Trace, current),
          Effect.scoped,
          Effect.result,
        );

        if (result._tag === "Success") return;
        // Only host preparation can retry, on a fresh browser after confirmed closure.
        // The ready boundary is set before any generated code or model call.
        if (
          ordinal === 2 ||
          continuing ||
          current.snapshot().readyAt !== null ||
          result.failure.code !== "browser"
        )
          return yield* result.failure;
      }
    }).pipe(
      Effect.scoped,
      Effect.timeoutOrElse({
        duration: "12 minutes",
        orElse: () =>
          Effect.fail(
            LabError.make({
              code: "browser",
              message: "The twelve-minute run deadline was reached.",
            }),
          ),
      }),
      Effect.onExit((exit) => {
        if (current.snapshot().finishedAt === null) current.update({ finishedAt: current.now() });

        const cleanup = current.measure("cleanup", "Close browser", closeReference()).pipe(
          Effect.match({
            onSuccess: () => current.update({ cleanup: "closed" }),
            onFailure: () => {
              current.update({
                cleanup: "failed",
                status: "failed",
                message: "Browser cleanup failed. Close the browser to retry cleanup.",
              });
            },
          }),
        );

        if (Exit.isSuccess(exit) && retainChat)
          return Effect.gen(function* () {
            const now = yield* Clock.currentTimeMillis;

            yield* store.alarm(Math.min(store.read().reference?.expiresAt ?? now, now + 600_000));
            current.update({ cleanup: "retained" });
          }).pipe(Effect.catch(() => cleanup));

        return cleanup;
      }),
    );

    const child = yield* Effect.forkChild(
      work.pipe(
        Effect.withSpan("browser-use.run", {
          attributes: { "run.id": input.id, "conversation.id": input.chatId ?? input.id },
        }),
        Effect.provide(traceOtlp(input.id)),
        Effect.provideService(Trace, current),
      ),
    );

    running = child;

    const result = yield* Fiber.await(child).pipe(
      Effect.ensuring(
        Fiber.interrupt(child).pipe(
          Effect.andThen(
            Effect.sync(() => {
              running = undefined;
            }),
          ),
        ),
      ),
    );

    running = undefined;
    if (Exit.isFailure(result)) {
      const error = Cause.findErrorOption(result.cause);

      current.update({
        status: Cause.hasInterrupts(result.cause) ? "cancelled" : "failed",
        message: Option.isSome(error)
          ? error.value.message
          : Cause.hasInterrupts(result.cause)
            ? "Stopped. No actions will be replayed."
            : "The run failed unexpectedly. Inspect its trace.",
      });
    }
    current.update({ elapsed: current.now() });
    current.event("run.settled", current.snapshot());
    const report = current.snapshot();

    patch({ active: null, report }, "settled");
    yield* traces.archive(input.id).pipe(
      Effect.catch((error) =>
        Effect.sync(() => {
          notice = error.message;
        }),
      ),
    );

    return report;
  });

  return { snapshot, run, stop, close, view, privateEntry, cancelPrivateEntry, authorizeEntry };
};
