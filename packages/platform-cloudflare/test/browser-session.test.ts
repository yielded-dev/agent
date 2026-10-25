import { expect, it } from "@effect/vitest";
import { BrowserUseError } from "@yielded/agent/browser-use";
import * as FailureDiagnostic from "@yielded/agent/failure-diagnostic";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as ErrorReporter from "effect/ErrorReporter";
import * as Fiber from "effect/Fiber";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import { beforeEach, vi } from "vite-plus/test";

import {
  BrowserCredentialAccess,
  CredentialForm,
  FillCredentialRequest,
  LoginCredential,
} from "../src/BrowserCredentials.ts";
import {
  BrowserRunHandoffRequest,
  BrowserSessionError,
  BrowserSessionOptions,
  BrowserSessions,
} from "../src/BrowserSession.ts";
import * as BrowserUse from "../src/BrowserUse.ts";
import { BrowserRunBinding } from "../src/internal/browser-binding.ts";
import { BrowserRunReadonlyLiveView } from "../src/internal/browser-readonly-live-view.ts";
import {
  BrowserRunCleanupError,
  BrowserRunSessionLifecycle,
} from "../src/internal/browser-session-lifecycle.ts";
import { browserResponse } from "./browser-response.ts";

const provider = vi.hoisted(() => ({
  alive: false,
  pages: 0,
  title: "Checkout",
  human: false,
  pageFailure: false,
  malformedHandoff: false,
  lostWriteReply: false,
  prepareReply: undefined as (() => Promise<void>) | undefined,
  writeReply: undefined as ((count: number) => Promise<void>) | undefined,
  credentialWrites: 0,
  disposedFields: 0,
  cleanupPending: false,
  acquired: 0,
  closed: [] as string[],
  retirements: 0,
  delayRetirement: undefined as (() => void) | undefined,
  acknowledgeRetirement: undefined as (() => void) | undefined,
  frameFailure: undefined as "target-info" | "attach" | "send" | undefined,
  synchronousTargetFailure: false,
  cdpFailure: undefined as string | undefined,
  cdpDetachFailure: false,
  cdpAttachReply: undefined as (() => Promise<void>) | undefined,
  cdpDetached: 0,
  cdpDetachReply: undefined as (() => Promise<void>) | undefined,
  cdpSendReply: undefined as ((method: string) => Promise<void>) | undefined,
}));

// Substitute only the SDK/provider seam. The real attachment transport, scopes, authority,
// timeout and exact-session cleanup wiring run in workerd; Chromium owns DOM tests separately.
vi.mock("puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js", () => ({
  default: {
    connect: async ({ transport }: { transport: { onclose?: () => void } }) => {
      if (!provider.alive) throw new Error("private-expired-provider");
      let connected = true;

      transport.onclose = () => {
        connected = false;
        provider.retirements++;
      };

      const fields = {
        prepare: async () => {
          await provider.prepareReply?.();

          return { action: "https://merchant.test/login", shape: "[]" };
        },
        target: () => ({ action: "https://merchant.test/login", shape: "[]" }),
        fill: () => {
          provider.credentialWrites++;
          if (provider.lostWriteReply) throw new Error("private-write-reply");

          return provider.writeReply?.(provider.credentialWrites).then(() => "filled") ?? "filled";
        },
      };

      const frame = {
        _id: "checkout-frame",
        parentFrame: () => null,
        detached: false,
        url: () => "https://merchant.test/login",
        client: {
          send: () => {
            if (provider.frameFailure === "target-info") {
              const error = Object.assign(new Error("private-provider-url-and-credential"), {
                name: provider.synchronousTargetFailure ? "ConnectionClosedError" : "ProtocolError",
              });

              if (provider.synchronousTargetFailure) throw error;

              return Promise.reject(error);
            }

            return Promise.resolve({ targetInfo: { targetId: "checkout-target" } });
          },
          connection: () => ({
            createSession: async () => {
              if (provider.frameFailure === "attach")
                throw Object.assign(new Error("private-provider-url-and-credential"), {
                  name: "TargetCloseError",
                });

              return {
                send: async () => {
                  if (provider.frameFailure === "send")
                    throw Object.assign(new Error("private-provider-url-and-credential"), {
                      name: "ProtocolError",
                    });

                  return {};
                },
                detach: async () => {},
              };
            },
          }),
        },
        isolatedRealm: () => ({
          evaluateHandle: async () => ({
            evaluate: async (callback: (...args: unknown[]) => unknown, ...args: unknown[]) =>
              callback(fields, ...args),
            dispose: async () => {
              provider.disposedFields++;
            },
          }),
        }),
      };

      const page = {
        url: frame.url,
        mainFrame: () => frame,
        frames: () => [frame],
        isClosed: () => false,
        browserContext: () => ({ id: "owner-context" }),
        title: async () => provider.title,
        goto: async (url: string) => {
          if (!connected) throw new Error("retired connection");
          provider.title = url;

          return null;
        },
        createCDPSession: async () => {
          await provider.cdpAttachReply?.();

          return {
            send: async (method: string) => {
              await provider.cdpSendReply?.(method);
              if (provider.cdpFailure === method) throw new Error("private-cdp-command");
              if (method === "Target.getTargetInfo")
                return { targetInfo: { targetId: "owner-page" } };
              if (method === "Cloudflare.handoff") {
                provider.human = true;

                return provider.malformedHandoff ? {} : { handoffId: "human-1" };
              }
              if (method === "Cloudflare.getHandoffState")
                return { active: provider.human, handoffId: "human-1" };
              throw new Error("unsupported fixture command");
            },
            detach: async () => {
              provider.cdpDetached++;
              await provider.cdpDetachReply?.();
              if (provider.cdpDetachFailure) throw new Error("private-cdp-detach");
            },
          };
        },
      };

      const context = {
        id: "owner-context",
        pages: async () => [page],
        newPage: async () => {
          if (provider.pageFailure) throw new Error("private-page-setup");
          provider.pages++;

          return page;
        },
      };

      return {
        isConnected: () => connected,
        browserContexts: () => [context],
        createBrowserContext: async () => context,
      };
    },
  },
}));

const id = "00000000-0000-4000-8000-000000000092";

const options = BrowserSessionOptions.make({
  maxElapsedMillis: 60_000,
  commandTimeoutMillis: 1_000,
});

const layer = BrowserSessions.layerNoDeps.pipe(
  Layer.provide(
    BrowserRunReadonlyLiveView.layer({
      accountId: "1234567890abcdef1234567890abcdef",
      apiToken: Redacted.make("fixture-token"),
    }).pipe(Layer.provide(FetchHttpClient.layer)),
  ),
  Layer.provide(
    BrowserRunBinding.layer({
      fetch: async (_input, init) => {
        if (init?.method === "POST") {
          provider.alive = true;
          provider.acquired++;
        }

        const response = browserResponse(init, id);

        if (init?.method !== "POST" && provider.delayRetirement !== undefined) {
          const socket = response.webSocket!;
          const close = socket.close.bind(socket);
          let state: number = WebSocket.OPEN;

          Object.defineProperty(socket, "readyState", {
            get: () => state,
            configurable: true,
          });
          Object.defineProperty(socket, "close", {
            value: () => {
              state = WebSocket.CLOSING;
              provider.acknowledgeRetirement = () => {
                state = WebSocket.CLOSED;
                close();
              };
              provider.delayRetirement?.();
            },
          });
        }

        return response;
      },
    }),
  ),
  Layer.provide(
    Layer.succeed(BrowserRunSessionLifecycle, {
      close: Effect.fnUntraced(function* (sessionId) {
        provider.closed.push(Redacted.value(sessionId));
        if (provider.cleanupPending)
          return yield* BrowserRunCleanupError.make({ reason: "pending" });
        provider.alive = false;
      }),
    }),
  ),
);

beforeEach(() =>
  Object.assign(provider, {
    alive: false,
    pages: 0,
    title: "Checkout",
    human: false,
    pageFailure: false,
    malformedHandoff: false,
    lostWriteReply: false,
    prepareReply: undefined,
    writeReply: undefined,
    credentialWrites: 0,
    disposedFields: 0,
    cleanupPending: false,
    acquired: 0,
    closed: [],
    retirements: 0,
    delayRetirement: undefined,
    acknowledgeRetirement: undefined,
    frameFailure: undefined,
    synchronousTargetFailure: false,
    cdpFailure: undefined,
    cdpDetachFailure: false,
    cdpAttachReply: undefined,
    cdpDetached: 0,
    cdpDetachReply: undefined,
    cdpSendReply: undefined,
  }),
);

it.effect("retains native causes privately through the session and CDP bridge", () =>
  Effect.gen(function* () {
    const host = yield* BrowserSessions;
    const reference = yield* host.create(options, () => Effect.void);
    const session = yield* host.attach(reference);
    const original = new Error("native command failed", { cause: new Error("socket disappeared") });
    const error = yield* session.run(Effect.void, () => Promise.reject(original)).pipe(Effect.flip);

    expect(JSON.stringify(FailureDiagnostic.capture(error))).toContain("socket disappeared");
    expect(JSON.stringify(error)).not.toContain("socket disappeared");

    const { client } = yield* BrowserUse.make(session, {
      authorize: () => Effect.void,
      maxCommands: null,
      maxReturnedBytes: 4096,
    });

    provider.frameFailure = "target-info";

    const commandError = yield* client
      .send({ method: "Runtime.evaluate", frameId: "checkout-frame" })
      .pipe(Effect.flip);

    expect(JSON.stringify(FailureDiagnostic.capture(commandError))).toContain(
      "private-provider-url-and-credential",
    );
    expect(JSON.stringify(commandError)).not.toContain("private-provider-url-and-credential");
  }).pipe(Effect.provide(layer), Effect.scoped),
);

it.effect(
  "allows an unbounded command count while finite attachments still stop before dispatch",
  () =>
    Effect.gen(function* () {
      const host = yield* BrowserSessions;
      const reference = yield* host.create(options, () => Effect.void);
      const session = yield* host.attach(reference);

      const unlimited = yield* BrowserUse.make(session, {
        authorize: () => Effect.void,
        maxCommands: null,
        maxReturnedBytes: 4096,
      });

      for (let index = 0; index < 11; index++) yield* unlimited.client.frames({});

      const limited = yield* BrowserUse.make(session, {
        authorize: () => Effect.void,
        maxCommands: 1,
        maxReturnedBytes: 4096,
      });

      yield* limited.client.frames({});
      const error = yield* limited.client.frames({}).pipe(Effect.flip);

      expect(error).toMatchObject({ code: "invalid", dispatch: "not-dispatched" });
    }).pipe(Effect.provide(layer), Effect.scoped),
);

// https://github.com/yielded-dev/agent/commit/0090e61f
// An unpredictable provider rejection must retain its stage without SDK content.
it.effect.each([
  { stage: "target-info", synchronous: false },
  { stage: "target-info", synchronous: true },
  { stage: "attach", synchronous: false },
  { stage: "send", synchronous: false },
] as const)(
  "preserves the private frame $stage failure stage and precise dispatch evidence (synchronous=$synchronous)",
  ({ stage, synchronous }) => {
    const reports: Array<Cause.Cause<unknown>> = [];
    const attachmentReports: Array<Cause.Cause<unknown>> = [];

    return Effect.gen(function* () {
      const host = yield* BrowserSessions;
      const reference = yield* host.create(options, () => Effect.void);
      const session = yield* host.attach(reference);

      const { client } = yield* BrowserUse.make(session, {
        authorize: () => Effect.void,
        maxCommands: 10,
        maxReturnedBytes: 4096,
      });

      provider.frameFailure = stage;
      provider.synchronousTargetFailure = synchronous;

      const error = yield* client
        .send({
          method: "Runtime.evaluate",
          frameId: "checkout-frame",
          params: { expression: "private-provider-url-and-credential" },
        })
        .pipe(
          Effect.provide(
            ErrorReporter.layer([ErrorReporter.make(({ cause }) => reports.push(cause))]),
          ),
          Effect.flip,
        );

      expect(error).toMatchObject({
        code: "browser",
        dispatch: stage === "send" ? "possibly-dispatched" : "not-dispatched",
      });
      expect(provider.closed).toEqual([]);
      expect(reports).toHaveLength(1);
      expect(attachmentReports).toHaveLength(0);
      expect(ErrorReporter.isIgnored(error)).toBe(false);
      expect(Cause.findErrorOption(reports[0])).toMatchObject({
        value: {
          _tag: "BrowserRunFailure",
          operation: `browser-use.frame.${stage}`,
          reason: "provider",
          cause: {
            name: synchronous
              ? "ConnectionClosedError"
              : stage === "attach"
                ? "TargetCloseError"
                : "ProtocolError",
          },
        },
      });
      expect(JSON.stringify(reports)).not.toContain("private-provider-url-and-credential");
      expect(reports.map((cause) => Cause.pretty(cause)).join("\n")).not.toContain(
        "private-provider-url-and-credential",
      );
      expect(JSON.stringify(error)).not.toContain("private-provider-url-and-credential");
    }).pipe(
      Effect.scoped,
      Effect.provide([
        layer,
        ErrorReporter.layer([ErrorReporter.make(({ cause }) => attachmentReports.push(cause))]),
      ]),
    );
  },
);

// https://github.com/yielded-dev/agent/commit/ea396e5a0
// Failed acquisition and its failed cleanup must both remain available to the host.
it.effect.each([
  { method: "create", cleanup: "confirmed" },
  { method: "createAttached", cleanup: "confirmed" },
  { method: "create", cleanup: "unconfirmed" },
  { method: "createAttached", cleanup: "unconfirmed" },
] as const)(
  "reports $cleanup cleanup after $method fails before retention",
  ({ method, cleanup }) =>
    Effect.gen(function* () {
      const host = yield* BrowserSessions;

      provider.pageFailure = true;
      provider.cleanupPending = cleanup === "unconfirmed";
      let retained = false;

      const retain = () =>
        Effect.sync(() => {
          retained = true;
        });

      const attempt =
        method === "create"
          ? host.create(options, retain).pipe(Effect.asVoid)
          : host.createAttached(options, retain).pipe(Effect.asVoid);

      const failed = yield* attempt.pipe(Effect.exit);

      expect(failed._tag).toBe("Failure");
      if (failed._tag !== "Failure") return;
      const error = Cause.findErrorOption(failed.cause);

      expect(error).toMatchObject({
        value: { reason: "provider", dispatch: "not-dispatched", cleanup },
      });
      if (error._tag === "Some") expect(ErrorReporter.isIgnored(error.value)).toBe(true);
      const diagnostic = JSON.stringify(FailureDiagnostic.capture(failed));

      expect(diagnostic).toContain("private-page-setup");
      if (cleanup === "unconfirmed") expect(diagnostic).toContain("BrowserRunCleanupError");
      expect(provider.closed).toEqual([id]);
      expect(retained).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

// https://github.com/yielded-dev/agent/commit/252b88a66
it.effect.each([false, true])(
  "preserves authorization causes without dispatch or provider reclassification (defect=%s)",
  (withDefect) =>
    Effect.gen(function* () {
      const host = yield* BrowserSessions;
      const reference = yield* host.create(options, () => Effect.void);
      const session = yield* host.attach(reference);
      const defect = new Error("private-authorization-cleanup");
      let dispatched = false;

      const failed = yield* session
        .run(
          Effect.failCause(
            withDefect
              ? Cause.combine(Cause.fail("authorization-denied"), Cause.die(defect))
              : Cause.fail("authorization-denied"),
          ),
          async () => {
            dispatched = true;
          },
        )
        .pipe(Effect.exit);

      expect(failed._tag).toBe("Failure");
      if (failed._tag !== "Failure") return;
      expect(Cause.findErrorOption(failed.cause)).toMatchObject({ value: "authorization-denied" });
      if (withDefect) expect(Cause.findDefect(failed.cause)).toMatchObject({ success: defect });
      expect(dispatched).toBe(false);
      expect(provider.closed).toEqual(withDefect ? [id] : []);
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

// https://github.com/yielded-dev/agent/commit/0090e61fb
it.effect.each(["Target.getTargetInfo", "Cloudflare.handoff"])(
  "preserves both %s and CDP detach failures privately",
  (method) =>
    Effect.gen(function* () {
      const host = yield* BrowserSessions;

      const session =
        method === "Target.getTargetInfo"
          ? undefined
          : yield* host.createAttached(options, () => Effect.void);

      provider.cdpFailure = method;
      provider.cdpDetachFailure = true;

      const failed = yield* (
        session === undefined
          ? host.create(options, () => Effect.void).pipe(Effect.asVoid)
          : session
              .handoff(
                Effect.void,
                BrowserRunHandoffRequest.make({ instructions: "Review", timeout: 1000 }),
              )
              .pipe(Effect.asVoid)
      ).pipe(Effect.exit);

      expect(failed._tag).toBe("Failure");
      if (failed._tag !== "Failure") return;
      const diagnostic = JSON.stringify(FailureDiagnostic.capture(failed));

      expect(diagnostic).toContain("private-cdp-command");
      expect(diagnostic).toContain("private-cdp-detach");
      for (const reason of failed.cause.reasons) {
        if (
          Cause.isFailReason(reason) &&
          Schema.is(BrowserSessionError)(reason.error) &&
          Object.prototype.isPrototypeOf.call(BrowserSessionError.prototype, reason.error)
        ) {
          const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(BrowserSessionError))(
            reason.error,
          );

          expect(encoded).not.toContain("private-cdp");
        }
      }
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("preserves the whole failed retention cause when annotating allocation cleanup", () =>
  Effect.gen(function* () {
    const host = yield* BrowserSessions;

    const original = BrowserSessionError.make({
      reason: "provider",
      dispatch: "not-dispatched",
      cleanup: "not-requested",
    });

    const defect = new Error("private-retention-defect");

    const failed = yield* host
      .createAttached(options, () =>
        Effect.failCause(Cause.combine(Cause.fail(original), Cause.die(defect))),
      )
      .pipe(Effect.exit);

    expect(failed._tag).toBe("Failure");
    if (failed._tag !== "Failure") return;
    expect(failed.cause.reasons).toHaveLength(2);
    expect(Cause.findErrorOption(failed.cause)).toMatchObject({ value: { cleanup: "confirmed" } });
    expect(Cause.findDefect(failed.cause)).toMatchObject({ success: defect });
    expect(provider.closed).toEqual([id]);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect.each(["create", "command"] as const)(
  "keeps a detach-only failure typed during %s",
  (stage) =>
    Effect.gen(function* () {
      const host = yield* BrowserSessions;

      const session =
        stage === "command" ? yield* host.createAttached(options, () => Effect.void) : undefined;

      provider.cdpDetachFailure = true;

      const failed = yield* (
        session === undefined
          ? host.create(options, () => Effect.void).pipe(Effect.asVoid)
          : session.getHandoffState(Effect.void).pipe(Effect.asVoid)
      ).pipe(Effect.exit);

      expect(failed._tag).toBe("Failure");
      if (failed._tag !== "Failure") return;
      expect(Cause.findErrorOption(failed.cause)).toMatchObject({
        value: {
          _tag: "BrowserSessionError",
          reason: "provider",
          dispatch: stage === "create" ? "not-dispatched" : "possibly-dispatched",
        },
      });
      expect(Cause.hasDies(failed.cause)).toBe(false);
      expect(JSON.stringify(FailureDiagnostic.capture(failed))).toContain("private-cdp-detach");
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

// https://github.com/yielded-dev/agent/commit/72e07a3501
it.effect.each(["connect", "command"] as const)(
  "retains mixed causes through BrowserUse %s",
  (stage) =>
    Effect.gen(function* () {
      const host = yield* BrowserSessions;
      const session = yield* host.createAttached(options, () => Effect.void);

      const error = BrowserUseError.make({
        code: "invalid",
        dispatch: "not-dispatched",
        message: "denied",
      });

      const defect = new Error("private-authority-finalizer");

      const bridge = BrowserUse.make(session, {
        authorize: (command) =>
          (command.kind === "connect") === (stage === "connect")
            ? Effect.failCause(Cause.combine(Cause.fail(error), Cause.die(defect)))
            : Effect.void,
        maxCommands: null,
        maxReturnedBytes: 4096,
      });

      const failed = yield* (
        stage === "connect"
          ? bridge.pipe(Effect.asVoid)
          : bridge.pipe(
              Effect.flatMap(({ client }) => client.frames({})),
              Effect.asVoid,
            )
      ).pipe(Effect.exit);

      expect(failed._tag).toBe("Failure");
      if (failed._tag !== "Failure") return;
      expect(Cause.findErrorOption(failed.cause)).toMatchObject({ value: error });
      expect(Cause.findDefect(failed.cause)).toMatchObject({ success: defect });
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

// https://github.com/yielded-dev/agent/commit/72e07a3501
it.effect("retains the browser command failure when its diagnostic reporter throws", () =>
  Effect.gen(function* () {
    const host = yield* BrowserSessions;
    const session = yield* host.createAttached(options, () => Effect.void);

    const { client } = yield* BrowserUse.make(session, {
      authorize: () => Effect.void,
      maxCommands: null,
      maxReturnedBytes: 4096,
    });

    provider.frameFailure = "send";

    const failed = yield* client
      .send({ method: "Runtime.evaluate", frameId: "checkout-frame" })
      .pipe(
        Effect.provide(
          ErrorReporter.layer([
            ErrorReporter.make(() => {
              throw new Error("reporter-defect");
            }),
          ]),
        ),
        Effect.exit,
      );

    expect(failed._tag).toBe("Failure");
    if (failed._tag !== "Failure") return;
    expect(JSON.stringify(FailureDiagnostic.capture(failed))).toContain(
      "private-provider-url-and-credential",
    );
    const error = Cause.findErrorOption(failed.cause);

    expect(error._tag).toBe("Some");
    if (error._tag !== "Some") return;
    expect(error.value).toMatchObject({
      _tag: "BrowserUseError",
      code: "browser",
      dispatch: "possibly-dispatched",
    });
    if (error.value._tag !== "BrowserUseError") return;
    const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(BrowserUseError))(error.value);

    expect(encoded).not.toContain("private-provider-url-and-credential");
    expect(JSON.stringify(error.value)).not.toContain("private-provider-url-and-credential");
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect.each(["connect", "command"] as const)(
  "keeps typed cleanup failure through interrupted BrowserUse %s",
  (stage) =>
    Effect.gen(function* () {
      const host = yield* BrowserSessions;
      const session = yield* host.createAttached(options, () => Effect.void);
      const started = yield* Deferred.make<void>();

      const error = BrowserUseError.make({
        code: "browser",
        message: "private-authorization-cleanup",
        dispatch: "not-dispatched",
      });

      const bridge = BrowserUse.make(session, {
        maxCommands: null,
        maxReturnedBytes: 4096,
        authorize: (command) =>
          command.kind === "connect" && stage !== "connect"
            ? Effect.void
            : Effect.acquireUseRelease(
                Effect.void,
                () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
                () => Effect.fail(error),
              ),
      });

      const operation =
        stage === "connect"
          ? bridge.pipe(Effect.asVoid)
          : bridge.pipe(
              Effect.flatMap(({ client }) => client.frames({})),
              Effect.asVoid,
            );

      const fiber = yield* operation.pipe(Effect.forkChild);

      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      const failed = yield* Fiber.await(fiber);

      expect(failed._tag).toBe("Failure");
      if (failed._tag !== "Failure") return;
      expect(Cause.hasInterrupts(failed.cause)).toBe(true);
      expect(Cause.findErrorOption(failed.cause)).toMatchObject({ value: error });
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

// https://github.com/yielded-dev/agent/commit/72e07a3501
it.effect("detaches a late CDP attachment without delaying the command timeout", () =>
  Effect.gen(function* () {
    const host = yield* BrowserSessions;
    const session = yield* host.createAttached(options, () => Effect.void);
    const started = yield* Deferred.make<void>();
    const reply = yield* Deferred.make<void>();
    const returned = yield* Deferred.make<void>();
    const detached = yield* Deferred.make<void>();
    const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());

    provider.cdpDetachReply = () =>
      runPromise(Deferred.succeed(detached, undefined).pipe(Effect.asVoid));

    provider.cdpAttachReply = async () => {
      await runPromise(Deferred.succeed(started, undefined));
      await runPromise(Deferred.await(reply));
      await runPromise(Deferred.succeed(returned, undefined));
    };
    const before = provider.cdpDetached;

    const fiber = yield* session
      .handoff(
        Effect.void,
        BrowserRunHandoffRequest.make({ instructions: "Review", timeout: 1000 }),
      )
      .pipe(Effect.exit, Effect.forkChild);

    yield* Deferred.await(started);
    yield* TestClock.adjust(1000);
    expect(fiber.pollUnsafe()).toBeDefined();
    const failed = yield* Fiber.join(fiber);

    expect(failed._tag).toBe("Failure");
    expect(provider.closed).toEqual([id]);
    yield* Deferred.succeed(reply, undefined);
    yield* Deferred.await(returned);
    yield* Deferred.await(detached);
    expect(provider.cdpDetached).toBe(before + 1);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("detaches CDP when cancellation wins the acquisition handoff", () =>
  Effect.gen(function* () {
    const host = yield* BrowserSessions;
    const session = yield* host.createAttached(options, () => Effect.void);
    const before = provider.cdpDetached;
    let interrupt: (() => void) | undefined;

    provider.cdpAttachReply = async () => {
      let remaining = 2;

      const schedule = () => {
        if (remaining-- > 0) queueMicrotask(schedule);
        else interrupt?.();
      };

      queueMicrotask(schedule);
    };

    const fiber = yield* session
      .handoff(
        Effect.void,
        BrowserRunHandoffRequest.make({ instructions: "Review", timeout: 1000 }),
      )
      .pipe(Effect.forkChild);

    interrupt = () => fiber.interruptUnsafe();
    yield* Fiber.await(fiber);
    yield* Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 5)));
    expect(provider.cdpDetached).toBe(before + 1);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("bounds a pending CDP release with the command timeout", () =>
  Effect.gen(function* () {
    const host = yield* BrowserSessions;
    const session = yield* host.createAttached(options, () => Effect.void);
    const releasing = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());

    provider.cdpDetachReply = async () => {
      await runPromise(Deferred.succeed(releasing, undefined));
      await runPromise(Deferred.await(release));
    };

    const fiber = yield* session
      .handoff(
        Effect.void,
        BrowserRunHandoffRequest.make({ instructions: "Review", timeout: 1000 }),
      )
      .pipe(Effect.exit, Effect.forkChild);

    yield* Deferred.await(releasing);
    yield* TestClock.adjust(1000);
    yield* Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 0)));
    const settled = fiber.pollUnsafe();

    yield* Deferred.succeed(release, undefined);
    const failed = yield* Fiber.join(fiber);

    expect(settled).toBeDefined();
    expect(failed._tag).toBe("Failure");
    if (failed._tag !== "Failure") return;
    expect(JSON.stringify(FailureDiagnostic.capture(failed))).toContain("cleanup");
    expect(JSON.stringify(FailureDiagnostic.capture(failed))).toContain("timeout");
    expect(Cause.findErrorOption(failed.cause)).toMatchObject({
      value: { dispatch: "possibly-dispatched" },
    });
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("keeps a typed CDP cleanup failure when the caller cancels during release", () =>
  Effect.gen(function* () {
    const host = yield* BrowserSessions;
    const session = yield* host.createAttached(options, () => Effect.void);
    const releasing = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());

    provider.cdpDetachReply = async () => {
      await runPromise(Deferred.succeed(releasing, undefined));
      await runPromise(Deferred.await(release));
    };
    provider.cdpDetachFailure = true;
    const fiber = yield* session.getHandoffState(Effect.void).pipe(Effect.forkChild);

    yield* Deferred.await(releasing);
    fiber.interruptUnsafe();
    yield* Deferred.succeed(release, undefined);
    const failed = yield* Fiber.await(fiber);

    expect(failed._tag).toBe("Failure");
    if (failed._tag !== "Failure") return;
    expect(Cause.hasInterrupts(failed.cause)).toBe(true);
    expect(Cause.findErrorOption(failed.cause)).toMatchObject({
      value: { _tag: "BrowserSessionError", reason: "provider" },
    });
    expect(Cause.hasDies(failed.cause)).toBe(false);
    expect(JSON.stringify(FailureDiagnostic.capture(failed))).toContain("private-cdp-detach");
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("preserves acquisition finalizer defects when creation times out", () =>
  Effect.gen(function* () {
    const host = yield* BrowserSessions;
    const started = yield* Deferred.make<void>();
    const defect = new Error("private-retention-finalizer");

    const fiber = yield* host
      .create(options, () =>
        Effect.addFinalizer(() => Effect.die(defect)).pipe(
          Effect.andThen(Deferred.succeed(started, undefined)),
          Effect.andThen(Effect.never),
        ),
      )
      .pipe(Effect.exit, Effect.forkChild);

    yield* Deferred.await(started);
    yield* TestClock.adjust(30_000);
    const failed = yield* Fiber.join(fiber);

    expect(failed._tag).toBe("Failure");
    if (failed._tag !== "Failure") return;
    expect(Cause.findErrorOption(failed.cause)).toMatchObject({
      value: { reason: "timeout", cleanup: "confirmed" },
    });
    expect(Cause.findDefect(failed.cause)).toMatchObject({ success: defect });
    expect(provider.closed).toEqual([id]);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

// https://github.com/yielded-dev/agent/commit/72e07a3501
it.effect("timeout retains authorization finalizer failure", () =>
  Effect.gen(function* () {
    const host = yield* BrowserSessions;
    const session = yield* host.createAttached(options, () => Effect.void);
    const started = yield* Deferred.make<void>();

    const authorization = Deferred.succeed(started, undefined).pipe(
      Effect.andThen(Effect.never),
      Effect.ensuring(Effect.die(new Error("private-auth-finalizer"))),
    );

    const fiber = yield* session
      .run(authorization, async () => "must not dispatch")
      .pipe(Effect.exit, Effect.forkChild);

    yield* Deferred.await(started);
    yield* TestClock.adjust(1000);
    const failed = yield* Fiber.join(fiber);

    expect(failed._tag).toBe("Failure");
    const diagnostic = JSON.stringify(FailureDiagnostic.capture(failed));

    expect(diagnostic).toContain("private-auth-finalizer");
    expect(diagnostic).toContain("timeout");
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

// https://github.com/yielded-dev/agent/commit/72e07a3501
it.effect("timeout retains target probe cleanup failure", () =>
  Effect.gen(function* () {
    const host = yield* BrowserSessions;
    const started = yield* Deferred.make<void>();
    const reply = yield* Deferred.make<void>();
    const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());

    provider.cdpSendReply = async () => {
      await runPromise(Deferred.succeed(started, undefined));
      await runPromise(Deferred.await(reply));
    };
    provider.cdpDetachFailure = true;

    const fiber = yield* host
      .create(options, () => Effect.void)
      .pipe(Effect.exit, Effect.forkChild);

    yield* Deferred.await(started);
    yield* TestClock.adjust(30_000);
    const failed = yield* Fiber.join(fiber);

    yield* Deferred.succeed(reply, undefined);
    expect(failed._tag).toBe("Failure");
    const diagnostic = JSON.stringify(FailureDiagnostic.capture(failed));

    expect(diagnostic).toContain("private-cdp-detach");
    expect(diagnostic).toContain("timeout");
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

// Actual cancellation must preserve the target probe's typed cleanup failure.
it.effect.each(["create", "createAttached"] as const)(
  "preserves target probe cleanup during interrupted %s",
  (method) =>
    Effect.gen(function* () {
      const host = yield* BrowserSessions;
      const started = yield* Deferred.make<void>();
      const reply = yield* Deferred.make<void>();
      const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());

      provider.cdpSendReply = async () => {
        await runPromise(Deferred.succeed(started, undefined));
        await runPromise(Deferred.await(reply));
      };
      provider.cdpDetachFailure = true;

      const fiber = yield* host[method](options, () => Effect.void).pipe(Effect.forkChild);

      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      const failed = yield* Fiber.await(fiber);

      yield* Deferred.succeed(reply, undefined);
      expect(failed._tag).toBe("Failure");
      if (failed._tag !== "Failure") return;
      expect(Cause.hasInterrupts(failed.cause)).toBe(true);
      expect(Cause.findErrorOption(failed.cause)).toMatchObject({
        value: { _tag: "BrowserSessionError", reason: "provider" },
      });
      expect(JSON.stringify(FailureDiagnostic.capture(failed))).toContain("private-cdp-detach");
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("closes a late allocation reply without retaining or connecting it", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const respond = yield* Deferred.make<void>();
    const cleaned = yield* Deferred.make<void>();
    const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());
    let retained = false;

    const lateLayer = BrowserSessions.layerNoDeps.pipe(
      Layer.provide(
        BrowserRunReadonlyLiveView.layer({
          accountId: "1234567890abcdef1234567890abcdef",
          apiToken: Redacted.make("fixture-token"),
        }).pipe(Layer.provide(FetchHttpClient.layer)),
      ),
      Layer.provide(
        BrowserRunBinding.layer({
          fetch: async (_input, init) => {
            await runPromise(Deferred.succeed(started, undefined));
            await runPromise(Deferred.await(respond));

            return browserResponse(init, id);
          },
        }),
      ),
      Layer.provide(
        Layer.succeed(BrowserRunSessionLifecycle, {
          close: (sessionId) =>
            Effect.sync(() => provider.closed.push(Redacted.value(sessionId))).pipe(
              Effect.andThen(Deferred.succeed(cleaned, undefined)),
              Effect.asVoid,
            ),
        }),
      ),
    );

    const fiber = yield* BrowserSessions.use((host) =>
      host.create(options, () =>
        Effect.sync(() => {
          retained = true;
        }),
      ),
    ).pipe(Effect.provide(lateLayer), Effect.forkChild);

    yield* Deferred.await(started);
    yield* Fiber.interrupt(fiber);
    yield* Deferred.succeed(respond, undefined);
    yield* Deferred.await(cleaned);
    expect(provider.closed).toEqual([id]);
    expect(provider.pages).toBe(0);
    expect(retained).toBe(false);
  }),
);

it.effect("closes an uncertain handoff even when the provider reply is malformed", () =>
  Effect.gen(function* () {
    const host = yield* BrowserSessions;
    const reference = yield* host.create(options, () => Effect.void);
    const session = yield* host.attach(reference);

    provider.malformedHandoff = true;
    expect(
      yield* session
        .handoff(
          Effect.void,
          BrowserRunHandoffRequest.make({ instructions: "Review", timeout: 30_000 }),
        )
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "provider", dispatch: "possibly-dispatched", cleanup: "confirmed" });
    expect(provider.closed).toEqual([id]);
    expect(yield* session.run(Effect.void, (page) => page.title()).pipe(Effect.flip)).toMatchObject(
      { reason: "closed" },
    );
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

// Regression in ae4b2da8: preparation could outlive the session while holding its lock.
it.effect("bounds credential preparation by the remaining session lifetime", () =>
  Effect.gen(function* () {
    const host = yield* BrowserSessions;
    const reference = yield* host.create(options, () => Effect.void);
    const session = yield* host.attach(reference);
    const waiting = yield* Deferred.make<void>();
    const reply = yield* Deferred.make<void>();
    const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());

    yield* Effect.addFinalizer(() => Deferred.succeed(reply, undefined));
    provider.prepareReply = async () => {
      await runPromise(Deferred.succeed(waiting, undefined));
      await runPromise(Deferred.await(reply));
    };
    yield* TestClock.adjust(59_750);

    const fiber = yield* session
      .prepareCredential(
        CredentialForm.make({
          kind: "login",
          fields: [
            { selector: "#username", role: "username" },
            { selector: "#password", role: "password" },
          ],
        }),
      )
      .pipe(Effect.flip, Effect.forkChild);

    yield* Deferred.await(waiting);
    yield* TestClock.adjust(250);
    yield* Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 0)));
    expect(fiber.pollUnsafe()).toBeDefined();
    expect(yield* Fiber.join(fiber)).toMatchObject({
      reason: "timeout",
      dispatch: "not-dispatched",
      cleanup: "confirmed",
    });
    expect(provider.closed).toEqual([id]);
    expect(provider.disposedFields).toBe(1);
    expect(yield* session.run(Effect.void, (page) => page.title()).pipe(Effect.flip)).toMatchObject(
      {
        reason: "closed",
      },
    );
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect.each([
  {
    stage: "write reply",
    writes: 2,
    dispatch: "possibly-dispatched",
    filled: 1,
    cleanup: "confirmed",
    budget: 1_000,
  },
  {
    stage: "write reply",
    writes: 2,
    dispatch: "possibly-dispatched",
    filled: 1,
    cleanup: "unconfirmed",
    budget: 1_000,
  },
] as const)(
  "retains credential progress on timeout at $stage ($cleanup cleanup, $budget ms)",
  ({ writes, dispatch, filled, cleanup, budget }) =>
    Effect.gen(function* () {
      const host = yield* BrowserSessions;
      const reference = yield* host.create(options, () => Effect.void);
      const session = yield* host.attach(reference);
      const waiting = yield* Deferred.make<void>();
      const reply = yield* Deferred.make<void>();
      const replied = yield* Deferred.make<void>();
      const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());

      provider.cleanupPending = cleanup === "unconfirmed";
      if (budget < reference.commandTimeoutMillis) yield* TestClock.adjust(60_000 - budget);
      provider.writeReply = async (count) => {
        if (count !== 2) return;
        await runPromise(Deferred.succeed(waiting, undefined));
        await runPromise(Deferred.await(reply));
        await runPromise(Deferred.succeed(replied, undefined));
      };

      const fiber = yield* session
        .fillCredential(
          FillCredentialRequest.make({
            credential: "login-1",
            kind: "login",
            fields: [
              { selector: "#username", role: "username" },
              { selector: "#password", role: "password" },
            ],
          }),
        )
        .pipe(
          Effect.provideService(BrowserCredentialAccess, {
            authorize: () => Effect.void,
            resolve: () =>
              Effect.succeed(
                LoginCredential.make({
                  username: Redacted.make("user"),
                  password: Redacted.make("dummy-password"),
                }),
              ),
          }),
          Effect.flip,
          Effect.forkChild,
        );

      yield* Deferred.await(waiting);
      yield* TestClock.adjust(budget);
      const error = yield* Fiber.join(fiber);

      expect(error).toMatchObject({
        _tag: "CredentialFillError",
        reason: "timeout",
        dispatch,
        filled,
        cleanup,
      });
      expect(provider.closed).toEqual([id]);
      expect(provider.retirements).toBe(2);
      expect(provider.disposedFields).toBe(1);
      expect(
        yield* session.run(Effect.void, (page) => page.title()).pipe(Effect.flip),
      ).toMatchObject({ reason: "closed" });
      {
        yield* Deferred.succeed(reply, undefined);
        yield* Deferred.await(replied);
      }
      expect(provider.credentialWrites).toBe(writes);
      expect(error).toMatchObject({ dispatch, filled, cleanup });
      expect(JSON.stringify(error)).not.toContain("dummy-password");
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("preserves a completed command while a delayed disconnect acknowledgment arrives", () =>
  Effect.gen(function* () {
    const effectContext = yield* Effect.context<never>();

    const host = yield* BrowserSessions;
    const reference = yield* host.create(options, () => Effect.void);
    const closing = yield* Deferred.make<void>();

    provider.delayRetirement = () =>
      Effect.runSyncWith(effectContext)(Deferred.succeed(closing, undefined));
    let completed = false;

    const attempt = yield* Effect.gen(function* () {
      const session = yield* host.attach(reference);

      return yield* session.run(Effect.void, (page) => page.title());
    }).pipe(
      Effect.scoped,
      Effect.tap(() =>
        Effect.sync(() => {
          completed = true;
        }),
      ),
      Effect.forkChild,
    );

    yield* Deferred.await(closing);
    yield* TestClock.adjust(1_500);
    expect(completed).toBe(false);
    expect(provider.retirements).toBe(2);
    provider.acknowledgeRetirement?.();
    expect(yield* Fiber.join(attempt)).toBe("Checkout");
    expect(provider.closed).toEqual([]);
    expect(provider.retirements).toBe(2);
  }).pipe(Effect.provide(layer)),
);
