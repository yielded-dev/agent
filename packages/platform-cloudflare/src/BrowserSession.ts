/// <reference types="@cloudflare/workers-types" />

import * as FailureDiagnostic from "@yielded/agent/failure-diagnostic";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import type {
  Browser,
  CDPSession,
  Page,
} from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import {
  CredentialFillError,
  type CredentialBinding,
  type CredentialForm,
  type BrowserCredentialAccess,
  type CredentialFillResult,
  type FillCredentialRequest,
} from "./BrowserCredentials.ts";
import {
  BrowserRunHandoffRequest,
  BrowserRunHandoffResult,
  BrowserRunHandoffState,
  BrowserRunLiveViewRequest,
  BrowserRunLiveViewResult,
} from "./InteractiveBrowser.ts";
import { BrowserRunBinding, type BrowserRunAttachment } from "./internal/browser-binding.ts";
import {
  BrowserSessionPage,
  fillCredential,
  prepareCredential,
} from "./internal/browser-credentials.ts";
import {
  inheritBrowserReport,
  reportBrowserCause,
  reportedBrowserError,
} from "./internal/browser-failure.ts";
import { BrowserRunReadonlyLiveView } from "./internal/browser-readonly-live-view.ts";
import {
  BrowserRunSessionLifecycle,
  type BrowserRunLifecycleOptions,
} from "./internal/browser-session-lifecycle.ts";

export {
  BrowserRunHandoffRequest,
  BrowserRunHandoffResult,
  BrowserRunHandoffState,
  BrowserRunLiveViewRequest,
  BrowserRunLiveViewResult,
};

const PositiveMillis = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);

const SessionId = Schema.Redacted(Schema.String.check(Schema.isUUID()));
const ProviderId = Schema.Redacted(Schema.NonEmptyString.check(Schema.isMaxLength(256)));

/** Private host record, not a model capability. Persist together with the owner's controller state. */
export class BrowserSessionReference extends Schema.Class<BrowserSessionReference>(
  "BrowserSessionReference",
)({
  version: Schema.Literal(1),
  sessionId: SessionId,
  contextId: ProviderId,
  targetId: ProviderId,
  expiresAt: PositiveMillis,
  commandTimeoutMillis: PositiveMillis,
}) {}

export class BrowserSessionOptions extends Schema.Class<BrowserSessionOptions>(
  "BrowserSessionOptions",
)({
  maxElapsedMillis: PositiveMillis,
  keepAliveMillis: Schema.optionalKey(PositiveMillis.check(Schema.isLessThanOrEqualTo(600_000))),
  commandTimeoutMillis: Schema.optionalKey(PositiveMillis),
}) {}

/** No SDK text, selectors, page data, or credential values are retained in public failures. */
export class BrowserSessionError extends Schema.TaggedError<BrowserSessionError>()(
  "BrowserSessionError",
  {
    reason: Schema.Literals([
      "invalid",
      "expired",
      "missing-page",
      "busy",
      "closed",
      "provider",
      "timeout",
      "cleanup",
    ]),
    dispatch: Schema.Literals(["not-dispatched", "possibly-dispatched"]),
    cleanup: Schema.Literals(["not-requested", "confirmed", "unconfirmed"]),
  },
) {
  /** Original host-only cause. Excluded from the error Schema and JSON transport. */
  declare readonly cause?: unknown;
}

/**
 * One local attachment to an application-owned browser. Scope release disconnects; it never
 * transfers ownership or closes a healthy remote session. The owner serializes attachments,
 * fences old attempts, authorizes human access, and calls BrowserSessions.close on expiry/stop.
 * Native callbacks are trusted host code: never start unawaited work. Adapters retaining SDK
 * handles must scope their release and use run for every operation on the retained page.
 */
export interface BrowserSession {
  readonly reference: BrowserSessionReference;
  /**
   * Check current authority under the lock before native dispatch. A settled SDK rejection leaves
   * the session available for inspection, but may have changed the website; never retry blindly.
   * Timeout/interruption fences outstanding SDK work and terminates the exact browser.
   * An optional timeoutMillis narrows the command deadline, for example for bounded preparation.
   */
  readonly run: <A, E, R>(
    authorize: Effect.Effect<void, E, R>,
    action: (page: Page) => Promise<A>,
    options?: { readonly timeoutMillis: number },
  ) => Effect.Effect<A, E | BrowserSessionError, R>;
  /**
   * Prepare a delayed private-entry request within the command/session deadline; timeout closes
   * the browser. The host owns persistence, expiry and one-use consumption.
   */
  readonly prepareCredential: (
    request: CredentialForm,
  ) => Effect.Effect<CredentialBinding, CredentialFillError | BrowserSessionError>;
  /**
   * Resolves host-owned material under fresh credential grants. Never submits or retries a write.
   * Credential timeouts retain acknowledged writes and dispatch evidence alongside cleanup.
   */
  readonly fillCredential: (
    request: FillCredentialRequest,
  ) => Effect.Effect<
    CredentialFillResult,
    CredentialFillError | BrowserSessionError,
    BrowserCredentialAccess
  >;
  readonly handoff: <E, R>(
    authorize: Effect.Effect<void, E, R>,
    request: BrowserRunHandoffRequest,
  ) => Effect.Effect<BrowserRunHandoffResult, E | BrowserSessionError, R>;
  readonly getLiveView: <E, R>(
    authorize: Effect.Effect<void, E, R>,
    request: BrowserRunLiveViewRequest,
  ) => Effect.Effect<BrowserRunLiveViewResult, E | BrowserSessionError, R>;
  /**
   * Mint a provider-enforced read-only connection to this exact page. Requires REST credentials;
   * missing read-only confirmation fails closed, never falling back to an interactive URL.
   * Expiry limits new connections, not established connections or access to visible page data.
   */
  readonly getReadOnlyLiveView: <E, R>(
    authorize: Effect.Effect<void, E, R>,
    request: BrowserRunLiveViewRequest,
  ) => Effect.Effect<BrowserRunLiveViewResult, E | BrowserSessionError, R>;
  readonly getHandoffState: <E, R>(
    authorize: Effect.Effect<void, E, R>,
  ) => Effect.Effect<BrowserRunHandoffState, E | BrowserSessionError, R>;
}

const failure = (
  reason: BrowserSessionError["reason"],
  dispatch: BrowserSessionError["dispatch"] = "not-dispatched",
  cleanup: BrowserSessionError["cleanup"] = "not-requested",
  cause?: unknown,
) => {
  const error = BrowserSessionError.make({ reason, dispatch, cleanup });

  return cause === undefined ? error : FailureDiagnostic.withCause(error, cause);
};

const decode = <A>(schema: Schema.Codec<A>, input: unknown) =>
  Schema.decodeUnknownEffect(schema, { onExcessProperty: "error" })(input).pipe(
    Effect.catchCause((cause) =>
      Effect.failCause(
        Cause.map(cause, (error) => failure("invalid", "not-dispatched", "not-requested", error)),
      ),
    ),
  );

/** Scope finalizers require E=never. Preserve pure cancellation and wrap every other cause privately. */
const cleanupFinalizer = <A, R>(effect: Effect.Effect<A, BrowserSessionError, R>) =>
  effect.pipe(
    Effect.asVoid,
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.failCause(Cause.fromReasons<never>(cause.reasons.filter(Cause.isInterruptReason)))
        : Effect.die(failure("cleanup", "not-dispatched", "unconfirmed", cause)),
    ),
  );

/** A deadline owns its work until stopped; cleanup failures join the timeout outcome. */
const timeout =
  <B, E2, R2>(options: {
    readonly duration: Duration.Input;
    readonly orElse: () => Effect.Effect<B, E2, R2>;
  }) =>
  <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A | B, E | E2, R | R2> =>
    Effect.suspend(() => {
      let consumed = false;

      const stop = (fiber: Fiber.Fiber<A, E>) =>
        Effect.uninterruptible(
          Effect.gen(function* () {
            if (consumed) return;
            yield* Fiber.interrupt(fiber);
            const exit = yield* Fiber.await(fiber);

            consumed = true;
            if (Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause))
              return yield* Effect.failCause(exit.cause);
          }),
        );

      return Effect.uninterruptibleMask((restore) =>
        Effect.acquireUseRelease(
          Effect.gen(function* () {
            // Own the fiber separately without replacing the operation's resource Scope.
            const scope = yield* Scope.make();
            const fiber = yield* Effect.forkIn(self.pipe(Effect.interruptible), scope);

            return { scope, fiber };
          }),
          ({ fiber }) =>
            restore(
              Fiber.await(fiber).pipe(
                Effect.timeoutOrElse({
                  duration: options.duration,
                  orElse: () => Effect.succeed(undefined),
                }),
              ),
            ).pipe(
              Effect.flatMap((exit): Effect.Effect<A | B, E | E2, R2> => {
                if (exit !== undefined) {
                  consumed = true;

                  return exit;
                }

                return Effect.uninterruptible(
                  Effect.exit(stop(fiber)).pipe(
                    Effect.flatMap((stopped) =>
                      options
                        .orElse()
                        .pipe(
                          Effect.onExit(() =>
                            Exit.isFailure(stopped) ? Effect.failCause(stopped.cause) : Effect.void,
                          ),
                        ),
                    ),
                  ),
                );
              }),
            ),
          ({ scope, fiber }) => stop(fiber).pipe(Effect.onExit((exit) => Scope.close(scope, exit))),
        ),
      );
    });

// Catch inside the Promise before workerd can report an unhandled foreign diagnostic.
const native = Effect.fnUntraced(function* <A>(
  operation: string,
  action: (signal: AbortSignal) => Promise<A>,
) {
  const runReport = Effect.runPromiseWith(yield* Effect.context<never>());

  return yield* Effect.tryPromise({
    try: async (signal) => {
      try {
        return { ok: true as const, value: await action(signal) };
      } catch (cause) {
        if (!signal.aborted || cause !== signal.reason)
          await runReport(reportBrowserCause(operation, Cause.fail(cause)));

        return { ok: false as const, cause };
      }
    },
    catch: (cause) => failure("provider", "not-dispatched", "not-requested", cause),
  }).pipe(
    Effect.flatMap((result) =>
      result.ok
        ? Effect.succeed(result.value)
        : Effect.fail(
            reportedBrowserError(
              failure("provider", "not-dispatched", "not-requested", result.cause),
            ),
          ),
    ),
    Effect.withTracerEnabled(false),
  );
});

const withCdp = Effect.fnUntraced(function* <A, E, R>(
  page: Page,
  commandTimeoutMillis: number,
  action: (cdp: CDPSession) => Effect.Effect<A, E, R>,
) {
  const runCleanup = Effect.runPromiseWith(yield* Effect.context<never>());
  let acquired: CDPSession | undefined;
  let released = false;

  const release = Effect.uninterruptible(
    Effect.suspend(() => {
      if (acquired === undefined || released) return Effect.void;
      released = true;
      const cdp = acquired;

      return cleanupFinalizer(
        native("session.cdp.detach", () => cdp.detach()).pipe(
          timeout({
            duration: commandTimeoutMillis,
            orElse: () =>
              Effect.fail(failure("cleanup", "not-dispatched", "unconfirmed", failure("timeout"))),
          }),
        ),
      );
    }),
  );

  return yield* Effect.uninterruptibleMask((restore) =>
    Effect.acquireUseRelease(
      restore(
        native("session.cdp.attach", async (signal) => {
          const cdp = await page.createCDPSession();

          acquired = cdp;

          // The SDK cannot cancel attachment; dispose a handle that arrives after cancellation.
          if (signal.aborted) {
            await runCleanup(release);
            signal.throwIfAborted();
          }

          return cdp;
        }),
      ),
      (cdp) => restore(action(cdp)),
      () => release,
    ).pipe(
      // Cover cancellation after the Promise resolves but before acquisition resumes.
      Effect.onExit(() => release),
    ),
  );
});

const targetId = (page: Page, commandTimeoutMillis: number) =>
  withCdp(page, commandTimeoutMillis, (cdp) =>
    native(
      "session.target",
      async () => (await cdp.send("Target.getTargetInfo")).targetInfo.targetId,
    ),
  );

export interface BrowserSessionsOptions extends BrowserRunLifecycleOptions {
  readonly browser: Pick<BrowserRun, "fetch">;
}

/** Native Cloudflare sessions; no framework browser workflow, checkpoint transfer, or observation policy. */
export class BrowserSessions extends Context.Service<
  BrowserSessions,
  {
    /**
     * Allocate once and commit its private reference in retain. Failed/uncommitted creation closes
     * the allocation. After retain succeeds the application owns remote cleanup, independent of
     * attempt Scopes. A lost allocation reply can remain indeterminate until provider idle expiry.
     */
    readonly create: <E, R>(
      options: BrowserSessionOptions,
      retain: (reference: BrowserSessionReference) => Effect.Effect<void, E, R>,
    ) => Effect.Effect<BrowserSessionReference, E | BrowserSessionError, R>;
    /**
     * Allocate and retain as create does, keeping the initial attachment in the caller's Scope.
     * No session is exposed before retain succeeds. Failed acquisition releases its attachment
     * immediately; after retention the application owns remote cleanup. The 30-second acquisition
     * timeout ends before use; each command retains its own timeout and the fixed session expiry.
     * Failed acquisition reports confirmed cleanup only after exact-session termination succeeds.
     */
    readonly createAttached: <E, R>(
      options: BrowserSessionOptions,
      retain: (reference: BrowserSessionReference) => Effect.Effect<void, E, R>,
    ) => Effect.Effect<BrowserSession, E | BrowserSessionError, R | Scope.Scope>;
    /** Attach only the exact saved context/page. Never recreate missing/expired state. */
    readonly attach: (
      reference: BrowserSessionReference,
    ) => Effect.Effect<BrowserSession, BrowserSessionError, Scope.Scope>;
    /** Refresh provider inactivity only; the owner must still enforce its expiresAt. */
    readonly keepAlive: (
      sessionId: Redacted.Redacted<string>,
    ) => Effect.Effect<void, BrowserSessionError>;
    readonly close: (
      sessionId: Redacted.Redacted<string>,
    ) => Effect.Effect<void, BrowserSessionError>;
  }
>()("@effect-agent/platform-cloudflare/BrowserSessions") {
  static readonly layerNoDeps = Layer.effect(
    this,
    Effect.gen(function* () {
      const binding = yield* BrowserRunBinding;
      const lifecycle = yield* BrowserRunSessionLifecycle;
      const readonlyLiveView = yield* BrowserRunReadonlyLiveView;

      const close = (sessionId: Redacted.Redacted<string>) =>
        decode(SessionId, sessionId).pipe(
          Effect.flatMap((id) =>
            lifecycle
              .close(id)
              .pipe(
                Effect.catchCause((cause) =>
                  Effect.failCause(
                    Cause.map(cause, (error) =>
                      failure("cleanup", "not-dispatched", "unconfirmed", error),
                    ),
                  ),
                ),
              ),
          ),
        );

      const cleanup = (sessionId: Redacted.Redacted<string>) =>
        close(sessionId).pipe(
          Effect.as("confirmed" as const),
          Effect.tapCause((cause) => reportBrowserCause("session.close", cause)),
        );

      const connect = Effect.fnUntraced(function* (sessionId: Redacted.Redacted<string>) {
        let attachment: BrowserRunAttachment | undefined;

        const retire = Effect.suspend(() =>
          attachment === undefined
            ? Effect.void
            : attachment.retire.pipe(
                // Dispatch is already fenced; give the socket close handshake the same
                // bounded window as keepalive instead of failing a settled command after 1s.
                timeout({
                  duration: "10 seconds",
                  orElse: () => Effect.fail(failure("cleanup")),
                }),
                Effect.tapCause((cause) => reportBrowserCause("session.disconnect", cause)),
                Effect.catchCause((cause) =>
                  Effect.failCause(
                    Cause.map(cause, (error) =>
                      failure("cleanup", "not-dispatched", "unconfirmed", error),
                    ),
                  ),
                ),
              ),
        );

        yield* Effect.addFinalizer(() => cleanupFinalizer(retire));

        const browser = yield* native("session.connect", (signal) => {
          attachment = binding.connect(Redacted.value(sessionId), "session.connect", signal);

          return attachment.browser;
        });

        return { browser, retire };
      });

      const allocate = Effect.fnUntraced(function* <E, R>(
        input: BrowserSessionOptions,
        retain: (reference: BrowserSessionReference) => Effect.Effect<void, E, R>,
        cleanupState: { result: BrowserSessionError["cleanup"] },
      ) {
        const options = yield* decode(BrowserSessionOptions, input);
        const expiresAt = (yield* Clock.currentTimeMillis) + options.maxElapsedMillis;

        if (!Number.isSafeInteger(expiresAt)) return yield* failure("invalid");
        let sessionId: Redacted.Redacted<string> | undefined;
        let retained = false;
        const runCleanup = Effect.runPromiseWith(yield* Effect.context<never>());

        yield* Effect.addFinalizer(() =>
          sessionId === undefined || retained
            ? Effect.void
            : cleanupFinalizer(
                cleanup(sessionId).pipe(
                  Effect.tap((result) =>
                    Effect.sync(() => {
                      cleanupState.result = result;
                    }),
                  ),
                ),
              ),
        );
        cleanupState.result = "unconfirmed";
        sessionId = yield* native("session.acquire", async (signal) => {
          const id = Redacted.make(
            await binding.acquire(options.keepAliveMillis ?? 600_000, "session.acquire"),
          );

          if (signal.aborted) {
            await runCleanup(cleanup(id));
            signal.throwIfAborted();
          }

          return id;
        });
        const connection = yield* connect(sessionId);

        const page = yield* native("session.page", async () => {
          const context = await connection.browser.createBrowserContext();

          return await context.newPage();
        });

        const reference = yield* decode(BrowserSessionReference, {
          version: 1,
          sessionId,
          contextId: Redacted.make(page.browserContext().id ?? ""),
          targetId: Redacted.make(yield* targetId(page, options.commandTimeoutMillis ?? 30_000)),
          expiresAt,
          commandTimeoutMillis: options.commandTimeoutMillis ?? 30_000,
        });

        yield* Effect.uninterruptibleMask((restore) =>
          restore(retain(reference)).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                retained = true;
                cleanupState.result = "not-requested";
              }),
            ),
          ),
        );

        return { reference, page, ...connection };
      });

      const create = Effect.fnUntraced(function* <E, R>(
        input: BrowserSessionOptions,
        retain: (reference: BrowserSessionReference) => Effect.Effect<void, E, R>,
      ) {
        const cleanupState: { result: BrowserSessionError["cleanup"] } = {
          result: "not-requested",
        };

        return yield* allocate(input, retain, cleanupState).pipe(
          Effect.map(({ reference }) => reference),
          timeout({
            duration: "30 seconds",
            orElse: () => Effect.fail(failure("timeout")),
          }),
          Effect.scoped,
          Effect.catchCause((cause) =>
            Effect.failCause(
              Cause.map(cause, (error) =>
                error instanceof BrowserSessionError
                  ? inheritBrowserReport(
                      error,
                      FailureDiagnostic.withCause(
                        BrowserSessionError.make({ ...error, cleanup: cleanupState.result }),
                        error,
                      ),
                    )
                  : error,
              ),
            ),
          ),
          Effect.withTracerEnabled(false),
        );
      });

      const makeSession = Effect.fnUntraced(function* (
        reference: BrowserSessionReference,
        browser: Browser,
        currentPage: Page,
        retire: Effect.Effect<void, BrowserSessionError>,
      ) {
        const scope = yield* Effect.scope;
        const lock = yield* Semaphore.make(1);
        let invalid = false;
        let cleanupResult: BrowserSessionError["cleanup"] = "not-requested";

        const terminate = Effect.suspend(() => {
          invalid = true;
          cleanupResult = "unconfirmed";

          return retire.pipe(
            Effect.onExit(() =>
              cleanup(reference.sessionId).pipe(
                Effect.tap((result) =>
                  Effect.sync(() => {
                    cleanupResult = result;
                  }),
                ),
                Effect.asVoid,
              ),
            ),
          );
        });

        const failAfterTermination = <E, E2>(
          cause: Cause.Cause<E>,
          annotate: (error: E, cleanup: BrowserSessionError["cleanup"]) => E2,
        ) =>
          terminate.pipe(
            Effect.exit,
            Effect.flatMap((exit) => {
              const original = Cause.map(cause, (error) => annotate(error, cleanupResult));

              return Effect.failCause(
                Exit.isFailure(exit) ? Cause.combine(original, exit.cause) : original,
              );
            }),
          );

        const runEffect = <A, E, R>(
          action: (commandTimeoutMillis: number) => Effect.Effect<A, E, R>,
        ) =>
          lock
            .withPermitsIfAvailable(1)(
              Effect.gen(function* () {
                if (invalid || scope.state._tag === "Closed" || !browser.isConnected())
                  return yield* failure("closed");
                const remaining = reference.expiresAt - (yield* Clock.currentTimeMillis);

                if (remaining <= 0) {
                  return yield* failAfterTermination(
                    Cause.fail(failure("expired")),
                    (error, cleanup) => failure(error.reason, error.dispatch, cleanup, error),
                  );
                }

                return yield* action(Math.min(remaining, reference.commandTimeoutMillis)).pipe(
                  Effect.onExit((exit) =>
                    !invalid &&
                    Exit.isFailure(exit) &&
                    (Cause.hasDies(exit.cause) || Cause.hasInterrupts(exit.cause))
                      ? (Cause.hasInterruptsOnly(exit.cause)
                          ? Effect.void
                          : reportBrowserCause("session.command", exit.cause)
                        ).pipe(Effect.onExit(() => terminate))
                      : Effect.void,
                  ),
                );
              }),
            )
            .pipe(
              Effect.flatMap(Effect.fromOption(() => failure("busy"))),
              Effect.withTracerEnabled(false),
            );

        const runAction = <A, E, R>(
          authorize: Effect.Effect<void, E, R>,
          action: (commandTimeoutMillis: number) => Effect.Effect<A, BrowserSessionError>,
          terminateOnError = false,
          timeoutMillis?: number,
        ) =>
          runEffect((commandTimeoutMillis) =>
            authorize.pipe(
              Effect.andThen(() =>
                action(Math.min(commandTimeoutMillis, timeoutMillis ?? commandTimeoutMillis)).pipe(
                  Effect.catchCause((cause) => {
                    const annotate = (
                      error: BrowserSessionError,
                      cleanup: BrowserSessionError["cleanup"],
                    ) =>
                      inheritBrowserReport(
                        error,
                        failure(error.reason, "possibly-dispatched", cleanup, error),
                      );

                    return terminateOnError && !invalid
                      ? failAfterTermination(cause, annotate)
                      : Effect.failCause(
                          Cause.map(cause, (error) => annotate(error, error.cleanup)),
                        );
                  }),
                ),
              ),
              timeout({
                duration: Math.min(commandTimeoutMillis, timeoutMillis ?? commandTimeoutMillis),
                orElse: () =>
                  failAfterTermination(
                    Cause.fail(failure("timeout", "possibly-dispatched")),
                    (error, cleanup) => failure(error.reason, error.dispatch, cleanup, error),
                  ),
              }),
            ),
          );

        const run = <A, E, R>(
          authorize: Effect.Effect<void, E, R>,
          action: (page: Page) => Promise<A>,
          terminateOnError = false,
          timeoutMillis?: number,
        ) =>
          runAction(
            authorize,
            () => native("session.command", () => action(currentPage)),
            terminateOnError,
            timeoutMillis,
          );

        const command = <A, E, R>(
          authorize: Effect.Effect<void, E, R>,
          method: string,
          parameters: Record<string, string | number>,
          schema: Schema.Codec<A>,
        ) =>
          runAction(
            authorize,
            (commandTimeoutMillis) =>
              withCdp(currentPage, commandTimeoutMillis, (cdp) =>
                native("session.command", (): Promise<unknown> =>
                  Reflect.apply(Reflect.get(cdp, "send"), cdp, [method, parameters]),
                ).pipe(
                  Effect.flatMap((result) =>
                    Schema.decodeUnknownEffect(schema)(result).pipe(
                      Effect.catchCause((cause) =>
                        Effect.failCause(
                          Cause.map(cause, (error) =>
                            failure("provider", "not-dispatched", "not-requested", error),
                          ),
                        ),
                      ),
                    ),
                  ),
                ),
              ),
            method === "Cloudflare.handoff",
          );

        const session: BrowserSession = {
          reference,
          run: (authorize, action, options) =>
            options === undefined
              ? run(authorize, action)
              : decode(PositiveMillis, options.timeoutMillis).pipe(
                  Effect.flatMap((timeoutMillis) => run(authorize, action, false, timeoutMillis)),
                ),
          prepareCredential: (request) =>
            runEffect((commandTimeoutMillis) =>
              prepareCredential(request).pipe(
                Effect.provideService(BrowserSessionPage, {
                  page: currentPage,
                  commandTimeoutMillis,
                }),
                timeout({
                  duration: commandTimeoutMillis,
                  orElse: () =>
                    failAfterTermination(Cause.fail(failure("timeout")), (error, cleanup) =>
                      failure(error.reason, error.dispatch, cleanup, error),
                    ),
                }),
              ),
            ),
          fillCredential: (request) =>
            runEffect((commandTimeoutMillis) =>
              fillCredential(request).pipe(
                Effect.provideService(BrowserSessionPage, {
                  page: currentPage,
                  commandTimeoutMillis,
                }),
                Effect.catchCauseIf(
                  (cause) =>
                    cause.reasons.some(
                      (reason) =>
                        Cause.isFailReason(reason) &&
                        (reason.error.reason === "timeout" ||
                          reason.error.dispatch === "possibly-dispatched"),
                    ),
                  (cause) =>
                    failAfterTermination(cause, (error, cleanup) =>
                      FailureDiagnostic.withCause(
                        CredentialFillError.make({ ...error, cleanup }),
                        error,
                      ),
                    ),
                ),
              ),
            ),
          handoff: (authorize, request) =>
            decode(BrowserRunHandoffRequest, request).pipe(
              Effect.flatMap((value) =>
                command(
                  authorize,
                  "Cloudflare.handoff",
                  { instructions: value.instructions, timeout: value.timeout },
                  Schema.Struct({
                    handoffId: Schema.NonEmptyString.check(Schema.isMaxLength(1024)),
                  }),
                ),
              ),
              Effect.map((value) =>
                BrowserRunHandoffResult.make({ handoffId: Redacted.make(value.handoffId) }),
              ),
            ),
          getLiveView: (authorize, request) =>
            decode(BrowserRunLiveViewRequest, request).pipe(
              Effect.flatMap((value) =>
                command(
                  authorize,
                  "Cloudflare.getLiveView",
                  { mode: value.mode, expiresInMs: value.expiresInMs },
                  Schema.Struct({ devtoolsFrontendUrl: Schema.String }),
                ),
              ),
              Effect.flatMap((value) =>
                decode(BrowserRunLiveViewResult, {
                  devtoolsFrontendUrl: Redacted.make(value.devtoolsFrontendUrl),
                }),
              ),
            ),
          getReadOnlyLiveView: (authorize, request) =>
            decode(BrowserRunLiveViewRequest, request).pipe(
              Effect.flatMap((value) =>
                runEffect((commandTimeoutMillis) =>
                  authorize.pipe(
                    Effect.andThen(() =>
                      readonlyLiveView.mint(reference.sessionId, reference.targetId, value).pipe(
                        Effect.tapCause((cause) =>
                          reportBrowserCause("session.readonlyLiveView", cause),
                        ),
                        Effect.catchCause((cause) =>
                          Effect.failCause(
                            Cause.map(cause, (error) =>
                              reportedBrowserError(
                                failure("provider", "not-dispatched", "not-requested", error),
                              ),
                            ),
                          ),
                        ),
                      ),
                    ),
                    timeout({
                      duration: commandTimeoutMillis,
                      orElse: () => Effect.fail(failure("timeout")),
                    }),
                  ),
                ),
              ),
            ),
          getHandoffState: (authorize) =>
            command(
              authorize,
              "Cloudflare.getHandoffState",
              {},
              Schema.Struct({
                active: Schema.Boolean,
                handoffId: Schema.optionalKey(
                  Schema.NonEmptyString.check(Schema.isMaxLength(1024)),
                ),
                durationMs: Schema.optionalKey(Schema.Natural),
              }),
            ).pipe(
              Effect.map((value) =>
                BrowserRunHandoffState.make({
                  active: value.active,
                  ...(value.handoffId === undefined
                    ? {}
                    : { handoffId: Redacted.make(value.handoffId) }),
                  ...(value.durationMs === undefined ? {} : { durationMs: value.durationMs }),
                }),
              ),
            ),
        };

        return session;
      });

      const createAttached = Effect.fnUntraced(function* <E, R>(
        input: BrowserSessionOptions,
        retain: (reference: BrowserSessionReference) => Effect.Effect<void, E, R>,
      ) {
        const scope = yield* Scope.fork(yield* Effect.scope);

        const cleanupState: { result: BrowserSessionError["cleanup"] } = {
          result: "not-requested",
        };

        return yield* allocate(input, retain, cleanupState).pipe(
          Effect.flatMap(({ reference, browser, page, retire }) =>
            makeSession(reference, browser, page, retire),
          ),
          Effect.provideService(Scope.Scope, scope),
          timeout({
            duration: "30 seconds",
            orElse: () => Effect.fail(failure("timeout")),
          }),
          Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
          Effect.catchCause((cause) =>
            Effect.failCause(
              Cause.map(cause, (error) =>
                error instanceof BrowserSessionError
                  ? inheritBrowserReport(
                      error,
                      FailureDiagnostic.withCause(
                        BrowserSessionError.make({ ...error, cleanup: cleanupState.result }),
                        error,
                      ),
                    )
                  : error,
              ),
            ),
          ),
          Effect.withTracerEnabled(false),
        );
      });

      const attachPage = Effect.fnUntraced(function* (input: BrowserSessionReference) {
        const reference = yield* decode(BrowserSessionReference, input);

        if (reference.expiresAt <= (yield* Clock.currentTimeMillis))
          return yield* failure("expired");

        const { browser, retire } = yield* connect(reference.sessionId);

        const context = browser
          .browserContexts()
          .find((value) => value.id === Redacted.value(reference.contextId));

        if (context === undefined) return yield* failure("missing-page");

        for (const page of yield* native("session.pages", () => context.pages())) {
          if (
            (yield* targetId(page, reference.commandTimeoutMillis)) ===
            Redacted.value(reference.targetId)
          )
            return yield* makeSession(reference, browser, page, retire);
        }

        return yield* failure("missing-page");
      });

      const attach = Effect.fnUntraced(function* (input: BrowserSessionReference) {
        const reference = yield* decode(BrowserSessionReference, input);
        const scope = yield* Scope.fork(yield* Effect.scope);

        return yield* attachPage(reference).pipe(
          Effect.provideService(Scope.Scope, scope),
          timeout({
            duration: reference.commandTimeoutMillis,
            orElse: () =>
              reportBrowserCause("session.connect", Cause.fail(failure("timeout"))).pipe(
                Effect.andThen(Effect.fail(reportedBrowserError(failure("timeout")))),
              ),
          }),
          Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
        );
      });

      return BrowserSessions.of({
        create,
        createAttached,
        attach,
        close,
        keepAlive: (sessionId) =>
          decode(SessionId, sessionId).pipe(
            Effect.flatMap((id) =>
              binding.keepAlive(Redacted.value(id)).pipe(
                timeout({
                  duration: "10 seconds",
                  orElse: () => Effect.fail(failure("timeout")),
                }),
                Effect.tapCause((cause) => reportBrowserCause("session.keepAlive", cause)),
                Effect.catchCause((cause) =>
                  Effect.failCause(
                    Cause.map(cause, (error) =>
                      reportedBrowserError(
                        failure(
                          error.reason === "timeout" ? "timeout" : "provider",
                          "not-dispatched",
                          "not-requested",
                          error,
                        ),
                      ),
                    ),
                  ),
                ),
              ),
            ),
            Effect.withTracerEnabled(false),
          ),
      });
    }),
  );

  static layer(options: BrowserSessionsOptions) {
    return this.layerNoDeps.pipe(
      Layer.provide(BrowserRunBinding.layer(options.browser)),
      Layer.provide(BrowserRunSessionLifecycle.layer(options)),
      Layer.provide(BrowserRunReadonlyLiveView.layer(options)),
    );
  }
}
