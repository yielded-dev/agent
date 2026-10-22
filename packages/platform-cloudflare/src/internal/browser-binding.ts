/// <reference types="@cloudflare/workers-types" />
import * as FailureDiagnostic from "@yielded/agent/failure-diagnostic";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import puppeteer, {
  type Browser,
  type ConnectionTransport,
} from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import {
  browserFailure,
  BrowserRunFailure,
  reportBrowserCause,
  reportedBrowserError,
} from "./browser-failure.ts";

/** Headers establish a failure before optional diagnostic I/O; owners retain it during interruption. */
export class BrowserRunRefusal {
  constructor(
    readonly error: BrowserRunFailure,
    readonly collect: Effect.Effect<void>,
  ) {}

  get failure(): Effect.Effect<never, BrowserRunFailure> {
    return Effect.fail(this.error).pipe(Effect.ensuring(this.collect));
  }
}

const ProviderError = Schema.Struct({
  code: Schema.optionalKey(
    Schema.Union([Schema.Finite, Schema.String.check(Schema.isMaxLength(256))]),
  ),
  message: Schema.String.check(Schema.isMaxLength(4096)),
});

const ProviderResponse = Schema.fromJsonString(
  Schema.Struct({
    errors: Schema.optionalKey(Schema.Array(ProviderError).check(Schema.isMaxLength(64))),
    error: Schema.optionalKey(
      Schema.Union([ProviderError, Schema.String.check(Schema.isMaxLength(4096))]),
    ),
  }),
);

const acquisitionRefusal = (response: Response, operation: string) => {
  const header = (name: string) => {
    const value = response.headers.get(name);

    return value !== null && value.length <= 256 ? value : undefined;
  };

  const detail = {
    name: "BrowserProviderResponse",
    status: response.status,
    requestId: header("cf-ray") ?? header("x-request-id"),
    retryAfter: header("retry-after"),
    rawBody: Redacted.make(new Uint8Array()),
    errors: [] as unknown[],
  };

  const error = FailureDiagnostic.withPrivateCause(
    new BrowserRunFailure({ operation, reason: "provider", status: response.status }),
    detail,
  );

  let collected = false;
  const collectionFinished = Deferred.makeUnsafe<void>();

  const collect = Effect.uninterruptible(
    Effect.suspend(() => {
      if (collected) return Deferred.await(collectionFinished);
      collected = true;

      return Effect.gen(function* () {
        const limit = 16_384;
        const chunks: Uint8Array[] = [];
        let length = 0;
        let accepting = true;

        const opened = yield* Effect.try({
          try: () => response.body?.getReader(),
          catch: (cause) => browserFailure(`${operation}.response.open`, cause),
        }).pipe(Effect.exit);

        if (Exit.isFailure(opened)) {
          detail.errors.push(opened.cause);

          return;
        }
        const reader = opened.value;

        if (reader === undefined) return;

        // Optional diagnostic reads and cancellation each get one second; neither can hold a failed acquisition indefinitely.
        const capture = <A>(work: Effect.Effect<A, BrowserRunFailure>, stage: string) =>
          work.pipe(
            Effect.interruptible,
            Effect.timeoutOrElse({
              duration: "1 second",
              orElse: () =>
                Effect.fail(
                  new BrowserRunFailure({
                    operation: `${operation}.response.${stage}`,
                    reason: "timeout",
                  }),
                ),
            }),
            Effect.exit,
            Effect.tap((exit) =>
              Effect.sync(() => {
                if (Exit.isFailure(exit)) detail.errors.push(exit.cause);
              }),
            ),
            Effect.asVoid,
          );

        yield* capture(
          Effect.tryPromise({
            try: async () => {
              while (accepting && length < limit) {
                const part = await reader.read();

                if (!accepting || part.done) break;
                const bytes = part.value.subarray(0, limit - length);

                chunks.push(bytes);
                length += bytes.byteLength;
              }
            },
            catch: (cause) => browserFailure(`${operation}.response.read`, cause),
          }),
          "read",
        );
        accepting = false;
        yield* capture(
          Effect.tryPromise({
            try: () => reader.cancel(),
            catch: (cause) => browserFailure(`${operation}.response.cancel`, cause),
          }),
          "cancel",
        );
        yield* capture(
          Effect.try({
            try: () => reader.releaseLock(),
            catch: (cause) => browserFailure(`${operation}.response.release`, cause),
          }),
          "release",
        );
        if (length === limit)
          detail.errors.push({
            message: "Provider response diagnostic stopped at its 16384-byte limit",
          });
        const body = new Uint8Array(length);
        let offset = 0;

        for (const chunk of chunks) {
          body.set(chunk, offset);
          offset += chunk.byteLength;
        }
        detail.rawBody = Redacted.make(body);
        const decoded = Schema.decodeOption(ProviderResponse)(new TextDecoder().decode(body));

        if (decoded._tag === "Some") {
          detail.errors.unshift(...(decoded.value.errors ?? []));
          if (decoded.value.error !== undefined)
            detail.errors.unshift(
              typeof decoded.value.error === "string"
                ? { message: decoded.value.error }
                : decoded.value.error,
            );
        }
      }).pipe(Effect.onExit((exit) => Deferred.done(collectionFinished, exit)));
    }),
  );

  return new BrowserRunRefusal(error, collect);
};

const Acquired = Schema.Struct({ sessionId: Schema.String.check(Schema.isUUID()) });

const VersionReply = Schema.fromJsonString(
  Schema.Struct({
    id: Schema.optionalKey(Schema.Int),
    result: Schema.optionalKey(Schema.Unknown),
    error: Schema.optionalKey(Schema.Unknown),
  }),
);

const Version = Schema.Struct({
  product: Schema.NonEmptyString,
  protocolVersion: Schema.NonEmptyString,
});

/** Private attachment ownership is available before asynchronous SDK initialization completes. */
export interface BrowserRunAttachment {
  readonly browser: Promise<Browser>;
  /** Fence SDK dispatch/callbacks and await local socket closure; never terminate the provider. */
  readonly retire: Effect.Effect<void, BrowserRunFailure>;
}

/** Native transport port shared by both scoped adapters; the Layer owns the foreign binding. */
export class BrowserRunBinding extends Context.Service<
  BrowserRunBinding,
  {
    readonly acquire: (
      keepAliveMillis: number,
      operation: "interactive.acquire" | "session.acquire",
    ) => Promise<string | BrowserRunRefusal>;
    readonly connect: (
      sessionId: string,
      operation: string,
      signal?: AbortSignal,
    ) => BrowserRunAttachment;
    readonly keepAlive: (sessionId: string) => Effect.Effect<void, BrowserRunFailure>;
  }
>()("@effect-agent/platform-cloudflare/internal/BrowserRunBinding") {
  static layer(browser: Pick<BrowserRun, "fetch">) {
    const upgrade = async (sessionId: string, operation: string, signal?: AbortSignal) => {
      const response = await browser.fetch(
        `https://browser-rendering.cloudflare.com/v1/devtools/browser/${sessionId}`,
        { headers: { Upgrade: "websocket" }, ...(signal === undefined ? {} : { signal }) },
      );

      if (response.status !== 101) {
        const failure = new BrowserRunFailure({
          operation,
          reason: "provider",
          status: response.status,
        });

        try {
          await response.body?.cancel();
        } catch {
          // Preserve the provider refusal when local response release also fails.
        }
        // oxlint-disable-next-line effect/no-untyped-throw -- Native BrowserRun acquisition Promise preserves the provider refusal after body cleanup.
        throw failure;
      }
      const socket = response.webSocket;

      // oxlint-disable-next-line effect/no-untyped-throw -- Native BrowserRun upgrade Promise must reject a malformed WebSocket response.
      if (socket === null) throw new BrowserRunFailure({ operation, reason: "malformed" });

      return socket;
    };

    return Layer.succeed(this)({
      // One allocation path, with recording explicitly disabled for private sessions.
      acquire: async (keepAliveMillis, operation) => {
        const response = await browser.fetch(
          `https://browser-rendering.cloudflare.com/v1/devtools/browser?keep_alive=${keepAliveMillis}&recording=false`,
          { method: "POST" },
        );

        if (response.status !== 200) return acquisitionRefusal(response, operation);

        return Schema.decodeUnknownSync(Acquired)(await response.json()).sessionId;
      },
      connect: (sessionId, operation, signal) => {
        let socket: WebSocket | undefined;
        let retired = false;
        let closedByAbort = false;
        let retirementFailure: BrowserRunFailure | undefined;

        const disconnectOperation =
          operation === "session.connect" ? "session.disconnect" : "interactive.disconnect";

        const transport: ConnectionTransport = {
          send: (message) => {
            if (retired || socket === undefined)
              // oxlint-disable-next-line effect/no-untyped-throw -- Native Puppeteer transport rejects fenced dispatch or initialization; its Effect owner maps the failure.
              throw new BrowserRunFailure({ operation, reason: "provider" });
            socket.send(message);
          },
          close: () => retireNow(),
        };

        const onMessage = (event: MessageEvent) => {
          if (!retired) transport.onmessage?.(event.data);
        };

        const fence = () => {
          if (retired) return;
          retired = true;
          const notify = transport.onclose;

          transport.onmessage = undefined;
          transport.onclose = undefined;
          // Notify the SDK before touching the socket: disposal must reject pending CDP calls
          // even when the raw transport cannot close. The send fence also covers queued work.
          try {
            notify?.();
          } finally {
            socket?.removeEventListener("message", onMessage);
          }
        };

        const retireNow = () => {
          try {
            fence();
          } catch (cause) {
            retirementFailure ??= browserFailure(disconnectOperation, cause);
          }
          try {
            // Manual-close runtimes also require a reply when the peer leaves us CLOSING.
            if (socket !== undefined && socket.readyState < WebSocket.CLOSED) socket.close();
          } catch (cause) {
            retirementFailure ??= browserFailure(disconnectOperation, cause);
          }
          // oxlint-disable-next-line effect/no-untyped-throw -- Native transport close callback preserves the first retirement failure for its Effect owner.
          if (retirementFailure !== undefined) throw retirementFailure;
        };

        const abort = () => {
          closedByAbort = socket?.readyState === WebSocket.OPEN;
          try {
            retireNow();
          } catch {
            // The attachment owner observes this separately from the initiating failure.
          }
        };

        signal?.addEventListener("abort", abort, { once: true });

        const initialized = (async () => {
          try {
            signal?.throwIfAborted();
            socket = await upgrade(sessionId, operation, signal);
            socket.accept();
            if (retired || signal?.aborted) {
              // No SDK command may escape a late upgrade. Preserve a late local-close
              // failure independently of the cancellation already returned to the owner.
              retireNow();
              signal?.throwIfAborted();
              // oxlint-disable-next-line effect/no-untyped-throw -- Native Puppeteer transport rejects fenced dispatch or initialization; its Effect owner maps the failure.
              throw new BrowserRunFailure({ operation, reason: "provider" });
            }
            socket.addEventListener("message", onMessage);
            socket.addEventListener(
              "close",
              () => {
                try {
                  retireNow();
                } catch (cause) {
                  retirementFailure ??= browserFailure(disconnectOperation, cause);
                }
              },
              { once: true },
            );

            // Attaching must not replace the viewport owned by the host or another viewer.
            return await puppeteer.connect({ transport, defaultViewport: null });
          } catch (cause) {
            try {
              retireNow();
            } catch {
              // Preserve both facts: initialization failed, and retirement may be unconfirmed.
            }
            const failure = browserFailure(operation, cause);

            const localCancellation =
              cause instanceof Error &&
              signal?.aborted === true &&
              (cause === signal.reason || (closedByAbort && cause.name === "TargetCloseError"));

            // oxlint-disable-next-line effect/no-untyped-throw -- Native SDK initialization Promise preserves cancellation provenance after transport retirement.
            throw localCancellation ? reportedBrowserError(failure) : failure;
          } finally {
            signal?.removeEventListener("abort", abort);
          }
        })();

        return {
          browser: initialized,
          retire: Effect.callback<void, BrowserRunFailure>((resume) => {
            const connected = socket;
            const onClose = () => resume(Effect.void);

            try {
              retireNow();
              if (connected === undefined || connected.readyState === WebSocket.CLOSED)
                resume(Effect.void);
              else connected.addEventListener("close", onClose, { once: true });
            } catch (cause) {
              resume(Effect.fail(browserFailure(disconnectOperation, cause)));
            }

            return Effect.sync(() => connected?.removeEventListener("close", onClose));
          }),
        };
      },
      keepAlive: Effect.fnUntraced(function* (sessionId: string) {
        const operation = "session.keepAlive";
        const runReport = Effect.runPromiseWith(yield* Effect.context<never>());

        const release = (socket: WebSocket) =>
          Effect.sync(() => socket.close()).pipe(
            Effect.catchCause((cause) => reportBrowserCause(operation, cause)),
          );

        const socket = yield* Effect.acquireRelease(
          Effect.tryPromise({
            try: async (signal) => {
              let acquired: WebSocket | undefined;

              try {
                acquired = await upgrade(sessionId, operation, signal);
                acquired.accept();
                // The binding may resolve an upgrade after the Effect was interrupted.
                signal.throwIfAborted();

                return acquired;
              } catch (cause) {
                if (acquired !== undefined) await runReport(release(acquired));
                if (signal.aborted && cause !== signal.reason)
                  await runReport(reportBrowserCause(operation, Cause.fail(cause)));
                // oxlint-disable-next-line effect/no-untyped-throw -- Rethrow the foreign upgrade failure after late-acquisition cleanup, preserving abort identity.
                throw cause;
              }
            },
            catch: (cause) => browserFailure(operation, cause),
          }),
          release,
          { interruptible: true },
        );

        yield* Effect.callback<void, BrowserRunFailure>((resume) => {
          const finish = (result: Effect.Effect<void, BrowserRunFailure>) => {
            removeListeners();
            resume(result);
          };

          const fail = (reason: BrowserRunFailure["reason"]) =>
            finish(Effect.fail(new BrowserRunFailure({ operation, reason })));

          const onMessage = (event: MessageEvent) => {
            try {
              if (typeof event.data !== "string" || event.data.length > 16_384) {
                fail("malformed");

                return;
              }
              const reply = Schema.decodeSync(VersionReply)(event.data);

              if (reply.id !== 1) return;
              if (reply.error !== undefined) {
                fail("provider");

                return;
              }
              Schema.decodeUnknownSync(Version)(reply.result);
              finish(Effect.void);
            } catch {
              fail("malformed");
            }
          };

          const onClose = () => fail("provider");

          const removeListeners = () => {
            socket.removeEventListener("message", onMessage);
            socket.removeEventListener("close", onClose);
            socket.removeEventListener("error", onClose);
          };

          socket.addEventListener("message", onMessage);
          socket.addEventListener("close", onClose);
          socket.addEventListener("error", onClose);
          try {
            socket.send(JSON.stringify({ id: 1, method: "Browser.getVersion" }));
          } catch (cause) {
            finish(Effect.fail(browserFailure(operation, cause)));
          }

          return Effect.sync(removeListeners);
        });
      }, Effect.scoped),
    });
  }
}
