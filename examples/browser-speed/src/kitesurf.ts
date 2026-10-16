import {
  BrowserSessionError,
  type BrowserSession,
} from "@yielded/agent-platform-cloudflare/browser-session";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import puppeteer, {
  type ConnectionTransport,
} from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import { browserCommandTimeoutMillis, LabError } from "./contract.ts";

const Identity = Schema.Struct({
  product: Schema.NonEmptyString,
  revision: Schema.NonEmptyString,
  userAgent: Schema.NonEmptyString,
});

const IdentityReply = Schema.fromJsonString(
  Schema.Struct({ id: Schema.Literal(0), result: Identity }),
);

export type KitesurfPage = Pick<BrowserSession, "run"> & {
  readonly identity: typeof Identity.Type;
};

/** Kitesurf exists only for this socket's lifetime; it has no resumable BrowserSession reference. */
export const connectKitesurf = Effect.fnUntraced(function* (
  config: { accountId: string; apiToken: Redacted.Redacted<string> },
  retainClose: (close: Effect.Effect<void, LabError>) => void,
) {
  let socket: WebSocket | undefined;
  let fenced = false;

  const transport: ConnectionTransport = {
    send: (message) => {
      // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- Puppeteer requires synchronous transport.send exceptions to reject CDP commands.
      if (fenced || socket === undefined) throw new Error("Kitesurf connection is closed");
      socket.send(message);
    },
    close: () => fence(),
  };

  const onMessage = (event: MessageEvent) => {
    if (!fenced) transport.onmessage?.(event.data);
  };

  const fence = () => {
    if (!fenced) {
      fenced = true;
      const notify = transport.onclose;

      transport.onmessage = undefined;
      transport.onclose = undefined;
      socket?.removeEventListener("message", onMessage);
      try {
        notify?.();
      } finally {
        if (socket !== undefined && socket.readyState < WebSocket.CLOSED) socket.close();
      }
    }
    if (socket !== undefined && socket.readyState < WebSocket.CLOSED) socket.close();
  };

  const close = Effect.callback<void, LabError>((resume) => {
    const connected = socket;
    const finish = () => resume(Effect.void);

    try {
      connected?.addEventListener("close", finish, { once: true });
      fence();
      if (connected === undefined || connected.readyState === WebSocket.CLOSED) finish();
    } catch {
      resume(
        Effect.fail(LabError.make({ code: "browser", message: "Kitesurf socket cleanup failed." })),
      );
    }

    return Effect.sync(() => connected?.removeEventListener("close", finish));
  }).pipe(
    Effect.timeoutOrElse({
      duration: "5 seconds",
      orElse: () =>
        Effect.fail(
          LabError.make({ code: "browser", message: "Kitesurf socket closure was not confirmed." }),
        ),
    }),
  );

  // Retain cleanup before any asynchronous acquisition. The owner retries failed closure;
  // scope release also fences the socket on setup failure, timeout, interruption or defect.
  retainClose(close);
  yield* Effect.addFinalizer(() => close.pipe(Effect.ignore));

  const connectionFailed = () =>
    LabError.make({ code: "browser", message: "Kitesurf CDP connection failed." });

  const connected = yield* Effect.gen(function* () {
    const connectedSocket = yield* Effect.tryPromise({
      try: async (signal) => {
        const response = await fetch(
          `https://api.cloudflare.com/client/v4/accounts/${config.accountId}/browser-run/devtools/browser?browser=kitesurf`,
          {
            headers: {
              Upgrade: "websocket",
              Authorization: `Bearer ${Redacted.value(config.apiToken)}`,
            },
            signal,
          },
        );

        if (response.status !== 101 || response.webSocket === null) {
          await response.body?.cancel();

          return null;
        }
        // fetch can complete after interruption. Retain and fence even a late socket.
        socket = response.webSocket;
        socket.accept();
        if (fenced || signal.aborted) {
          fence();

          return null;
        }
        socket.addEventListener("message", onMessage);
        socket.addEventListener("close", fence, { once: true });
        socket.addEventListener("error", fence, { once: true });

        return socket;
      },
      catch: connectionFailed,
    });

    if (connectedSocket === null) return yield* connectionFailed();

    // Read identity on the root socket before Puppeteer owns command IDs. Kitesurf's
    // secondary page sessions are unnecessary for this browser-level CDP command.
    const identity = yield* Effect.callback<typeof Identity.Type, LabError>((resume) => {
      const rejectClosed = () => resume(Effect.fail(connectionFailed()));

      const receive = (event: MessageEvent) => {
        const reply = Schema.decodeUnknownOption(IdentityReply)(event.data);

        if (Option.isSome(reply)) resume(Effect.succeed(reply.value.result));
      };

      connectedSocket.addEventListener("message", receive);
      connectedSocket.addEventListener("close", rejectClosed, { once: true });
      try {
        connectedSocket.send(JSON.stringify({ id: 0, method: "Browser.getVersion" }));
      } catch {
        rejectClosed();
      }

      return Effect.sync(() => {
        connectedSocket.removeEventListener("message", receive);
        connectedSocket.removeEventListener("close", rejectClosed);
      });
    });

    const browser = yield* Effect.tryPromise({
      try: () =>
        puppeteer.connect({
          transport,
          defaultViewport: null,
          protocolTimeout: browserCommandTimeoutMillis + 5_000,
        }),
      catch: connectionFailed,
    });

    return { browser, identity };
  }).pipe(
    Effect.timeoutOrElse({
      duration: "25 seconds",
      orElse: () =>
        Effect.fail(
          LabError.make({ code: "browser", message: "Kitesurf CDP connection timed out." }),
        ),
    }),
  );

  const page = yield* Effect.tryPromise({
    try: () => connected.browser.newPage(),
    catch: () => LabError.make({ code: "browser", message: "Kitesurf page creation failed." }),
  });

  const lock = yield* Semaphore.make(1);

  return {
    identity: connected.identity,
    run: (authorize, action, options) =>
      lock.withPermit(
        Effect.gen(function* () {
          yield* authorize;
          if (fenced)
            return yield* BrowserSessionError.make({
              reason: "expired",
              dispatch: "not-dispatched",
              cleanup: "not-requested",
            });

          return yield* Effect.tryPromise({
            try: () => action(page),
            catch: () =>
              BrowserSessionError.make({
                reason: "provider",
                dispatch: "possibly-dispatched",
                cleanup: "not-requested",
              }),
          }).pipe(
            Effect.timeoutOrElse({
              duration: Math.min(
                browserCommandTimeoutMillis,
                options?.timeoutMillis ?? browserCommandTimeoutMillis,
              ),
              orElse: () =>
                close.pipe(
                  Effect.ignore,
                  Effect.andThen(
                    Effect.fail(
                      BrowserSessionError.make({
                        reason: "timeout",
                        dispatch: "possibly-dispatched",
                        cleanup: "unconfirmed",
                      }),
                    ),
                  ),
                ),
            }),
            Effect.onInterrupt(() => close.pipe(Effect.ignore)),
          );
        }),
      ),
  } satisfies KitesurfPage;
});
