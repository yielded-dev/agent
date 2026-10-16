import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { browserFailure } from "./browser-failure.ts";

export class BrowserRunCleanupError extends Schema.TaggedError<BrowserRunCleanupError>()(
  "BrowserRunCleanupError",
  {
    reason: Schema.Literals([
      "configuration",
      "authorization",
      "rate-limited",
      "provider",
      "malformed",
      "timeout",
      "pending",
    ]),
    status: Schema.optionalKey(Schema.Int),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

export interface BrowserRunLifecycleOptions {
  readonly accountId: string;
  readonly apiToken: Redacted.Redacted<string>;
}

const Identity = Schema.String.check(Schema.isUUID());
const Account = Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/));
const Closed = Schema.Struct({ status: Schema.Literals(["closing", "closed"]) });
const Metadata = Schema.Struct({ sessionId: Identity, endTime: Schema.optionalKey(Schema.Finite) });
// This exact provider response is accepted only on a fixed-origin, authenticated,
// exact-session request. An arbitrary 404 or a listing omission is never absence.
const Absent = Schema.Struct({ error: Schema.Literal("Session not found") });

export class BrowserRunSessionLifecycle extends Context.Service<
  BrowserRunSessionLifecycle,
  {
    readonly close: (
      sessionId: Redacted.Redacted<string>,
    ) => Effect.Effect<void, BrowserRunCleanupError>;
  }
>()("@effect-agent/platform-cloudflare/BrowserRunSessionLifecycle") {
  static layer(options: BrowserRunLifecycleOptions) {
    return Layer.effect(
      this,
      Effect.gen(function* () {
        if (
          !Schema.is(Account)(options.accountId) ||
          Redacted.value(options.apiToken).length === 0
        ) {
          return yield* BrowserRunCleanupError.make({ reason: "configuration" });
        }
        const client = yield* HttpClient.HttpClient;

        const request = Effect.fnUntraced(function* (method: "GET" | "DELETE", sessionId: string) {
          const path = method === "DELETE" ? "browser" : "session";

          const response = yield* client
            .execute(
              HttpClientRequest.make(method)(
                `https://api.cloudflare.com/client/v4/accounts/${options.accountId}/browser-rendering/devtools/${path}/${sessionId}`,
              ).pipe(HttpClientRequest.bearerToken(options.apiToken)),
            )
            .pipe(
              // workerd supports manual/follow, not error. Reject every redirect below.
              Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
              Effect.mapError((cause) =>
                BrowserRunCleanupError.make({
                  reason: "provider",
                  cause: browserFailure("browser.close.request", cause),
                }),
              ),
            );

          if (response.status === 401 || response.status === 403)
            return yield* BrowserRunCleanupError.make({
              reason: "authorization",
              status: response.status,
            });
          if (response.status === 429)
            return yield* BrowserRunCleanupError.make({
              reason: "rate-limited",
              status: response.status,
            });
          if (response.status !== 200 && response.status !== 404)
            return yield* BrowserRunCleanupError.make({
              reason: "provider",
              status: response.status,
            });
          if (response.headers["content-type"]?.split(";", 1)[0]?.trim() !== "application/json")
            return yield* BrowserRunCleanupError.make({ reason: "malformed" });

          const bytes = yield* Stream.runFoldEffect(
            response.stream,
            () => new Uint8Array(),
            (body, chunk) => {
              if (body.byteLength + chunk.byteLength > 16_384)
                return Effect.fail(BrowserRunCleanupError.make({ reason: "malformed" }));
              const combined = new Uint8Array(body.byteLength + chunk.byteLength);

              combined.set(body);
              combined.set(chunk, body.byteLength);

              return Effect.succeed(combined);
            },
          ).pipe(
            Effect.mapError((cause) =>
              BrowserRunCleanupError.make({
                reason: "malformed",
                cause: browserFailure("browser.close.response", cause),
              }),
            ),
          );

          const body = yield* Effect.try({
            try: () => new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(bytes),
            catch: (cause) =>
              BrowserRunCleanupError.make({
                reason: "malformed",
                cause: browserFailure("browser.close.decode", cause),
              }),
          });

          if (response.status === 404) {
            const absent = Schema.decodeOption(Schema.fromJsonString(Absent))(body, {
              onExcessProperty: "error",
            });

            if (Option.isSome(absent)) return true;

            return yield* BrowserRunCleanupError.make({ reason: "malformed" });
          }
          if (method === "DELETE") {
            const result = yield* Schema.decodeEffect(Schema.fromJsonString(Closed))(body).pipe(
              Effect.mapError((cause) =>
                BrowserRunCleanupError.make({
                  reason: "malformed",
                  cause: browserFailure("browser.close.response", cause),
                }),
              ),
            );

            return result.status === "closed";
          }

          const result = yield* Schema.decodeEffect(Schema.fromJsonString(Metadata))(body).pipe(
            Effect.mapError((cause) =>
              BrowserRunCleanupError.make({
                reason: "malformed",
                cause: browserFailure("browser.close.response", cause),
              }),
            ),
          );

          if (result.sessionId !== sessionId)
            return yield* BrowserRunCleanupError.make({ reason: "malformed" });

          return result.endTime !== undefined && result.endTime > 0;
        });

        const close = Effect.fnUntraced(
          function* (sessionId: Redacted.Redacted<string>) {
            const id = yield* Schema.decodeEffect(Identity)(Redacted.value(sessionId)).pipe(
              Effect.mapError(() => BrowserRunCleanupError.make({ reason: "configuration" })),
            );

            if (yield* request("DELETE", id)) return;
            for (let read = 0; read < 2; read++) {
              if (yield* request("GET", id)) return;
            }

            return yield* BrowserRunCleanupError.make({ reason: "pending" });
          },
          Effect.timeoutOrElse({
            duration: "10 seconds",
            orElse: () => Effect.fail(BrowserRunCleanupError.make({ reason: "timeout" })),
          }),
          Effect.withTracerEnabled(false),
        );

        return { close };
      }),
    );
  }
}
