import { DurableObject } from "cloudflare:workers";
import { Effect, Layer, ManagedRuntime, Schema } from "effect";
import { CloudflareTracer, WorkerEnvironment } from "effect-cf";

import { AccountError, AccountSession } from "./account";
import { emailDeliveryLayer } from "./email-delivery";
import { AuthHost, makeAuthHost } from "./host";
import { AuthConfiguration } from "./server";

/** Only the planner Worker can reach this object; it has no public service route. */
export class PlannerAuthV2 extends DurableObject<Cloudflare.Env> {
  // Auth's delivery worker and provider scopes belong to this Object incarnation.
  private readonly runtime = ManagedRuntime.make(
    Layer.effect(
      AuthHost,
      Effect.gen({ self: this }, function* () {
        const config = yield* Schema.decodeEffect(AuthConfiguration)(this.env);

        return yield* makeAuthHost(
          this.ctx.storage,
          config,
          emailDeliveryLayer(this.env.AUTH_EMAIL, config.AUTH_EMAIL_FROM),
          Boolean(this.env.SERVER_OPENAI_KEY),
        );
      }),
    ).pipe(Layer.provide(CloudflareTracer.layer)),
  );

  async fetch(request: Request): Promise<Response> {
    try {
      return await this.runtime.runPromise(Effect.flatMap(AuthHost, (host) => host.fetch(request)));
    } catch {
      return new Response("Authentication is temporarily unavailable.", {
        status: 503,
        headers: { "cache-control": "no-store" },
      });
    }
  }
}

export const authenticate = Effect.fn("Planner.requireSession")(function* (request: Request) {
  if (!request.headers.get("cookie"))
    return yield* new AccountError({ code: "unauthorized", message: "Sign in to continue." });

  const unavailable = () =>
    new AccountError({
      code: "unavailable",
      message: "Authentication is temporarily unavailable.",
    });

  const url = new URL(request.url);

  url.pathname = "/_internal/session";
  url.search = "";

  const env = yield* WorkerEnvironment;

  const response = yield* Effect.tryPromise({
    try: () =>
      env.AUTH_V2.getByName("auth-v1").fetch(
        new Request(url, { headers: { cookie: request.headers.get("cookie") ?? "" } }),
      ),
    catch: unavailable,
  });

  if (response.status === 401)
    return yield* new AccountError({ code: "unauthorized", message: "Sign in to continue." });
  if (!response.ok) return yield* unavailable();

  return yield* Effect.tryPromise({ try: () => response.json(), catch: unavailable }).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(AccountSession)),
    Effect.mapError(unavailable),
  );
});
