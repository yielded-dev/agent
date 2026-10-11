import { EmailDelivery } from "@yielded/auth/EmailDelivery";
import { DurableObject } from "cloudflare:workers";
import { Effect, Layer, ManagedRuntime, Redacted } from "effect";
import { WorkerEnvironment } from "effect-cf";

import { handleRequest } from "../../src/worker";
export { TravelPlannerThread } from "./worker";
import { AuthHost, makeAuthHost } from "../../src/auth/host";

const key = btoa(String.fromCharCode(...new Uint8Array(32).fill(42)))
  .replace(/=/g, "")
  .replace(/\+/g, "-")
  .replace(/\//g, "_");

export const fixtureAuthConfig = {
  AUTH_ORIGIN: "https://planner.test",
  AUTH_BINDING_KEY: key,
  AUTH_PROOF_KEY: key,
  AUTH_TRANSACTION_KEY: key,
  AUTH_GITHUB_CLIENT_ID: "fixture-github",
  AUTH_GITHUB_CLIENT_SECRET: "fixture-github-secret",
  AUTH_YIELDED_ISSUER: "https://auth.yielded.dev",
  AUTH_YIELDED_CLIENT_SECRET: "local-preview",
  AUTH_EMAIL_FROM: "signin@example.invalid",
};

export class AuthFixture extends DurableObject {
  private deliveries: Array<{ code: string; email: string }> = [];
  private readonly runtime = ManagedRuntime.make(
    Layer.effect(
      AuthHost,
      Effect.suspend(() => {
        const delivery = Layer.succeed(EmailDelivery, {
          send: (message) =>
            Effect.sync(() => {
              const code = Redacted.value(message.text).match(/\b\d{6}\b/)?.[0];

              if (code === undefined) throw new Error("Missing verification code");
              this.deliveries.push({
                code,
                email: message.to,
              });
            }),
        });

        return makeAuthHost(this.ctx.storage, fixtureAuthConfig, delivery);
      }),
    ),
  );

  fetch(request: Request) {
    if (new URL(request.url).pathname === "/_fixture/delivery")
      return Promise.resolve(
        Response.json({ ...this.deliveries.at(-1), count: this.deliveries.length }),
      );

    return this.runtime.runPromise(Effect.flatMap(AuthHost, (host) => host.fetch(request)));
  }
}

export default {
  fetch: (request: Request, env: Cloudflare.Env, ctx: ExecutionContext) =>
    new URL(request.url).pathname.startsWith("/_fixture/")
      ? env.AUTH_V2.getByName("auth-v1").fetch(request)
      : Effect.runPromise(
          handleRequest()(request, env, ctx).pipe(Effect.provideService(WorkerEnvironment, env)),
        ),
};
