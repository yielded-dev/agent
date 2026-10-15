import * as Auth from "@yielded/auth/Auth";
import * as Email from "@yielded/auth/Email";
import * as GitHub from "@yielded/auth/GitHub";
import * as AuthHttp from "@yielded/auth/Http";
import * as OAuth from "@yielded/auth/OAuth";
import * as OpenIdConnect from "@yielded/auth/OpenIdConnect";
import { ProofKeys } from "@yielded/auth/Proofs";
import * as Sessions from "@yielded/auth/Sessions";
import { Context, Effect, Exit, Layer, Redacted, Schema } from "effect";

import { LoginApi, Registration } from "./contract";
import { requirement } from "./schema";

export const AuthConfiguration = Schema.Struct({
  AUTH_ORIGIN: Schema.String.check(
    Schema.makeFilter((value) => {
      try {
        const url = new URL(value);

        return url.protocol === "https:" && url.origin === value;
      } catch {
        return false;
      }
    }),
  ),
  AUTH_BINDING_KEY: Schema.NonEmptyString,
  AUTH_PROOF_KEY: Schema.NonEmptyString,
  AUTH_TRANSACTION_KEY: Schema.NonEmptyString,
  AUTH_GITHUB_CLIENT_ID: Schema.NonEmptyString,
  AUTH_GITHUB_CLIENT_SECRET: Schema.NonEmptyString,
  AUTH_YIELDED_ISSUER: Schema.String.check(
    Schema.makeFilter((value) => {
      try {
        const url = new URL(value);

        return url.protocol === "https:" && url.origin === value;
      } catch {
        return false;
      }
    }),
  ),
  AUTH_YIELDED_CLIENT_SECRET: Schema.NonEmptyString,
  AUTH_EMAIL_FROM: Schema.NonEmptyString,
});

export type AuthConfiguration = typeof AuthConfiguration.Type;

export const keyring = (material: string) => ({
  activeKeyId: "v1",
  keys: [{ id: "v1", material: Redacted.make(material) }],
});

export const makeAuth = (config: AuthConfiguration) => {
  const email = {
    namespace: "travel-planner/email",
    digits: 6,
    policy: {
      lifetimeMillis: 300_000,
      maximumFailedAttempts: 5,
      abuse: {
        issues: { limit: 5, windowMillis: 3_600_000 },
        attempts: { limit: 10, windowMillis: 300_000 },
        subjectIssues: { limit: 5, windowMillis: 3_600_000 },
        subjectAttempts: { limit: 10, windowMillis: 300_000 },
        actionIssues: { limit: 1000, windowMillis: 3_600_000 },
        actionAttempts: { limit: 1000, windowMillis: 300_000 },
        resendCooldownMillis: 30_000,
      },
    },
  } as const;

  const AppAuth = Auth.make(LoginApi, {
    sessions: Sessions.stateful({ idleTimeout: "30 days", maxAge: "30 days", renewAfter: "1 day" }),
    strategies: {
      email: Email.makeCode(email),
      emailRegistration: Email.makeRegistration({ ...email, registration: Registration }),
      oauth: OAuth.makeRegistration({
        authenticate: true,
        namespace: "travel-planner/oauth",
        registration: Registration,
        policy: {
          generation: 1,
          lifetimeMillis: 300_000,
        },
        registrationPolicy: {
          lifetimeMillis: 300_000,
          maximumVerificationAgeMillis: requirement.maximumAgeMillis,
          retentionMillis: 600_000,
        },
      }),
    },
    defaultStrategy: "oauth",
  });

  const http = AuthHttp.make(AppAuth, {
    origin: config.AUTH_ORIGIN,
    maximumBodyBytes: 32 * 1024,
    cookie: { prefix: "__Host-elsewhere-auth-" },
  });

  const security = Layer.mergeAll(
    ProofKeys.layer(keyring(config.AUTH_PROOF_KEY)),
    Auth.RequestBindingConfig.layer({
      generation: 1,
      lifetimeMillis: 600_000,
      keyring: keyring(config.AUTH_BINDING_KEY),
    }),
    OAuth.OAuthTransactionProtector.layer(keyring(config.AUTH_TRANSACTION_KEY)),
    OAuth.OAuthReturnTargets.exactRoutes(["/travel/", "/browser-use/"]),
    Email.EmailReturnTargets.exactRoutes(["/travel/", "/browser-use/"]),
  );

  // This adapter discovers OIDC metadata at construction. Build it only for
  // Yielded operations so an issuer outage cannot disable other sign-in methods.
  const providers = Layer.effect(
    OAuth.OAuthProtocol,
    Effect.gen(function* () {
      const github = Context.get(
        yield* Layer.build(
          GitHub.layer({
            configurationGeneration: 2,
            clientId: config.AUTH_GITHUB_CLIENT_ID,
            clientSecret: Redacted.make(config.AUTH_GITHUB_CLIENT_SECRET),
            redirectUri: `${config.AUTH_ORIGIN}/auth/github/callback`,
          }),
        ),
        OAuth.OAuthProtocol,
      );

      const context = yield* Effect.context<OpenIdConnect.Requirements>();

      // Retain successful discovery, but let a recovered issuer be retried.
      const yielded = yield* Effect.cachedWithTTL(
        Layer.build(
          OpenIdConnect.layer({
            providers: [
              {
                protocol: "oidc",
                provider: "yielded",
                issuer: config.AUTH_YIELDED_ISSUER,
                clientId: "yielded-agent",
                clientSecret: Redacted.make(config.AUTH_YIELDED_CLIENT_SECRET),
                redirectUri: `${config.AUTH_ORIGIN}/auth/yielded/callback`,
                scopes: ["openid", "profile"],
                // Reuse recent issuer sessions; leave a minute for callback completion.
                maxAgeSeconds: Math.floor(requirement.maximumAgeMillis / 1000) - 60,
              },
            ],
          }),
        ).pipe(
          Effect.map((services) => Context.get(services, OAuth.OAuthProtocol)),
          Effect.provide(context),
          Effect.mapError(() => OAuth.OAuthUnavailable.make({})),
        ),
        (exit) => (Exit.isSuccess(exit) ? Infinity : 0),
      );

      const select = (provider: string) =>
        provider === "yielded" ? yielded : Effect.succeed(github);

      return OAuth.OAuthProtocol.of({
        prepareAuthorization: (input) =>
          Effect.flatMap(select(input.provider), (protocol) =>
            protocol.prepareAuthorization(input),
          ),
        exchangeVerifiedIdentity: (input) =>
          Effect.flatMap(select(input.configuration.provider), (protocol) =>
            protocol.exchangeVerifiedIdentity(input),
          ),
      });
    }),
  );

  return { AppAuth, http, security, providers };
};

export type AppAuth = ReturnType<typeof makeAuth>["AppAuth"];
