import { OAuthScope, OAuthServer } from "@yielded/auth";
import { Jwks } from "@yielded/jose";
import { Cause, Context, Effect, Layer, Redacted, Schema } from "effect";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Signature from "effect/crypto/Signature";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";

import { AccountClaims, AccountError } from "./contract";

export interface AuthEnvironment {
  readonly AUTH_ISSUER?: string;
  readonly AUTH_RESOURCE?: string;
  readonly AUTH_JWKS_URL?: string;
  readonly AUTH_REQUIRED_SCOPE?: string;
  readonly AUTH_HOSTED_MODEL_SCOPE?: string;
}

const Configuration = Schema.Struct({
  AUTH_ISSUER: Schema.NonEmptyString,
  AUTH_RESOURCE: Schema.NonEmptyString,
  AUTH_JWKS_URL: Schema.NonEmptyString,
  AUTH_REQUIRED_SCOPE: OAuthScope.OAuthScope,
  AUTH_HOSTED_MODEL_SCOPE: OAuthScope.OAuthScope,
});

const unavailable = () =>
  AccountError.make({ code: "unavailable", message: "Authentication is temporarily unavailable." });

const unauthorized = () =>
  AccountError.make({ code: "unauthorized", message: "Sign in to continue." });

const accessCookie = Effect.fnUntraced(function* (request: Request) {
  const cookie = request.headers.get("cookie") ?? "";

  const values = cookie
    .split(";")
    .map((value) => value.trim())
    .filter((value) => value.startsWith("__Host-yielded-access="));

  const value = values[0];

  if (values.length !== 1 || value === undefined) return yield* unauthorized();

  return value.slice("__Host-yielded-access=".length);
});

/** Verify the host's HttpOnly access cookie with its scoped Jwks and Signature services.
 * The host owns the trusted key endpoint and must retain bounded verification on Workers.
 * No account or permission store exists here.
 */
export const authenticateRequest = Effect.fnUntraced(function* (
  request: Request,
  env: AuthEnvironment,
) {
  const credential = yield* accessCookie(request);

  const config = yield* Schema.decodeUnknownEffect(Configuration)(env).pipe(
    Effect.mapError(unavailable),
  );

  if (credential.length > 16_384) return yield* unauthorized();

  const verifier = yield* OAuthServer.makeAccessTokenVerifierWithKeySet({
    issuer: config.AUTH_ISSUER,
    resource: config.AUTH_RESOURCE,
  }).pipe(Effect.mapError(unavailable));

  const access = yield* verifier
    .verify(Redacted.make(credential))
    .pipe(
      Effect.catchCause((cause) =>
        Effect.failCause(
          Cause.map(cause, (error) =>
            error._tag === "OAuthServerInvalidToken" ? unauthorized() : unavailable(),
          ),
        ),
      ),
    );

  if (!access.scopes.includes(config.AUTH_REQUIRED_SCOPE)) return yield* unauthorized();

  const claims = yield* Schema.decodeUnknownEffect(AccountClaims)(access.claims).pipe(
    Effect.mapError(unauthorized),
  );

  return { subjectId: access.subjectId, ...claims, scopes: access.scopes };
});

export type AuthenticatedAccount = Effect.Success<ReturnType<typeof authenticateRequest>>;

/** Application-owned verification; each layer owns one scoped remote key resolver. */
export class Authentication extends Context.Service<
  Authentication,
  { readonly authenticate: (request: Request) => Effect.Effect<AuthenticatedAccount, AccountError> }
>()("agent-example-auth/Authentication") {
  static readonly layer = (env: AuthEnvironment) =>
    Layer.effect(
      Authentication,
      Effect.gen(function* () {
        const keys = yield* Jwks.Jwks;
        const signature = yield* Signature.Signature;

        return {
          authenticate: (request: Request) =>
            authenticateRequest(request, env).pipe(
              Effect.provideService(Jwks.Jwks, keys),
              Effect.provideService(Signature.Signature, signature),
            ),
        };
      }),
    ).pipe(
      Layer.provide(
        Jwks.layerRemote({
          url: env.AUTH_JWKS_URL ?? "",
          maxKeys: 16,
          cacheTimeMs: 300_000,
        }),
      ),
      Layer.provide([
        FetchHttpClient.layer,
        Signature.layerWebCrypto.pipe(Layer.provide(SecureRandom.layerWebCrypto)),
      ]),
      // Unconfigured authentication must not prevent public assets from being served.
      Layer.catchCause((cause) =>
        Layer.succeed(Authentication, {
          authenticate: (request) =>
            accessCookie(request).pipe(
              Effect.andThen(Effect.failCause(Cause.map(cause, unavailable))),
            ),
        }),
      ),
    );
}

/** Durable work asks the issuer for current funding; access-token snapshots cannot authorize recovery. */
export const hostedModelAccess = Effect.fnUntraced(
  function* (
    accountId: string,
    env: AuthEnvironment & { readonly AUTH_YIELDED_CLIENT_SECRET?: string },
  ) {
    if (!env.AUTH_ISSUER || !env.AUTH_YIELDED_CLIENT_SECRET) return yield* unavailable();
    const http = yield* HttpClient.HttpClient;

    const request = yield* HttpClientRequest.post(`${env.AUTH_ISSUER}/internal/agent/access`).pipe(
      HttpClientRequest.bearerToken(Redacted.make(env.AUTH_YIELDED_CLIENT_SECRET)),
      HttpClientRequest.bodyJson({ accountId }),
    );

    const response = yield* http.execute(request);

    if (response.status !== 200) return yield* unavailable();

    return (yield* response.json.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Schema.Struct({ allowed: Schema.Boolean }))),
    )).allowed;
  },
  Effect.scoped,
  Effect.timeout("5 seconds"),
  Effect.mapError(unavailable),
  Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual", credentials: "omit" }),
  Effect.provide(FetchHttpClient.layer),
);
