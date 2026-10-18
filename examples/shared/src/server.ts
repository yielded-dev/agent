import { OAuthScope, OAuthServer } from "@yielded/auth";
import * as Portable from "@yielded/crypto/Portable";
import { Jwk } from "@yielded/jose";
import { Clock, Effect, Redacted, Schema } from "effect";
import { Base64Url } from "effect/encoding";
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

const KeySet = Schema.Struct({
  keys: Schema.NonEmptyArray(Jwk.PublicJwk).check(Schema.isMaxLength(16)),
});

const KeyHint = Schema.fromJsonString(
  Schema.Struct({ kid: Schema.NonEmptyString.check(Schema.isMaxLength(256)) }),
);

const unavailable = () =>
  AccountError.make({ code: "unavailable", message: "Authentication is temporarily unavailable." });

const unauthorized = () =>
  AccountError.make({ code: "unauthorized", message: "Sign in to continue." });

type KeySnapshot = {
  readonly publicKeys: typeof KeySet.Type.keys;
  readonly kids: ReadonlyArray<string | undefined>;
  readonly expiresAt: number;
};
type KeyCache = {
  pending?: Promise<void>;
  snapshot?: KeySnapshot;
  nextAttemptAt: number;
};
// Only trusted deployment configuration selects an entry. Tokens and authorization are never cached.
const keyCaches = new Map<string, KeyCache>();

const keysFor = Effect.fnUntraced(function* (config: typeof Configuration.Type, kid: string) {
  const key = JSON.stringify([config.AUTH_ISSUER, config.AUTH_RESOURCE, config.AUTH_JWKS_URL]);
  let cache = keyCaches.get(key);

  if (cache === undefined) {
    cache = { nextAttemptAt: 0 };
    keyCaches.set(key, cache);
  }
  const current = cache;
  const now = yield* Clock.currentTimeMillis;

  if (current.snapshot && current.snapshot.expiresAt > now && current.snapshot.kids.includes(kid))
    return current.snapshot.publicKeys;
  const { waitUntil } = yield* Effect.promise(() => import("cloudflare:workers"));

  // Native promise reactions resume in each waiting Worker's request context.
  // Effect semaphore observers resume in the releasing request instead.
  const acquire: Effect.Effect<() => void> = Effect.suspend(() => {
    const pending = current.pending;

    if (pending)
      return Effect.promise(() => pending).pipe(Effect.interruptible, Effect.andThen(acquire));
    let complete: () => void = () => undefined;

    current.pending = new Promise<void>((resolve) => {
      complete = resolve;
    });
    // Finish the bounded refresh even if its initiating request disconnects.
    waitUntil(current.pending);

    return Effect.succeed(() => {
      current.pending = undefined;
      complete();
    });
  });

  return yield* Effect.acquireUseRelease(
    acquire,
    Effect.fnUntraced(function* () {
      const now = yield* Clock.currentTimeMillis;
      const previous = current.snapshot;

      if (previous && previous.expiresAt > now && previous.kids.includes(kid))
        return previous.publicKeys;
      if (current.nextAttemptAt > now) {
        if (previous && previous.expiresAt > now) return previous.publicKeys;

        return yield* unavailable();
      }
      // A token may suggest a key ID, never a URL. Unknown IDs trigger at most one fetch per 30s.
      current.nextAttemptAt = now + 30_000;

      return yield* Effect.gen(function* () {
        const http = yield* HttpClient.HttpClient;
        const response = yield* http.get(config.AUTH_JWKS_URL);

        if (response.status !== 200) return yield* unavailable();

        const keys = yield* response.json.pipe(Effect.flatMap(Schema.decodeUnknownEffect(KeySet)));

        current.snapshot = {
          publicKeys: keys.keys,
          kids: keys.keys.map((key) => key.kid),
          expiresAt: now + 300_000,
        };

        return keys.keys;
      }).pipe(
        Effect.scoped,
        Effect.timeout("5 seconds"),
        Effect.mapError(unavailable),
        Effect.catch((error) =>
          previous && previous.expiresAt > now
            ? Effect.succeed(previous.publicKeys)
            : Effect.fail(error),
        ),
        Effect.provideService(FetchHttpClient.RequestInit, {
          redirect: "manual",
          credentials: "omit",
        }),
        Effect.provide(FetchHttpClient.layer),
      );
    }),
    (release) => Effect.sync(release),
  );
});

/** Verify the host's HttpOnly access cookie locally; no account or permission store exists here. */
export const authenticateRequest = Effect.fnUntraced(function* (
  request: Request,
  env: AuthEnvironment,
) {
  const cookie = request.headers.get("cookie") ?? "";

  const values = cookie
    .split(";")
    .map((value) => value.trim())
    .filter((value) => value.startsWith("__Host-yielded-access="));

  if (values.length !== 1) return yield* unauthorized();

  const config = yield* Schema.decodeUnknownEffect(Configuration)(env).pipe(
    Effect.mapError(unavailable),
  );

  const credential = values[0]!.slice("__Host-yielded-access=".length);

  if (credential.length > 16_384) return yield* unauthorized();

  // This untrusted hint only selects keys from the configured issuer's JWKS.
  const hint = yield* Effect.fromResult(Base64Url.decodeString(credential.split(".")[0]!)).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(KeyHint)),
    Effect.mapError(unauthorized),
  );

  const publicKeys = yield* keysFor(config, hint.kid);

  // Verifiers capture Effect services; keep that context inside the current Worker request.
  const verifier = yield* OAuthServer.makeAccessTokenVerifier({
    issuer: config.AUTH_ISSUER,
    resource: config.AUTH_RESOURCE,
    publicKeys,
  }).pipe(
    Effect.provide(Portable.layerWithoutKdf(globalThis.crypto.subtle)),
    Effect.mapError(unavailable),
  );

  const access = yield* verifier
    .verify(Redacted.make(credential))
    .pipe(
      Effect.mapError((error) =>
        error._tag === "OAuthServerInvalidToken" ? unauthorized() : unavailable(),
      ),
    );

  if (!access.scopes.includes(config.AUTH_REQUIRED_SCOPE)) return yield* unauthorized();

  const claims = yield* Schema.decodeUnknownEffect(AccountClaims)(access.claims).pipe(
    Effect.mapError(unauthorized),
  );

  return { subjectId: access.subjectId, ...claims, scopes: access.scopes };
});

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
