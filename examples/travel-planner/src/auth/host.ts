import * as SqliteClient from "@effect/sql-sqlite-do/SqliteClient";
import * as Drizzle from "@yielded/auth-persistence-drizzle/SqliteDo";
import type { EmailDelivery } from "@yielded/auth/EmailDelivery";
import { LifecycleHooks } from "@yielded/auth/Hooks";
import { ProofUnavailable, ProofRequestContext } from "@yielded/auth/Proofs";
import * as KdfAdmission from "@yielded/crypto/KdfAdmission";
import * as Portable from "@yielded/crypto/Portable";
import * as WebCrypto from "@yielded/crypto/WebCrypto";
import { Context, Effect, Layer, Redacted, Schema } from "effect";
import {
  FetchHttpClient,
  HttpRouter,
  HttpServer,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/http";
import { HttpApiBuilder } from "effect/http-api";

import { FundingApi, FundingError } from "../funding-domain";
import { AccountId } from "./account";
import { emailRendererLayer } from "./email-delivery";
import { makeFundingStore } from "./funding";
import { persistenceLayer } from "./persistence";
import { makeAuth, type AuthConfiguration } from "./server";
import { initializeAuthStorage } from "./storage";

export class AuthHost extends Context.Service<
  AuthHost,
  {
    readonly fetch: (
      request: Request,
    ) => Effect.Effect<Response, Schema.SchemaError | FundingError>;
  }
>()("travel-planner/AuthHost") {}

/** The Auth object owns only auth state; planner data never passes through it. */
export const makeAuthHost = Effect.fn("Auth.host")(function* (
  storage: DurableObjectStorage,
  config: AuthConfiguration,
  delivery: Layer.Layer<EmailDelivery>,
  serverKeyConfigured = false,
) {
  yield* initializeAuthStorage(storage);
  const { AppAuth, http, security, providers } = makeAuth(config);

  const database = persistenceLayer(AppAuth).pipe(
    Layer.provideMerge(Drizzle.databaseLayer),
    Layer.provide(SqliteClient.layer({ storage })),
    Layer.provide(LifecycleHooks.empty),
  );

  const live = AppAuth.layer.pipe(
    Layer.provide([database, security, providers, delivery, emailRendererLayer]),
    Layer.provide([
      FetchHttpClient.layer,
      WebCrypto.layerCryptoWeb,
      Portable.layer(globalThis.crypto.subtle).pipe(Layer.provide(KdfAdmission.layer())),
    ]),
  );

  const authorized = HttpRouter.add(
    "GET",
    "/_internal/session",
    Effect.gen(function* () {
      const auth = yield* AppAuth;
      const session = yield* auth.requireSession();

      return yield* HttpServerResponse.json({
        subjectId: session.subjectId,
        displayName: session.claims.displayName,
      });
    }).pipe(
      Effect.catchTag("AuthenticationRequired", () =>
        Effect.succeed(HttpServerResponse.empty({ status: 401 })),
      ),
      Effect.catch(() => Effect.succeed(HttpServerResponse.empty({ status: 503 }))),
    ),
  ).pipe(http.middleware);

  const fundingHandlers = HttpApiBuilder.group(
    FundingApi,
    "funding",
    Effect.fn(function* (handlers) {
      const store = yield* makeFundingStore(storage);

      const actor = Effect.gen(function* () {
        const auth = yield* AppAuth;
        const request = yield* HttpServerRequest.HttpServerRequest;

        const session = yield* auth
          .requireSession()
          .pipe(Effect.mapError(() => FundingError.make({ message: "Sign in to continue." })));

        if (request.headers["x-elsewhere-account"] !== session.subjectId)
          return yield* FundingError.make({ message: "Account changed. Sign in again." });
        if (request.method !== "GET" && request.headers.origin !== config.AUTH_ORIGIN)
          return yield* FundingError.make({ message: "Invalid request origin." });

        return session.subjectId;
      });

      return handlers
        .handle("status", () =>
          Effect.gen(function* () {
            const status = yield* store.status(yield* actor);

            return { ...status, configured: serverKeyConfigured };
          }),
        )
        .handle("list", ({ query }) => Effect.flatMap(actor, (id) => store.list(id, query.after)))
        .handle("grant", ({ payload }) => Effect.flatMap(actor, (id) => store.grant(id, payload)))
        .handle("revoke", ({ payload }) =>
          Effect.flatMap(actor, (id) => store.revoke(id, payload)),
        );
    }),
  );

  const fundingRoutes = HttpApiBuilder.layer(FundingApi).pipe(
    Layer.provide(fundingHandlers),
    http.middleware,
  );

  const routes = Layer.mergeAll(http.routes(), authorized, fundingRoutes).pipe(
    Layer.provide(live),
    Layer.provide(HttpServer.layerServices),
  );

  const web = yield* Effect.acquireRelease(
    Effect.sync(() =>
      HttpRouter.toWebHandler(routes, {
        disableLogger: true,
        middleware: Effect.provideService(
          ProofRequestContext,
          Effect.gen(function* () {
            // The public Cloudflare edge supplies this header; the Auth Object is private.
            const request = yield* HttpServerRequest.HttpServerRequest;
            const address = request.headers["cf-connecting-ip"];

            if (address === undefined || address.length === 0)
              return yield* ProofUnavailable.make({});

            return { networkKey: Redacted.make(address) };
          }),
        ),
      }),
    ),
    (web) => Effect.promise(() => web.dispose()),
  );

  return AuthHost.of({
    fetch: Effect.fn("Auth.fetch")(function* (request) {
      const url = new URL(request.url);

      if (url.pathname.startsWith("/_internal/funding/")) {
        const id = yield* Schema.decodeEffect(AccountId)(
          url.pathname.slice("/_internal/funding/".length),
        );

        const store = yield* makeFundingStore(storage);

        return Response.json(yield* store.status(id));
      }
      const response = yield* Effect.promise(() => web.handler(request));
      const body = yield* Effect.promise(() => response.arrayBuffer());

      return new Response(body, { status: response.status, headers: response.headers });
    }),
  });
});
