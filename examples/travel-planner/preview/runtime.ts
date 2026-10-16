import { generateKeyPairSync, sign } from "node:crypto";
import { fileURLToPath } from "node:url";

import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";

export class PreviewError extends Schema.TaggedError<PreviewError>()("PreviewError", {
  message: Schema.String,
  cause: Schema.Defect(),
}) {}

const root = fileURLToPath(new URL("../", import.meta.url));

/** A fresh, loopback-only runtime. No deployed bindings or credentials are read. */
export const localPreview = Effect.fn("localPreview")(function* (
  port: number,
  assets = { server: `${root}dist/server`, client: `${root}dist/client` },
) {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "travel-preview-" });
  const origin = `https://127.0.0.1:${port}`;
  const server = assets.server;
  const files = yield* fs.readDirectory(server, { recursive: true });

  // Alchemy uploads Vite assets under its base path; mirror that manifest in Miniflare.
  const publicDirectory = `${directory}/public`;

  yield* fs.makeDirectory(publicDirectory);
  yield* fs.copy(assets.client, `${publicDirectory}/travel`);

  const modules = ["worker.js", ...files.filter((file) => file !== "worker.js")]
    .filter((file) => file.endsWith(".js") || file.endsWith(".css"))
    .map((file) => ({
      type: file.endsWith(".css") ? ("Text" as const) : ("ESModule" as const),
      path: `${server}/${file}`,
    }));

  const bundle = yield* Effect.tryPromise({
    try: () =>
      build({
        entryPoints: [`${root}preview/planner.ts`],
        bundle: true,
        write: false,
        format: "esm",
        target: "es2022",
        platform: "browser",
        conditions: ["workerd", "worker", "browser"],
        external: ["cloudflare:*", "node:*"],
        alias: { "@tanstack/react-start/server-entry": `${root}test/fixtures/start.ts` },
        banner: {
          js: 'import { createRequire } from "node:module"; const require = createRequire("/preview.mjs");',
        },
        logLevel: "silent",
      }),
    catch: (cause) => PreviewError.make({ message: "Could not build the offline planner.", cause }),
  });

  const keys = generateKeyPairSync("rsa", { modulusLength: 2048 });

  const jwks = {
    keys: [{ ...keys.publicKey.export({ format: "jwk" }), kid: "preview", alg: "RS256" }],
  };

  const claims = {
    accountId: "00000000-0000-4000-8000-000000000001",
    displayName: "Local preview",
  };

  const scopes = ["preview:planner"];

  const bindings = {
    AUTH_ISSUER: origin,
    AUTH_RESOURCE: origin,
    AUTH_JWKS_URL: `${origin}/jwks`,
    AUTH_REQUIRED_SCOPE: scopes[0],
    AUTH_HOSTED_MODEL_SCOPE: "preview:hosted-model",
    AUTH_YIELDED_CLIENT_SECRET: "local-preview",
    BYOK_ENCRYPTION_KEY: Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString("base64"),
  };

  // Explicit loopback fixture: its ephemeral signing key never reaches a production bundle.
  const authResponse = (request: Request) => {
    const path = new URL(request.url).pathname;

    if (path === "/auth/logout")
      return new Response(null, {
        status: 204,
        headers: {
          "set-cookie": "__Host-yielded-access=; Secure; HttpOnly; SameSite=Lax; Path=/; Max-Age=0",
        },
      });
    if (path === "/auth/session" && !request.headers.get("cookie")) return Response.json(null);
    if (!["/auth/login", "/auth/session", "/auth/refresh"].includes(path))
      return new Response("Not found", { status: 404 });
    const now = Math.floor(Date.now() / 1000);

    const header = Buffer.from(
      JSON.stringify({ alg: "RS256", typ: "at+jwt", kid: "preview" }),
    ).toString("base64url");

    const payload = Buffer.from(
      JSON.stringify({
        iss: origin,
        sub: "local-preview",
        aud: origin,
        iat: now,
        exp: now + 300,
        jti: Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString("base64url"),
        grant_id: Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString("base64url"),
        client_id: "preview",
        scope: scopes.join(" "),
        auth_time: now,
        sid: "preview",
        claims,
      }),
    ).toString("base64url");

    const input = `${header}.${payload}`;
    const token = `${input}.${Buffer.from(sign("RSA-SHA256", Buffer.from(input), keys.privateKey)).toString("base64url")}`;

    const headers = {
      "set-cookie": `__Host-yielded-access=${token}; Secure; HttpOnly; SameSite=Lax; Path=/; Max-Age=300`,
      "cache-control": "no-store",
    };

    return path === "/auth/login"
      ? new Response(null, { status: 303, headers: { ...headers, location: "/travel/" } })
      : Response.json(
          { subjectId: "local-preview", ...claims, scopes, expiresAtMillis: (now + 300) * 1000 },
          { headers },
        );
  };

  const preview = yield* Effect.acquireRelease(
    Effect.try({
      try: () =>
        new Miniflare(
          convertV4MiniflareOptions({
            host: "127.0.0.1",
            port,
            https: true,
            rootPath: directory,
            resourceTmpPath: directory,
            workers: [
              {
                name: "preview-gateway",
                modules: true,
                script:
                  'export default { fetch(request, env) { return new URL(request.url).pathname.startsWith("/auth/") ? env.AUTH.fetch(request) : env.APP.fetch(request) } }',
                serviceBindings: { AUTH: authResponse, APP: "preview-ui" },
              },
              {
                name: "preview-ui",
                modules,
                modulesRoot: server,
                compatibilityDate: "2026-07-01",
                compatibilityFlags: ["nodejs_compat"],
                bindings,
                durableObjects: {
                  AUTH_V2: { className: "PlannerAuthV2", useSQLite: true },
                  PLANNER_THREADS_V3: {
                    className: "TravelPlannerThread",
                    scriptName: "preview-planner",
                    useSQLite: true,
                  },
                },
                assets: {
                  directory: publicDirectory,
                  binding: "ASSETS",
                  run_worker_first: true,
                  routerConfig: { has_user_worker: true },
                },
                outboundService: (request) =>
                  request.url === `${origin}/jwks`
                    ? Response.json(jwks)
                    : new Response("Unavailable in local preview", { status: 503 }),
              },
              {
                name: "preview-planner",
                modules: true,
                script: bundle.outputFiles[0]!.text,
                modulesRoot: "/",
                compatibilityDate: "2026-07-01",
                compatibilityFlags: ["nodejs_compat"],
                bindings,
                durableObjects: {
                  PLANNER_THREADS_V3: { className: "TravelPlannerThread", useSQLite: true },
                  AUTH_V2: {
                    className: "PlannerAuthV2",
                    scriptName: "preview-ui",
                    useSQLite: true,
                  },
                },
                r2Buckets: ["APP_BUILDS"],
                outboundService: (request) =>
                  new Response(null, {
                    status:
                      request.url === "https://api.openai.com/v1/models" &&
                      request.headers.get("authorization") === "Bearer sk-preview-local"
                        ? 200
                        : 503,
                  }),
              },
            ],
          }),
        ),
      catch: (cause) =>
        PreviewError.make({ message: "Could not create the preview runtime.", cause }),
    }),
    (runtime) => Effect.promise(() => runtime.dispose()),
  );

  yield* Effect.tryPromise({
    try: () => preview.ready,
    catch: (cause) => PreviewError.make({ message: "Could not start the preview runtime.", cause }),
  });

  return { preview, origin, directory };
});
