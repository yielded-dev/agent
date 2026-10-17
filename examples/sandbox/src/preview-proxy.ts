import {
  CAPACITY_NAME,
  NativeSandboxNamespace,
  PREVIEW_TOKEN_HEADER,
  PreviewRoute,
  SandboxOperationError,
  decodeNativeReply,
  previewTokenHash,
} from "@yielded/sandbox/cloudflare";
import { Clock, Context, Effect, Layer, Schema } from "effect";

import { PREVIEW_LIFETIME_MS, WorkspacePreviewGrant } from "./contract.ts";

export const PREVIEW_HANDOFF_PATH = "/__preview/session";
const cookieName = "__Host-yielded-preview";
const bodyLimit = 8 * 1024;

const Configuration = Schema.Struct({
  applicationOrigin: Schema.String.check(
    Schema.makeFilter(
      (origin: string) => {
        try {
          const url = new URL(origin);

          return url.protocol === "https:" && url.origin === origin;
        } catch {
          return false;
        }
      },
      { expected: "an HTTPS application origin" },
    ),
  ),
  previewDomain: Schema.String.check(
    Schema.isMaxLength(218),
    Schema.isPattern(
      /^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?$/u,
    ),
  ),
});

export type WorkspacePreviewConfiguration = typeof Configuration.Type;

const Handoff = Schema.Struct({
  token: WorkspacePreviewGrant.fields.token,
  expiresAt: Schema.NumberFromString.check(Schema.isInt(), Schema.isGreaterThan(0)),
  // The form carries a decoded relative file path; encode each segment only at redirect.
  path: Schema.String.check(
    Schema.isMaxLength(4096),
    Schema.makeFilter(
      (path: string) =>
        !path.startsWith("/") &&
        !/[\\%]/u.test(path) &&
        Array.from(path).every((char) => char.charCodeAt(0) >= 32 && char.charCodeAt(0) !== 127) &&
        path
          .split("/")
          .every((part) => part !== "." && part !== ".." && !part.startsWith(".sandbox-agent")),
      { expected: "a relative workspace file path" },
    ),
  ),
});

const Route = PreviewRoute.check(
  Schema.makeFilter((route) => /^[a-f0-9]{64}$/u.test(route.sandboxId), {
    expected: "a native sandbox owner ID",
  }),
);

class RequestRejected extends Schema.TaggedError<RequestRejected>()("PreviewRequestRejected", {
  status: Schema.Literals([400, 403, 404, 405, 413]),
}) {}

const failure = (cause: unknown) =>
  new SandboxOperationError({ operation: "preview.proxy", cause });

const secureHeaders = {
  "cache-control": "private, no-store",
  "referrer-policy": "no-referrer",
  "x-content-type-options": "nosniff",
  "x-frame-options": "DENY",
  "cross-origin-opener-policy": "same-origin",
  "cross-origin-resource-policy": "same-origin",
  "origin-agent-cluster": "?1",
  "content-security-policy":
    "default-src 'self' data: blob:; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'; worker-src 'none'; object-src 'none'; frame-src 'none'; frame-ancestors 'none'; base-uri 'self'; form-action 'none'; sandbox allow-scripts allow-same-origin",
};

const response = (status: number, headers?: Record<string, string>) =>
  new Response(null, { status, headers: { ...secureHeaders, ...headers } });

const readHandoff = Effect.fnUntraced(function* (request: Request) {
  if (
    request.headers.get("content-type")?.split(";", 1)[0]?.trim() !==
    "application/x-www-form-urlencoded"
  )
    return yield* RequestRejected.make({ status: 400 });

  const text = yield* Effect.tryPromise({
    try: async () => {
      const reader = request.body?.getReader();

      if (!reader) return "";
      const decoder = new TextDecoder();
      let size = 0;
      let text = "";

      try {
        while (true) {
          const next = await reader.read();

          if (next.done) return text + decoder.decode();
          size += next.value.byteLength;
          if (size > bodyLimit) {
            await reader.cancel();

            return null;
          }
          text += decoder.decode(next.value, { stream: true });
        }
      } finally {
        reader.releaseLock();
      }
    },
    catch: failure,
  });

  if (text === null) return yield* RequestRejected.make({ status: 413 });
  const form = new URLSearchParams(text);

  if (["token", "expiresAt", "path"].some((key) => form.getAll(key).length !== 1))
    return yield* RequestRejected.make({ status: 400 });

  return yield* Schema.decodeUnknownEffect(Handoff)(Object.fromEntries(form)).pipe(
    Effect.mapError(() => RequestRejected.make({ status: 400 })),
  );
});

/** Route this before application assets. The host issues grants only after owner authorization. */
export class WorkspacePreviewProxy extends Context.Service<
  WorkspacePreviewProxy,
  {
    readonly handoffUrl: (token: string) => Effect.Effect<string, SandboxOperationError>;
    readonly handle: (request: Request) => Effect.Effect<Response | null, SandboxOperationError>;
  }
>()("sandbox-example/WorkspacePreviewProxy") {
  static readonly layer = (configuration: WorkspacePreviewConfiguration) =>
    Layer.effect(
      this,
      Effect.gen(function* () {
        const config = yield* Schema.decodeEffect(Configuration)(configuration);
        const namespace = yield* NativeSandboxNamespace;

        const originForHash = (hash: string) =>
          `https://p-${hash.slice(0, 32)}.${config.previewDomain}`;

        const handoffUrl = Effect.fnUntraced(function* (input: string) {
          const token = yield* Schema.decodeEffect(WorkspacePreviewGrant.fields.token)(input);
          const origin = originForHash(yield* previewTokenHash(token));

          if (origin === config.applicationOrigin)
            return yield* failure("Preview and application origins must differ");

          return origin + PREVIEW_HANDOFF_PATH;
        }, Effect.mapError(failure));

        const resolve = Effect.fnUntraced(function* (token: string, origin: string) {
          const hash = yield* previewTokenHash(token);

          if (originForHash(hash) !== origin) return yield* RequestRejected.make({ status: 404 });
          const coordinator = yield* namespace.getByName(CAPACITY_NAME);
          const reply = yield* namespace.call(coordinator, "resolvePreview", hash);
          const route = yield* decodeNativeReply("preview.resolve", reply);

          if (route === null) return yield* RequestRejected.make({ status: 404 });

          return yield* Schema.decodeEffect(Route)(route);
        });

        const handle = Effect.fnUntraced(
          function* (request: Request) {
            const url = new URL(request.url);

            if (url.origin === config.applicationOrigin) return null;
            if (
              url.hostname !== config.previewDomain &&
              !url.hostname.endsWith(`.${config.previewDomain}`)
            )
              return null;
            const label = url.hostname.slice(0, -(config.previewDomain.length + 1));

            if (url.protocol !== "https:" || url.port !== "" || !/^p-[a-f0-9]{32}$/u.test(label))
              return yield* RequestRejected.make({ status: 404 });
            if (
              request.headers.has("upgrade") ||
              request.headers.has("service-worker") ||
              ["serviceworker", "worker", "sharedworker"].includes(
                request.headers.get("sec-fetch-dest") ?? "",
              )
            )
              return yield* RequestRejected.make({ status: 403 });
            if (url.pathname === PREVIEW_HANDOFF_PATH) {
              if (request.method !== "POST") return yield* RequestRejected.make({ status: 405 });
              if (request.headers.get("origin") !== config.applicationOrigin || url.search !== "")
                return yield* RequestRejected.make({ status: 403 });
              const form = yield* readHandoff(request);

              yield* resolve(form.token, url.origin);
              const now = yield* Clock.currentTimeMillis;

              const maxAge = Math.floor(
                (Math.min(form.expiresAt, now + PREVIEW_LIFETIME_MS) - now) / 1000,
              );

              if (maxAge <= 0) return yield* RequestRejected.make({ status: 404 });

              const path = yield* Effect.try({
                try: () => "/" + form.path.split("/").map(encodeURIComponent).join("/"),
                catch: () => RequestRejected.make({ status: 400 }),
              });

              return response(303, {
                location: path,
                "set-cookie": `${cookieName}=${form.token}; Path=/; Secure; HttpOnly; SameSite=Lax; Max-Age=${maxAge}`,
              });
            }
            if (request.method !== "GET" && request.method !== "HEAD")
              return yield* RequestRejected.make({ status: 405 });
            if (url.pathname.startsWith("/__preview/"))
              return yield* RequestRejected.make({ status: 404 });

            const cookies = (request.headers.get("cookie") ?? "")
              .split(";")
              .map((part) => part.trim())
              .filter((part) => part.startsWith(`${cookieName}=`));

            const cookie = cookies[0];

            if (cookies.length !== 1 || cookie === undefined)
              return yield* RequestRejected.make({ status: 404 });

            const token = yield* Schema.decodeEffect(WorkspacePreviewGrant.fields.token)(
              cookie.slice(cookieName.length + 1),
            ).pipe(Effect.mapError(() => RequestRejected.make({ status: 404 })));

            const route = yield* resolve(token, url.origin);
            const stub = yield* namespace.get(yield* namespace.idFromString(route.sandboxId));

            // Build fresh headers: browser credentials and native/internal headers never cross this boundary.
            const upstream = yield* namespace.fetch(
              stub,
              new Request(url, {
                method: request.method,
                headers: { [PREVIEW_TOKEN_HEADER]: token },
                redirect: "manual",
                signal: request.signal,
              }),
            );

            const headers = new Headers(secureHeaders);

            for (const name of ["content-type", "content-length"]) {
              const value = upstream.headers.get(name);

              if (value !== null) headers.set(name, value);
            }
            const location = upstream.headers.get("location");

            if (location !== null) {
              const target = new URL(location, url);

              if (target.origin === url.origin)
                headers.set("location", target.pathname + target.search);
            }

            // No upstream cookies, CORS, service-worker scope, or security-policy overrides.
            return new Response(request.method === "HEAD" ? null : upstream.body, {
              status: upstream.status,
              headers,
            });
          },
          Effect.catchTag("PreviewRequestRejected", ({ status }) =>
            Effect.succeed(response(status)),
          ),
          Effect.mapError(failure),
        );

        return WorkspacePreviewProxy.of({ handoffUrl, handle });
      }),
    );
}
