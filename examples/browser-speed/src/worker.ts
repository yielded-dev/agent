import { BrowserSessions } from "@yielded/agent-platform-cloudflare/browser-session";
import { DurableObject } from "cloudflare:workers";
import { Context, Effect, Layer, Redacted, Schema } from "effect";
import { FetchHttpClient, HttpRouter, HttpServer } from "effect/http";
import { HttpApiBuilder } from "effect/http-api";

import { type Account, LabApi, LabError, ModelApi, modelChoices } from "./contract.ts";
import { connectKitesurf } from "./kitesurf.ts";
import { Control, emptyControl, makeOwner } from "./owner.ts";

export interface Env {
  readonly LAB: DurableObjectNamespace<BrowserLab>;
  readonly BROWSER: BrowserRun;
  readonly ASSETS: Fetcher;
  readonly CLOUDFLARE_ACCOUNT_ID?: string;
  readonly BROWSER_RENDERING_API_TOKEN?: string;
  readonly OPENAI_API_KEY?: string;
  readonly OPENAI_API_URL?: string;
  readonly OPENAI_API_TYPE?: string;
  readonly OPENAI_MODEL?: string;
  readonly WORKERS_AI_API_KEY?: string;
  readonly TYPESAFE_API_KEY?: string;
  /** Mercury 2.5 writes Jev field text when set; otherwise GPT-6 Luna through OPENAI_API_KEY. */
  readonly OPENROUTER_API_KEY?: string;
  /** "true" runs on visitor keys only, without the scripted baseline. */
  readonly LAB_PUBLIC?: string;
  /** Per-address run admission for public labs. */
  readonly RUN_LIMIT?: RateLimit;
  /** The travel planner's auth on this origin; its allowlisted accounts run on FUNDED_* keys. */
  readonly AUTH?: DurableObjectNamespace;
  readonly FUNDED_OPENAI_API_KEY?: string;
  readonly FUNDED_TYPESAFE_API_KEY?: string;
  readonly FUNDED_OPENROUTER_API_KEY?: string;
}

/** Served under this path on agent.yielded.dev; the unprefixed API remains for local tools. */
const basePath = "/browser-use";

const codec = Schema.fromJsonString(Control);

const PlannerSession = Schema.Struct({
  subjectId: Schema.String.check(Schema.isUUID()),
  displayName: Schema.String,
});

const PlannerFunding = Schema.Struct({ allowed: Schema.Boolean });

/** Reads the request's travel planner session. Any failure reads as signed out. */
/** The travel planner's sign-in, read from a request's same-origin session cookie. */
export class PlannerAccounts extends Context.Service<
  PlannerAccounts,
  { readonly read: (request: Request) => Effect.Effect<Account | null> }
>()("browser-speed/PlannerAccounts") {
  /** Reads through the planner's auth Durable Object. Without it, or on any failure: anonymous. */
  static readonly layer = (auth: DurableObjectNamespace | undefined) =>
    Layer.succeed(PlannerAccounts, {
      read: Effect.fnUntraced(
        function* (request: Request) {
          const cookie = request.headers.get("cookie");

          if (auth === undefined || !cookie) return null;
          const planner = auth.getByName("auth-v1");

          const read = Effect.fnUntraced(function* (path: string, headers: HeadersInit = {}) {
            const response = yield* Effect.tryPromise(() =>
              planner.fetch(new Request(new URL(path, request.url), { headers })),
            );

            if (!response.ok) return yield* Effect.fail(response.status);

            return yield* Effect.tryPromise(() => response.json());
          });

          const session = yield* read("/_internal/session", { cookie }).pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(PlannerSession)),
          );

          const funding = yield* read(`/_internal/funding/${session.subjectId}`).pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(PlannerFunding)),
          );

          return { displayName: session.displayName, funded: funding.allowed } satisfies Account;
        },
        Effect.catch(() => Effect.succeed(null)),
      ),
    });
}

export class BrowserLab extends DurableObject<Env> {
  private readonly owner;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const hit = (location: string) => this.failpoint(location);

    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS browser_lab (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL)",
    );
    this.owner = makeOwner(
      {
        read: () => {
          const row = ctx.storage.sql
            .exec<{ value: string }>("SELECT value FROM browser_lab WHERE id=1")
            .toArray()[0];

          return row === undefined ? emptyControl : Schema.decodeSync(codec)(row.value);
        },
        write: (state, transition) => {
          this.failpoint(`before:${transition}`);
          ctx.storage.sql.exec(
            "INSERT OR REPLACE INTO browser_lab(id,value) VALUES (1,?)",
            Schema.encodeSync(codec)(state),
          );
          this.failpoint(`after:${transition}`);
        },
        alarm: Effect.fnUntraced(function* (at: number | null) {
          const transition = at === null ? "alarm-clear" : "alarm-set";

          hit(`before:${transition}`);
          yield* Effect.tryPromise({
            try: () => (at === null ? ctx.storage.deleteAlarm() : ctx.storage.setAlarm(at)),
            catch: () =>
              new LabError({ code: "storage", message: "Could not schedule browser cleanup." }),
          });
          hit(`after:${transition}`);
        }),
      },
      {
        model: env.OPENAI_MODEL || "gpt-6-luna",
        apiKey: env.OPENAI_API_KEY ?? "",
        ...(env.OPENAI_API_URL ? { apiUrl: env.OPENAI_API_URL } : {}),
        apiType: Schema.decodeUnknownSync(ModelApi)(env.OPENAI_API_TYPE || "responses"),
        browserConfigured: Boolean(env.CLOUDFLARE_ACCOUNT_ID && env.BROWSER_RENDERING_API_TOKEN),
        models: modelChoices.map(({ id, label }) => ({
          model: id,
          label,
          apiKey: id.startsWith("@cf/")
            ? (env.WORKERS_AI_API_KEY ?? "")
            : (env.OPENAI_API_KEY ?? ""),
          apiUrl: id.startsWith("@cf/")
            ? `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/ai/v1`
            : "https://api.openai.com/v1",
          apiType: id.startsWith("@cf/") ? "chat-completions" : "responses",
        })),
        jevApiKey: env.TYPESAFE_API_KEY,
        public: env.LAB_PUBLIC === "true",
        ...(env.OPENROUTER_API_KEY
          ? {
              jevText: {
                provider: "openrouter",
                model: "inception/mercury-2.5",
                reasoning: "none",
                apiKey: Redacted.make(env.OPENROUTER_API_KEY),
              },
            }
          : env.OPENAI_API_KEY
            ? {
                jevText: {
                  provider: "openai",
                  model: "gpt-6-luna",
                  reasoning: "low",
                  apiKey: Redacted.make(env.OPENAI_API_KEY),
                },
              }
            : {}),
        kitesurf: (retainClose) =>
          connectKitesurf(
            {
              accountId: env.CLOUDFLARE_ACCOUNT_ID ?? "",
              apiToken: Redacted.make(env.BROWSER_RENDERING_API_TOKEN ?? ""),
            },
            retainClose,
          ),
      },
    );
  }

  /** Overridden only by a test subclass; never selected by an HTTP request. */
  protected failpoint(_location: string): void {}

  private get sessions() {
    return BrowserSessions.layer({
      browser: this.env.BROWSER,
      accountId: this.env.CLOUDFLARE_ACCOUNT_ID ?? "",
      apiToken: Redacted.make(this.env.BROWSER_RENDERING_API_TOKEN ?? ""),
    }).pipe(Layer.provide(FetchHttpClient.layer));
  }

  private withBrowser<A>(effect: Effect.Effect<A, LabError, BrowserSessions>) {
    return effect.pipe(
      Effect.provide(this.sessions),
      Effect.catchTag(
        "BrowserRunCleanupError",
        () =>
          new LabError({
            code: "configuration",
            message: "Browser lifecycle credentials are invalid or unavailable.",
          }),
      ),
    );
  }

  fetch(request: Request): Promise<Response> {
    const handlers = HttpApiBuilder.group(LabApi, "lab", (group) =>
      group
        .handle("account", () =>
          Effect.gen(function* () {
            return yield* (yield* PlannerAccounts).read(request);
          }),
        )
        .handle("snapshot", () => this.owner.snapshot())
        .handle("run", ({ payload, headers }) =>
          Effect.gen({ self: this }, function* () {
            // Lab keys fill only what the visitor left out, and only for same-origin requests.
            const funded =
              request.headers.get("origin") === new URL(request.url).origin &&
              (yield* (yield* PlannerAccounts).read(request))?.funded === true;

            return yield* this.withBrowser(
              this.owner.run(
                payload,
                {
                  openai:
                    headers["x-lab-openai-key"] ??
                    (funded ? this.env.FUNDED_OPENAI_API_KEY : undefined),
                  typesafe:
                    headers["x-lab-typesafe-key"] ??
                    (funded ? this.env.FUNDED_TYPESAFE_API_KEY : undefined),
                  openrouter:
                    headers["x-lab-openrouter-key"] ??
                    (funded ? this.env.FUNDED_OPENROUTER_API_KEY : undefined),
                },
                { funded },
              ),
            );
          }),
        )
        .handle("stop", () => this.owner.stop())
        .handle("close", () => this.withBrowser(this.owner.close())),
    );

    const routes = HttpApiBuilder.layer(LabApi).pipe(
      Layer.provide(handlers),
      HttpRouter.provideRequest(PlannerAccounts.layer(this.env.AUTH)),
      Layer.provide(HttpServer.layerServices),
    );

    return Effect.runPromise(
      Effect.gen(function* () {
        const web = yield* Effect.acquireRelease(
          Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
          (value) => Effect.promise(() => value.dispose()),
        );

        const response = yield* Effect.promise(() => web.handler(request));
        const body = yield* Effect.promise(() => response.arrayBuffer());

        return new Response(body.byteLength === 0 ? null : body, {
          status: response.status,
          headers: { ...Object.fromEntries(response.headers), "cache-control": "no-store" },
        });
      }).pipe(Effect.scoped),
    );
  }

  alarm(): Promise<void> {
    return Effect.runPromise(
      this.withBrowser(this.owner.stop().pipe(Effect.andThen(this.owner.close()))),
    );
  }
}

const errorResponse = (
  message: string,
  status: number,
  code: (typeof LabError.fields.code)["Type"] = "configuration",
) =>
  new Response(
    Schema.encodeSync(Schema.fromJsonString(LabError))(new LabError({ code, message })),
    { status, headers: { "content-type": "application/json", "cache-control": "no-store" } },
  );

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const original = new URL(request.url);

    if (original.pathname === "/") return Response.redirect(new URL(`${basePath}/`, original), 302);
    const prefixed = original.pathname.startsWith(`${basePath}/api/`);

    if (!prefixed && !original.pathname.startsWith("/api/")) return env.ASSETS.fetch(request);
    const url = new URL(original);

    if (prefixed) url.pathname = url.pathname.slice(basePath.length);
    request = prefixed ? new Request(url, request) : request;
    if (
      request.method !== "GET" &&
      request.headers.has("origin") &&
      request.headers.get("origin") !== url.origin
    )
      return errorResponse("Request origin does not match the lab.", 400);

    const session = Schema.decodeUnknownOption(Schema.String.check(Schema.isUUID()))(
      request.headers.get("x-lab-session"),
    );

    if (session._tag === "None") return errorResponse("Invalid lab session.", 400);
    if (
      url.pathname !== "/api/snapshot" &&
      url.pathname !== "/api/account" &&
      (!env.CLOUDFLARE_ACCOUNT_ID || !env.BROWSER_RENDERING_API_TOKEN)
    )
      return errorResponse(
        "Set CLOUDFLARE_ACCOUNT_ID and BROWSER_RENDERING_API_TOKEN in the Worker.",
        400,
      );

    if (env.RUN_LIMIT !== undefined && url.pathname === "/api/run") {
      const { success } = await env.RUN_LIMIT.limit({
        key: request.headers.get("cf-connecting-ip") ?? "unknown",
      });

      if (!success)
        return errorResponse(
          "Too many runs from your address. Wait a minute, then try again.",
          429,
          "busy",
        );
    }

    return env.LAB.get(env.LAB.idFromName(session.value)).fetch(request);
  },
} satisfies ExportedHandler<Env>;
