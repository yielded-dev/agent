import type { Sandbox } from "@cloudflare/sandbox";
import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect, Layer, Redacted, Schema } from "effect";

const state = Layer.unwrap(
  Config.Boolean("ALCHEMY_LOCAL_STATE").pipe(
    Config.withDefault(false),
    Effect.map((local) => (local ? Alchemy.localState() : Cloudflare.state())),
    Effect.orDie,
  ),
);

export interface PlannerDeployment {
  readonly stackName: string;
  readonly workerName: string;
  readonly rootDir: string;
  readonly artifactsNamespace: string;
  readonly appDomain: string;
  readonly appZoneId?: string;
  readonly domain?: string;
  readonly routes?: ReadonlyArray<{
    readonly pattern: string;
    readonly zoneName?: string;
    readonly zoneId?: string;
  }>;
  readonly workersDev?: boolean;
}

/** The caller owns hosted names, routes and policy. Logical resource IDs preserve stored data. */
export const plannerStack = (deployment: PlannerDeployment) =>
  Alchemy.Stack(
    deployment.stackName,
    {
      providers: Cloudflare.providers(),
      state,
    },
    Effect.gen(function* () {
      const { accountId } = yield* yield* Cloudflare.CloudflareEnvironment;

      const serverOpenAiKey = yield* Config.Redacted("SERVER_OPENAI_KEY").pipe(
        Config.withDefault(Redacted.make("")),
      );

      const artifacts = yield* Cloudflare.Artifacts.Namespace("ARTIFACTS", {
        namespace: deployment.artifactsNamespace,
      });

      const zoneId = deployment.appZoneId;

      if (zoneId !== undefined)
        yield* Cloudflare.DNS.Record("TripAppsWildcard", {
          zoneId,
          name: `*.${deployment.appDomain}`,
          type: "A",
          content: "192.0.2.1",
          proxied: true,
          ttl: "1",
        });
      const builds = yield* Cloudflare.R2.Bucket("TripAppBuilds");

      const app = yield* Cloudflare.Website.Vite("Planner", {
        name: deployment.workerName,
        ...(deployment.domain === undefined ? {} : { domain: deployment.domain }),
        routes: deployment.routes === undefined ? [] : [...deployment.routes],
        workersDev: { enabled: deployment.workersDev ?? false, previewsEnabled: false },
        rootDir: deployment.rootDir,
        main: "src/worker.ts",
        compatibility: { date: "2026-07-01", flags: ["nodejs_compat"] },
        assets: { runWorkerFirst: true },
        env: {
          ...(Redacted.value(serverOpenAiKey) ? { SERVER_OPENAI_KEY: serverOpenAiKey } : {}),
          // New binding keys AND classes create empty namespaces. A class-only
          // rename under the old key preserves data. Removed bindings delete it.
          PLANNER_THREADS_V3: Cloudflare.DurableObject("PlannerThreadsV3", {
            className: "PlannerThreadV3",
          }),
          AUTH_V2: Cloudflare.DurableObject("AuthV2", { className: "PlannerAuthV2" }),
          AUTH_ISSUER: Config.NonEmptyString("AUTH_ISSUER"),
          AUTH_RESOURCE: Config.NonEmptyString("AUTH_RESOURCE"),
          AUTH_JWKS_URL: Config.NonEmptyString("AUTH_JWKS_URL"),
          AUTH_REQUIRED_SCOPE: Config.NonEmptyString("AUTH_REQUIRED_SCOPE"),
          AUTH_HOSTED_MODEL_SCOPE: Config.NonEmptyString("AUTH_HOSTED_MODEL_SCOPE"),
          AUTH_YIELDED_CLIENT_SECRET: Config.schema(
            Schema.Redacted(Schema.NonEmptyString),
            "AUTH_YIELDED_CLIENT_SECRET",
          ),
          ARTIFACTS: artifacts,
          APP_BUILDS: builds,
          APP_LOADER: Cloudflare.WorkerLoader("APP_LOADER"),
          APP_DOMAIN: deployment.appDomain,
          SITE_BUILD: Cloudflare.Workflow("SiteBuild", { className: "SiteBuild" }),
          APP_SANDBOX: Cloudflare.Container<Sandbox>("AppSandbox", {
            className: "Sandbox",
            context: `${deployment.rootDir}/site-builder`,
            // Explicit standard-3 resources: the named tier alone did not resize
            // the deployed application through the current Alchemy provider.
            vcpu: 2,
            memory: "8GiB",
            disk: { size_mb: 16000 },
            maxInstances: 4,
          }),
          ARTIFACTS_GIT_BASE: `https://${accountId}.artifacts.cloudflare.net/git/${artifacts.namespace}`,
          BROWSER: Cloudflare.Browser(),
          BYOK_ENCRYPTION_KEY: Config.schema(
            Schema.Redacted(Schema.NonEmptyString),
            "BYOK_ENCRYPTION_KEY",
          ),
        },
        // The hosted composition verifies query-string redaction before enabling traces.
        // Keep collection off here until Alchemy's SDK supports redact_query_string.
        observability: {
          enabled: true,
          logs: { enabled: true, invocationLogs: false },
          traces: { enabled: false },
        },
        logpush: false,
        memo: {
          include: ["src/**", "site-builder/**", "vite.config.ts", "package.json"],
          lockfile: true,
        },
      });

      return { url: app.url };
    }),
  );
