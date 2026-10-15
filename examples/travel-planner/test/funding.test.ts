import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Schema } from "effect";
import { build } from "esbuild";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

const admin = "00000000-0000-0000-0000-000000000001";
const email = "00000000-0000-0000-0000-000000000002";
let mf: Miniflare;
let directory: string;
let script: string;
let linkedGithubIds = ["424242"];
let identityLookupUnavailable = false;

const start = () =>
  new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script,
      modulesRoot: "/",
      compatibilityDate: "2026-07-01",
      compatibilityFlags: ["nodejs_compat"],
      durableObjects: { STORE: { className: "FundingFixture", useSQLite: true } },
      resourcePersistencePath: directory,
      outboundService: async (request) => {
        if (request.url === "https://api.github.com/users/fixture-reader")
          return Response.json({ id: 424242, login: "fixture-reader" });
        if (
          request.url === "https://auth.yielded.dev/internal/agent/identities" &&
          request.method === "POST" &&
          request.headers.get("authorization") === "Bearer fixture-yielded-client-secret" &&
          Schema.decodeUnknownSync(Schema.Struct({ subjectId: Schema.String }))(
            await request.json(),
          ).subjectId === "yielded-reader"
        )
          return identityLookupUnavailable
            ? new Response("Unavailable", { status: 503 })
            : Response.json({ githubIds: linkedGithubIds });

        return new Response("Unexpected provider request", { status: 500 });
      },
    }),
  );

beforeAll(async () => {
  const bundle = await build({
    entryPoints: [join(import.meta.dirname, "fixtures/funding-worker.ts")],
    bundle: true,
    write: false,
    format: "esm",
    target: "es2022",
    platform: "browser",
    conditions: ["workerd", "worker", "browser"],
    external: ["cloudflare:*", "node:*"],
    logLevel: "silent",
  });

  script = bundle.outputFiles[0]!.text;
  directory = await mkdtemp(join(tmpdir(), "travel-funding-"));
  mf = start();
});
afterAll(async () => {
  await mf.dispose();
  await rm(directory, { recursive: true, force: true });
});

const request = (store: string, path: string, input?: object, actor = admin) =>
  mf.dispatchFetch(
    `https://fixture${path}${path.includes("?") ? "&" : "?"}store=${store}&actor=${actor}`,
    {
      method: input ? "POST" : "GET",
      ...(input
        ? { headers: { "content-type": "application/json" }, body: JSON.stringify(input) }
        : {}),
    },
  );

const json = async (store: string, path: string, input?: object, actor = admin) =>
  (await request(store, path, input, actor)).json();

// e79665ba (#812) introduced shared Yielded sign-in without its funding identity.
it("funds the verified Yielded owner and rejects the same subject from another issuer", async () => {
  const owner = "00000000-0000-0000-0000-000000000003";
  const otherIssuer = "00000000-0000-0000-0000-000000000004";

  await json("shared-sign-in", "/seed");
  expect(await json("shared-sign-in", "/status", undefined, owner)).toEqual({
    admin: true,
    allowed: true,
  });
  expect(await json("shared-sign-in", "/resolve", undefined, owner)).toEqual({ lastFour: "9876" });
  expect((await request("shared-sign-in", "/resolve", undefined, otherIssuer)).status).toBe(403);
});

// The current-link and outage transitions require controlled issuer responses at the funding boundary.
it("applies pre-registration GitHub grants through Yielded only while the identity is linked", async () => {
  const reader = "00000000-0000-0000-0000-000000000005";

  await json("preapproved", "/seed");
  await json("preapproved", "/grant", { kind: "github", value: "fixture-reader" });
  await json("preapproved", "/register-yielded");
  expect(await json("preapproved", "/status", undefined, reader)).toEqual({
    admin: false,
    allowed: true,
  });
  expect(await json("preapproved", "/resolve", undefined, reader)).toEqual({ lastFour: "9876" });

  linkedGithubIds = [];
  expect((await request("preapproved", "/resolve", undefined, reader)).status).toBe(403);
  linkedGithubIds = ["424242"];
  identityLookupUnavailable = true;
  expect((await request("preapproved", "/resolve", undefined, reader)).status).toBe(403);
  identityLookupUnavailable = false;
});

it("resolves server credentials only while a current grant applies, including after object restart", async () => {
  await json("credentials", "/seed");
  expect((await request("credentials", "/resolve", undefined, email)).status).toBe(403);
  await json("credentials", "/grant", { kind: "email", value: "reader@gmail.com" });
  expect(await json("credentials", "/resolve", undefined, email)).toEqual({ lastFour: "9876" });
  await mf.dispose();
  mf = start();
  expect(await json("credentials", "/resolve", undefined, email)).toEqual({ lastFour: "9876" });
  await json("credentials", "/revoke", { kind: "email", target: "reader@gmail.com" });
  expect((await request("credentials", "/resolve", undefined, email)).status).toBe(403);
});

it("preserves committed grants and revocations across lost acknowledgements", async () => {
  await json("faults", "/seed");
  expect(
    (await request("faults", "/grant?point=grant:after", { kind: "account", value: email })).status,
  ).toBe(400);
  expect(await json("faults", "/status", undefined, email)).toMatchObject({ allowed: true });
  expect(
    (await request("faults", "/revoke?point=revoke:after", { kind: "account", target: email }))
      .status,
  ).toBe(400);
  expect(await json("faults", "/status", undefined, email)).toMatchObject({ allowed: false });
});
