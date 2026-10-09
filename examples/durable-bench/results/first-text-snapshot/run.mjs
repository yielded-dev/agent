import { spawn, execFileSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import {
  appendFileSync,
  chmodSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  renameSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { NodeRuntime } from "@effect/platform-node";
import { Effect, Schema } from "effect";

import { fingerprint, history, next, payload, turn } from "../../src/plan.ts";
import { responseText } from "./network/text.ts";

// Fixed disposable experiment, adapted from replay-cpu-deployment.ts and the
// effect-eval-cost/real-turn runner. Native APIs form this controller's I/O edge.
const RunError = Schema.TaggedError()("FirstTextRunError", { message: Schema.String });
const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../../../..");
const action = process.argv.slice(2).filter((arg) => arg !== "--")[0] ?? "dry-run";
const account = process.env.CLOUDFLARE_ACCOUNT_ID;
const token = process.env.CLOUDFLARE_API_TOKEN;
const hash = (value) => createHash("sha256").update(value).digest("hex");
const load = (file) => JSON.parse(readFileSync(file, "utf8"));

const save = (file, value) => {
  mkdirSync(dirname(file), { recursive: true });
  writeFileSync(file + ".tmp", clean(JSON.stringify(value, null, 2)) + "\n");
  renameSync(file + ".tmp", file);
};

const sleep = (ms) => new Promise((done) => setTimeout(done, ms));
const pointer = join(here, ".private-path");
let resources;
let privateState;
const privateDirectory = () => readFileSync(pointer, "utf8").trim();
const saveResources = () =>
  writeFileSync(join(privateDirectory(), "resources.json"), JSON.stringify(resources, null, 2));

const clean = (value) => {
  let text = String(value);
  for (const secret of [
    account,
    token,
    privateState?.token,
    privateState?.accountName,
    privateState?.subdomain,
  ])
    if (secret) text = text.replaceAll(secret, "[private]");
  return text
    .replace(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}/g, "[private-email]")
    .replaceAll(/\b[0-9a-f]{32}\b/g, "[private-resource-id]");
};

const api = (route, body) =>
  fetch(`https://api.cloudflare.com/client/v4/accounts/${account}${route ? "/" + route : ""}`, {
    method: body === undefined ? "GET" : "POST",
    headers: {
      authorization: `Bearer ${token}`,
      ...(body === undefined ? {} : { "content-type": "application/json" }),
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(45_000),
  });

const apiJson = async (route, body) => {
  const response = await api(route, body);

  if (!response.ok) {
    appendFileSync(
      join(here, "control-non-ok.jsonl"),
      JSON.stringify({
        at: new Date().toISOString(),
        operation: route.split("/").slice(0, 2).join("/"),
        status: response.status,
      }) + "\n",
    );
    const detail = response.headers.get("content-type")?.includes("json")
      ? JSON.stringify((await response.json()).errors ?? []).slice(0, 2000)
      : `non-JSON gateway response; ray ${response.headers.get("cf-ray") ?? "unavailable"}`;

    throw new Error(clean(`Cloudflare ${route.split("?")[0]}: ${response.status} ${detail}`));
  }
  const result = await response.json();

  if (!result.success) throw new Error(clean(JSON.stringify(result.errors)));

  return result;
};

const listNamespaces = async () => {
  const all = [];

  for (let page = 1; page <= 100; page++) {
    const data = await apiJson(`workers/durable_objects/namespaces?page=${page}&per_page=100`);

    all.push(...data.result);
    if (data.result.length < 100) return all;
  }
  throw new Error("Namespace listing bound exceeded");
};

const prefixedResources = async () => ({
  workers: (await apiJson("workers/scripts")).result
    .filter((item) => item.id.startsWith("first-text"))
    .map(({ id }) => id),
  namespaces: (await listNamespaces())
    .filter((item) => item.script?.startsWith("first-text") || item.name?.startsWith("first-text"))
    .map(({ name, script }) => ({ name, script })),
});

const open = async () => {
  resources = load(join(privateDirectory(), "resources.json"));
  privateState = load(join(privateDirectory(), "private.json"));
  if (privateState.account !== account) throw new Error("Account differs from deployment owner");
};
const init = async () => {
  if (!account || !token) throw new Error("Use direnv exec . with Cloudflare account and token");
  if (existsSync(pointer)) throw new Error("An experiment already owns private state");
  const existing = await prefixedResources();
  if (existing.workers.length || existing.namespaces.length)
    throw new Error("Pre-existing first-text resources; ownership is unknown");
  const directory = mkdtempSync("/private/tmp/first-text-");
  chmodSync(directory, 0o700);
  symlinkSync(join(root, "node_modules"), join(directory, "node_modules"));
  writeFileSync(join(directory, "package.json"), '{"private":true,"type":"module"}\n');
  privateState = {
    token: randomBytes(32).toString("hex"),
    account,
    accountName: (await apiJson("")).result.name,
    subdomain: (await apiJson("workers/subdomain")).result.subdomain,
  };
  writeFileSync(join(directory, "private.json"), JSON.stringify(privateState), { mode: 0o600 });
  writeFileSync(pointer, directory + "\n");
  resources = {
    run: randomBytes(4).toString("hex"),
    privateDirectory: directory,
    startedAt: Date.now(),
    targets: [],
    baselineRevision: execFileSync("vp", ["exec", "git", "rev-parse", "HEAD"], {
      cwd: root,
      encoding: "utf8",
    }).trim(),
  };
  saveResources();
  save(join(here, "cleanup.json"), { complete: false, status: "initialized; cleanup pending" });
  console.log("Initialized private mode-700 Alchemy state; account/token access verified");
};

const alchemy = (target, operation) =>
  new Promise((done, fail) => {
    const child = spawn(
      "vp",
      ["exec", "alchemy", operation, join(here, "stack.ts"), "--stage", target.name, "--yes"],
      {
        cwd: resources.privateDirectory,
        stdio: ["ignore", "pipe", "pipe"],
        env: {
          PATH: process.env.PATH,
          HOME: process.env.HOME,
          CI: "true",
          NO_COLOR: "1",
          ALCHEMY_HOME: join(resources.privateDirectory, "auth"),
          CLOUDFLARE_ACCOUNT_ID: account,
          CLOUDFLARE_API_TOKEN: token,
          FIRST_TEXT_WORKER: target.name,
          FIRST_TEXT_BUNDLE: target.bundle,
          FIRST_TEXT_TOKEN: privateState.token,
          FIRST_TEXT_KIND: target.kind,
          FIRST_TEXT_PHASE: target.phase ?? "seed",
          FIRST_TEXT_PROVIDER: target.provider ?? "",
          FIRST_TEXT_UPLOAD_REVISION: target.uploadRevision ?? "",
        },
      },
    );

    let captured = "";

    child.stdout.on("data", (data) => {
      captured += data.toString();
    });
    child.stderr.on("data", (data) => {
      captured += data.toString();
    });
    child.on("error", fail);
    child.on("close", (code) => {
      appendFileSync(join(privateDirectory(), `${target.name}-${operation}.log`), clean(captured));
      if (code === 0) done();
      else
        fail(
          new Error(`Alchemy ${operation} ${target.name} exited ${code}; inspect sanitized log`),
        );
    });
  });

const deployOne = async (role, buildName, provider = "") => {
  if (resources.targets.some((item) => item.role === role))
    throw new Error(`Role ${role} already recorded; inspect existing deployment`);

  const build = load(join(here, "build-identities/all.json")).findLast(
    (item) => item.name === buildName,
  );

  if (!build) throw new Error(`No build for ${buildName}`);
  const name = `first-text-${resources.run}-${role}`;
  const bundle = join(privateDirectory(), "bundles", build.name, `${build.bundleSha256}.mjs`);

  if (hash(readFileSync(bundle)) !== build.bundleSha256)
    throw new Error("Local bundle digest mismatch");
  if ((await api(`workers/scripts/${name}`)).status !== 404)
    throw new Error("Worker already exists or existence query failed");
  const subdomain = (await apiJson("workers/subdomain")).result.subdomain;

  const target = {
    role,
    kind: buildName,
    name,
    bundle,
    build,
    phase: "seed",
    provider,
    url: `https://${name}.${subdomain}.workers.dev`,
    deployedAt: Date.now(),
    cleanupRequired: true,
    cleanupComplete: false,
  };

  resources.targets.push(target);
  saveResources();
  await alchemy(target, "deploy");
  await verifyUpload(target);
  console.log(`Deployed ${role}: ${build.bundleBytes} bytes; sha256 ${build.bundleSha256}`);

  return target;
};

const verifyUpload = async (target) => {
  const uploaded = await api(`workers/scripts/${target.name}`);

  if (!uploaded.ok || !(await uploaded.text()).includes(readFileSync(target.bundle, "utf8")))
    throw new Error("Uploaded module differs from recorded bytes");
  const settings = (await apiJson(`workers/scripts/${target.name}/settings`)).result;

  const deployment = (await apiJson(`workers/scripts/${target.name}/deployments`)).result
    .deployments[0];

  if (deployment?.versions?.length !== 1 || deployment.versions[0].percentage !== 100)
    throw new Error("Expected one fully active Worker version");
  target.expectedVersion = deployment.versions[0].version_id;
  const version = (
    await apiJson(`workers/scripts/${target.name}/versions/${target.expectedVersion}`)
  ).result;
  if (version.id !== target.expectedVersion)
    throw new Error("Startup metadata does not match the active version");
  target.startupTimeMs =
    typeof version.startup_time_ms === "number" ? version.startup_time_ms : null;
  target.startupEvidence = {
    version: version.id,
    number: version.number,
    startupTimeMs: target.startupTimeMs,
    compatibilityDate: version.compatibility_date,
    limits: version.limits,
    uploadRevision:
      version.bindings?.find(
        (binding) => binding.type === "plain_text" && binding.name === "UPLOAD_REVISION",
      )?.text ?? null,
    capturedAt: Date.now(),
  };
  target.uploadedModuleSha256 = target.build.bundleSha256;
  target.observedUploadRevision = target.startupEvidence.uploadRevision;
  target.settings = {
    placement: settings.placement,
    limits: settings.limits,
    compatibilityDate: settings.compatibility_date,
    compatibilityFlags: settings.compatibility_flags,
    observability: settings.observability,
  };
  target.namespaces = (await listNamespaces())
    .filter((item) => item.script === target.name)
    .map(({ name, script }) => ({ name, script }));
  saveResources();
};

const request = async (target, path, input, phase, object) => {
  const startedAt = Date.now();
  const start = performance.now();
  const url = new URL(path, target.url);

  if (object) url.searchParams.set("object", object);
  const sample = input?.sample ?? url.searchParams.get("sample");

  if (sample) url.searchParams.set("sample", sample);

  const attempt = {
    target: target.role,
    framework: url.searchParams.get("target"),
    worker: target.name,
    path: url.pathname + url.search,
    phase,
    object,
    sample,
    startedAt,
    input,
  };

  appendFileSync(join(privateDirectory(), "attempted.jsonl"), JSON.stringify(attempt) + "\n");
  let response;

  try {
    response = await fetch(url, {
      method: input === undefined ? "GET" : "POST",
      headers: {
        authorization: `Bearer ${privateState.token}`,
        ...(input === undefined ? {} : { "content-type": "application/json" }),
      },
      ...(input === undefined ? {} : { body: JSON.stringify(input) }),
      signal: AbortSignal.timeout(180_000),
    });
    const raw = await response.text();

    const row = {
      ...attempt,
      endedAt: Date.now(),
      clientWallMs: performance.now() - start,
      status: response.status,
      cfRay: response.headers.get("cf-ray"),
      response: (() => {
        try {
          return JSON.parse(raw);
        } catch {
          return { error: raw.slice(0, 2000) };
        }
      })(),
    };

    appendFileSync(join(privateDirectory(), "requests.jsonl"), clean(JSON.stringify(row)) + "\n");
    if (!response.ok)
      throw new Error(`Worker ${target.role} ${url.pathname} returned ${response.status}`);

    return row;
  } catch (cause) {
    if (!response)
      appendFileSync(
        join(privateDirectory(), "requests.jsonl"),
        clean(
          JSON.stringify({
            ...attempt,
            endedAt: Date.now(),
            clientWallMs: performance.now() - start,
            status: null,
            error: String(cause),
            cause: String(cause?.cause),
            code: cause?.cause?.code,
          }),
        ) + "\n",
      );
    throw cause;
  }
};

const readiness = async (target, path, object, accept = () => true) => {
  for (let attempt = 0; attempt < 20; attempt++) {
    try {
      const row = await request(
        target,
        path,
        undefined,
        "readiness",
        typeof object === "function" ? object(attempt) : object,
      );

      if (!accept(row)) throw new Error("Readiness payload has not propagated");

      return row;
    } catch (error) {
      if (attempt === 19) throw error;
      await sleep(2000);
    }
  }
};

const target = (role) => {
  const found = resources.targets.find(
    (item) => item.role === (role === "primary" ? primaryRole : role),
  );

  if (!found) throw new Error(`Missing role: ${role}`);

  return found;
};

const shuffle = (values, seed) => {
  let state = seed | 0;
  const result = [...values];

  for (let i = result.length - 1; i > 0; i--) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    const j = (state >>> 0) % (i + 1);

    [result[i], result[j]] = [result[j], result[i]];
  }

  return result;
};

const telemetry = async (onlyRole, tailOnly = false) => {
  await open();

  const selected = resources.targets.filter(
    (item) => onlyRole === undefined || item.role === onlyRole,
  );

  if (!selected.length) throw new Error("No recorded Worker for telemetry selection");
  for (const item of selected) {
    const existing = join(privateDirectory(), `telemetry-${item.role}.json`);
    const previous = existsSync(existing) ? load(existing) : { events: [], polls: [] };

    const scrub = (event) => {
      const request = event.$workers?.event?.request;

      if (request) {
        delete request.headers;
        if (request.cf)
          request.cf = {
            colo: request.cf.colo,
            clientTcpRtt: request.cf.clientTcpRtt,
            httpProtocol: request.cf.httpProtocol,
          };
      }

      return event;
    };

    const eventIdentity = (event) =>
      event.$metadata?.id
        ? `${event.$workers?.scriptName}/${event.$metadata.id}`
        : JSON.stringify(event);

    const all = new Map(previous.events.map(scrub).map((event) => [eventIdentity(event), event]));

    const queryWindow = async (from, to) => {
      const query = {
        queryId: item.name,
        dry: true,
        view: "events",
        limit: 2000,
        timeframe: { from, to },
        parameters: {
          filterCombination: "and",
          filters: [
            { key: "$workers.scriptName", operation: "eq", type: "string", value: item.name },
          ],
        },
      };

      const data = await apiJson("workers/observability/telemetry/query", query);
      const events = data.result.events;

      if (events.count >= 2000 || events.count > events.events.length) {
        if (to - from <= 1000)
          throw new Error("Telemetry exceeds bounded window; do not silently truncate");
        const middle = Math.floor((from + to) / 2);

        await queryWindow(from, middle);
        await queryWindow(middle, to);

        return;
      }
      const sanitized = JSON.parse(clean(JSON.stringify(events.events))).map(scrub);

      for (const event of sanitized) all.set(eventIdentity(event), event);
      previous.polls.push({
        at: Date.now(),
        from,
        to,
        count: events.count,
        rows: sanitized.length,
        statistics: data.result.statistics,
        sampleIntervals: [
          ...new Set(
            (events.series ?? []).flatMap((row) => row.data.map((value) => value.sampleInterval)),
          ),
        ],
      });
    };

    // Regular collection re-queries the complete run. After deletion, retain
    // the final invocation tail with a minute of overlap for late ingestion.
    const previousUpper = Math.max(0, ...previous.polls.map((poll) => poll.to));

    const from =
      tailOnly && previousUpper
        ? Math.max(item.deployedAt - 60_000, previousUpper - 60_000)
        : item.deployedAt - 60_000;

    await queryWindow(from, Date.now() + 1000);
    save(existing, {
      worker: item.name,
      events: [...all.values()],
      polls: previous.polls,
    });
    console.log(`Telemetry ${item.role}: ${all.size} distinct events`);
  }
};

const expectedSeed = { 50: "b017b487524e44a4", 250: "dcea9f30b0917245" };
const networkUrl = (path, item, sample) =>
  `${path}?${new URLSearchParams({
    target: item.framework,
    history: String(item.history),
    ttftMs: "400",
    chunkDelayMs: "25",
    sample,
    variant: item.framework === "yielded" ? "production" : "inline",
    ...(prototype ? { liveText: "1" } : {}),
    ...(item.afterSequence === undefined ? {} : { afterSequence: String(item.afterSequence) }),
  })}`;
const evidence = (file, row) => appendFileSync(join(here, file), clean(JSON.stringify(row)) + "\n");
const phase = process.argv.slice(3).filter((arg) => arg !== "--")[0] ?? "baseline";
const prototype = phase.includes("prototype");
const primaryRole = phase.includes("native")
  ? "native"
  : phase.includes("plain")
    ? "plain"
    : "candidate";

const refresh = async (generation) => {
  await open();
  let provider = resources.targets.find((item) => item.role === "provider");
  if (!provider) provider = await deployOne("provider", "provider");
  await readiness(provider, "/echo");
  let primary = resources.targets.find((item) => item.role === primaryRole);
  if (!primary) primary = await deployOne(primaryRole, "network", `${provider.url}/v1`);
  else {
    const builds = load(join(here, "build-identities/all.json"));
    const build = phase.startsWith("control")
      ? builds.find(
          (item) =>
            item.bundleSha256 === load(join(here, "baseline-source-proof.json")).bundleSha256,
        )
      : primary.build;
    primary.deployments ??= [];
    primary.deployments.push({
      build: primary.build,
      phase: primary.phase,
      version: primary.expectedVersion,
    });
    primary.build = build;
    primary.bundle = phase.startsWith("control")
      ? join(privateDirectory(), "baseline/worker.mjs")
      : join(privateDirectory(), "bundles", build.name, `${build.bundleSha256}.mjs`);
    primary.phase = generation;
    saveResources();
    await alchemy(primary, "deploy");
    await verifyUpload(primary);
  }
  await readiness(
    primary,
    "/driver",
    undefined,
    (row) =>
      row.response.version === primary.expectedVersion && row.response.generation === generation,
  );
  save(join(here, `deployment-${phase}-${generation}.json`), {
    baselineRevision: resources.baselineRevision,
    bundleSha256: primary.build.bundleSha256,
    providerSha256: provider.build.bundleSha256,
    settings: primary.settings,
    privateStateMode: "0700",
    stackPrefix: "first-text",
    generation,
    at: new Date().toISOString(),
  });
};

const refreshProvider = async () => {
  await open();
  const item = target("provider");
  item.build = load(join(here, "build-identities/all.json")).findLast(
    (build) => build.name === "provider",
  );
  item.bundle = join(privateDirectory(), "bundles", item.build.name, "worker.mjs");
  saveResources();
  await alchemy(item, "deploy");
  await verifyUpload(item);
  await readiness(item, "/echo");
};

const freezeBaseline = async () => {
  await open();
  const primary = resources.targets.find((item) => item.role === "primary");
  if (!primary || primary.phase !== "measure")
    throw new Error("Measured baseline required before freezing");
  const inputs = load(
    join(privateDirectory(), `network-${primary.build.bundleSha256}-inputs.json`),
  );
  const packages = inputs.filter((input) => input.path.startsWith("packages/"));
  for (const input of packages) {
    const baseline = execFileSync(
      "vp",
      ["exec", "git", "show", `${resources.baselineRevision}:${input.path}`],
      { cwd: root },
    );
    if (hash(baseline) !== input.sha256)
      throw new Error(`Baseline framework source changed: ${input.path}`);
  }
  if (hash(readFileSync(primary.bundle)) !== primary.build.bundleSha256)
    throw new Error("Baseline bundle was overwritten");
  const directory = join(privateDirectory(), "baseline");
  mkdirSync(directory, { recursive: true });
  const frozen = join(directory, "worker.mjs");
  if (primary.bundle !== frozen) copyFileSync(primary.bundle, frozen);
  save(join(here, "baseline-source-proof.json"), {
    revision: resources.baselineRevision,
    packageInputsChecked: packages.length,
    allMatch: true,
    bundleSha256: primary.build.bundleSha256,
  });
  save(
    join(here, "build-identities/baseline-sources.json"),
    inputs.filter(
      (input) =>
        input.path.startsWith("packages/") ||
        input.path.startsWith("examples/durable-bench/results/first-text-snapshot/"),
    ),
  );
  console.log(
    `Frozen baseline bundle; ${packages.length} framework sources match the baseline revision`,
  );
};

const reference = async (size, samples) => {
  const messages = [];
  let finalSeed;
  const add = async (input, measured) => {
    messages.push({ role: "user", text: input.text });
    const calls = [];
    while (true) {
      const fingerprintValue = await fingerprint(messages);
      const step = next(messages);
      const text = measured ? responseText(step) : "answer" in step ? step.answer : "";
      if (measured) calls.push({ fingerprint: fingerprintValue, text });
      else finalSeed = fingerprintValue;
      messages.push({ role: "assistant", text, ...("call" in step ? { calls: [step.call] } : {}) });
      if ("answer" in step) break;
      messages.push({ role: "tool", text: payload(step.call) });
    }
    return calls;
  };
  for (const input of history(0, size)) await add(input, false);
  if (finalSeed !== expectedSeed[size]) throw new Error("Seed reference mismatch");
  const turns = {};
  for (let i = 0; i < samples; i++) turns[`m${i}`] = await add(turn(`m${i}`, 8), true);
  return { seedFingerprint: finalSeed, turns };
};

const pool = async (items, concurrency, operation) => {
  let cursor = 0;
  const outcomes = await Promise.allSettled(
    Array.from({ length: concurrency }, async () => {
      while (cursor < items.length) await operation(items[cursor++]);
    }),
  );
  const rejected = outcomes.filter((item) => item.status === "rejected");
  if (rejected.length)
    throw new AggregateError(
      rejected.map((item) => item.reason),
      "One or more experiment lanes failed; receipts retained",
    );
};

const seed = async () => {
  await open();
  if (target("primary").phase !== "seed") throw new Error("Seed deployment required");
  const planFile = join(here, `${phase}-plan.json`);
  if (existsSync(planFile))
    throw new Error("Do not replay a recorded plan; use a new phase after uncertain inputs");
  const pilot = phase.startsWith("pilot");
  const frameworks = prototype || phase.startsWith("control") ? ["yielded"] : ["yielded", "pi"];
  const histories = pilot ? [50] : [50, 250];
  const samples = pilot ? 2 : 5;
  const cohorts = [];
  for (const h of histories)
    for (let o = 0; o < (pilot ? 1 : 3); o++)
      for (const framework of frameworks)
        cohorts.push({
          framework,
          history: h,
          index: o,
          object: `first-text-${resources.run}-${phase}-h${h}-d400-o${o}`,
        });
  const references = {};
  for (const h of histories) references[h] = await reference(h, samples);
  const plan = { phase, samples, cohorts: shuffle(cohorts, 413), references };
  save(planFile, plan);
  const seeds = [];
  await pool(plan.cohorts, 2, async (item) => {
    try {
      let last;
      for (let from = 0; from < item.history; from += 10)
        last = await request(
          target("primary"),
          networkUrl("/seed", item, `seed-${from}`),
          { from, to: Math.min(from + 10, item.history) },
          `${phase}-seed`,
          item.object,
        );
      if (last.response.fingerprint !== expectedSeed[item.history])
        throw new Error("Seed fingerprint mismatch");
      let afterSequence;
      if (item.framework === "yielded")
        afterSequence = (
          await request(
            target("primary"),
            networkUrl("/cursor", item, "cursor"),
            undefined,
            `${phase}-cursor`,
            item.object,
          )
        ).response.afterSequence;
      seeds.push({ ...item, afterSequence, fingerprint: last.response.fingerprint });
      save(join(here, `${phase}-seeds.json`), seeds);
      console.log(
        `Seeded ${seeds.length}/${plan.cohorts.length}: ${item.framework}/${item.history}/o${item.index}`,
      );
    } catch (cause) {
      evidence("non-ok.jsonl", {
        stage: "seed",
        phase,
        ...item,
        error: String(cause),
        noRetry: true,
      });
      throw cause;
    }
  });
};

const measure = async () => {
  await open();
  const primary = target("primary");
  if (primary.phase !== "measure") throw new Error("Measurement deployment required");
  const plan = load(join(here, `${phase}-plan.json`));
  const seeds = load(join(here, `${phase}-seeds.json`));
  const parity =
    prototype || phase.startsWith("control")
      ? load(join(here, "baseline-request-parity.json"))
      : {};
  const attempts = readFileSync(join(privateDirectory(), "attempted.jsonl"), "utf8")
    .trim()
    .split("\n")
    .map(JSON.parse);
  if (attempts.some((row) => row.phase === phase && row.path.startsWith("/run?")))
    throw new Error("Prior measured input exists; do not replay an uncertain turn");
  await pool(plan.cohorts, 2, async (cohort) => {
    const item = { ...cohort };
    const seeded = seeds.find(
      (seed) => seed.object === item.object && seed.framework === item.framework,
    );
    if (!seeded) throw new Error("Missing verified seed");
    item.afterSequence = seeded.afterSequence;
    let incarnation;
    const reset = await request(
      primary,
      networkUrl("/cold", item, "cold"),
      {},
      `${phase}-cold`,
      item.object,
    );
    for (let n = 0; n < plan.samples; n++) {
      const sample = `m${n}`;
      try {
        const row = await request(
          primary,
          networkUrl("/run", item, sample),
          {},
          phase,
          item.object,
        );
        const r = row.response;
        const m = r.metrics;
        const expected = plan.references[item.history].turns[sample];
        if (
          !r.ok ||
          r.driverVersion !== primary.expectedVersion ||
          m.version !== primary.expectedVersion ||
          m.seedFingerprint !== expectedSeed[item.history]
        )
          throw new Error("Deployment or seed identity mismatch");
        if (m.calls.length !== 9 || r.observations.length !== 9)
          throw new Error(
            `Expected nine model calls and visible responses; got ${m.calls.length}/${r.observations.length}`,
          );
        for (let c = 0; c < 9; c++) {
          const call = m.calls[c],
            visible = r.observations[c];
          if (
            call.status !== 200 ||
            call.error ||
            !call.sseDone ||
            call.providerReceipt?.fingerprint !== expected[c].fingerprint
          )
            throw new Error(
              `Provider/transcript mismatch at call ${c}: ${call.providerReceipt?.fingerprint} expected ${expected[c].fingerprint}`,
            );
          if (
            visible.text !== expected[c].text ||
            visible.completeMs === undefined ||
            visible.finalizedMs === undefined
          )
            throw new Error(`Visible text incomplete or different at call ${c}`);
          if (
            prototype &&
            (visible.source !== "provisional" ||
              visible.discarded ||
              visible.canonicalMs === undefined)
          )
            throw new Error(`Missing provisional-to-canonical reconciliation at call ${c}`);
          if (item.framework === "yielded" && call.invocation?.kind !== "alarm")
            throw new Error("Model ran outside native alarm");
          const parityKey = `${item.history}/${sample}/${c}`;
          const fingerprint = call.providerReceipt.modelVisibleFingerprint;
          if (!fingerprint)
            throw new Error(`Missing model-visible request fingerprint: ${parityKey}`);
          {
            const previous = parity[parityKey];
            if ((prototype || phase.startsWith("control")) && !previous)
              throw new Error(`Missing baseline request fingerprint: ${parityKey}`);
            if (previous && previous.fingerprint !== fingerprint) {
              evidence("request-parity-failures.jsonl", {
                phase,
                key: parityKey,
                previous,
                current: { framework: item.framework, ...call.providerReceipt },
              });
              throw new Error(`Model-visible request parity failed: ${parityKey}`);
            }
            parity[parityKey] = {
              fingerprint,
              framework: item.framework,
              framing: call.providerReceipt.framing,
              tools: call.providerReceipt.tools,
              messageShape: call.providerReceipt.messageShape,
              messageTail: call.providerReceipt.messageTail,
            };
          }
        }
        save(
          join(here, `${phase}-request-parity.json`),
          Object.fromEntries(
            Object.entries(parity).map(([key, value]) => [
              key,
              { fingerprint: value.fingerprint, framework: value.framework },
            ]),
          ),
        );
        if (n === 0) {
          if (
            !reset.response.ok ||
            m.incarnation === reset.response.before.incarnation ||
            m.objectId !== reset.response.before.objectId ||
            !m.entry.firstHarnessRequest ||
            m.entry.priorAlarmStarts !== 0
          )
            throw new Error("Cold reconstruction proof failed");
        } else if (m.incarnation !== incarnation)
          throw new Error("Warm Object incarnation changed");
        incarnation = m.incarnation;
        item.afterSequence = r.afterSequence;
        evidence("turns.jsonl", {
          phase,
          framework: item.framework,
          history: item.history,
          objectIndex: item.index,
          sample,
          state: n === 0 ? "cold" : n === 1 ? "settling" : "warm",
          bundleSha256: primary.build.bundleSha256,
          providerSha256: target("provider").build.bundleSha256,
          firstVisibleMs: r.firstVisibleMs,
          turnMs: r.turnMs,
          submitStartedMs: r.submitStartedMs,
          settlementObservedMs: r.settlementObservedMs,
          admissionMs:
            r.receiptReceivedMs === undefined ? null : r.receiptReceivedMs - r.submitStartedMs,
          observationSetupMs:
            r.observationReadyMs === undefined ? null : r.observationReadyMs - r.submitStartedMs,
          coldVerified: n === 0,
          warmVerified: n > 0,
          transcriptVerified: true,
          productionAlarmVerified: item.framework === "yielded",
          reconstruction: {
            objectDigest: hash(m.objectId),
            incarnationDigest: hash(m.incarnation),
            coldPredecessorDigest: hash(reset.response.before.incarnation),
            versionDigest: hash(m.version),
            driverVersionDigest: hash(r.driverVersion),
            entry: m.entry,
            constructedMs: m.constructedMs,
          },
          clocks: r.clocks,
          calls: m.calls.map((call, c) => ({
            call: c,
            provider: {
              ...call.providerReceipt,
              requestId: undefined,
              requestIdSha256: hash(call.providerReceipt.requestId),
              framing: undefined,
              tools: undefined,
              messageShape: undefined,
              messageTail: undefined,
            },
            visible: {
              ...r.observations[c],
              runId: undefined,
              runIdSha256:
                r.observations[c].runId === undefined ? undefined : hash(r.observations[c].runId),
              turnId: undefined,
              turnIdSha256:
                r.observations[c].turnId === undefined ? undefined : hash(r.observations[c].turnId),
              attemptId: undefined,
              attemptIdSha256:
                r.observations[c].attemptId === undefined
                  ? undefined
                  : hash(r.observations[c].attemptId),
              text: undefined,
              textSha256: hash(r.observations[c].text),
            },
            objectReceipts: {
              fetchStartedMs: call.fetchStartedMs,
              headersMs: call.headersMs,
              firstByteMs: call.firstByteMs,
              endMs: call.endMs,
              invocationKind: call.invocation?.kind,
            },
            nativeStreamComplete: call.sseDone,
          })),
        });
        console.log(
          `${phase} ${item.framework}/${item.history}/o${item.index} ${sample}: first ${r.firstVisibleMs} ms; settled ${r.turnMs} ms`,
        );
      } catch (cause) {
        evidence("non-ok.jsonl", {
          stage: "measure",
          phase,
          ...item,
          sample,
          error: String(cause),
          noRetry: true,
        });
        throw cause;
      }
    }
  });
};

const observerNoise = async () => {
  await open();
  if (!prototype || target("primary").phase !== "measure")
    throw new Error("Live prototype required");
  const attempted = readFileSync(join(privateDirectory(), "attempted.jsonl"), "utf8")
    .trim()
    .split("\n")
    .filter(Boolean)
    .map(JSON.parse);
  if (attempted.some((row) => row.phase === phase))
    throw new Error("Do not replay an attempted noise phase");
  const prior = readFileSync(join(privateDirectory(), "requests.jsonl"), "utf8")
    .trim()
    .split("\n")
    .filter(Boolean)
    .map(JSON.parse);
  const plan = load(join(here, "snapshot-prototype-plan.json"));
  const referenceTurns = (await reference(250, 10)).turns;
  const parity = {};
  const cohorts = plan.cohorts.filter((item) => item.history === 250);
  await pool(cohorts, 3, async (cohort) => {
    const previous = prior.findLast(
      (row) => row.phase === "snapshot-prototype" && row.object === cohort.object && row.sample === "m4",
    );
    if (!previous?.response?.ok) throw new Error("Missing completed prototype predecessor");
    const item = { ...cohort, afterSequence: previous.response.afterSequence };
    let incarnation;
    for (let n = 5; n < 10; n++) {
      const live = n === 5 || (n + item.index) % 2 === 0;
      const sample = `m${n}`;
      const path = new URL(networkUrl("/run", item, sample), "https://first-text");
      if (!live) path.searchParams.delete("liveText");
      try {
        const row = await request(
          target("primary"),
          path.pathname + path.search,
          {},
          phase,
          item.object,
        );
        const r = row.response,
          m = r.metrics;
        if (
          !r.ok ||
          r.driverVersion !== target("primary").expectedVersion ||
          m.version !== r.driverVersion ||
          r.outcome !== "completed" ||
          m.calls.length !== 9 ||
          r.observations.length !== 9
        )
          throw new Error("Noise turn/version/call-count mismatch");
        for (let c = 0; c < 9; c++) {
          const call = m.calls[c],
            visible = r.observations[c],
            expected = referenceTurns[sample][c];
          if (
            call.status !== 200 ||
            call.error ||
            !call.sseDone ||
            call.invocation?.kind !== "alarm" ||
            call.providerReceipt?.fingerprint !== expected.fingerprint ||
            visible.text !== expected.text
          )
            throw new Error(`Noise transcript or provider failure: ${sample}/${c}`);
          if (
            live &&
            (visible.source !== "provisional" ||
              visible.discarded ||
              visible.canonicalMs === undefined)
          )
            throw new Error("Missing noise-turn reconciliation");
          const key = `${sample}/${c}`,
            fingerprint = call.providerReceipt.modelVisibleFingerprint;
          if (!fingerprint || (parity[key] && parity[key] !== fingerprint))
            throw new Error("Noise exact-request parity failed across observation modes");
          parity[key] = fingerprint;
        }
        if (n > 5 && incarnation !== m.incarnation)
          throw new Error("Noise Object incarnation changed");
        incarnation = m.incarnation;
        item.afterSequence = r.afterSequence;
        evidence("observer-noise.jsonl", {
          phase,
          history: 250,
          objectIndex: item.index,
          sample,
          warmup: n === 5,
          live,
          bundleSha256: target("primary").build.bundleSha256,
          providerSha256: target("provider").build.bundleSha256,
          objectDigest: hash(m.objectId),
          incarnationDigest: hash(m.incarnation),
          firstVisibleMs: r.firstVisibleMs,
          turnMs: r.turnMs,
          observationSetupMs:
            r.observationReadyMs === undefined ? 0 : r.observationReadyMs - r.submitStartedMs,
          submitStartedMs: r.submitStartedMs,
          settlementObservedMs: r.settlementObservedMs,
          afterSequence: r.afterSequence,
          clocks: r.clocks,
          calls: m.calls.map((call, c) => ({
            call: c,
            requestSha256: call.providerReceipt.modelVisibleFingerprint,
            providerArrivalMs: call.providerReceipt.arrivalMs,
            providerEndMs: call.providerReceipt.endMs,
            providerColo: call.providerReceipt.colo,
            fetchStartedMs: call.fetchStartedMs,
            headersMs: call.headersMs,
            endMs: call.endMs,
            firstTextMs: r.observations[c].firstTextMs,
            canonicalMs: r.observations[c].canonicalMs,
            textSha256: hash(r.observations[c].text),
            source: r.observations[c].source,
          })),
        });
        console.log(
          `Observer noise o${item.index} ${sample} ${live ? "on" : "off"}: first ${r.firstVisibleMs} ms; settled ${r.turnMs} ms`,
        );
      } catch (cause) {
        evidence("non-ok.jsonl", {
          stage: "observer-noise",
          phase,
          objectIndex: item.index,
          sample,
          error: String(cause),
          noRetry: true,
        });
        throw cause;
      }
    }
  });
  save(join(here, "observer-noise-parity.json"), parity);
};

const summarizeNonOk = async () => {
  const events = [];
  for (const item of resources.targets) {
    const file = join(privateDirectory(), `telemetry-${item.role}.json`);
    if (!existsSync(file)) continue;
    for (const event of load(file).events) {
      const outcome = event.$workers?.outcome;
      if (outcome && outcome !== "ok")
        events.push({
          worker: item.role,
          outcome,
          type: event.$workers?.eventType,
          entrypoint: event.$workers?.entrypoint,
          cpuTimeMs: event.$workers?.cpuTimeMs,
          wallTimeMs: event.$workers?.wallTimeMs,
          at: event.$metadata?.timestamp ?? event.timestamp,
        });
    }
  }
  const requests = readFileSync(join(privateDirectory(), "requests.jsonl"), "utf8")
    .trim()
    .split("\n")
    .filter(Boolean)
    .map(JSON.parse);
  const failedRequests = requests
    .filter((row) => row.status !== 200)
    .map((row) => ({
      phase: row.phase,
      path: row.path.split("?")[0],
      status: row.status,
      startedAt: row.startedAt,
      endedAt: row.endedAt,
      error:
        row.status === 404 && row.phase === "readiness"
          ? "workers.dev route not yet available during readiness polling"
          : clean(String(row.response?.error ?? row.error)).slice(0, 300),
    }));
  const counts = {};
  for (const event of events) {
    const key = [event.worker, event.outcome, event.type, event.entrypoint ?? "Worker"].join("/");
    counts[key] = (counts[key] ?? 0) + 1;
  }
  save(join(here, "platform-non-ok.json"), {
    counts,
    events,
    failedRequests,
    scope: "All captured non-ok outcomes; Cloudflare invocation telemetry may be sampled",
  });
};

const prove = async () => {
  await open();
  if (!prototype || target("primary").phase !== "measure")
    throw new Error("Deployed prototype measurement generation required");
  const primary = target("primary");
  const attempts = readFileSync(join(privateDirectory(), "attempted.jsonl"), "utf8")
    .trim()
    .split("\n")
    .filter(Boolean)
    .map(JSON.parse);
  if (attempts.some((row) => row.phase === phase))
    throw new Error("Proof phase was already attempted; do not replay uncertain inputs");
  for (const mode of ["denied", "abort", "cancel", "wide", "slow", "late"]) {
    const object = `first-text-${resources.run}-${phase}-proof-${mode}-h50-d400-o0`;
    const item = { framework: "yielded", history: 50 };
    const sample = mode === "wide" || mode === "slow" ? `proof-wide-${mode}` : `proof-${mode}`;
    try {
      const row = await request(
        primary,
        networkUrl("/proof", item, sample) + `&mode=${mode}`,
        {},
        phase,
        object,
      );
      if (row.response.ok !== true || row.response.mode !== mode)
        throw new Error(`Proof did not pass: ${mode}`);
      evidence("proofs.jsonl", {
        phase,
        mode,
        bundleSha256: primary.build.bundleSha256,
        providerSha256: target("provider").build.bundleSha256,
        result: row.response,
      });
      console.log(`Deployed proof passed: ${mode}`);
    } catch (cause) {
      evidence("non-ok.jsonl", {
        stage: "proof",
        phase,
        mode,
        error: String(cause),
        noRetry: true,
      });
      throw cause;
    }
  }
};

const compatibility = async () => {
  await open();
  const object = `first-text-${resources.run}-${phase}-probe-h50-d400-o0`;
  try {
    const row = await request(
      target("primary"),
      networkUrl("/preview-probe", { framework: "yielded", history: 50 }, "compatibility"),
      {},
      `${phase}-compatibility`,
      object,
    );
    if (!row.response.ok || row.response.frame !== "Reset") throw new Error("Missing Reset");
    evidence("compatibility.jsonl", { phase, compatible: true, noSubmission: true });
    console.log(`${phase}: public watchText returned Reset and cancelled successfully`);
  } catch (cause) {
    evidence("compatibility.jsonl", {
      phase,
      compatible: false,
      noSubmission: true,
      error: String(cause),
    });
    evidence("non-ok.jsonl", {
      stage: "compatibility",
      phase,
      error: String(cause),
      noRetry: true,
    });
    console.log(`${phase}: incompatible; failure retained without submitting a turn`);
  }
};

const cleanup = async () => {
  await open();
  const telemetryFailures = [];
  try {
    await telemetry();
  } catch (cause) {
    telemetryFailures.push(clean(String(cause)));
  }
  const checks = [];
  for (const item of [...resources.targets].reverse()) {
    if (!item.cleanupComplete) await alchemy(item, "destroy");
    const status = (await api(`workers/scripts/${item.name}`)).status;
    const remaining = (await listNamespaces()).filter((ns) => ns.script === item.name);
    checks.push({ worker: item.name, status, remainingNamespaces: remaining.length });
    item.cleanupComplete = status === 404 && remaining.length === 0;
    saveResources();
    if (!item.cleanupComplete) throw new Error("Deletion not verified");
  }
  const remaining = await prefixedResources();
  if (remaining.workers.length || remaining.namespaces.length)
    throw new Error("first-text prefix resources remain");
  await sleep(15000);
  try {
    await telemetry(undefined, true);
  } catch (cause) {
    telemetryFailures.push(clean(String(cause)));
  }
  await summarizeNonOk();
  // Preserve replay provenance and exact credential scan, never credential values.
  const files = execFileSync("vp", ["exec", "rg", "--files", "--hidden", here], {
    encoding: "utf8",
  })
    .trim()
    .split("\n")
    .filter((file) => !file.endsWith(".private-path"));
  const secrets = [
    account,
    token,
    privateState.token,
    privateState.accountName,
    privateState.subdomain,
  ].filter(Boolean);
  const matches = files.filter((file) =>
    secrets.some((secret) => readFileSync(file).includes(Buffer.from(secret))),
  );
  if (matches.length)
    throw new Error(`Private value scan failed for ${matches.length} evidence files`);
  rmSync(privateDirectory(), { recursive: true });
  rmSync(pointer);
  save(join(here, "cleanup.json"), {
    complete: true,
    checkedAt: new Date().toISOString(),
    checks,
    remaining,
    privateStateRemoved: true,
    privateValueScanPassed: true,
    filesScanned: files.length,
    telemetryTailCollected: telemetryFailures.length === 0,
    telemetryFailures,
  });
  console.log(
    "Cleanup verified: no first-text Workers or Durable Object namespaces; private state removed",
  );
};

export const run = Effect.tryPromise({
  try: async () => {
    if (action === "init") return init();
    if (action === "deploy-seed") return refresh("seed");
    if (action === "deploy-provider") return refreshProvider();
    if (action === "freeze-baseline") return freezeBaseline();
    if (action === "deploy-measure") return refresh("measure");
    if (action === "seed") return seed();
    if (action === "measure") return measure();
    if (action === "observer-noise") return observerNoise();
    if (action === "telemetry") return telemetry();
    if (action === "prove") return prove();
    if (action === "compatibility") return compatibility();
    if (action === "cleanup") return cleanup();
    throw new Error(
      "Usage: init | deploy-seed PHASE | seed PHASE | deploy-measure PHASE | measure PHASE | freeze-baseline | observer-noise PHASE | prove PHASE | telemetry | cleanup",
    );
  },
  catch: (cause) => new RunError({ message: clean(String(cause)) }),
});
if (import.meta.url === `file://${process.argv[1]}`) NodeRuntime.runMain(run);
