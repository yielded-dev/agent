import { createHash, randomBytes } from "node:crypto";
import { appendFileSync, chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, renameSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import { gunzipSync } from "node:zlib";
import { Effect, Schema } from "effect";
import { NodeRuntime } from "@effect/platform-node";

// Fixed disposable experiment, adapted from replay-cpu-deployment.ts and the
// effect-eval-cost/real-turn runner. Native APIs form this controller's I/O edge.
const RunError = Schema.TaggedError()("CfLatencyRunError", { message: Schema.String });
const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../../../..");
const action = process.argv.slice(2).filter((arg) => arg !== "--")[0] ?? "dry-run";
const account = process.env.CLOUDFLARE_ACCOUNT_ID;
const token = process.env.CLOUDFLARE_API_TOKEN;
const hash = (value) => createHash("sha256").update(value).digest("hex");
const load = (file) => JSON.parse(readFileSync(file, "utf8"));
const save = (file, value) => { mkdirSync(dirname(file), { recursive: true }); writeFileSync(file + ".tmp", clean(JSON.stringify(value, null, 2)) + "\n"); renameSync(file + ".tmp", file); };
const sleep = (ms) => new Promise((done) => setTimeout(done, ms));
let resources;
let privateState;
const clean = (value) => {
  let text = String(value);
  for (const secret of [account, token, privateState?.token]) if (secret) text = text.replaceAll(secret, "[redacted]");
  return text;
};
const api = (route, body) => fetch(`https://api.cloudflare.com/client/v4/accounts/${account}${route ? "/" + route : ""}`, {
  method: body === undefined ? "GET" : "POST",
  headers: { authorization: `Bearer ${token}`, ...(body === undefined ? {} : { "content-type": "application/json" }) },
  ...(body === undefined ? {} : { body: JSON.stringify(body) }), signal: AbortSignal.timeout(45_000),
});
const apiJson = async (route, body) => {
  const response = await api(route, body);
  if (!response.ok) throw new Error(clean(`Cloudflare ${route.split("?")[0]}: ${response.status} ${await response.text()}`));
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
  workers: (await apiJson("workers/scripts")).result.filter((item) => item.id.startsWith("cf-latency")).map(({ id }) => id),
  namespaces: (await listNamespaces()).filter((item) => item.script?.startsWith("cf-latency") || item.name?.startsWith("cf-latency")).map(({ name, script }) => ({ name, script })),
});
const open = async () => {
  resources = load(join(here, "resources.json"));
  if (resources.accountDigest !== hash(account)) throw new Error("Account differs from recorded deployment ownership");
  const name = (await apiJson("")).result.name;
  if (name !== resources.accountName) throw new Error("Cloudflare account name differs from deployment record");
  privateState = load(join(resources.privateDirectory, "private.json"));
  if (privateState.account !== account) throw new Error("Private account ownership mismatch");
};
const init = async () => {
  if (existsSync(join(here, "resources.json"))) throw new Error("Resources already recorded; never initialize over an existing run");
  const accountName = (await apiJson("")).result.name;
  const existing = await prefixedResources();
  if (existing.workers.length || existing.namespaces.length) throw new Error("Pre-existing cf-latency resources found; do not assume ownership");
  const privateDirectory = mkdtempSync("/private/tmp/cf-latency-");
  chmodSync(privateDirectory, 0o700);
  symlinkSync(join(root, "node_modules"), join(privateDirectory, "node_modules"));
  writeFileSync(join(privateDirectory, "package.json"), '{"private":true,"type":"module"}\n');
  privateState = { token: randomBytes(32).toString("hex"), account };
  writeFileSync(join(privateDirectory, "private.json"), JSON.stringify(privateState), { mode: 0o600 });
  resources = { run: randomBytes(4).toString("hex"), accountName, accountDigest: hash(account), privateDirectory, startedAt: Date.now(), targets: [] };
  save(join(here, "resources.json"), resources);
  save(join(here, "cleanup.json"), { complete: false, accountName, status: "initialized; cleanup verification pending" });
  console.log(JSON.stringify({ accountName, privateDirectory, existing }));
};
const alchemy = (target, operation) => new Promise((done, fail) => {
  const child = spawn("vp", ["exec", "alchemy", operation, join(here, "stack.ts"), "--stage", target.name, "--yes"], {
    cwd: resources.privateDirectory, stdio: ["ignore", "pipe", "pipe"],
    env: { PATH: process.env.PATH, HOME: process.env.HOME, CI: "true", NO_COLOR: "1",
      ALCHEMY_HOME: join(resources.privateDirectory, "auth"), CLOUDFLARE_ACCOUNT_ID: account, CLOUDFLARE_API_TOKEN: token,
      CF_LATENCY_WORKER: target.name, CF_LATENCY_BUNDLE: target.bundle, CF_LATENCY_TOKEN: privateState.token,
      CF_LATENCY_KIND: target.kind, CF_LATENCY_PHASE: target.phase ?? "seed", CF_LATENCY_PROVIDER: target.provider ?? "",
      CF_LATENCY_UPLOAD_REVISION: target.uploadRevision ?? "" },
  });
  let captured = "";
  child.stdout.on("data", (data) => { captured += data.toString(); });
  child.stderr.on("data", (data) => { captured += data.toString(); });
  child.on("error", fail);
  child.on("close", (code) => {
    mkdirSync(join(here, "alchemy"), { recursive: true });
    appendFileSync(join(here, "alchemy", `${target.name}-${operation}.log`), clean(captured));
    if (code === 0) done(); else fail(new Error(`Alchemy ${operation} ${target.name} exited ${code}; inspect sanitized log`));
  });
});
const deployOne = async (role, buildName, provider = "") => {
  if (resources.targets.some((item) => item.role === role)) throw new Error(`Role ${role} already recorded; inspect existing deployment`);
  const build = load(join(here, "build-identities/all.json")).find((item) => item.name === buildName);
  if (!build) throw new Error(`No build for ${buildName}`);
  const name = `cf-latency-${resources.run}-${role}`;
  const bundle = join(build.output, "worker.mjs");
  if (hash(readFileSync(bundle)) !== build.bundleSha256) throw new Error("Local bundle digest mismatch");
  if ((await api(`workers/scripts/${name}`)).status !== 404) throw new Error("Worker already exists or existence query failed");
  const subdomain = (await apiJson("workers/subdomain")).result.subdomain;
  const target = { role, kind: buildName, name, bundle, build, phase: "seed", provider,
    url: `https://${name}.${subdomain}.workers.dev`, deployedAt: Date.now(), cleanupRequired: true, cleanupComplete: false };
  resources.targets.push(target);
  save(join(here, "resources.json"), resources);
  await alchemy(target, "deploy");
  await verifyUpload(target);
  console.log(`Deployed ${role}: ${build.bundleBytes} bytes; sha256 ${build.bundleSha256}`);
  return target;
};
const verifyUpload = async (target) => {
  const uploaded = await api(`workers/scripts/${target.name}`);
  if (!uploaded.ok || !(await uploaded.text()).includes(readFileSync(target.bundle, "utf8"))) throw new Error("Uploaded module differs from recorded bytes");
  const settings = (await apiJson(`workers/scripts/${target.name}/settings`)).result;
  const deployment = (await apiJson(`workers/scripts/${target.name}/deployments`)).result.deployments[0];
  if (deployment?.versions?.length !== 1 || deployment.versions[0].percentage !== 100)
    throw new Error("Expected one fully active Worker version");
  target.expectedVersion = deployment.versions[0].version_id;
  const script = (await apiJson("workers/scripts")).result.find((item) => item.id === target.name);
  if (!script?.tag) throw new Error("Deployed Worker metadata is missing its version lookup identity");
  const version = (await apiJson(`workers/workers/${script.tag}/versions/${target.expectedVersion}`)).result;
  if (version.id !== target.expectedVersion) throw new Error("Startup metadata does not match the active version");
  target.startupTimeMs = typeof version.startup_time_ms === "number" ? version.startup_time_ms : null;
  target.startupEvidence = { version: version.id, number: version.number, startupTimeMs: target.startupTimeMs,
    compatibilityDate: version.compatibility_date, limits: version.limits, capturedAt: Date.now() };
  target.uploadedModuleSha256 = target.build.bundleSha256;
  target.settings = { limits: settings.limits, compatibilityDate: settings.compatibility_date, compatibilityFlags: settings.compatibility_flags, observability: settings.observability };
  target.namespaces = (await listNamespaces()).filter((item) => item.script === target.name).map(({ name, script }) => ({ name, script }));
  save(join(here, "resources.json"), resources);
};
const request = async (target, path, input, phase, object) => {
  const startedAt = Date.now();
  const start = performance.now();
  const url = new URL(path, target.url);
  if (object) url.searchParams.set("object", object);
  const sample = input?.sample ?? url.searchParams.get("sample");
  if (sample) url.searchParams.set("sample", sample);
  const attempt = { target: target.role, framework: url.searchParams.get("target"), worker: target.name, path: url.pathname + url.search, phase, object, sample, startedAt, input };
  appendFileSync(join(here, "attempted.jsonl"), JSON.stringify(attempt) + "\n");
  let response;
  try {
    response = await fetch(url, { method: input === undefined ? "GET" : "POST",
      headers: { authorization: `Bearer ${privateState.token}`, ...(input === undefined ? {} : { "content-type": "application/json" }) },
      ...(input === undefined ? {} : { body: JSON.stringify(input) }), signal: AbortSignal.timeout(180_000) });
    const raw = await response.text();
    const row = { ...attempt, endedAt: Date.now(), clientWallMs: performance.now() - start, status: response.status,
      cfRay: response.headers.get("cf-ray"), response: (() => { try { return JSON.parse(raw); } catch { return { error: raw.slice(0, 2000) }; } })() };
    appendFileSync(join(here, "requests.jsonl"), clean(JSON.stringify(row)) + "\n");
    if (!response.ok) throw new Error(`Worker ${target.role} ${url.pathname} returned ${response.status}`);
    return row;
  } catch (cause) {
    if (!response) appendFileSync(join(here, "requests.jsonl"), clean(JSON.stringify({ ...attempt, endedAt: Date.now(), clientWallMs: performance.now() - start, status: null, error: String(cause), cause: String(cause?.cause), code: cause?.cause?.code })) + "\n");
    throw cause;
  }
};
const readiness = async (target, path, object, accept = () => true) => {
  for (let attempt = 0; attempt < 20; attempt++) {
    try {
      const row = await request(target, path, undefined, "readiness", typeof object === "function" ? object(attempt) : object);
      if (!accept(row)) throw new Error("Readiness payload has not propagated");
      return row;
    }
    catch (error) { if (attempt === 19) throw error; await sleep(2000); }
  }
};
const target = (role) => {
  const found = resources.targets.find((item) => item.role === role);
  if (!found) throw new Error(`Missing role: ${role}`);
  return found;
};
const shuffle = (values, seed) => {
  let state = seed | 0;
  const result = [...values];
  for (let i = result.length - 1; i > 0; i--) {
    state ^= state << 13; state ^= state >>> 17; state ^= state << 5;
    const j = (state >>> 0) % (i + 1);
    [result[i], result[j]] = [result[j], result[i]];
  }
  return result;
};
const deployProbe = async () => {
  await open();
  const echo = await deployOne("echo", "echo");
  await readiness(echo, "/echo");
  const probe = await deployOne("probe", "probe", echo.url);
  await readiness(probe, "/prepare", "cf-latency-readiness");
};
const calibrate = async (pilot) => {
  await open();
  const probe = target("probe");
  const phase = pilot ? `probe-pilot${pilot === true ? "" : pilot}` : "calibration";
  const planFile = join(here, `${phase}-plan.json`);
  if (existsSync(planFile)) throw new Error("Do not repeat a recorded calibration plan");
  const sizes = pilot ? [0, 16384, 131072] : [0, 1024, 16384, 65536, 131072, 524288];
  const transactions = pilot ? [1, 12] : [1, 4, 12];
  const modes = ["return", "fetch", "sync-end", "sync-each"];
  const cases = sizes.flatMap((bytes) => transactions.flatMap((transactions) => modes.map((mode) => ({ bytes, transactions, mode }))));
  const objects = pilot ? 2 : 8;
  const repetitions = pilot ? 1 : 5;
  const schedule = [];
  for (let object = 0; object < objects; object++) for (let repeat = 0; repeat < repetitions; repeat++) {
    for (const spec of shuffle(cases, 319 + object * 41 + repeat * 109)) {
      schedule.push({ object: `cf-latency-${phase}-${object}`, repeat, input: { ...spec, sample: `${phase}-o${object}-r${repeat}-b${spec.bytes}-t${spec.transactions}-${spec.mode}` } });
    }
  }
  save(planFile, { objects, repetitions, locationHint: "wnam", bytesMeaning: "total blob payload bytes per request, divided across transactionSync calls", schedule });
  for (let object = 0; object < objects; object++) {
    const name = `cf-latency-${phase}-${object}`;
    await request(probe, "/prepare", undefined, "prepare", name);
    for (let i = 0; i < 4; i++) await request(probe, "/probe", { sample: `${phase}-warm-${object}-${i}`, bytes: 16384, transactions: 1, mode: "sync-end" }, "probe-warmup", name);
    for (const item of schedule.filter((item) => item.object === name)) await request(probe, "/probe", item.input, phase, name);
    console.log(`${phase}: object ${object + 1}/${objects} complete`);
  }
};
const alarms = async (suffix = "") => {
  await open();
  const probe = target("probe");
  const phase = `alarm${suffix}`;
  if (existsSync(join(here, `${phase}-plan.json`))) throw new Error("Alarm plan already recorded");
  const cases = [0, 20_000_000, 80_000_000].flatMap((iterations) => [0, 110, 230].map((afterArmMs) => ({ iterations, afterArmMs })));
  save(join(here, `${phase}-plan.json`), { objects: 8, repetitions: 4, leadMs: 200, cases });
  for (let object = 0; object < 8; object++) {
    const name = `cf-latency-${phase}-${object}`;
    await request(probe, "/prepare", undefined, "prepare", name);
    for (let repeat = 0; repeat < 4; repeat++) for (const spec of shuffle(cases, 679 + object * 71 + repeat * 11)) {
      const sample = `${phase}-o${object}-r${repeat}-n${spec.iterations}-d${spec.afterArmMs}`;
      await request(probe, `/arm?sample=${sample}&leadMs=200&iterations=${spec.iterations}`, undefined, `${phase}-arm`, name);
      if (spec.afterArmMs) await sleep(spec.afterArmMs);
      await request(probe, "/probe", { sample, bytes: 0, transactions: 1, mode: "return" }, `${phase}-probe`, name);
      await sleep(600);
    }
    console.log(`Alarm interference: object ${object + 1}/8 complete`);
  }
};
const networkUrl = (path, cohort, sample, extra = {}) => {
  const query = new URLSearchParams({ target: cohort.framework, history: String(cohort.history),
    ttftMs: String(cohort.ttftMs), chunkDelayMs: String(cohort.ttftMs === 0 ? 0 : 10), sample, ...extra });
  return `${path}?${query}`;
};
const transport = async () => {
  await open();
  const probe = target("probe");
  if (existsSync(join(here, "transport-plan.json"))) throw new Error("Transport comparison already recorded");
  const cases = [0, 131072].flatMap((bytes) => ["rpc", "fetch"].map((transport) => ({ bytes, transport })));
  save(join(here, "transport-plan.json"), { objects: 8, repetitions: 5, cases, purpose: "same-Object native fetch versus RPC invocation-span calibration" });
  for (let object = 0; object < 8; object++) {
    const name = `cf-latency-transport-${object}`;
    await request(probe, "/prepare", undefined, "prepare", name);
    for (const transport of ["rpc", "fetch", "rpc", "fetch"]) await request(probe, transport === "rpc" ? "/probe" : "/fetch-probe", { sample: `transport-warm-${object}-${transport}`, bytes: 0, transactions: 1, mode: "return" }, "transport-warmup", name);
    for (let repeat = 0; repeat < 5; repeat++) for (const spec of shuffle(cases, 937 + object * 71 + repeat * 31)) {
      const sample = `transport-o${object}-r${repeat}-b${spec.bytes}-${spec.transport}`;
      await request(probe, spec.transport === "rpc" ? "/probe" : "/fetch-probe", { sample, bytes: spec.bytes, transactions: 1, mode: "return" }, "transport", name);
    }
    console.log(`Transport comparison: object ${object + 1}/8 complete`);
  }
};
const receipt = (row) => row.response.thread ?? row.response;
const expectedFingerprint = { 50: "b017b487524e44a4", 250: "dcea9f30b0917245" };
const networkPlan = () => {
  const cohorts = [];
  for (const history of [50, 250]) for (const ttftMs of [0, 400]) for (let object = 0; object < 7; object++) {
    const roles = [
      { role: "primary", framework: "yielded" },
      { role: "primary", framework: "pi" },
      { role: "primary", framework: "tardie" },
      { role: "control", framework: "yielded" },
    ];
    for (const role of shuffle(roles, 541 + object * 61 + history + ttftMs)) {
      cohorts.push({ ...role, history, ttftMs, index: object,
        object: `cf-latency-net-h${history}-d${ttftMs}-o${object}` });
    }
  }
  return { objectsPerRole: 7, turns: 6, coldTurn: 0, settlingTurn: 1, warmTurns: [2, 3, 4, 5],
    variantSettlingTurns: [6, 7], variantTurns: [8, 9, 10, 11, 12, 13],
    realisticProvider: { ttftMs: 400, chunkDelayMs: 10 }, locationHint: "wnam", cohorts };
};
const cohortKey = (cohort) => `${cohort.role}/${cohort.framework}/${cohort.object}`;
const deployNetwork = async () => {
  await open();
  const provider = await deployOne("provider", "provider");
  await readiness(provider, "/echo");
  for (const role of ["primary", "control"]) {
    const item = await deployOne(role, "network", `${provider.url}/v1`);
    await readiness(item, networkUrl("/identity", { framework: "yielded", history: 50, ttftMs: 0 }, "ready"), `cf-latency-ready-${role}`);
  }
};
const refreshNetwork = async (phase) => {
  await open();
  const provider = target("provider");
  for (const role of ["primary", "control"]) {
    let item = resources.targets.find((candidate) => candidate.role === role);
    if (!item) item = await deployOne(role, "network", `${provider.url}/v1`);
    else {
      const build = load(join(here, "build-identities/all.json")).find((candidate) => candidate.name === "network");
      item.deployments ??= [];
      item.deployments.push({ build: item.build, phase: item.phase, version: item.version, startupEvidence: item.startupEvidence, replacedAt: Date.now() });
      if (phase) item.phase = phase;
      item.build = build;
      item.bundle = join(build.output, "worker.mjs");
      save(join(here, "resources.json"), resources);
      await alchemy(item, "deploy");
      await verifyUpload(item);
    }
    const ready = await readiness(item, networkUrl("/identity", { framework: "yielded", history: 50, ttftMs: 0 }, "ready"), (attempt) => `cf-latency-ready-${role}-${Date.now()}-${attempt}`, (row) => receipt(row).generation === item.phase && receipt(row).version === item.expectedVersion);
    if (!receipt(ready).ok || receipt(ready).generation !== item.phase) throw new Error("Network readiness/generation failed");
    item.version = receipt(ready).version;
    save(join(here, "resources.json"), resources);
    console.log(`Verified network ${role}`);
  }
};
const refreshNonNetwork = async (role) => {
  await open();
  const item = target(role);
  const build = load(join(here, "build-identities/all.json")).find((candidate) => candidate.name === item.kind);
  item.deployments ??= [];
  item.deployments.push({ build: item.build, phase: item.phase, version: item.version, startupEvidence: item.startupEvidence, replacedAt: Date.now() });
  item.build = build;
  item.bundle = join(build.output, "worker.mjs");
  save(join(here, "resources.json"), resources);
  await alchemy(item, "deploy");
  await verifyUpload(item);
  await readiness(item, role === "probe" ? "/prepare" : "/echo", role === "probe" ? `cf-latency-alarm-ready-${Date.now()}` : undefined);
};
const seedNetwork = async (pilot = false) => {
  await open();
  const name = pilot ? `network-pilot${pilot === true ? "" : pilot}` : "network";
  const planFile = join(here, `${name}-plan.json`);
  let plan;
  if (existsSync(planFile)) plan = load(planFile);
  else {
    plan = networkPlan();
    if (pilot) {
      plan.cohorts = ["yielded", "pi", "tardie"].map((framework) => ({
        role: "primary", framework, history: 50, ttftMs: 0, index: 0,
        object: `cf-latency-${name}-h50-d0-o0`,
      }));
      plan.turns = 2;
      if (pilot === 3) plan.cohorts = [
        ...["yielded", "pi", "tardie"].map((framework) => ({
          role: "primary", framework, history: 50, ttftMs: 400, index: 0,
          object: `cf-latency-${name}-h50-d400-o0`,
        })),
        { role: "primary", framework: "tardie", history: 50, ttftMs: 0, index: 0,
          object: `cf-latency-${name}-h50-d0-o0` },
      ];
    }
    save(planFile, plan);
  }
  const finishedFile = join(here, `${name}-seeds.json`);
  const finished = existsSync(finishedFile) ? load(finishedFile) : [];
  const rows = existsSync(join(here, "requests.jsonl")) ? readFileSync(join(here, "requests.jsonl"), "utf8").trim().split("\n").map(JSON.parse) : [];
  const attempts = existsSync(join(here, "attempted.jsonl")) ? readFileSync(join(here, "attempted.jsonl"), "utf8").trim().split("\n").map(JSON.parse) : [];
  // Seeding is outside measurements. Bound it to twelve independent Objects;
  // measured turns are always serial, with no seeding running alongside them.
  let next = 0;
  let stopped = false;
  const seedErrors = [];
  const seedOne = async (cohort) => {
    if (finished.some((item) => item.key === cohortKey(cohort))) return;
    const previous = rows.filter((row) => row.phase === `${name}-seed` && row.target === cohort.role && row.framework === cohort.framework && row.object === cohort.object);
    const previousAttempts = attempts.filter((row) => row.phase === `${name}-seed` && row.target === cohort.role && row.framework === cohort.framework && row.object === cohort.object);
    if (previousAttempts.some((attempt) => !previous.some((row) => row.startedAt === attempt.startedAt))) throw new Error(`Seed has an unanswered attempt: ${cohortKey(cohort)}; never repeat it`);
    if (previous.some((row) => row.status !== 200)) throw new Error(`Seed had an uncertain/failed attempt: ${cohortKey(cohort)}; retain it and use a fresh cohort`);
    let from = previous.reduce((count, row) => Math.max(count, receipt(row).to ?? 0), 0);
    let last = previous.at(-1);
    while (from < cohort.history) {
      if (stopped) return;
      const to = Math.min(cohort.history, from + 10);
      last = await request(target(cohort.role), networkUrl("/seed", cohort, `seed-${from}-${to}`), { from, to }, `${name}-seed`, cohort.object);
      if (!receipt(last).ok || receipt(last).to !== to) throw new Error("Bad seed receipt");
      from = to;
    }
    const result = receipt(last);
    if (result.fingerprint !== expectedFingerprint[cohort.history]) throw new Error("Golden seed fingerprint mismatch");
    finished.push({ key: cohortKey(cohort), cohort, receipt: result });
    save(finishedFile, finished);
    console.log(`${name} seeded ${finished.length}/${plan.cohorts.length}: ${cohortKey(cohort)} ${result.fingerprint}`);
  };
  await Promise.all(Array.from({ length: pilot ? 3 : 12 }, async () => {
    while (!stopped && next < plan.cohorts.length) {
      const cohort = plan.cohorts[next++];
      try { await seedOne(cohort); }
      catch (cause) {
        stopped = true;
        seedErrors.push({ key: cohortKey(cohort), at: Date.now(), message: clean(String(cause)), noReplay: true });
      }
    }
  }));
  if (seedErrors.length) {
    for (const error of seedErrors) appendFileSync(join(here, `${name}-seed-errors.jsonl`), JSON.stringify(error) + "\n");
    throw new Error(`Seeding stopped after ${seedErrors.length} failed/unknown cohort(s); all other in-flight seed chunks settled`);
  }
};
const activateNetwork = async (phase) => {
  await open();
  for (const role of ["primary", "control"]) {
    const item = target(role);
    const old = item.phase;
    item.phase = phase;
    item.activatedAt = Date.now();
    save(join(here, "resources.json"), resources);
    await alchemy(item, "deploy");
    await verifyUpload(item);
    const activation = await readiness(item, networkUrl("/identity", { framework: "yielded", history: 50, ttftMs: 0 }, "activation"), (attempt) => `cf-latency-activation-${phase}-${item.activatedAt}-${attempt}`, (row) => receipt(row).generation === phase && receipt(row).version === item.expectedVersion);
    if (receipt(activation).generation !== phase) throw new Error("Updated Worker generation has not propagated");
    item.version = receipt(activation).version;
    save(join(here, "resources.json"), resources);
    console.log(`Activated ${role}: ${old} -> ${phase}`);
  }
};
const measureNetwork = async (pilot = false) => {
  await open();
  const name = pilot ? `network-pilot${pilot === true ? "" : pilot}` : "network";
  const plan = load(join(here, `${name}-plan.json`));
  const seeds = load(join(here, `${name}-seeds.json`));
  const finishedFile = join(here, `${name}-completed.json`);
  const finished = existsSync(finishedFile) ? load(finishedFile) : [];
  const failuresFile = join(here, `${name}-failures.json`);
  const failures = existsSync(failuresFile) ? load(failuresFile) : [];
  const rows = readFileSync(join(here, "requests.jsonl"), "utf8").trim().split("\n").map(JSON.parse);
  const attempts = readFileSync(join(here, "attempted.jsonl"), "utf8").trim().split("\n").map(JSON.parse);
  for (let i = 0; i < 5; i++) await request(target("provider"), `/echo?sample=${name}-clock-start-${i}`, undefined, "clock-calibration");
  for (const cohort of plan.cohorts) {
    const key = cohortKey(cohort);
    if (finished.some((item) => item.key === key) || failures.some((item) => item.key === key)) continue;
    if (attempts.some((row) => row.phase === name && row.target === cohort.role && row.framework === cohort.framework && row.object === cohort.object)) {
      failures.push({ key, at: Date.now(), error: "Interrupted measured cohort; no canonical turn is replayed", completedRequests: rows.filter((row) => row.phase === name && row.target === cohort.role && row.framework === cohort.framework && row.object === cohort.object).length });
      save(failuresFile, failures);
      continue;
    }
    const seeded = seeds.find((item) => item.key === key);
    if (!seeded) throw new Error(`Unseeded cohort: ${key}`);
    const item = target(cohort.role);
    if (item.phase !== "measure") throw new Error("Activate the measurement generation first");
    try {
    // Deliberate eviction on fully settled fixture state. The outer Worker
    // records pre-reset identities and the observed, intentional DO aborts.
    const cold = await request(item, networkUrl("/cold", cohort, "cold-reset"), {}, "cold-reset", cohort.object);
    if (!cold.response.ok || !cold.response.coldRequested || !cold.response.before) throw new Error("Cold reset was not acknowledged");
    let incarnation;
    for (let index = 0; index < plan.turns; index++) {
      const row = await request(item, networkUrl("/run", cohort, `m${index}`), {}, name, cohort.object);
      const result = receipt(row);
      if (!result.ok || result.seedFingerprint !== expectedFingerprint[cohort.history] || result.calls.length !== 9 || result.version !== item.version)
        throw new Error(`Measured identity/provider/seed failure: ${key}/m${index}`);
      if (index === 0) {
        if (result.incarnation === cold.response.before.incarnation || result.objectId !== cold.response.before.objectId)
          throw new Error("Cold Object did not acquire a new incarnation of the same identity");
        if (cohort.framework === "tardie" && row.response.directory?.incarnation === cold.response.directoryBefore?.incarnation)
          throw new Error("Tardie directory retained pre-reset incarnation");
        incarnation = result.incarnation;
      } else if (result.incarnation !== incarnation) throw new Error("Incarnation changed inside warmed cohort");
    }
    // Small same-Object interventions on the zero-delay Yielded cohorts.
    if (!pilot && cohort.role === "primary" && cohort.framework === "yielded" && cohort.ttftMs === 0) {
      let index = plan.turns;
      // At h50, tool #97 is in m7. Keep that intentionally large tool result
      // outside the intervention comparison; all six variant turns have 256 B results.
      for (const sample of plan.variantSettlingTurns) {
        await request(item, networkUrl("/run", cohort, `m${sample}`), {}, "network-variant-settle", cohort.object);
        index++;
      }
      for (let repeat = 0; repeat < 2; repeat++) for (const variant of shuffle(["baseline", "defer-wakes", "sync"], 887 + cohort.index * 37 + repeat * 31)) {
        const extra = variant === "sync" ? { syncBeforeFetch: "true" } : { variant };
        await request(item, networkUrl("/run", cohort, `m${index++}`, extra), {}, "network-variant", cohort.object);
      }
    }
    // Fully completed fixtures no longer need to keep their runtimes resident.
    // Persisted history is retained; this is the same explicit, observed abort
    // used for cold entry, outside every measured interval and for all targets.
    const release = await request(item, networkUrl("/cold", cohort, "post-cohort-release"), {}, "cohort-release", cohort.object);
    if (!release.response.ok || !release.response.coldRequested) throw new Error("Completed cohort release was not acknowledged");
    finished.push({ key, incarnation, completedAt: Date.now() });
    save(finishedFile, finished);
    console.log(`${name} measured ${finished.length}/${plan.cohorts.length}: ${key}`);
    } catch (cause) {
      failures.push({ key, at: Date.now(), error: clean(String(cause)), noRetry: true });
      save(failuresFile, failures);
      console.log(`${name} retained failed cohort without replay: ${key}`);
      if (pilot || failures.length >= 3) throw cause;
    }
  }
  for (let i = 0; i < 5; i++) await request(target("provider"), `/echo?sample=${name}-clock-end-${i}`, undefined, "clock-calibration");
};
const measureInstrumentation = async () => {
  await open();
  const mainPlan = load(join(here, "network-plan.json"));
  const mainDone = load(join(here, "network-completed.json"));
  if (mainDone.length !== mainPlan.cohorts.length) throw new Error("Finish the native matrix before the observation calibration");
  const planFile = join(here, "instrumentation-plan.json");
  const plan = existsSync(planFile) ? load(planFile) : {
    purpose: "Same-Object Tardie directory observation on/off; native operations unchanged",
    warmupSamples: ["m6", "m7"], repetitions: 3,
    cohorts: mainPlan.cohorts.filter((c) => c.role === "primary" && c.framework === "tardie" && c.ttftMs === 0),
  };
  if (!existsSync(planFile)) save(planFile, plan);
  const doneFile = join(here, "instrumentation-completed.json");
  const done = existsSync(doneFile) ? load(doneFile) : [];
  const failuresFile = join(here, "instrumentation-failures.json");
  const failures = existsSync(failuresFile) ? load(failuresFile) : [];
  const attempts = readFileSync(join(here, "attempted.jsonl"), "utf8").trim().split("\n").map(JSON.parse);
  for (const cohort of plan.cohorts) {
    const key = cohortKey(cohort);
    if (done.some((c) => c.key === key) || failures.some((c) => c.key === key)) continue;
    if (attempts.some((r) => r.phase.startsWith("instrumentation") && r.framework === "tardie" && r.object === cohort.object)) {
      failures.push({ key, at: Date.now(), noRetry: true, error: "Interrupted observation cohort; no canonical input is replayed" });
      save(failuresFile, failures);
      continue;
    }
    const item = target(cohort.role);
    try {
      const cold = await request(item, networkUrl("/cold", cohort, "observation-reset"), {}, "instrumentation-reset", cohort.object);
      if (!cold.response.ok || !cold.response.coldRequested) throw new Error("Observation reset not acknowledged");
      const settings = [true, true, ...Array.from({ length: 3 }, (_, repeat) => shuffle([true, false], 331 + cohort.index * 41 + repeat * 13)).flat()];
      let incarnation;
      let actorIncarnation;
      for (const [offset, enabled] of settings.entries()) {
        const sample = `m${offset + 6}`;
        const row = await request(item, networkUrl("/run", cohort, sample, { directoryMetrics: String(enabled) }), {}, offset < 2 ? "instrumentation-warmup" : "instrumentation", cohort.object);
        const r = receipt(row);
        if (!r.ok || r.seedFingerprint !== expectedFingerprint[cohort.history] || r.calls?.length !== 9 || r.version !== item.version)
          throw new Error("Observation provider/seed/version receipt failed");
        if (r.objectId !== cold.response.before.objectId || r.incarnation === cold.response.before.incarnation || (incarnation && incarnation !== r.incarnation))
          throw new Error("Observation Thread identity changed");
        incarnation = r.incarnation;
        if (enabled) {
          const actor = row.response.directory;
          if (!actor || actor.objectId !== cold.response.directoryBefore.objectId || actor.incarnation === cold.response.directoryBefore.incarnation || actor.incarnation !== row.response.directoryStart?.incarnation || (actorIncarnation && actorIncarnation !== actor.incarnation))
            throw new Error("Observation Actor identity changed");
          actorIncarnation = actor.incarnation;
        } else if (row.response.thread || row.response.directory) throw new Error("Directory observation toggle was not deployed");
      }
      const release = await request(item, networkUrl("/cold", cohort, "observation-release"), {}, "instrumentation-release", cohort.object);
      if (!release.response.ok || !release.response.coldRequested || release.response.before?.incarnation !== incarnation || release.response.directoryBefore?.incarnation !== actorIncarnation)
        throw new Error("Observation release or final identity failed");
      done.push({ key, incarnation, actorIncarnation, version: item.version, completedAt: Date.now() });
      save(doneFile, done);
      console.log(`Observation measured ${done.length}/${plan.cohorts.length}: ${key}`);
    } catch (cause) {
      failures.push({ key, at: Date.now(), noRetry: true, error: clean(String(cause)) });
      save(failuresFile, failures);
      throw cause;
    }
  }
};
const measureMinification = async () => {
  await open();
  const main = load(join(here, "network-plan.json"));
  if (load(join(here, "network-completed.json")).length !== main.cohorts.length ||
      load(join(here, "instrumentation-completed.json")).length !== load(join(here, "instrumentation-plan.json")).cohorts.length)
    throw new Error("Finish both native measurement phases before changing their deployed bytes");
  const file = join(here, "minification.json");
  if (existsSync(file)) throw new Error("Minification already attempted; never replay a measured canonical turn");
  const builds = load(join(here, "build-identities/all.json"));
  const plain = builds.find((build) => build.name === "network");
  const minified = builds.find((build) => build.name === "network-min");
  if (!plain || !minified || plain.inputsSha256 !== minified.inputsSha256 || plain.fixtureSha256 !== minified.fixtureSha256 ||
      plain.repositoryCommit !== minified.repositoryCommit || plain.minify || !minified.minify ||
      ["esbuildVersion", "effectVersion", "target"].some((key) => plain[key] !== minified[key]))
    throw new Error("Minification must compare the same source/fixture with only the build flag changed");
  const plan = {
    purpose: "Combined native bundle minification: eight upload rounds, same-Object cold requests, unchanged-code temporal control",
    startedAt: Date.now(), complete: false,
    // ABBA BAAB balances linear progression; the control uploads plain bytes throughout.
    rounds: [false, true, true, false, true, false, false, true],
    cohorts: main.cohorts.filter((c) => c.framework === "yielded" && c.history === 50 && c.ttftMs === 400)
      .map((c) => ({ ...c, ttftMs: 0 })),
    builds: { plain, minified }, deployments: [], completed: [],
  };
  save(file, plan);
  try {
    // These Objects ended the main matrix at m5 in both Workers. Preserve that
    // history and append m6/m7 before comparison (lookup #97 is in m7).
    for (const cohort of shuffle(plan.cohorts, 771)) {
      const item = target(cohort.role);
      for (const sample of ["m6", "m7"]) {
        const row = await request(item, networkUrl("/run", cohort, sample), {}, "minification-prelude", cohort.object);
        const r = receipt(row);
        if (!r.ok || r.seedFingerprint !== expectedFingerprint[50] || r.calls?.length !== 9 || r.version !== item.version)
          throw new Error("Minification prelude identity/provider failure");
      }
      const released = await request(item, networkUrl("/cold", cohort, "minify-prelude-release"), {}, "minification-release", cohort.object);
      if (!released.response.ok || !released.response.coldRequested) throw new Error("Minification prelude release not acknowledged");
    }
    for (const [round, enabled] of plan.rounds.entries()) {
      for (const role of shuffle(["primary", "control"], 401 + round * 23)) {
        const item = target(role);
        const build = role === "primary" && enabled ? minified : plain;
        item.deployments ??= [];
        item.deployments.push({ build: item.build, phase: item.phase, version: item.version, startupEvidence: item.startupEvidence, uploadRevision: item.uploadRevision, replacedAt: Date.now() });
        item.build = build;
        item.bundle = join(build.output, "worker.mjs");
        if (hash(readFileSync(item.bundle)) !== build.bundleSha256) throw new Error("Minification bundle digest mismatch");
        item.uploadRevision = `cf-latency-minify-${round}`;
        const previousVersion = item.expectedVersion;
        save(join(here, "resources.json"), resources);
        await alchemy(item, "deploy");
        await verifyUpload(item);
        if (item.expectedVersion === previousVersion) throw new Error("Repeated startup round did not create a new active version");
        const ready = await readiness(item, networkUrl("/identity", { framework: "yielded", history: 50, ttftMs: 0 }, "minify-ready"),
          (attempt) => `cf-latency-minify-ready-${role}-${round}-${attempt}`,
          (row) => receipt(row).generation === "measure" && receipt(row).version === item.expectedVersion);
        item.version = receipt(ready).version;
        save(join(here, "resources.json"), resources);
        plan.deployments.push({ round, role, minify: build.minify, version: item.version, bundleSha256: build.bundleSha256,
          bundleBytes: build.bundleBytes, gzipBytes: build.gzipBytes, startupTimeMs: item.startupTimeMs ?? null,
          startupEvidence: item.startupEvidence, uploadRevision: item.uploadRevision, observedAt: Date.now() });
        save(file, plan);
      }
      for (const cohort of shuffle(plan.cohorts, 977 + round * 53)) {
        const item = target(cohort.role);
        const sample = `m${8 + round}`;
        const cold = await request(item, networkUrl("/cold", cohort, `minify-reset-${sample}`), {}, "minification-reset", cohort.object);
        if (!cold.response.ok || !cold.response.coldRequested || cold.response.before?.version !== item.version)
          throw new Error("Minification cold reset or current-version identity not acknowledged");
        const row = await request(item, networkUrl("/run", cohort, sample, {
          minificationRound: String(round), minified: String(item.build.minify),
        }), {}, "minification", cohort.object);
        const r = receipt(row);
        if (!r.ok || r.seedFingerprint !== expectedFingerprint[50] || r.calls?.length !== 9 || r.version !== item.version ||
            r.objectId !== cold.response.before.objectId || r.incarnation === cold.response.before.incarnation)
          throw new Error("Minification cold/provider/identity failure");
        const release = await request(item, networkUrl("/cold", cohort, `minify-release-${sample}`), {}, "minification-release", cohort.object);
        if (!release.response.ok || !release.response.coldRequested) throw new Error("Minification release not acknowledged");
        plan.completed.push({ round, key: cohortKey(cohort), sample, version: r.version, incarnation: r.incarnation,
          firstEntry: r.entry, completedAt: Date.now() });
        save(file, plan);
      }
      console.log(`Minification round ${round + 1}/${plan.rounds.length}: primary ${enabled ? "minified" : "plain"}, control plain`);
    }
    plan.complete = true;
    plan.finishedAt = Date.now();
    save(file, plan);
  } catch (cause) {
    plan.failure = { at: Date.now(), error: clean(String(cause)), noRetry: true };
    save(file, plan);
    throw cause;
  }
};
const telemetry = async () => {
  await open();
  for (const item of resources.targets) {
    const existing = join(here, `telemetry-${item.role}.json`);
    const previous = existsSync(existing) ? load(existing) : { events: [], polls: [] };
    const scrub = (event) => {
      const request = event.$workers?.event?.request;
      if (request) {
        delete request.headers;
        if (request.cf) request.cf = { colo: request.cf.colo, clientTcpRtt: request.cf.clientTcpRtt, httpProtocol: request.cf.httpProtocol };
      }
      return event;
    };
    const all = new Map(previous.events.map(scrub).map((event) => [event.$metadata?.id ?? JSON.stringify(event), event]));
    const queryWindow = async (from, to) => {
      const query = { queryId: item.name, dry: true, view: "events", limit: 2000, timeframe: { from, to },
        parameters: { filterCombination: "and", filters: [{ key: "$workers.scriptName", operation: "eq", type: "string", value: item.name }] } };
      const data = await apiJson("workers/observability/telemetry/query", query);
      const events = data.result.events;
      if (events.count >= 2000 || events.count > events.events.length) {
        if (to - from <= 1000) throw new Error("Telemetry exceeds bounded window; do not silently truncate");
        const middle = Math.floor((from + to) / 2);
        await queryWindow(from, middle);
        await queryWindow(middle, to);
        return;
      }
      const sanitized = JSON.parse(clean(JSON.stringify(events.events))).map(scrub);
      for (const event of sanitized) all.set(event.$metadata?.id ?? JSON.stringify(event), event);
      previous.polls.push({ at: Date.now(), from, to, count: events.count, rows: sanitized.length,
        statistics: data.result.statistics, sampleIntervals: [...new Set((events.series ?? []).flatMap((row) => row.data.map((value) => value.sampleInterval)))] });
    };
    // Re-query the complete bounded run: ingestion can arrive late or out of order.
    await queryWindow(item.deployedAt - 60_000, Date.now() + 1000);
    save(existing, { worker: item.name, accountName: resources.accountName, events: [...all.values()], polls: previous.polls });
    console.log(`Telemetry ${item.role}: ${all.size} distinct events`);
  }
};
const cleanup = async () => {
  await open();
  const checks = [];
  for (const item of [...resources.targets].reverse()) {
    if (!item.cleanupComplete) await alchemy(item, "destroy");
    const workerGetStatus = (await api(`workers/scripts/${item.name}`)).status;
    const remaining = (await listNamespaces()).filter((ns) => ns.script === item.name);
    checks.push({ worker: item.name, workerGetStatus, remainingNamespaces: remaining.map(({ name, script }) => ({ name, script })), checkedAt: new Date().toISOString() });
    if (workerGetStatus !== 404 || remaining.length) throw new Error(`Cleanup not verified: ${item.name}`);
    item.cleanupComplete = true;
    save(join(here, "resources.json"), resources);
    console.log(`Verified deletion: ${item.name}`);
  }
  const remaining = await prefixedResources();
  if (remaining.workers.length || remaining.namespaces.length) throw new Error("cf-latency prefix resources remain in recorded account");
  rmSync(resources.privateDirectory, { recursive: true });
  save(join(here, "cleanup.json"), { complete: true, accountName: resources.accountName, accountDigest: resources.accountDigest,
    checkedAt: new Date().toISOString(), checks, remaining, secretStateRemoved: true });
};
const scanSecrets = async () => {
  await open();
  const files = [];
  const walk = (directory) => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const file = join(directory, entry.name);
      if (entry.isDirectory()) walk(file);
      else if (entry.isFile()) files.push(file);
      else throw new Error("Unexpected non-file in committed evidence");
    }
  };
  walk(here);
  files.push(join(root, "package.json"), join(root, "bun.lock"), join(here, "../../vite.config.ts"));
  const secrets = [account, token, privateState.token];
  const matches = [];
  let gzipFiles = 0;
  for (const file of files) {
    const raw = readFileSync(file);
    const data = file.endsWith(".gz") ? (gzipFiles++, gunzipSync(raw)) : raw;
    if (secrets.some((secret) => data.includes(Buffer.from(secret)))) matches.push(file.replace(root + "/", ""));
  }
  save(join(here, "secret-scan.json"), { accountName: resources.accountName, checkedAt: new Date().toISOString(),
    filesChecked: files.length, gzipFilesChecked: gzipFiles, knownCredentialValuesChecked: secrets.length, matches,
    passed: matches.length === 0, scope: "Exact account ID, Cloudflare API token, and private benchmark token; raw and decompressed result artifacts plus changed root manifests/task config" });
  if (matches.length) throw new Error(`Known credentials found in ${matches.length} evidence files; do not commit`);
  console.log(`Credential scan passed: ${files.length} files, including ${gzipFiles} compressed artifacts`);
};

export const run = Effect.tryPromise({
  try: async () => {
    if (action === "dry-run") {
      console.log(JSON.stringify({ stack: "cf-latency", timing: "deployed Cloudflare only", locationHint: "wnam", cpuLimitMs: 300000,
        calibration: { bytes: [0, 1024, 16384, 65536, 131072, 524288], transactions: [1, 4, 12], modes: ["return", "fetch", "sync-end", "sync-each"], objects: 8, repetitions: 5 },
        actions: ["init", "deploy-probe", "probe-pilot", "calibrate", "alarms2", "transport", "deploy-network", "seed-pilot", "seed", "activate", "measure-pilot", "measure", "metadata", "refresh-network-measure", "instrumentation", "minification", "telemetry", "cleanup"] }, null, 2));
      return;
    }
    if (!account || !token) throw new Error("Cloudflare credentials missing; use checkout direnv");
    if (action === "init") await init();
    else if (action === "deploy-probe") await deployProbe();
    else if (action === "probe-pilot") await calibrate(true);
    else if (action === "probe-pilot2") await calibrate(2);
    else if (action === "calibrate") await calibrate(false);
    else if (action === "alarms") await alarms();
    else if (action === "alarms2") await alarms("2");
    else if (action === "transport") await transport();
    else if (action === "refresh-probe") await refreshNonNetwork("probe");
    else if (action === "refresh-provider") await refreshNonNetwork("provider");
    else if (action === "deploy-network") await deployNetwork();
    else if (action === "refresh-network") await refreshNetwork();
    else if (action === "refresh-network-seed") await refreshNetwork("seed");
    else if (action === "refresh-network-measure") await refreshNetwork("measure");
    else if (action === "seed-pilot") await seedNetwork(true);
    else if (action === "seed-pilot2") await seedNetwork(2);
    else if (action === "seed-pilot3") await seedNetwork(3);
    else if (action === "seed") await seedNetwork(false);
    else if (action === "activate") await activateNetwork("measure");
    else if (action === "seed-generation") await activateNetwork("seed");
    else if (action === "measure-pilot") await measureNetwork(true);
    else if (action === "measure-pilot2") await measureNetwork(2);
    else if (action === "measure-pilot3") await measureNetwork(3);
    else if (action === "measure") await measureNetwork(false);
    else if (action === "instrumentation") await measureInstrumentation();
    else if (action === "minification") await measureMinification();
    else if (action === "metadata") {
      await open();
      for (const item of resources.targets) {
        await verifyUpload(item);
        console.log(`Verified ${item.role} startup metadata: ${item.startupTimeMs ?? "missing"} ms`);
      }
    }
    else if (action === "telemetry") await telemetry();
    else if (action === "secret-scan") await scanSecrets();
    else if (action === "cleanup") await cleanup();
    else throw new Error(`Unknown action: ${action}`);
  }, catch: (cause) => {
    const message = clean(String(cause));
    appendFileSync(join(here, "controller-errors.jsonl"), JSON.stringify({ action, at: Date.now(), message }) + "\n");
    return new RunError({ message });
  },
});
if (import.meta.url === `file://${process.argv[1]}`) NodeRuntime.runMain(run);
