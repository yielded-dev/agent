import { createHash, randomBytes } from "node:crypto";
import { readFileSync, writeFileSync, mkdirSync, chmodSync, symlinkSync, existsSync, rmSync, appendFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import { NodeRuntime } from "@effect/platform-node";
import { Effect, Schema } from "effect";

// Fixed experiment, adapted from replay-cpu-deployment.ts and replay-cpu-main.ts.
// The native APIs are the controller's I/O boundary; benchmark clocks are remote.
const RunError = Schema.TaggedError()("CalibrationRunError", { message: Schema.String });
const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../../../../..");
const output = join(here, "hosted");
const action = process.argv[2] ?? "dry-run";
const account = process.env.CLOUDFLARE_ACCOUNT_ID;
const token = process.env.CLOUDFLARE_API_TOKEN;
const hash = (data) => createHash("sha256").update(data).digest("hex");
const load = (file) => JSON.parse(readFileSync(file, "utf8"));
const save = (file, data) => { mkdirSync(dirname(file), { recursive: true }); writeFileSync(file + ".tmp", JSON.stringify(data, null, 2) + "\n"); const { renameSync } = requireFs; renameSync(file + ".tmp", file); };
import * as requireFs from "node:fs";
const sleep = (ms) => new Promise((done) => setTimeout(done, ms));
const clean = (value) => String(value).replaceAll(token ?? "__unset__", "[api-token]").replaceAll(account ?? "__unset__", "[account]");
let resources;
let privateState;

const api = async (route, body) => {
  const response = await fetch(`https://api.cloudflare.com/client/v4/accounts/${account}/${route}`, {
    method: body === undefined ? "GET" : "POST",
    headers: { authorization: `Bearer ${token}`, ...(body === undefined ? {} : { "content-type": "application/json" }) },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }), signal: AbortSignal.timeout(60_000),
  });
  return response;
};
const apiJson = async (route, body) => {
  const response = await api(route, body);
  if (!response.ok) throw new Error(clean(`Cloudflare ${route.split("?")[0]}: ${response.status} ${await response.text()}`));
  const result = await response.json();
  if (!result.success) throw new Error(clean(JSON.stringify(result.errors)));
  return result;
};
const namespaces = async (name, expectedIds = []) => {
  const matching = [];
  for (let page = 1; page <= 100; page++) {
    const data = await apiJson(`workers/durable_objects/namespaces?page=${page}&per_page=100`);
    matching.push(...data.result.filter((item) => item.script === name || expectedIds.includes(item.id)).map(({ id, name, script }) => ({ id, name, script })));
    if (data.result.length < 100) return matching;
  }
  throw new Error("Namespace listing exceeded bound");
};
const alchemy = (target, operation) => new Promise((done, fail) => {
  const child = spawn("vp", ["exec", "alchemy", operation, join(here, "stack.ts"), "--stage", target.name, "--yes"], {
    cwd: resources.privateDirectory, stdio: ["ignore", "pipe", "pipe"],
    env: { PATH: process.env.PATH, HOME: process.env.HOME, CI: "true", NO_COLOR: "1",
      ALCHEMY_HOME: join(resources.privateDirectory, "auth"), CLOUDFLARE_ACCOUNT_ID: account, CLOUDFLARE_API_TOKEN: token,
      EVAL_COST_WORKER: target.name, EVAL_COST_BUNDLE: target.bundle, EVAL_COST_TOKEN: privateState.token, EVAL_COST_KIND: "durable", EVAL_COST_PHASE: target.generation ?? "seed" },
  });
  let captured = "";
  const capture = (bytes) => { captured += bytes.toString(); };
  child.stdout.on("data", capture); child.stderr.on("data", capture);
  child.on("error", fail);
  child.on("close", (code) => {
    mkdirSync(join(output, "alchemy"), { recursive: true });
    appendFileSync(join(output, "alchemy", `${target.name}-${operation}.log`), clean(captured).replaceAll(privateState.token, "[benchmark-token]"));
    if (code === 0) done(); else fail(new Error(`Alchemy ${operation} ${target.name} exited ${code}`));
  });
});
const open = () => {
  resources = load(join(output, "resources.json"));
  if (resources.accountSha256 !== hash(account)) throw new Error("Account ownership mismatch");
  privateState = load(join(resources.privateDirectory, "private.json"));
};
const request = async (target, path, input, phase) => {
  const startedAt = Date.now();
  const start = performance.now();
  const objectSuffix = target.objectName ? `${path.includes("?") ? "&" : "?"}object=${encodeURIComponent(target.objectName)}` : "";
  appendFileSync(join(output, "attempted.jsonl"), JSON.stringify({ target: target.role, worker: target.name, phase, startedAt, input }) + "\n");
  let response;
  try { response = await fetch(`${target.url}${path}${objectSuffix}`, { method: input === undefined ? "GET" : "POST",
    headers: { authorization: `Bearer ${privateState.token}`, ...(input === undefined ? {} : { "content-type": "application/json" }) },
    ...(input === undefined ? {} : { body: JSON.stringify(input) }), signal: AbortSignal.timeout(120_000) }); }
  catch (cause) {
    appendFileSync(join(output, "requests.jsonl"), JSON.stringify({ target: target.role, worker: target.name, phase, startedAt, endedAt: Date.now(), clientWallMs: performance.now() - start, input, status: null, response: null, error: clean(String(cause)), code: cause?.cause?.code }) + "\n");
    throw cause;
  }
  const body = await response.text();
  const row = { target: target.role, worker: target.name, phase, startedAt, endedAt: Date.now(), clientWallMs: performance.now() - start,
    status: response.status, cfRay: response.headers.get("cf-ray"), input, response: (() => { try { return JSON.parse(body); } catch { return { error: body.slice(0, 1000) }; } })() };
  appendFileSync(join(output, "requests.jsonl"), clean(JSON.stringify(row)).replaceAll(privateState.token, "[benchmark-token]") + "\n");
  if (!response.ok) throw new Error(`Worker ${target.role} ${path} returned ${response.status}`);
  return row.response;
};
const deploy = async () => {
  if (existsSync(join(output, "resources.json"))) throw new Error("Resources already recorded; use the existing run");
  const run = randomBytes(4).toString("hex");
  const privateDirectory = join(process.env.EVAL_COST_PRIVATE ?? "/private/tmp/effect-eval-cost-20261008", `state-${run}`);
  mkdirSync(privateDirectory, { mode: 0o700 }); chmodSync(privateDirectory, 0o700);
  symlinkSync(join(root, "node_modules"), join(privateDirectory, "node_modules"));
  writeFileSync(join(privateDirectory, "package.json"), '{"private":true,"type":"module"}\n');
  privateState = { token: randomBytes(32).toString("hex") };
  writeFileSync(join(privateDirectory, "private.json"), JSON.stringify(privateState), { mode: 0o600 });
  resources = { run, accountSha256: hash(account), privateDirectory, startedAt: Date.now(), targets: [] };
  save(join(output, "resources.json"), resources);
  save(join(here, "cleanup.json"), { complete: false, activeRun: run, status: "resources recorded; cleanup pending" });
  const subdomain = (await apiJson("workers/subdomain")).result.subdomain;
  const builds = load(join(here, "build-identities/all.json"));
  for (const [role, buildName] of [["pin", "pin"], ["pin-control", "pin"]]) {
    const build = builds.find((item) => item.name === buildName);
    const name = `effect-eval-cost-turn-${run}-${role}`;
    const bundle = join(build.output, "worker.mjs");
    if (hash(readFileSync(bundle)) !== build.bundleSha256) throw new Error("Bundle identity mismatch");
    const existing = await api(`workers/scripts/${name}`);
    if (existing.status !== 404) throw new Error("Worker name already exists or existence check failed");
    const target = { role, name, bundle, generation: "seed", url: `https://${name}.${subdomain}.workers.dev`, build, cleanupRequired: true, cleanupComplete: false };
    resources.targets.push(target); save(join(output, "resources.json"), resources);
    await alchemy(target, "deploy");
    const uploaded = await api(`workers/scripts/${name}`);
    const uploadedText = await uploaded.text();
    if (!uploaded.ok || !uploadedText.includes(readFileSync(bundle, "utf8"))) throw new Error("Uploaded module differs from measured bytes");
    target.uploadedModuleSha256 = build.bundleSha256;
    const settings = (await apiJson(`workers/scripts/${name}/settings`)).result;
    target.settings = { limits: settings.limits, compatibilityDate: settings.compatibility_date, compatibilityFlags: settings.compatibility_flags, observability: settings.observability };
    target.namespaces = await namespaces(name);
    if (target.namespaces.length !== 1) throw new Error("Expected one isolated DO namespace");
    save(join(output, "resources.json"), resources);
    let identity;
    for (let i = 0; i < 15; i++) {
      target.objectName = `calibration-${resources.run}-${i}`;
      try {
        identity = await request(target, "/identity", undefined, "readiness");
        if (identity.build !== build.buildId || identity.fixture !== build.fixtureSha256) throw new Error("Deployed identity mismatch");
        break;
      }
      catch (error) { if (i === 14) throw error; await sleep(2000); }
    }
    if (identity.build !== build.buildId || identity.fixture !== build.fixtureSha256) throw new Error("Deployed identity mismatch");
    target.identity = identity; save(join(output, "resources.json"), resources);
    console.log(`Deployed ${role}: ${build.bundleSha256}`);
  }
};
const readiness = async () => {
  open();
  for (const target of resources.targets) {
    target.objectName = `calibration-${resources.run}`;
    const identity = await request(target, "/identity", undefined, "verified-readiness");
    if (identity.build !== target.build.buildId || identity.fixture !== target.build.fixtureSha256) throw new Error("Deployed identity mismatch");
    target.identity = identity;
    save(join(output, "resources.json"), resources);
    console.log(`Verified readiness: ${target.role} ${identity.version}`);
  }
};
const objectName = (round) => `turn-${resources.run}-${round}`;
const seed = async () => {
  open();
  if (existsSync(join(output, "seeds.json"))) throw new Error("Seed receipts already exist");
  const rows = [];
  for (let round = 0; round < 7; round++) for (const target of round % 2 ? [...resources.targets].reverse() : resources.targets) {
    target.objectName = objectName(round);
    const history = Array.from({ length: 50 }, (_, i) => ({ id: `h${i}`, text: `turn h${i} tools=${[1, 1, 0][i % 3]}` }));
    const receipt = await request(target, "/seed", history, "seed");
    if (receipt.fingerprint !== "b017b487524e44a4" || receipt.generation !== "seed") throw new Error("Historical transcript or generation mismatch");
    rows.push({ round, target: target.role, receipt }); save(join(output, "seeds.json"), rows);
    console.log(`Seeded ${target.role} / ${round}: ${receipt.fingerprint}`);
  }
};
const activate = async () => {
  open();
  for (const target of resources.targets) {
    target.seedIdentity = target.identity; target.generation = "measure";
    save(join(output, "resources.json"), resources);
    await alchemy(target, "deploy");
    const content = await api(`workers/scripts/${target.name}`);
    if (!content.ok || !(await content.text()).includes(readFileSync(target.bundle, "utf8"))) throw new Error("Updated uploaded bytes mismatch");
    for (let attempt = 0; attempt < 30; attempt++) {
      target.objectName = `measure-ready-${resources.run}-${attempt}`;
      const receipt = await request(target, "/identity", undefined, "activation");
      if (receipt.generation === "measure" && receipt.version !== target.seedIdentity.version) {
        target.identity = receipt; break;
      }
      if (attempt === 29) throw new Error("Updated version did not reach the DO");
      await sleep(2000);
    }
    save(join(output, "resources.json"), resources);
    console.log(`Activated measure generation: ${target.role}`);
  }
};
const measure = async (resume = false) => {
  open();
  const seeds = load(join(output, "seeds.json"));
  if (seeds.length !== 14 || resources.targets.some((target) => target.generation !== "measure")) throw new Error("Seed and activation incomplete");
  if (existsSync(join(output, "plan.json")) && !resume) throw new Error("Never repeat these canonical turns; resume only untouched cohorts");
  let schedule = [];
  for (let round = 0; round < 7; round++) for (const target of round % 2 ? [...resources.targets].reverse() : resources.targets) for (let turn = 0; turn < 10; turn++) {
    schedule.push({ target: target.role, round, turn, input: { sampleId: `real-${round}-m${turn}`, turn: { id: `m${turn}`, text: `turn m${turn} tools=8` } } });
  }
  if (resume) schedule = load(join(output, "plan.json")).schedule;
  else save(join(output, "plan.json"), { schedule, rounds: 7, history: 50, locationHint: "wnam" });
  const fingerprints = resume && existsSync(join(output, "fingerprints.json")) ? load(join(output, "fingerprints.json")) : [];
  const previous = readFileSync(join(output, "requests.jsonl"), "utf8").trim().split("\n").map(JSON.parse);
  const abandoned = [];
  for (let round = 0; round < 7; round++) for (const target of round % 2 ? [...resources.targets].reverse() : resources.targets) {
    if (fingerprints.some((row) => row.round === round && row.target === target.role)) continue;
    if (previous.some((row) => row.phase === "measured" && row.target === target.role && row.input?.sampleId?.startsWith(`real-${round}-`))) {
      abandoned.push({ round, target: target.role, reason: "Interrupted cohort; preserve attempted turns and do not retry a turn with unknown execution outcome", unattempted: schedule.filter((item) => item.round === round && item.target === target.role && !previous.some((row) => row.phase === "measured" && row.target === item.target && row.input?.sampleId === item.input.sampleId)).map((item) => item.input.sampleId) });
      save(join(output, "abandoned.json"), abandoned); continue;
    }
    target.objectName = objectName(round);
    const seeded = seeds.find((row) => row.round === round && row.target === target.role).receipt;
    let opened;
    for (let attempt = 0; attempt < 30; attempt++) {
      opened = await request(target, "/identity", undefined, "reopen-identity");
      if (opened.version === target.identity.version && opened.instance !== seeded.instance && opened.incarnation !== seeded.incarnation) break;
      if (attempt === 29) throw new Error("Reopened DO still has the seeded runtime");
      await sleep(2000);
    }
    const wake = await request(target, "/wake", undefined, "wake-outside-measurement");
    if (wake.version !== target.identity.version || wake.instance !== opened.instance) throw new Error("Runtime changed during warm open");
    for (const item of schedule.filter((row) => row.round === round && row.target === target.role)) {
      const receipt = await request(target, `/run?sample=${item.input.sampleId}`, item.input, "measured");
      if (receipt.version !== target.identity.version || receipt.instance !== opened.instance || receipt.objectId !== seeded.objectId || receipt.build !== target.build.buildId || receipt.fixture !== target.build.fixtureSha256) throw new Error("Measured identity changed");
    }
    const receipt = await request(target, "/fingerprint", undefined, "measured-fingerprint");
    if (receipt.fingerprint !== "b73859cee894aca6") throw new Error("Measured transcript mismatch");
    fingerprints.push({ target: target.role, round, opened, wake, receipt }); save(join(output, "fingerprints.json"), fingerprints);
    console.log(`Measured ${target.role} / ${round}: ${receipt.fingerprint}`);
  }
};
const telemetry = async () => {
  open();
  for (const target of resources.targets) {
    const query = { queryId: target.name, dry: true, view: "events", limit: 2000,
      timeframe: { from: resources.startedAt - 60_000, to: Date.now() + 60_000 },
      parameters: { filterCombination: "and", filters: [{ key: "$workers.scriptName", operation: "eq", type: "string", value: target.name }] } };
    const data = await apiJson("workers/observability/telemetry/query", query);
    const events = data.result.events;
    const sanitized = events.events.map((event) => {
      const w = event.$workers ?? {}; const m = event.$metadata ?? {};
      let sample;
      try { sample = new URL(w.event?.request?.url).searchParams.get("sample"); } catch {}
      return { timestamp: event.timestamp, id: m.id, metadataType: m.type, traceId: m.traceId, requestId: w.requestId,
        scriptName: w.scriptName, executionModel: w.executionModel, eventType: w.eventType,
        cpuTimeMs: w.cpuTimeMs, wallTimeMs: w.wallTimeMs, outcome: w.outcome, durableObjectId: w.durableObjectId,
        scriptVersion: w.scriptVersion, truncated: w.truncated, rpcMethod: w.event?.rpcMethod,
        rpcMethods: w.event?.rpcMethods, rpcCallCount: w.event?.rpcCallCount, sample,
        evalCostSample: event.evalCostSample ?? event.message?.evalCostSample ?? event.source?.evalCostSample,
        error: event.source?.error === undefined ? undefined : clean(JSON.stringify(event.source.error)).replaceAll(privateState.token, "[benchmark-token]"),
        sourceKeys: Object.keys(event.source ?? {}),
        // Only shape metadata from unexpected structured-log envelopes is retained.
        topLevelKeys: Object.keys(event) };
    });
    const statistics = { abrLevel: data.result.statistics?.abr_level ?? 1, rowsRead: data.result.statistics?.rows_read, bytesRead: data.result.statistics?.bytes_read,
      sampleIntervals: [...new Set((events.series ?? []).flatMap((row) => row.data.map((value) => value.sampleInterval)))] };
    save(join(output, "telemetry-polls", `${target.role}-${Date.now()}.json`), { query, count: events.count, statistics, events: sanitized });
    const previousPath = join(output, `telemetry-${target.role}.json`);
    const previous = existsSync(previousPath) ? load(previousPath).events : [];
    const merged = [...new Map([...previous, ...sanitized].map((event) => [event.id, event])).values()];
    save(previousPath, { query, count: merged.length, latestResponseCount: events.count, statistics, events: merged });
    console.log(`Telemetry ${target.role}: ${events.count} / ${sanitized.length}, CPU events ${sanitized.filter((e) => e.cpuTimeMs !== undefined).length}, sample logs ${sanitized.filter((e) => e.evalCostSample).length}`);
    if (events.count !== sanitized.length || events.count >= 2000) throw new Error("Telemetry export truncated; split the query window before analysis");
  }
};
const cleanup = async () => {
  open();
  const checks = [];
  for (const target of resources.targets) {
    if (!target.cleanupComplete) await alchemy(target, "destroy");
    const status = (await api(`workers/scripts/${target.name}`)).status;
    const remaining = await namespaces(target.name, (target.namespaces ?? []).map((item) => item.id));
    checks.push({ worker: target.name, workerGetStatus: status, remainingNamespaces: remaining, checkedAt: new Date().toISOString() });
    if (status !== 404 || remaining.length !== 0) throw new Error(`Cleanup unverified for ${target.name}`);
    target.cleanupComplete = true; save(join(output, "resources.json"), resources);
    console.log(`Verified deletion: ${target.name}`);
  }
  rmSync(resources.privateDirectory, { recursive: true });
  const prior = existsSync(join(here, "failed-startup/cleanup.json")) ? load(join(here, "failed-startup/cleanup.json")) : null;
  save(join(here, "cleanup.json"), { complete: true, secretStateRemoved: true, checkedAt: new Date().toISOString(), checks, priorAttempts: prior ? [prior] : [] });
};

export const run = Effect.tryPromise({
  try: async () => {
    if (action === "dry-run") { console.log(JSON.stringify({ stack: "effect-eval-cost-turn", roles: ["pin", "pin-control"], history: 50, rounds: 7, turns: 10, timing: "DO measured RPC cpuTimeMs; wake/recovery is a separate invocation", locationHint: "wnam", cpuLimitMs: 300000, generation: "same bundle; environment update proves fresh module/runtime before measurement" }, null, 2)); return; }
    if (!account || !token) throw new Error("Cloudflare environment credentials missing");
    if (action === "deploy") await deploy();
    else if (action === "seed") await seed();
    else if (action === "activate") await activate();
    else if (action === "measure") await measure();
    else if (action === "resume") await measure(true);
    else if (action === "telemetry") await telemetry();
    else if (action === "cleanup") await cleanup();
    else if (action === "all") { try { await deploy(); await seed(); await activate(); await measure(); await sleep(15000); await telemetry(); } finally { if (existsSync(join(output, "resources.json"))) await cleanup(); } }
    else throw new Error("Use dry-run | deploy | seed | activate | measure | resume | telemetry | cleanup | all");
  }, catch: (cause) => new RunError({ message: clean(String(cause)) }),
});
if (import.meta.main) NodeRuntime.runMain(run);
