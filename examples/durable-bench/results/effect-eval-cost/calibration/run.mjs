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
      EVAL_COST_WORKER: target.name, EVAL_COST_BUNDLE: target.bundle, EVAL_COST_TOKEN: privateState.token, EVAL_COST_KIND: target.kind ?? "micro" },
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
  save(join(here, "../cleanup.json"), { complete: false, activeRun: run, status: "resources recorded; cleanup pending" });
  const subdomain = (await apiJson("workers/subdomain")).result.subdomain;
  const builds = load(join(here, "build-identities/all.json"));
  for (const [role, buildName] of [["pin", "pin"], ["pin-control", "pin"], ["base", "base"], ["head", "head"], ["base-control", "base"]]) {
    const build = builds.find((item) => item.name === buildName);
    const name = `effect-eval-cost-${run}-${role}`;
    const bundle = join(build.output, "worker.mjs");
    if (hash(readFileSync(bundle)) !== build.bundleSha256) throw new Error("Bundle identity mismatch");
    const existing = await api(`workers/scripts/${name}`);
    if (existing.status !== 404) throw new Error("Worker name already exists or existence check failed");
    const target = { role, name, bundle, url: `https://${name}.${subdomain}.workers.dev`, build, cleanupRequired: true, cleanupComplete: false };
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
const measure = async (pilot = false, resume = false) => {
  open();
  if (resources.targets.length !== 5 || resources.targets.some((target) => !target.identity)) throw new Error("All five deployments must pass readiness before sampling");
  const names = ["empty", "sync", "sync-reused", "gen8", "map8", "flatMap8", "errors-success", "fn-untraced", "fn-traced", "service8", "stream8", "schema-decode", "schema-encode", "sql", "scope", "semaphore", "interrupt-mask", "span", "failpoint", "allocate"];
  const iterationsFor = (name) => name === "allocate" ? 1_048_576 : name === "sql" ? 4096 : ["stream8", "fn-traced", "scope", "span"].includes(name) ? 16384 : 65536;
  const selected = pilot ? ["empty", "sync", "gen8", "fn-traced", "stream8", "sql", "allocate"] : names;
  let schedule = [];
  for (let round = 0; round < (pilot ? 1 : 7); round++) for (const name of selected) for (const scale of (pilot ? [1] : [1, 4])) {
    const targets = round % 2 === 0 ? resources.targets : [...resources.targets].reverse();
    for (const target of targets) for (const mode of round % 2 === 0 ? ["plain", "effect"] : ["effect", "plain"]) {
      schedule.push({ target: target.role, input: { id: `${pilot ? "pilot2" : "measured"}-${round}-${name}-${scale}-${mode}`, case: name, mode, iterations: iterationsFor(name) * scale }, round, scale });
    }
  }
  if (!pilot) for (let round = 0; round < 7; round++) for (const iterations of [4096, 16384, 1_048_576, 4_194_304]) {
    for (const target of round % 2 === 0 ? resources.targets : [...resources.targets].reverse()) {
      schedule.push({ target: target.role, input: { id: `empty-${round}-${iterations}`, case: "empty", mode: "plain", iterations }, round, scale: 0 });
    }
  }
  const planFile = join(output, pilot ? "pilot2-plan.json" : "plan.json");
  if (resume) schedule = load(planFile).schedule;
  else {
    if (existsSync(planFile)) throw new Error("Never repeat a scheduled invocation; use resume for unattempted entries");
    save(planFile, { createdAt: Date.now(), locationHint: "wnam", rounds: pilot ? 1 : 7, schedule });
  }
  const attempted = new Set(["requests.jsonl", "attempted.jsonl"].flatMap((file) => existsSync(join(output, file)) ? readFileSync(join(output, file), "utf8").trim().split("\n").filter(Boolean).map((line) => JSON.parse(line)) : []).filter((row) => row.phase === (pilot ? "pilot" : "measured") && row.input).map((row) => row.target + "|" + row.input.id));
  const expectedCounts = Object.fromEntries(["pin", "base", "head"].map((name) => [name, load(join(here, "counts", `${name}.json`))]));
  for (const item of schedule) {
    if (attempted.has(item.target + "|" + item.input.id)) continue;
    const target = resources.targets.find((target) => target.role === item.target);
    let result;
    try { result = await request(target, `/run?sample=${item.input.id}`, item.input, pilot ? "pilot" : "measured"); }
    catch (error) { console.error(`Retained failed attempt ${target.role}/${item.input.id}: ${clean(String(error))}`); continue; }
    if (result.build !== target.build.buildId || result.fixture !== target.build.fixtureSha256) throw new Error("Build changed during measurement");
    const counted = expectedCounts[target.build.name].find((row) => row.case === item.input.case && row.mode === item.input.mode && row.iterations === item.input.iterations);
    const remainder = item.input.iterations % 256;
    const expected = counted?.checksum ?? ((Math.floor(item.input.iterations / 256) * 32640 + remainder * (remainder - 1) / 2) | 0);
    if (result.checksum !== expected) throw new Error(`Deployed checksum mismatch: ${item.input.id}`);
    if (result.version !== target.identity.version) throw new Error("Deployment version changed during measurement");
    if (item.input.mode === "effect" && item.target === "base-control") console.log(`${pilot ? "Pilot" : "Round " + item.round} ${item.input.case} ×${item.scale}`);
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
const gc = async () => {
  open();
  const rows = [];
  for (const target of resources.targets) {
    const before = await request(target, "/prime-gc", undefined, "gc-prime");
    for (let round = 0; round < 3; round++) {
      await request(target, `/run?sample=gc-${round}`, { id: `gc-${round}`, case: "allocate", mode: "effect", iterations: 4_194_304 }, "gc-pressure");
      rows.push({ target: target.role, before, round, after: await request(target, "/identity", undefined, "gc-audit") });
    }
  }
  save(join(output, "gc.json"), rows);
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
    if (action === "dry-run") { console.log(JSON.stringify({ stack: "effect-eval-cost", roles: ["pin", "pin-control", "base", "head", "base-control"], metric: "deployed DO invocation cpuTimeMs", iterations: "fixed per case; 1× and 4×", rounds: 7, locationHint: "wnam", cpuLimitMs: 300000, state: "private temporary directory", cleanup: "Alchemy destroy and API absence checks" }, null, 2)); return; }
    if (!account || !token) throw new Error("Cloudflare environment credentials missing");
    if (action === "deploy") await deploy();
    else if (action === "readiness") await readiness();
    else if (action === "pilot") await measure(true);
    else if (action === "measure") await measure(false);
    else if (action === "resume") await measure(false, true);
    else if (action === "telemetry") await telemetry();
    else if (action === "gc") await gc();
    else if (action === "cleanup") await cleanup();
    else if (action === "all") {
      try { await deploy(); await measure(true); await measure(false); await gc(); await sleep(15000); await telemetry(); }
      finally { if (existsSync(join(output, "resources.json"))) await cleanup(); }
    }
    else throw new Error("Use dry-run | deploy | readiness | pilot | measure | resume | telemetry | gc | cleanup | all");
  }, catch: (cause) => new RunError({ message: clean(String(cause)) }),
});
if (import.meta.main) NodeRuntime.runMain(run);
