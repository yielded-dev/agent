import { spawn } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import {
  appendFileSync,
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  realpathSync,
  renameSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";

import { NodeRuntime } from "@effect/platform-node";
import { Effect, Schema } from "effect";

import { reference } from "./reference.mjs";

// Fixed disposable experiment, adapted from replay-cpu-deployment.ts and the
// effect-eval-cost/real-turn runner. Native APIs form this controller's I/O edge.
const RunError = Schema.TaggedError()("WakeDeferRunError", { message: Schema.String });
const sourceDirectory = dirname(fileURLToPath(import.meta.url));
const here = process.env.WAKE_DEFER_EVIDENCE_DIR ?? sourceDirectory;
const root = resolve(sourceDirectory, "../../../..");
let action = "dry-run";
const account = process.env.CLOUDFLARE_ACCOUNT_ID;
const token = process.env.CLOUDFLARE_API_TOKEN;
const hash = (value) => createHash("sha256").update(value).digest("hex");
const load = (file) => JSON.parse(readFileSync(file, "utf8"));
const opaqueLog = (text) =>
  String(text).replace(
    /\b[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}\b|\b[0-9a-f]{32}\b|\b[0-9a-f]{64}\b/gi,
    (id) => `[sha256:${hash(id)}]`,
  );
const save = (file, value) => {
  mkdirSync(dirname(file), { recursive: true });
  writeFileSync(file + ".tmp", clean(JSON.stringify(value, null, 2)) + "\n");
  renameSync(file + ".tmp", file);
};
const sleep = (ms) => new Promise((done) => setTimeout(done, ms));
let resources;
let privateState;
const clean = (value) => {
  let text = String(value);
  for (const secret of [account, token, privateState?.token])
    if (secret) text = text.replaceAll(secret, "[redacted]");
  return text;
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
    const detail = response.headers.get("content-type")?.includes("json")
      ? JSON.stringify((await response.json()).errors ?? []).slice(0, 2000)
      : `non-JSON gateway response; ray ${response.headers.get("cf-ray") ?? "unavailable"}`;
    throw new Error(clean(`Cloudflare ${route.split("?")[0]}: ${response.status} ${detail}`));
  }
  const result = await response.json();
  if (!result.success) throw new Error(clean(JSON.stringify(result.errors)));
  return result;
};
const listAll = async (route) => {
  const all = [];
  const seen = new Set();
  for (let page = 1; page <= 100; page++) {
    const data = await apiJson(`${route}?page=${page}&per_page=100`);
    if (!Array.isArray(data.result)) throw new Error("Resource listing did not return an array");
    for (const item of data.result) {
      if (!item.id || seen.has(item.id))
        throw new Error("Resource listing repeated or omitted identity; completeness unproven");
      seen.add(item.id);
    }
    all.push(...data.result);
    if (data.result.length < 100 || page === data.result_info?.total_pages) {
      if (data.result_info?.total_count > all.length) throw new Error("Resource listing truncated");
      return all;
    }
  }
  throw new Error("Resource listing bound exceeded");
};
const listNamespaces = () => listAll("workers/durable_objects/namespaces");
const listWorkers = () => listAll("workers/scripts");
const prefixedResources = async () => ({
  workers: (await listWorkers())
    .filter((item) => item.id.startsWith("wake-defer"))
    .map(({ id }) => id),
  namespaces: (await listNamespaces())
    .filter((item) => item.script?.startsWith("wake-defer") || item.name?.startsWith("wake-defer"))
    .map(({ name, script }) => ({ name, script })),
});
const privateDirectory = () => {
  const path = resources.privateDirectory;
  if (
    !path?.startsWith("/private/tmp/wake-defer-") ||
    realpathSync(path) !== path ||
    !lstatSync(path).isDirectory() ||
    (lstatSync(path).mode & 0o777) !== 0o700
  )
    throw new Error("Expected a private mode-700 wake-defer directory outside the checkout");
  return path;
};
const open = async () => {
  resources = load(join(here, "resources.json"));
  if (resources.accountDigest !== hash(account))
    throw new Error("Account differs from recorded deployment ownership");
  const name = (await apiJson("")).result.name;
  if (name !== resources.accountName)
    throw new Error("Cloudflare account name differs from deployment record");
  privateState = load(join(privateDirectory(), "private.json"));
  if (privateState.account !== account) throw new Error("Private account ownership mismatch");
};
const init = async () => {
  if (existsSync(join(here, "resources.json")))
    throw new Error("Resources already recorded; never initialize over an existing run");
  const accountName = (await apiJson("")).result.name;
  if (accountName !== "Danieljmerwe@gmail.com's Account")
    throw new Error("Unexpected Cloudflare account; initialization refused");
  const existing = await prefixedResources();
  if (existing.workers.length || existing.namespaces.length)
    throw new Error("Pre-existing wake-defer resources found; do not assume ownership");
  const privateDirectory = mkdtempSync("/private/tmp/wake-defer-");
  chmodSync(privateDirectory, 0o700);
  symlinkSync(join(root, "node_modules"), join(privateDirectory, "node_modules"));
  writeFileSync(join(privateDirectory, "package.json"), '{"private":true,"type":"module"}\n');
  privateState = { token: randomBytes(32).toString("hex"), account };
  writeFileSync(join(privateDirectory, "private.json"), JSON.stringify(privateState), {
    mode: 0o600,
  });
  resources = {
    run: randomBytes(4).toString("hex"),
    accountName,
    accountDigest: hash(account),
    privateDirectory,
    startedAt: Date.now(),
    targets: [],
  };
  save(join(here, "resources.json"), resources);
  save(join(here, "cleanup.json"), {
    complete: false,
    accountName,
    status: "initialized; cleanup verification pending",
  });
  console.log(JSON.stringify({ accountName, initialized: true, existing }));
};
const alchemy = (target, operation, adopt = false) =>
  new Promise((done, fail) => {
    const child = spawn(
      "vp",
      ["exec", "alchemy", operation, join(here, "stack.ts"), "--stage", target.name, "--yes", ...(adopt ? ["--adopt"] : [])],
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
          WAKE_DEFER_WORKER: target.name,
          WAKE_DEFER_BUNDLE: target.bundle,
          WAKE_DEFER_TOKEN: privateState.token,
          WAKE_DEFER_KIND: target.kind,
          WAKE_DEFER_PHASE: target.phase ?? "seed",
          WAKE_DEFER_PROVIDER: target.provider ?? "",
          WAKE_DEFER_TARGET: target.targetUrl ?? "",
          WAKE_DEFER_BUILD_MODE: target.build.mode,
          WAKE_DEFER_BUILD_ID: target.build.bundleSha256,
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
      mkdirSync(join(here, "alchemy"), { recursive: true });
      appendFileSync(
        join(here, "alchemy", `${target.name}-${operation}.log`),
        opaqueLog(clean(captured)),
      );
      if (code === 0) done();
      else
        fail(
          new Error(`Alchemy ${operation} ${target.name} exited ${code}; inspect sanitized log`),
        );
    });
  });
const deployOne = async (role, buildName, provider = "", targetUrl = "") => {
  const build = load(join(here, "build-identities/all.json")).find(
    (item) => item.name === buildName,
  );
  if (!build) throw new Error(`No build for ${buildName}`);
  const name = `wake-defer-${resources.run}-${role}`;
  const kind = buildName.startsWith("network-") ? "network" : buildName;
  const recorded = resources.targets.filter((item) => item.role === role);
  if (recorded.length) {
    const existing = recorded[0];
    if (
      recorded.length !== 1 || existing.name !== name || existing.kind !== kind ||
      existing.build.name !== buildName || existing.build.bundleSha256 !== build.bundleSha256 ||
      existing.provider !== provider || existing.targetUrl !== targetUrl || existing.phase !== "seed" ||
      existing.cleanupRequired !== true || existing.cleanupComplete !== false ||
      hash(readFileSync(existing.bundle)) !== build.bundleSha256
    ) throw new Error(`Recorded ${role} does not match the archived build and requested configuration`);
    // Resume only the owned, byte-identical upload. A 404 or mismatch fails;
    // never replace its ownership record or silently redeploy it.
    await verifyUpload(existing);
    console.log(`Verified existing ${role}: sha256 ${build.bundleSha256}; no redeploy`);
    return existing;
  }
  const bundle = build.output;
  if (hash(readFileSync(bundle)) !== build.bundleSha256)
    throw new Error("Local bundle digest mismatch");
  if ((await api(`workers/scripts/${name}`)).status !== 404)
    throw new Error("Worker already exists or existence query failed");
  const subdomain = (await apiJson("workers/subdomain")).result.subdomain;
  const target = {
    role,
    kind,
    name,
    bundle,
    build,
    phase: "seed",
    provider,
    targetUrl,
    url: `https://${name}.${subdomain}.workers.dev`,
    deployedAt: Date.now(),
    cleanupRequired: true,
    cleanupComplete: false,
  };
  resources.targets.push(target);
  save(join(here, "resources.json"), resources);
  await alchemy(target, "deploy");
  await verifyUpload(target);
  console.log(`Deployed ${role}: ${build.bundleBytes} bytes; sha256 ${build.bundleSha256}`);
  return target;
};
const verifyUpload = async (target) => {
  const uploaded = await api(`workers/scripts/${target.name}`);
  if (!uploaded.ok) throw new Error(`Cannot verify uploaded module: HTTP ${uploaded.status}`);
  const expected = readFileSync(target.bundle);
  const contentType = uploaded.headers.get("content-type") ?? "";
  let exact;
  if (contentType.startsWith("multipart/")) {
    const form = await new Response(uploaded.body, {
      headers: { "content-type": contentType.replace(/^multipart\/[^;]+/, "multipart/form-data") },
    }).formData();
    // CF retrieval can omit filename, so module parts decode as strings.
    const matches = await Promise.all(
      [...form.values()].map(async (part) =>
        (typeof part === "string" ? Buffer.from(part, "utf8") : Buffer.from(await part.arrayBuffer())).equals(expected)),
    );
    exact = matches.filter(Boolean).length === 1;
  } else exact = Buffer.from(await uploaded.arrayBuffer()).equals(expected);
  if (!exact) throw new Error("Uploaded module differs from exact archived bytes");
  const settings = (await apiJson(`workers/scripts/${target.name}/settings`)).result;
  const deployment = (await apiJson(`workers/scripts/${target.name}/deployments`)).result
    .deployments[0];
  if (deployment?.versions?.length !== 1 || deployment.versions[0].percentage !== 100)
    throw new Error("Expected one fully active Worker version");
  const rawVersion = deployment.versions[0].version_id;
  target.expectedVersion = hash(rawVersion);
  const script = (await listWorkers()).find((item) => item.id === target.name);
  if (!script?.tag)
    throw new Error("Deployed Worker metadata is missing its version lookup identity");
  // The beta metadata endpoint returns HTTP 500 for the targeted-placement driver.
  // Its standard version endpoint still verifies the active version and bindings.
  const standardVersion = target.kind === "driver";
  const version = (await apiJson(standardVersion
    ? `workers/scripts/${target.name}/versions/${rawVersion}`
    : `workers/workers/${script.tag}/versions/${rawVersion}`)).result;
  if (hash(version.id) !== target.expectedVersion)
    throw new Error("Startup metadata does not match the active version");
  const bindings = standardVersion ? version.resources?.bindings : version.bindings;
  const buildId = bindings?.find(
    (binding) => binding.type === "plain_text" && binding.name === "BUILD_ID",
  )?.text;
  if (buildId !== target.build.bundleSha256)
    throw new Error("Active version is not bound to the archived build");
  privateState.opaqueIds = [
    ...new Set([...(privateState.opaqueIds ?? []), rawVersion, script.tag]),
  ];
  writeFileSync(join(privateDirectory(), "private.json"), JSON.stringify(privateState), {
    mode: 0o600,
  });
  target.startupTimeMs =
    typeof version.startup_time_ms === "number" ? version.startup_time_ms : null;
  target.startupEvidence = {
    version: hash(version.id),
    number: version.number,
    startupTimeMs: target.startupTimeMs,
    metadataEndpoint: standardVersion ? "scripts/versions" : "workers/versions",
    startupTimeUnavailableReason: standardVersion ? "Beta version endpoint returned HTTP 500; standard version response has no startup_time_ms" : null,
    compatibilityDate: standardVersion ? version.resources?.script_runtime?.compatibility_date : version.compatibility_date,
    limits: standardVersion ? version.resources?.script_runtime?.limits : version.limits,
    buildId,
    capturedAt: Date.now(),
  };
  target.uploadedModuleSha256 = target.build.bundleSha256;
  target.settings = {
    placement: settings.placement,
    limits: settings.limits,
    compatibilityDate: settings.compatibility_date,
    compatibilityFlags: settings.compatibility_flags,
    observability: settings.observability,
  };
  if (target.kind === "driver") {
    const placement = version.resources?.script?.placement;
    if (placement?.mode !== "targeted" || placement.target?.length !== 1 ||
        placement.target[0].type !== "region" || placement.target[0].region !== "aws:us-west-1")
      throw new Error("Targeted driver placement not confirmed by the active Cloudflare version");
    target.settings.placement = {
      mode: placement.mode,
      region: placement.target[0].region,
      evidence: "active version resources.script.placement.target; opaque region ID omitted",
    };
  }
  target.namespaces = (await listNamespaces())
    .filter((item) => item.script === target.name)
    .map(({ name, script }) => ({ name, script }));
  save(join(here, "resources.json"), resources);
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
  appendFileSync(join(here, "attempted.jsonl"), JSON.stringify(attempt) + "\n");
  let response;
  let recorded = false;
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
      laptopLatencyMs: performance.now() - start,
      status: response.status,
      cfRay: response.headers.get("cf-ray"),
      cfPlacement: response.headers.get("cf-placement"),
      response: (() => {
        try {
          return scrubReceipt(JSON.parse(raw));
        } catch {
          return { error: opaqueLog(raw.slice(0, 2000)) };
        }
      })(),
    };
    appendFileSync(join(here, "requests.jsonl"), clean(JSON.stringify(row)) + "\n");
    recorded = true;
    if (!response.ok)
      throw new Error(`Worker ${target.role} ${url.pathname} returned ${response.status}`);
    return row;
  } catch (cause) {
    if (!recorded)
      appendFileSync(
        join(here, "requests.jsonl"),
        clean(
          JSON.stringify({
            ...attempt,
            endedAt: Date.now(),
            laptopLatencyMs: performance.now() - start,
            status: response?.status ?? null,
            error: opaqueLog(String(cause)),
            cause: opaqueLog(String(cause?.cause)),
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
  const found = resources.targets.find((item) => item.role === role);
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
    const existing = join(here, `telemetry-${item.role}.json`);
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
      const sanitized = JSON.parse(clean(JSON.stringify(events.events)))
        .map(sanitizeTelemetry)
        .map(scrub);
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
      // Preserve every accepted window if a later API request fails.
      save(existing, {
        worker: item.name,
        accountName: resources.accountName,
        events: [...all.values()],
        polls: previous.polls,
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
      accountName: resources.accountName,
      events: [...all.values()],
      polls: previous.polls,
    });
    console.log(`Telemetry ${item.role}: ${all.size} distinct events`);
  }
};
const cleanup = async () => {
  await open();
  const checks = [];
  const errors = [];
  const previous = existsSync(join(here, "cleanup.json")) ? load(join(here, "cleanup.json")) : {};
  const retainError = (step, cause) =>
    errors.push({ step, at: Date.now(), message: opaqueLog(clean(String(cause))) });
  for (const item of [...resources.targets].reverse()) {
    try {
      if (!item.cleanupComplete) await alchemy(item, "destroy");
    } catch (cause) {
      retainError("destroy " + item.name, cause);
    }
    try {
      const workerGetStatus = (await api(`workers/scripts/${item.name}`)).status;
      const remaining = (await listNamespaces()).filter((ns) => ns.script === item.name);
      checks.push({
        worker: item.name,
        workerGetStatus,
        remainingNamespaces: remaining.map(({ name, script }) => ({ name, script })),
        checkedAt: new Date().toISOString(),
      });
      item.cleanupComplete = workerGetStatus === 404 && remaining.length === 0;
      save(join(here, "resources.json"), resources);
      if (!item.cleanupComplete) retainError("verify " + item.name, "Worker or namespace remains");
      else console.log(`Verified deletion: ${item.name}`);
    } catch (cause) {
      retainError("verify " + item.name, cause);
    }
    // A failed target never prevents attempting the other owned stacks.
    save(join(here, "cleanup.json"), {
      complete: false,
      accountName: resources.accountName,
      checks,
      errors: [...(previous.errors ?? []), ...errors],
      secretStateRemoved: false,
    });
  }
  let remaining;
  try {
    remaining = await prefixedResources();
  } catch (cause) {
    retainError("prefix verification", cause);
  }
  const resourcesVerified =
    resources.targets.every((item) => item.cleanupComplete) &&
    remaining?.workers.length === 0 &&
    remaining?.namespaces.length === 0;
  if (!resourcesVerified)
    retainError(
      "complete deletion verification",
      "Resource absence not proven; private state retained",
    );
  let finalTelemetryTailCollected = false;
  try {
    await telemetry(undefined, true);
    finalTelemetryTailCollected = true;
  } catch (cause) {
    retainError("final telemetry", cause);
  }
  // Include the destroy logs while the exact private token is still available
  // for comparison. Nothing in this scan records credential values.
  let secretScanPassed = false;
  try {
    await scanSecrets();
    secretScanPassed = true;
  } catch (cause) {
    retainError("secret scan", cause);
  }
  const complete = resourcesVerified && finalTelemetryTailCollected && secretScanPassed;
  if (complete) rmSync(privateDirectory(), { recursive: true });
  save(join(here, "cleanup.json"), {
    complete,
    resourcesVerified,
    accountName: resources.accountName,
    accountDigest: resources.accountDigest,
    checkedAt: new Date().toISOString(),
    checks,
    remaining,
    errors: [...(previous.errors ?? []), ...errors],
    finalTelemetryTailCollected,
    secretScanPassed,
    secretStateRemoved: complete,
  });
  if (!complete)
    throw new Error(
      "Cleanup proof incomplete; inspect cleanup.json and retry cleanup using retained private state",
    );
};

// New opaque UUID fields fail safe without rehashing existing SHA-256 identities.
// Synthetic provider request IDs remain intact for the receipt/log join.
const scrubReceipt = (value, key = "") => {
  if (Array.isArray(value)) return value.map((item) => scrubReceipt(item, key));
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value)
        .filter(([name]) => !/^(?:accountId|account_id|authorization|cookie|headers)$/i.test(name))
        .map(([name, item]) => [name, scrubReceipt(item, name)]),
    );
  if (typeof value === "string") {
    if (/^(?:[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}|[0-9a-f]{32})$/i.test(value))
      return hash(value);
    if (/error|stack|message/i.test(key)) return opaqueLog(value);
  }
  return value;
};
const sanitizeTelemetry = (event) => {
  const workers = event.$workers;
  const metadata = event.$metadata;
  if (workers?.durableObjectId) workers.durableObjectId = hash(workers.durableObjectId);
  if (workers?.scriptVersion?.id) workers.scriptVersion.id = hash(workers.scriptVersion.id);
  if (metadata)
    for (const key of ["id", "requestId", "traceId", "spanId", "parentSpanId"]) {
      if (typeof metadata[key] === "string") metadata[key] = hash(metadata[key]);
    }
  // Harness source fields are already hashed in the Object. Other runtime logs
  // can contain native identifiers in annotations or diagnostics.
  const scrub = (value, key = "") => {
    if (Array.isArray(value)) return value.map((item) => scrub(item));
    if (value && typeof value === "object")
      return Object.fromEntries(
        Object.entries(value)
          .filter(
            ([name]) => !/^(?:accountId|account_id|headers|authorization|cookie)$/i.test(name),
          )
          .map(([name, item]) => [name, scrub(item, name)]),
      );
    if (typeof value === "string") {
      if (
        /^(?:objectId|durableObjectId|incarnation|versionId|namespaceId|threadId|submissionId|attemptId|producerId|ownershipToken)$/.test(
          key,
        )
      )
        return hash(value);
      if (/error|stack|message/i.test(key)) return opaqueLog(value);
    }
    return value;
  };
  if (event.source && !event.source.wakeDefer) event.source = scrub(event.source);
  if (workers?.exceptions) workers.exceptions = scrub(workers.exceptions);
  return scrubReceipt(event);
};

const fingerprints = { 50: "b017b487524e44a4", 250: "dcea9f30b0917245" };
const networkUrl = (path, cohort, sample, variant = "baseline") =>
  `${path}?${new URLSearchParams({
    target: "yielded",
    history: String(cohort.history),
    ttftMs: String(cohort.ttftMs),
    chunkDelayMs: String(cohort.ttftMs === 400 ? 10 : 0),
    object: cohort.object,
    sample,
    variant,
  })}`;
const plan = (probe = false) => {
  const cohorts = [];
  for (const history of probe ? [50] : [50, 250])
    for (const ttftMs of [0, 400])
      for (let index = 0; index < (probe ? 1 : 7); index++) {
        const run = resources?.cohortRun ?? resources?.run ?? "preview";
        const object = `wake-defer-${run}-${probe ? "probe" : "ab"}-h${history}-d${ttftMs}-o${index}`;
        const randomSeed = Number.parseInt(hash(object).slice(0, 8), 16);
        // Place lookup #97 (h50) / #194 (h250) in the prelude. All eight comparison
        // turns then have eight 256-byte results in both arms; canonical history grows.
        const settlingTurns = history === 50 ? 8 : 4;
        const schedule = Array.from({ length: settlingTurns }, (_, ordinal) => ({
          sample: `m${ordinal}`,
          mode: "warmup",
          repeat: null,
          variant: ordinal % 2 ? "candidate" : "baseline",
        }));
        const blocks = shuffle(
          [0, 1].flatMap((repeat) => ["warm", "cold"].map((mode) => ({ mode, repeat }))),
          randomSeed,
        );
        for (const [block, spec] of blocks.entries())
          for (const variant of shuffle(["baseline", "candidate"], randomSeed + block * 397 + 31)) {
            schedule.push({ sample: `m${schedule.length}`, ...spec, variant });
          }
        cohorts.push({ history, ttftMs, index, object, randomSeed, settlingTurns, schedule });
      }
  return {
    mode: probe ? "baseline-probe" : "ab",
    objectsPerCell: probe ? 1 : 7,
    locationHint: "wnam",
    driverPlacement: { mode: "targeted", region: "aws:us-west-1" },
    repeats: 2,
    expectedSeeds: fingerprints,
    randomization:
      "Two paired repeats per temperature per Object; shuffle temperature/repeat blocks and arm order within each block",
    cohorts: shuffle(cohorts, Number.parseInt(hash(resources?.cohortRun ?? resources?.run ?? "preview").slice(0, 8), 16)),
  };
};

const deploy = async (mode) => {
  await open();
  const provider = await deployOne("provider", "provider");
  await readiness(provider, "/echo");
  const network = await deployOne("network", `network-${mode}`, `${provider.url}/v1`);
  await readiness(
    network,
    "/health",
    undefined,
    (row) =>
      row.response.version === network.expectedVersion &&
      row.response.buildId === network.build.bundleSha256,
  );
  const driver = await deployOne("driver", "driver", "", network.url);
  await readiness(
    driver,
    "/health",
    undefined,
    (row) =>
      row.response.version === driver.expectedVersion &&
      row.response.buildId === driver.build.bundleSha256,
  );
};
const activate = async () => {
  await open();
  const network = target("network");
  network.phase = "measure";
  network.activatedAt = Date.now();
  save(join(here, "resources.json"), resources);
  await alchemy(network, "deploy");
  await verifyUpload(network);
  await readiness(
    network,
    "/health",
    undefined,
    (row) =>
      row.response.phase === "measure" &&
      row.response.version === network.expectedVersion &&
      row.response.buildId === network.build.bundleSha256,
  );
};
const refreshDeployments = async () => {
  await open();
  if (lines("attempted.jsonl").some((row) => ["measure", "warmup", "probe"].includes(row.phase)))
    throw new Error("Refresh is limited to setup before measured inputs");
  for (const item of resources.targets) {
    if (hash(readFileSync(item.bundle)) !== item.build.bundleSha256)
      throw new Error("Recorded bundle changed before setup refresh");
    await alchemy(item, "deploy");
    await verifyUpload(item);
    if (!item.settings.compatibilityFlags?.includes("global_fetch_strictly_public"))
      throw new Error("Worker-to-Worker public fetch flag not verified");
    await readiness(item, item.kind === "provider" ? "/echo" : "/health", undefined,
      (row) => item.kind === "provider" || row.response.version === item.expectedVersion);
    console.log(`Refreshed ${item.role}: public Worker-to-Worker fetch verified in settings`);
  }
};
// Session recovery only: all preceding receipts are archived before this action.
// Reuse the owned resources through Alchemy, rotate the lost disposable token,
// and require fresh Objects instead of replaying uncertain previous inputs.
const recoverState = async () => {
  resources = load(join(here, "resources.json"));
  if (resources.accountDigest !== hash(account) ||
      (await apiJson("")).result.name !== resources.accountName)
    throw new Error("Recovery account ownership mismatch");
  if (resources.cohortRun !== undefined || existsSync(resources.privateDirectory) ||
      existsSync(join(here, "requests.jsonl")) ||
      !existsSync(join(here, "interrupted-run", "resources.json")))
    throw new Error("Recover only missing private state after archiving the preceding run");
  const directory = mkdtempSync("/private/tmp/wake-defer-");
  chmodSync(directory, 0o700);
  symlinkSync(join(root, "node_modules"), join(directory, "node_modules"));
  writeFileSync(join(directory, "package.json"), '{"private":true,"type":"module"}\n');
  privateState = { token: randomBytes(32).toString("hex"), account };
  writeFileSync(join(directory, "private.json"), JSON.stringify(privateState), { mode: 0o600 });
  resources.privateDirectory = directory;
  resources.cohortRun = `${resources.run}-r2`;
  resources.recoveredAt = Date.now();
  for (const item of resources.targets) {
    if (!item.name.startsWith(`wake-defer-${resources.run}-`) ||
        !(await api(`workers/scripts/${item.name}`)).ok)
      throw new Error("Recovery target is not a recorded live task Worker");
    const bytes = gunzipSync(readFileSync(join(here, "build-identities",
      `${item.build.name}-${item.build.bundleSha256}.mjs.gz`)));
    if (hash(bytes) !== item.build.bundleSha256)
      throw new Error("Archived recovery bundle digest mismatch");
    item.bundle = join(directory, `${item.role}.mjs`);
    writeFileSync(item.bundle, bytes);
    item.phase = "seed";
  }
  save(join(here, "resources.json"), resources);
  for (const item of resources.targets) {
    await alchemy(item, "deploy", true);
    await verifyUpload(item);
    await readiness(item, item.kind === "provider" ? "/echo" : "/health", undefined,
      (row) => item.kind === "provider" || row.response.version === item.expectedVersion);
    console.log(`Recovered ${item.role} through Alchemy with the archived bundle`);
  }
};
const lines = (name) =>
  existsSync(join(here, name))
    ? readFileSync(join(here, name), "utf8").split("\n").filter(Boolean).map(JSON.parse)
    : [];
const seed = async (probe) => {
  await open();
  if (target("network").phase !== "seed") throw new Error("Seed before activate");
  const name = probe ? "probe" : "network";
  const planFile = join(here, `${name}-plan.json`);
  const selected = existsSync(planFile) ? load(planFile) : plan(probe);
  save(planFile, selected);
  for (const cohort of selected.cohorts) {
    const previous = lines("requests.jsonl").filter(
      (row) => row.phase === `${name}-seed` && row.object === cohort.object,
    );
    const attempts = lines("attempted.jsonl").filter(
      (row) => row.phase === `${name}-seed` && row.object === cohort.object,
    );
    if (
      attempts.length !== previous.length ||
      previous.some((row) => row.status !== 200 || !row.response.ok)
    )
      throw new Error(
        `Uncertain seed outcome for ${cohort.object}; retain it and use a fresh Object, never replay`,
      );
    let from = 0;
    for (const row of previous) {
      if (row.response.from !== from || row.response.to <= from)
        throw new Error("Seed receipt gap");
      from = row.response.to;
    }
    let last = previous.at(-1);
    while (from < cohort.history) {
      const to = Math.min(cohort.history, from + 10);
      last = await request(
        target("driver"),
        networkUrl("/seed", cohort, `seed-${from}-${to}`),
        { from, to },
        `${name}-seed`,
        cohort.object,
      );
      if (!last.response.ok || last.response.from !== from || last.response.to !== to)
        throw new Error("Invalid seed receipt");
      from = to;
    }
    if (last.response.fingerprint !== fingerprints[cohort.history])
      throw new Error("Golden seed fingerprint mismatch");
    cohort.seed = {
      objectId: last.response.objectId,
      fingerprint: last.response.fingerprint,
      version: last.response.version,
    };
    save(planFile, selected);
    await request(
      target("driver"),
      networkUrl("/cold", cohort, "seed-release"),
      {},
      "seed-release",
      cohort.object,
    );
    console.log(`Seeded ${cohort.object}: ${last.response.fingerprint}`);
  }
};

let expected;
const admit = (row, cohort, cold, expectedIncarnation) => {
  expected ??= reference();
  const value = row.response;
  const network = target("network");
  const reasons = [];
  const query = new URL(row.path, "https://wake-defer").searchParams;
  if (
    !value.ok ||
    value.seedFingerprint !== fingerprints[cohort.history] ||
    value.version !== network.expectedVersion ||
    value.buildId !== network.build.bundleSha256 ||
    value.objectId !== cohort.seed?.objectId ||
    value.object !== cohort.object ||
    value.sample !== row.sample ||
    value.variant !== query.get("variant")
  )
    reasons.push("Measured seed/build/identity admission failed");
  if (
    value.mechanismTruncated ||
    value.calls?.length !== 9 ||
    value.calls.some(
      (call, index) =>
        call.call !== index ||
        call.status !== 200 ||
        !call.sseDone ||
        call.error ||
        !call.providerReceipt,
    )
  )
    reasons.push("Incomplete nine-call provider or mechanism receipt");
  for (const [index, call] of (value.calls ?? []).entries()) {
    const p = call.providerReceipt;
    if (
      !p ||
      p.requestId !== call.providerRequest ||
      p.fingerprint !== expected[cohort.history]?.turns[row.sample]?.[index] ||
      p.call !== index ||
      p.sample !== row.sample ||
      p.object !== cohort.object ||
      p.variant !== value.variant ||
      p.history !== cohort.history ||
      p.ttftMs !== cohort.ttftMs ||
      p.chunkDelayMs !== (cohort.ttftMs === 400 ? 10 : 0) ||
      p.error !== null ||
      p.firstByteMs < p.arrivalMs ||
      p.endMs < p.firstByteMs
    )
      reasons.push(`Provider identity/transcript/clock receipt failed at call ${index}`);
  }
  if (new Set((value.calls ?? []).map((call) => call.providerRequest)).size !== 9)
    reasons.push("Provider requests are not unique");
  if (!Number.isFinite(value.driver?.workerLatencyMs) || value.driver.workerLatencyMs < 0)
    reasons.push("Deployed driver clock receipt missing");
  if (
    cold &&
    (!cold.response.ok ||
      !cold.response.coldRequested ||
      value.objectId !== cold.response.before?.objectId ||
      value.incarnation === cold.response.before?.incarnation ||
      !value.entry?.firstHarnessRequest ||
      value.entry.priorAlarmStarts !== 0 ||
      value.entry.activeAlarmIds?.length !== 0)
  )
    reasons.push("Cold same-Object/new-incarnation/first-entry proof failed");
  if (expectedIncarnation && value.incarnation !== expectedIncarnation)
    reasons.push("Warm incarnation changed");
  appendFileSync(
    join(here, "admissions.jsonl"),
    JSON.stringify({
      object: cohort.object,
      sample: row.sample,
      phase: row.phase,
      passed: reasons.length === 0,
      reasons,
      coldSample: cold?.sample,
      expectedIncarnation,
    }) + "\n",
  );
  if (reasons.length) throw new Error(`${cohort.object}/${row.sample}: ${reasons.join("; ")}`);
};
const probe = async () => {
  await open();
  const selected = load(join(here, "probe-plan.json"));
  if (target("network").phase !== "measure") throw new Error("Activate first");
  if (lines("attempted.jsonl").some((row) => row.phase === "probe"))
    throw new Error("Probe already attempted; never replay a measured input");
  for (const cohort of selected.cohorts) {
    const cold = await request(
      target("driver"),
      networkUrl("/cold", cohort, "cold-m0"),
      {},
      "probe-cold",
      cohort.object,
    );
    let incarnation;
    for (let index = 0; index < 3; index++) {
      const row = await request(
        target("driver"),
        networkUrl("/run", cohort, `m${index}`),
        {},
        "probe",
        cohort.object,
      );
      admit(row, cohort, index === 0 ? cold : undefined, index === 0 ? undefined : incarnation);
      incarnation = row.response.incarnation;
      await request(
        target("driver"),
        networkUrl("/drain", cohort, `tail-m${index}`),
        undefined,
        "probe-tail",
        cohort.object,
      );
    }
    await request(
      target("driver"),
      networkUrl("/cold", cohort, "release"),
      {},
      "release",
      cohort.object,
    );
  }
};

const measure = async () => {
  await open();
  if (target("network").phase !== "measure" || target("network").build.mode !== "ab")
    throw new Error("Activate an A/B network build first");
  const selected = load(join(here, "network-plan.json"));
  const completedPath = join(here, "completed.json");
  const completed = existsSync(completedPath) ? load(completedPath) : [];
  const attempts = lines("attempted.jsonl");
  for (const cohort of selected.cohorts) {
    if (completed.includes(cohort.object)) continue;
    if (
      attempts.some(
        (row) => ["measure", "warmup"].includes(row.phase) && row.object === cohort.object,
      )
    )
      throw new Error(`Interrupted cohort ${cohort.object}; inputs cannot be replayed`);
    if (!cohort.seed) throw new Error(`Unseeded Object: ${cohort.object}`);
    let incarnation;
    for (const item of cohort.schedule) {
      const cold =
        item.mode === "cold"
          ? await request(
              target("driver"),
              networkUrl("/cold", cohort, `cold-${item.sample}`, item.variant),
              {},
              "cold-reset",
              cohort.object,
            )
          : undefined;
      const phase = item.mode === "warmup" ? "warmup" : "measure";
      const row = await request(
        target("driver"),
        networkUrl("/run", cohort, item.sample, item.variant),
        {},
        phase,
        cohort.object,
      );
      admit(row, cohort, cold, item.mode === "cold" ? undefined : incarnation);
      incarnation = row.response.incarnation;
      await request(
        target("driver"),
        networkUrl("/drain", cohort, `tail-${item.sample}`, item.variant),
        undefined,
        "tail",
        cohort.object,
      );
    }
    await request(
      target("driver"),
      networkUrl("/cold", cohort, "release"),
      {},
      "release",
      cohort.object,
    );
    completed.push(cohort.object);
    save(completedPath, completed);
    console.log(`Measured ${completed.length}/${selected.cohorts.length}: ${cohort.object}`);
  }
};

export const run = Effect.tryPromise({
  try: async () => {
    const args = process.argv.slice(2).filter((arg) => arg !== "--");
    action = args[0] ?? "dry-run";
    if (["dry-run", "--help", "help"].includes(action)) {
      console.log(
        JSON.stringify(
          {
            stack: "wake-defer",
            localBuild: "vp exec node examples/durable-bench/results/wake-defer/build.mjs ab",
            credentials:
              "vp exec direnv exec . vp exec node examples/durable-bench/results/wake-defer/run.mjs <action>",
            actions: [
              "init",
              "deploy-baseline",
              "deploy-ab",
              "seed-probe",
              "seed",
              "activate",
              "refresh-deployments",
              "recover-state",
              "probe",
              "measure",
              "telemetry",
              "secret-scan",
              "cleanup",
            ],
            accountName: "Danieljmerwe@gmail.com's Account",
            locationHint: "wnam",
            driverPlacement: { mode: "targeted", region: "aws:us-west-1" },
            remoteActionsRequireOperatorExecution: true,
            plan: plan(false),
          },
          null,
          2,
        ),
      );
      return;
    }
    if (!account || !token) throw new Error("Cloudflare credentials missing; use direnv exec .");
    if (action === "init") await init();
    else if (action === "deploy-baseline") await deploy("baseline");
    else if (action === "deploy-ab") await deploy("ab");
    else if (action === "refresh-deployments") await refreshDeployments();
    else if (action === "recover-state") await recoverState();
    else if (action === "seed-probe") await seed(true);
    else if (action === "seed") await seed(false);
    else if (action === "activate") await activate();
    else if (action === "probe") await probe();
    else if (action === "measure") await measure();
    else if (action === "telemetry") await telemetry(args[1]);
    else if (action === "secret-scan") await scanSecrets();
    else if (action === "cleanup") await cleanup();
    else throw new Error(`Unknown action: ${action}`);
  },
  catch: (cause) => {
    const message = opaqueLog(clean(String(cause)));
    appendFileSync(
      join(here, "controller-errors.jsonl"),
      JSON.stringify({ action, at: Date.now(), message }) + "\n",
    );
    return new RunError({ message });
  },
});
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
  files.push(join(root, "package.json"), join(root, "bun.lock"));
  const secrets = [account, token, privateState.token, ...(privateState.opaqueIds ?? [])];
  const matches = [];
  let gzipFiles = 0;
  for (const file of files) {
    const raw = readFileSync(file);
    const data = file.endsWith(".gz") ? (gzipFiles++, gunzipSync(raw)) : raw;
    if (secrets.some((secret) => data.includes(Buffer.from(secret))))
      matches.push(file.replace(root + "/", ""));
  }
  save(join(here, "secret-scan.json"), {
    accountName: resources.accountName,
    checkedAt: new Date().toISOString(),
    filesChecked: files.length,
    gzipFilesChecked: gzipFiles,
    knownCredentialValuesChecked: secrets.length,
    matches,
    passed: matches.length === 0,
    scope:
      "Exact account ID, Cloudflare API token, private benchmark token, and known opaque upload IDs; raw and decompressed result artifacts plus root manifests/task config",
  });
  if (matches.length)
    throw new Error(`Known credentials found in ${matches.length} evidence files; do not commit`);
  console.log(
    `Credential scan passed: ${files.length} files, including ${gzipFiles} compressed artifacts`,
  );
};
if (import.meta.url === `file://${process.argv[1]}`) NodeRuntime.runMain(run);
