import { spawn, execFileSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import {
  appendFileSync,
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
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

import { fingerprint, history, next, payload, turn } from "../../src/plan.ts";

// Fixed disposable experiment, adapted from replay-cpu-deployment.ts and the
// effect-eval-cost/real-turn runner. Native APIs form this controller's I/O edge.
const RunError = Schema.TaggedError()("RebenchRunError", { message: Schema.String });
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
let resources;
let privateState;

const clean = (value) => {
  let text = String(value);

  for (const secret of [account, token, privateState?.token])
    if (secret) text = text.replaceAll(secret, "[redacted]");

  return text
    .replace(
      /workers\/workers\/[0-9a-f]{32}\/versions/g,
      "workers/workers/[redacted-worker-id]/versions",
    )
    .replace(/(Network(?:Pi|Yielded|Actor|Thread)DO: ['"])[0-9a-f]{32}(['"])/g, "$1[redacted-namespace-id]$2");
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
    .filter((item) => item.id.startsWith("rebench"))
    .map(({ id }) => id),
  namespaces: (await listNamespaces())
    .filter((item) => item.script?.startsWith("rebench") || item.name?.startsWith("rebench"))
    .map(({ name, script }) => ({ name, script })),
});

const open = async () => {
  resources = load(join(here, "resources.json"));
  if (resources.accountDigest !== hash(account))
    throw new Error("Account differs from recorded deployment ownership");
  const name = (await apiJson("")).result.name;

  if (name !== resources.accountName)
    throw new Error("Cloudflare account name differs from deployment record");
  privateState = load(join(resources.privateDirectory, "private.json"));
  if (privateState.account !== account) throw new Error("Private account ownership mismatch");
};

const init = async () => {
  if (existsSync(join(here, "resources.json")))
    throw new Error("Resources already recorded; never initialize over an existing run");
  const accountName = (await apiJson("")).result.name;
  if (accountName !== "Danieljmerwe@gmail.com's Account")
    throw new Error("Expected the personal Cloudflare account");
  const existing = await prefixedResources();

  if (existing.workers.length || existing.namespaces.length)
    throw new Error("Pre-existing rebench resources found; do not assume ownership");
  const privateDirectory = mkdtempSync("/private/tmp/rebench-");

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
    baselineRevision: execFileSync("vp", ["exec", "git", "rev-parse", "origin/main"], {
      cwd: root,
      encoding: "utf8",
    }).trim(),
  };
  save(join(here, "resources.json"), resources);
  save(join(here, "cleanup.json"), {
    complete: false,
    accountName,
    status: "initialized; cleanup verification pending",
  });
  console.log(JSON.stringify({ accountName, privateDirectory, existing }));
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
          REBENCH_WORKER: target.name,
          REBENCH_BUNDLE: target.bundle,
          REBENCH_TOKEN: privateState.token,
          REBENCH_KIND: target.kind,
          REBENCH_PHASE: target.phase ?? "seed",
          REBENCH_PROVIDER: target.provider ?? "",
          REBENCH_UPLOAD_REVISION: target.uploadRevision ?? "",
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
      appendFileSync(join(here, "alchemy", `${target.name}-${operation}.log`), clean(captured));
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

  const build = load(join(here, "build-identities/all.json")).find(
    (item) => item.name === buildName,
  );

  if (!build) throw new Error(`No build for ${buildName}`);
  const name = `rebench-${resources.run}-${role}`;
  const bundle = join(build.output, "worker.mjs");

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
  save(join(here, "resources.json"), resources);
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
  const script = (await apiJson("workers/scripts")).result.find((item) => item.id === target.name);

  if (!script?.tag)
    throw new Error("Deployed Worker metadata is missing its version lookup identity");
  let version;

  try {
    version = (await apiJson(`workers/workers/${script.tag}/versions/${target.expectedVersion}`))
      .result;
  } catch (cause) {
    // The beta version endpoint returns 500 for this targeted-placement Worker.
    // Exact bytes and active deployment remain mandatory; upload startup is optional.
    if (!/Cloudflare .*: 500 /.test(String(cause))) throw cause;
    target.startupMetadataFailure = clean(String(cause));
    version = (await apiJson(`workers/scripts/${target.name}/versions/${target.expectedVersion}`))
      .result;
  }
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

    appendFileSync(join(here, "requests.jsonl"), clean(JSON.stringify(row)) + "\n");
    if (!response.ok)
      throw new Error(`Worker ${target.role} ${url.pathname} returned ${response.status}`);

    return row;
  } catch (cause) {
    if (!response)
      appendFileSync(
        join(here, "requests.jsonl"),
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

  for (const item of [...resources.targets].reverse()) {
    if (!item.cleanupComplete) await alchemy(item, "destroy");
    const workerGetStatus = (await api(`workers/scripts/${item.name}`)).status;
    const remaining = (await listNamespaces()).filter((ns) => ns.script === item.name);

    checks.push({
      worker: item.name,
      workerGetStatus,
      remainingNamespaces: remaining.map(({ name, script }) => ({ name, script })),
      checkedAt: new Date().toISOString(),
    });
    if (workerGetStatus !== 404 || remaining.length)
      throw new Error(`Cleanup not verified: ${item.name}`);
    item.cleanupComplete = true;
    save(join(here, "resources.json"), resources);
    console.log(`Verified deletion: ${item.name}`);
  }
  const remaining = await prefixedResources();

  if (remaining.workers.length || remaining.namespaces.length)
    throw new Error("rebench prefix resources remain in recorded account");
  // Allow final abort/deletion invocation telemetry to reach ingestion.
  await sleep(15_000);
  await telemetry(undefined, true);
  // Include the destroy logs while the exact private token is still available
  // for comparison. Nothing in this scan records credential values.
  await scanSecrets();
  rmSync(resources.privateDirectory, { recursive: true });
  save(join(here, "cleanup.json"), {
    complete: true,
    accountName: resources.accountName,
    accountDigest: resources.accountDigest,
    checkedAt: new Date().toISOString(),
    checks,
    remaining,
    finalTelemetryTailCollected: true,
    secretStateRemoved: true,
  });
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
  files.push(join(root, "package.json"), join(root, "bun.lock"), join(here, "../../package.json"));
  const secrets = [account, token, privateState.token];
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
    historicalCredentialLimit: resources.controllerRecoveries?.length
      ? "The pre-restart benchmark token was sanitized at capture, then lost with the private directory; its exact value is unavailable for this final scan."
      : null,
    matches,
    passed: matches.length === 0,
    scope:
      "Exact account ID, Cloudflare API token, and private benchmark token; raw and decompressed result artifacts plus changed root manifests/task config",
  });
  if (matches.length)
    throw new Error(`Known credentials found in ${matches.length} evidence files; do not commit`);
  console.log(
    `Credential scan passed: ${files.length} files, including ${gzipFiles} compressed artifacts`,
  );
};

const expectedSeed = { 50: "b017b487524e44a4", 250: "dcea9f30b0917245" };

const networkUrl = (path, cohort, sample, variant = "inline") =>
  `${path}?${new URLSearchParams({
    target: cohort.framework,
    history: String(cohort.history),
    ttftMs: String(cohort.ttftMs),
    chunkDelayMs: cohort.ttftMs === 400 ? "10" : "0",
    sample,
    variant,
  })}`;

const refresh = async (phase) => {
  await open();
  let provider = resources.targets.find((item) => item.role === "provider");

  if (!provider) provider = await deployOne("provider", "provider");
  await readiness(provider, "/echo");
  let primary = resources.targets.find((item) => item.role === "primary");

  if (!primary) primary = await deployOne("primary", "network", `${provider.url}/v1`);
  else {
    const build = load(join(here, "build-identities/all.json")).find(
      (item) => item.name === "network",
    );

    primary.deployments ??= [];
    primary.deployments.push({
      build: primary.build,
      phase: primary.phase,
      version: primary.expectedVersion,
      startupEvidence: primary.startupEvidence,
      replacedAt: Date.now(),
    });
    primary.build = build;
    primary.bundle = join(build.output, "worker.mjs");
    primary.phase = phase;
    save(join(here, "resources.json"), resources);
    await alchemy(primary, "deploy");
    await verifyUpload(primary);
  }
  await readiness(
    primary,
    "/driver",
    undefined,
    (row) => row.response.version === primary.expectedVersion && row.response.generation === phase,
  );
  console.log(
    `Verified primary generation ${phase}; placement ${JSON.stringify(primary.settings.placement)}`,
  );
};

const reference = async (size, samples) => {
  const messages = [];
  let finalSeed;

  const add = async (input, measured) => {
    messages.push({ role: "user", text: input.text });
    const hashes = [];

    while (true) {
      const hash = await fingerprint(messages);

      if (measured) hashes.push(hash);
      else finalSeed = hash;
      const step = next(messages);

      if ("answer" in step) {
        messages.push({ role: "assistant", text: step.answer });
        break;
      }
      messages.push(
        { role: "assistant", text: "", calls: [step.call] },
        { role: "tool", text: payload(step.call) },
      );
    }

    return hashes;
  };

  for (const input of history(0, size)) await add(input, false);
  if (finalSeed !== expectedSeed[size]) throw new Error(`Reference seed mismatch: ${size}`);
  const turns = {};

  for (let i = 0; i < samples; i++) turns[`m${i}`] = await add(turn(`m${i}`, 8), true);

  return { seedFingerprint: finalSeed, turns };
};

const makePlan = async (phase, pilot) => {
  const cohorts = [];
  const groups = [];
  const histories = pilot ? [50] : [50, 250];
  const count = pilot ? 1 : 3;
  const measuredTurns = pilot ? 3 : 5;

  for (const h of histories)
    for (const delay of [0, 400])
      for (let o = 0; o < count; o++) {
        const object = `rebench-${phase}-h${h}-d${delay}-o${o}`;
        const members = ["yielded", "pi", "tardie"].map((framework) => ({
          framework, history: h, ttftMs: delay, index: o, object,
        }));
        cohorts.push(...members);
        const schedule = [];
        for (let i = 0; i < measuredTurns; i++)
          for (const member of shuffle(members, 701 + o * 19 + i * 43 + h + delay))
            schedule.push({
              ...member,
              sample: `m${i}`,
              variant: member.framework === "yielded" ? "production" : "inline",
              state: i === 0 ? "cold" : i === 1 ? "settling" : "warm",
            });
        groups.push({ object, schedule });
      }
  const references = {};
  for (const h of histories) references[h] = await reference(h, measuredTurns);
  return {
    phase, baselineRevision: resources.baselineRevision,
    objectsPerRole: count, measuredTurns,
    locationHint: "wnam", driverPlacement: "aws:us-west-1",
    cold: "Acknowledged storage.sync and ctx.abort before m0; new incarnation of existing Object, not guaranteed fresh isolate. Both Thread and Actor are reset for tardie.",
    idleHeartbeat: "Every 3 seconds, read /identity on idle comparison Objects while a measured turn is in flight. Verify their incarnations. No heartbeat reaches the actively measured Object.",
    cohorts, groups: shuffle(groups, 20261009), references,
  };
};

const key = (c) => `${c.framework}/${c.object}`;

const lines = (file) =>
  existsSync(join(here, file))
    ? readFileSync(join(here, file), "utf8").trim().split("\n").filter(Boolean).map(JSON.parse)
    : [];

const seedFixture = async (cohort, phase) => {
  let result;

  for (let from = 0; from < cohort.history; from += 10) {
    result = await request(
      target("primary"),
      networkUrl("/seed", cohort, `seed-${from}`),
      { from, to: Math.min(cohort.history, from + 10) },
      `${phase}-seed`,
      cohort.object,
    );
  }
  if (result.response.fingerprint !== expectedSeed[cohort.history])
    throw new Error("Seed fingerprint mismatch");

  const release = await request(
    target("primary"),
    networkUrl("/cold", cohort, "seed-release"),
    {},
    `${phase}-seed-release`,
    cohort.object,
  );

  if (!release.response.ok) throw new Error("Seed release not acknowledged");

  return {
    key: key(cohort),
    fingerprint: result.response.fingerprint,
    at: Date.now(),
    databaseBytes: result.response.databaseBytes,
  };
};

const seed = async (phase, pilot) => {
  await open();
  if (target("primary").phase !== "seed") throw new Error("Seed generation required");
  const file = join(here, `${phase}-plan.json`);

  if (existsSync(file))
    throw new Error(
      "Never reseed a recorded plan; use a new fixture identity after an uncertain seed",
    );
  const plan = await makePlan(phase, pilot);

  save(file, plan);
  const receipts = [];
  const failures = [];
  let cursor = 0;

  const worker = async () => {
    while (cursor < plan.cohorts.length) {
      const cohort = plan.cohorts[cursor++];

      try {
        receipts.push(await seedFixture(cohort, phase));
        save(join(here, `${phase}-seeds.json`), receipts);
        console.log(`${phase} seeded ${receipts.length}/${plan.cohorts.length}: ${key(cohort)}`);
      } catch (cause) {
        failures.push({
          key: key(cohort),
          error: clean(String(cause)),
          at: Date.now(),
          noRetry: true,
        });
        save(join(here, `${phase}-seed-failures.json`), failures);
      }
    }
  };

  await Promise.all(Array.from({ length: pilot ? 1 : 3 }, worker));
  if (failures.length)
    throw new Error(
      `${failures.length} seed fixtures failed; no failed or uncertain turn was retried`,
    );
};

const replaceFailedSeeds = async () => {
  await open();
  if (target("primary").phase !== "seed") throw new Error("Seed generation required");
  const planFile = join(here, "main-plan.json");
  const plan = load(planFile);
  const failures = load(join(here, "main-seed-failures.json"));
  const receipts = load(join(here, "main-seeds.json"));
  const initial = join(here, "main-plan-initial.json");
  if (!existsSync(initial)) save(initial, plan);
  const failedObjects = [...new Set(failures.map((f) => f.key.slice(f.key.indexOf("/") + 1)))];
  for (const failedObject of failedObjects) {
    const original = plan.groups.find((g) => g.object === failedObject);
    if (!original) continue;
    const object = failedObject.replace("rebench-main-", "rebench-main-sr1-");
    if (lines("attempted.jsonl").some((row) => row.object === object))
      throw new Error("Replacement identity already attempted; never replay a seed");
    const cohorts = plan.cohorts.filter((c) => c.object === failedObject).map((c) => ({ ...c, object }));
    plan.retiredSeedGroups ??= [];
    plan.retiredSeedGroups.push({ object: failedObject, replacement: object, reason: "Cloudflare memory reset during seeding; retire all three comparison Objects without replaying uncertain seed inputs.", failures: failures.filter((f) => f.key.endsWith("/" + failedObject)), originalGroup: original });
    plan.groups = plan.groups.map((g) => g === original ? { object, schedule: g.schedule.map((i) => ({ ...i, object })) } : g);
    plan.cohorts = plan.cohorts.map((c) => c.object === failedObject ? { ...c, object } : c);
    save(planFile, plan);
    for (const cohort of cohorts) {
      receipts.push(await seedFixture(cohort, "main-seed-replacement"));
      save(join(here, "main-seeds.json"), receipts);
      console.log(`Sequential replacement seeded: ${key(cohort)}`);
    }
  }
  if (!plan.cohorts.every((c) => receipts.some((r) => r.key === key(c))))
    throw new Error("Some active seed fixtures remain unverified");
};

const replaceInterruptedGroup = async () => {
  await open();
  if (target("primary").phase !== "seed")
    throw new Error("Replacement seeding requires seed generation");
  const planFile = join(here, "main-plan.json");
  const plan = load(planFile);
  const failure = load(join(here, "main-failures.json")).at(-1);

  const failedRequest = lines("requests.jsonl").findLast(
    (row) =>
      row.phase === "main" &&
      row.object === failure?.object &&
      row.framework === failure?.framework &&
      row.sample === failure?.sample,
  );

  const warmExclusion = failure?.error === "Error: Warm incarnation changed";

  const closedConnection =
    failedRequest?.status === 500 &&
    failedRequest.response?.error?.includes(
      "Connection closed: this Durable Object instance is no longer active",
    );

  const controllerInterrupted =
    failure?.error === "Controller process ended; client response missing and request outcome unknown" &&
    !failedRequest &&
    lines("attempted.jsonl").some(
      (row) =>
        row.phase === "main" &&
        row.startedAt === failure.startedAt &&
        row.object === failure.object &&
        row.framework === failure.framework &&
        row.sample === failure.sample,
    );

  if (!warmExclusion && !closedConnection && !controllerInterrupted)
    throw new Error(
      "Replacement requires an inspected warm-incarnation exclusion, closed Object connection, or interrupted controller request",
    );
  const original = plan.groups.find((group) => group.object === failure.object);

  if (!original) throw new Error("Failed group already replaced or absent; never reseed it");
  const originalPlanFile = join(here, "main-plan-initial.json");

  if (!existsSync(originalPlanFile)) save(originalPlanFile, plan);
  const index = (plan.retiredGroups?.length ?? 0) + 1;
  const object = failure.object.replace("rebench-main-", `rebench-main-r${index}-`);

  if (lines("attempted.jsonl").some((row) => row.object === object))
    throw new Error("Replacement identity already attempted");

  const cohorts = plan.cohorts
    .filter((c) => c.object === original.object)
    .map((c) => ({ ...c, object }));

  plan.retiredGroups ??= [];
  plan.retiredGroups.push({
    object: original.object,
    replacement: object,
    reason: warmExclusion
      ? "A settled turn arrived in a new Object incarnation and failed warm proof; retire the entire incomplete three-target group without replaying any input."
      : controllerInterrupted
        ? "The controller process ended while a production request was in flight. It has no response; its client-observed outcome remains unknown. Retire the entire incomplete three-target group without replaying any input."
        : "A cold request lost its Object connection. Its outcome remains uncertain; retire the entire incomplete three-target group without replaying any input.",
    failure,
    originalGroup: original,
    at: Date.now(),
  });
  plan.groups = plan.groups.map((group) =>
    group === original
      ? { object, schedule: group.schedule.map((item) => ({ ...item, object })) }
      : group,
  );
  plan.cohorts = plan.cohorts.map((c) => (c.object === original.object ? { ...c, object } : c));
  plan.idleHeartbeat =
    "Every 3 seconds, read /identity on the idle counterpart only while a measured turn is in flight. Verify its incarnation. No heartbeat reaches the actively measured Object.";
  save(planFile, plan);
  const receipts = load(join(here, "main-seeds.json"));

  for (const cohort of cohorts) {
    receipts.push(await seedFixture(cohort, "main-replacement"));
    save(join(here, "main-seeds.json"), receipts);
    console.log(`Seeded replacement ${key(cohort)}`);
  }
};

const measure = async (phase, resume = false) => {
  await open();
  const primary = target("primary");

  if (primary.phase !== "measure") throw new Error("Measurement generation required");
  const plan = load(join(here, `${phase}-plan.json`));
  const seeds = load(join(here, `${phase}-seeds.json`));
  const attempts = lines("attempted.jsonl");

  if (!resume && attempts.some((row) => row.phase === phase))
    throw new Error(
      "This measurement plan has attempted inputs; never replay an uncertain canonical turn",
    );
  const completed = resume ? load(join(here, `${phase}-completed.json`)) : [];

  const failures =
    resume && existsSync(join(here, `${phase}-failures.json`))
      ? load(join(here, `${phase}-failures.json`))
      : [];

  const previous = lines("requests.jsonl");

  for (const group of plan.groups) {
    if (
      resume &&
      group.schedule.every((item) =>
        completed.some(
          (done) =>
            key(done) === key(item) && done.sample === item.sample && done.transcriptVerified,
        ),
      )
    ) {
      if (
        !plan.cohorts
          .filter((c) => c.object === group.object)
          .every((c) =>
            previous.some(
              (row) =>
                row.phase === `${phase}-release` &&
                row.object === c.object &&
                row.framework === c.framework &&
                row.status === 200 &&
                row.response.ok,
            ),
          )
      )
        throw new Error(
          "Completed group lacks acknowledged releases; cannot resume across its boundary",
        );
      console.log(`Resume keeps completed group ${group.object}`);
      continue;
    }
    if (attempts.some((row) => row.phase === phase && row.object === group.object))
      throw new Error(
        "Remaining group has attempted inputs; replace it instead of replaying an uncertain turn",
      );
    const incarnations = new Map();

    for (const item of group.schedule) {
      if (!seeds.some((seed) => seed.key === key(item)))
        throw new Error(`Missing verified seed: ${key(item)}`);
      let reset;

      if (item.state === "cold") {
        reset = await request(
          primary,
          networkUrl("/cold", item, `cold-${item.sample}`),
          {},
          `${phase}-cold`,
          item.object,
        );
        if (!reset.response.ok) throw new Error("Cold reset not acknowledged");
      }
      try {
        const counterparts = plan.cohorts.filter(
          (c) =>
            c.object === item.object &&
            c.framework !== item.framework &&
            incarnations.has(c.framework),
        );

        let heartbeatFailure;
        let heartbeat;
        let heartbeats = 0;

        const beat = () => {
          if (!counterparts.length || heartbeat) return;
          heartbeat = Promise.all(counterparts.map((counterpart) => request(
            primary,
            networkUrl("/identity", counterpart, `keepalive-${item.sample}-${heartbeats++}`),
            undefined,
            `${phase}-keepalive`,
            counterpart.object,
          )
            .then((row) => {
              if (row.response.incarnation !== incarnations.get(counterpart.framework))
                throw new Error("Idle counterpart incarnation changed");
              if (counterpart.framework === "tardie" && row.response.directory?.incarnation !== incarnations.get("tardie-actor"))
                throw new Error("Idle tardie Actor incarnation changed");
            })))
            .catch((cause) => {
              heartbeatFailure = cause;
            })
            .finally(() => {
              heartbeat = undefined;
            });
        };

        beat();
        await heartbeat;
        if (heartbeatFailure) throw heartbeatFailure;
        const timer = counterparts.length ? setInterval(beat, 3000) : undefined;
        let row;

        try {
          row = await request(
            primary,
            networkUrl("/run", item, item.sample, item.variant),
            {},
            phase,
            item.object,
          );
        } finally {
          if (timer) clearInterval(timer);
          await heartbeat;
        }
        if (heartbeatFailure) throw heartbeatFailure;
        const result = row.response;
        const metrics = result.metrics;

        if (
          !result.ok ||
          result.driverVersion !== primary.expectedVersion ||
          metrics.version !== primary.expectedVersion ||
          metrics.seedFingerprint !== expectedSeed[item.history] ||
          metrics.calls.length !== 9
        )
          throw new Error("Identity, seed, version, or provider-count mismatch");
        const hashes = plan.references[item.history].turns[item.sample];

        if (
          metrics.calls.some(
            (call, i) =>
              call.status !== 200 ||
              call.error ||
              !call.sseDone ||
              call.providerReceipt?.fingerprint !== hashes[i],
          )
        )
          throw new Error("Native provider stream or reference transcript mismatch");
        if (
          item.variant === "production" &&
          metrics.calls.some((call) => call.invocation?.kind !== "alarm")
        )
          throw new Error("Production turn performed a model request outside an alarm");
        if (reset) {
          if (
            metrics.incarnation === reset.response.before.incarnation ||
            metrics.objectId !== reset.response.before.objectId ||
            !metrics.entry.firstHarnessRequest ||
            metrics.entry.priorAlarmStarts !== 0
          )
            throw new Error("Cold incarnation proof failed");
        } else if (metrics.incarnation !== incarnations.get(item.framework))
          throw new Error("Warm incarnation changed");
        if (item.framework === "tardie") {
          const directory = result.directoryStart;
          if (!directory || directory.version !== primary.expectedVersion)
            throw new Error("Missing tardie Actor identity");
          if (reset) {
            if (directory.incarnation === reset.response.directoryBefore?.incarnation ||
                directory.objectId !== reset.response.directoryBefore?.objectId ||
                !directory.entry.firstHarnessRequest || directory.entry.priorAlarmStarts !== 0)
              throw new Error("Cold tardie Actor incarnation proof failed");
          } else if (directory.incarnation !== incarnations.get("tardie-actor"))
            throw new Error("Warm tardie Actor incarnation changed");
          incarnations.set("tardie-actor", directory.incarnation);
        }
        incarnations.set(item.framework, metrics.incarnation);
        completed.push({
          ...item,
          at: Date.now(),
          incarnation: metrics.incarnation,
          turnMs: result.turnMs,
          coldVerified: !!reset,
          transcriptVerified: true,
          productionAlarmVerified: item.variant === "production",
        });
        save(join(here, `${phase}-completed.json`), completed);
        console.log(
          `${phase} ${item.history}/${item.ttftMs}/o${item.index} ${item.framework}/${item.variant} ${item.state}/${item.sample}: ${result.turnMs} ms`,
        );
      } catch (cause) {
        failures.push({ ...item, at: Date.now(), error: clean(String(cause)), noRetry: true });
        save(join(here, `${phase}-failures.json`), failures);
        throw cause;
      }
    }
    for (const cohort of plan.cohorts.filter((c) => c.object === group.object)) {
      const release = await request(
        primary,
        networkUrl("/cold", cohort, "release"),
        {},
        `${phase}-release`,
        cohort.object,
      );

      if (!release.response.ok) throw new Error("Final release not acknowledged");
    }
  }
};

export const run = Effect.tryPromise({
  try: async () => {
    if (action === "dry-run") {
      console.log(
        JSON.stringify(
          {
            stack: "rebench",
            baseline: "origin/main",
            timing: "deployed driver only",
            locationHint: "wnam",
            driverPlacement: "aws:us-west-1",
            cpuLimitMs: 300000,
            histories: [50, 250],
            ttftMs: [0, 400],
            objectsPerRole: 3,
            targets: ["yielded/production", "pi", "tardie"],
            turnsPerObject: { cold: 1, settling: 1, warm: 3 },
            actions: [
              "init",
              "deploy",
              "seed-pilot",
              "seed",
              "activate",
              "measure-pilot",
              "measure",
              "telemetry",
              "secret-scan",
              "cleanup",
            ],
          },
          null,
          2,
        ),
      );

      return;
    }
    if (!account || !token) throw new Error("Cloudflare credentials missing; use checkout direnv");
    if (action === "init") await init();
    else if (action === "deploy" || action === "refresh-seed") await refresh("seed");
    else if (action === "activate" || action === "refresh-measure") await refresh("measure");
    else if (action.startsWith("seed-pilot")) await seed(action.slice(5), true);
    else if (action === "seed") await seed("main", false);
    else if (action === "replace-failed-seeds") await replaceFailedSeeds();
    else if (action === "replace-interrupted-group") await replaceInterruptedGroup();
    else if (action.startsWith("measure-pilot")) await measure(action.slice(8));
    else if (action === "measure") await measure("main");
    else if (action === "measure-resume") await measure("main", true);
    else if (action === "metadata") {
      await open();
      for (const item of resources.targets) {
        await verifyUpload(item);
        console.log(
          `Verified ${item.role}: startup ${item.startupTimeMs} ms, placement ${JSON.stringify(item.settings.placement)}`,
        );
      }
      const primary = target("primary");

      await readiness(
        primary,
        "/driver",
        undefined,
        (row) =>
          row.response.version === primary.expectedVersion &&
          row.response.generation === primary.phase,
      );
    } else if (action === "telemetry")
      await telemetry(process.argv.slice(2).filter((arg) => arg !== "--")[1]);
    else if (action === "secret-scan") await scanSecrets();
    else if (action === "cleanup") await cleanup();
    else throw new Error(`Unknown action: ${action}`);
  },
  catch: (cause) => {
    const message = clean(String(cause));

    appendFileSync(
      join(here, "controller-errors.jsonl"),
      JSON.stringify({ action, at: Date.now(), message }) + "\n",
    );

    return new RunError({ message });
  },
});

if (import.meta.url === `file://${process.argv[1]}`) NodeRuntime.runMain(run);
