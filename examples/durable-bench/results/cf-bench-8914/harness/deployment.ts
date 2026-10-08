import {
  Clock,
  Config,
  Console,
  Crypto,
  Effect,
  FileSystem,
  Path,
  Redacted,
  Schema,
  Stream,
} from "effect";
import { HttpClient, HttpClientRequest } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import { sha256 } from "../../../../../tooling/context-continuity-eval/src/replay-cpu-build.ts";
import { BenchError, PrivateState, Resources, ROLES, SLUG, type Target } from "./contracts.ts";

export const requireBench = (valid: boolean, message: string) =>
  valid ? Effect.void : Effect.fail(new BenchError({ message }));

export const writeJson = Effect.fnUntraced(function* (file: string, value: unknown) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  yield* fs.makeDirectory(path.dirname(file), { recursive: true });
  yield* fs.writeFileString(`${file}.tmp`, `${JSON.stringify(value, null, 2)}\n`);
  yield* fs.rename(`${file}.tmp`, file);
});

export const readJson = <S extends Schema.Top>(file: string, schema: S) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;

    return yield* Schema.decodeUnknownEffect(schema)(
      yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(
        yield* fs.readFileString(file),
      ),
    );
  });

const credentials = Config.all({
  accountId: Config.NonEmptyString("CLOUDFLARE_ACCOUNT_ID"),
  apiToken: Config.Redacted("CLOUDFLARE_API_TOKEN"),
});

export const initialize = Effect.fnUntraced(function* (output: string, bundles: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const config = yield* credentials;

  yield* requireBench(
    !(yield* fs.exists(path.join(output, "resources.json"))),
    "Resources already initialized",
  );

  const privateDirectory = yield* fs.makeTempDirectory({
    directory: "/private/tmp",
    prefix: `${SLUG}-state-`,
  });

  yield* fs.chmod(privateDirectory, 0o700);

  const repository = path.resolve(
    path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))),
    "../../../../..",
  );

  yield* fs.symlink(
    path.join(repository, "node_modules"),
    path.join(privateDirectory, "node_modules"),
  );
  yield* fs.writeFileString(
    path.join(privateDirectory, "package.json"),
    '{"private":true,"type":"module"}\n',
  );
  const run = (yield* crypto.randomUUIDv4).replaceAll("-", "").slice(0, 8);
  const accountDigest = yield* sha256(config.accountId);

  yield* writeJson(path.join(privateDirectory, "private.json"), {
    accountDigest,
    token: (yield* crypto.randomUUIDv4) + (yield* crypto.randomUUIDv4),
  });
  yield* fs.chmod(path.join(privateDirectory, "private.json"), 0o600);
  yield* writeJson(path.join(output, "resources.json"), {
    run,
    accountDigest,
    privateDirectory,
    output,
    targets: ROLES.map((role) => ({
      role,
      name: `${SLUG}-${run}-${role}`,
      bundleDirectory: path.join(bundles, role),
      generation: "seed",
      namespaces: [],
      cleanupRequired: false,
      cleanupComplete: false,
    })),
  });
  yield* Console.log(`Initialized ${SLUG}/${run}; private Alchemy state outside repository`);
});

const NamespaceResponse = Schema.Struct({
  success: Schema.Literal(true),
  result: Schema.Array(
    Schema.Struct({ id: Schema.String, script: Schema.optionalKey(Schema.NullOr(Schema.String)) }),
  ),
});

const SubdomainResponse = Schema.Struct({
  success: Schema.Literal(true),
  result: Schema.Struct({ subdomain: Schema.String }),
});

const VersionList = Schema.Struct({
  success: Schema.Literal(true),
  result: Schema.Struct({
    items: Schema.Array(Schema.Struct({ id: Schema.String, number: Schema.Number })),
  }),
});

const ScriptList = Schema.Struct({
  success: Schema.Literal(true),
  result: Schema.Array(Schema.Struct({ id: Schema.String, tag: Schema.String })),
});

const Startup = Schema.Struct({
  success: Schema.Literal(true),
  result: Schema.Struct({
    id: Schema.String,
    number: Schema.Number,
    startup_time_ms: Schema.Number,
    limits: Schema.Struct({ cpu_ms: Schema.Number }),
    compatibility_date: Schema.String,
  }),
});

const RetirementReceipt = Schema.Struct({
  name: Schema.NonEmptyString,
  role: Schema.Literals(ROLES),
  workerStatus: Schema.Int,
  knownNamespaceIds: Schema.Array(Schema.NonEmptyString),
  namespaceIdsRemaining: Schema.Array(Schema.NonEmptyString),
  complete: Schema.Boolean,
});

/** Read-only inventory shared by cleanup and local receipt verification. */
export const readCleanupCoverage = Effect.fnUntraced(function* (resources: typeof Resources.Type) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  yield* requireBench(
    resources.targets.every(
      (target) => target.name === `${SLUG}-${resources.run}-${target.role}`,
    ) && new Set(resources.targets.map((target) => target.name)).size === resources.targets.length,
    "Cleanup targets differ from this task's ownership names",
  );

  const directory = path.join(resources.output, "retired");

  const files = (yield* fs.exists(directory))
    ? (yield* fs.readDirectory(directory)).filter((file) => file.endsWith(".json")).sort()
    : [];

  const retired = yield* Effect.forEach(
    files,
    Effect.fnUntraced(function* (file) {
      const receipt = yield* readJson(path.join(directory, file), RetirementReceipt).pipe(
        Effect.mapError(
          () => new BenchError({ message: `Invalid retirement receipt: retired/${file}` }),
        ),
      );

      yield* requireBench(
        resources.targets.some(
          (target) => target.name === receipt.name && target.role === receipt.role,
        ),
        `Retirement receipt is not owned by this task: retired/${file}`,
      );

      return { file: `retired/${file}`, ...receipt };
    }),
  );

  const targets = resources.targets.flatMap((target) => {
    const receipts = retired.filter((receipt) => receipt.name === target.name);

    if (!target.cleanupRequired && target.namespaces.length === 0 && receipts.length === 0)
      return [];

    // Failed retirements can also have discovered namespaces absent from the old ledger.
    const retiredIds = new Set(
      receipts.flatMap((receipt) => [
        ...receipt.knownNamespaceIds,
        ...receipt.namespaceIdsRemaining,
      ]),
    );

    return [
      {
        target,
        knownNamespaceIds: [...new Set([...target.namespaces, ...retiredIds])].sort(),
        currentNamespaceCount: new Set(target.namespaces).size,
        retiredNamespaceCount: retiredIds.size,
        retiredReceiptFiles: receipts.map((receipt) => receipt.file),
      },
    ];
  });

  return {
    targets,
    coverage: {
      workerCount: targets.length,
      currentNamespaceCount: new Set(targets.flatMap(({ target }) => target.namespaces)).size,
      retiredNamespaceCount: new Set(
        retired.flatMap((receipt) => [
          ...receipt.knownNamespaceIds,
          ...receipt.namespaceIdsRemaining,
        ]),
      ).size,
      namespaceCount: new Set(targets.flatMap((target) => target.knownNamespaceIds)).size,
      retiredReceiptCount: retired.length,
      retiredReceiptFiles: retired.map((receipt) => receipt.file),
    },
  };
});

export const openDeployment = Effect.fnUntraced(function* (output: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const client = yield* HttpClient.HttpClient;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const config = yield* credentials;
  const executablePath = yield* Config.String("PATH");
  const userHome = yield* Config.String("HOME");
  const stack = yield* path.fromFileUrl(new URL("./stack.ts", import.meta.url));
  let resources = yield* readJson(path.join(output, "resources.json"), Resources);

  const state = yield* readJson(
    path.join(resources.privateDirectory, "private.json"),
    PrivateState,
  );

  yield* requireBench(
    resources.accountDigest === (yield* sha256(config.accountId)) &&
      state.accountDigest === resources.accountDigest &&
      resources.output === output,
    "Account or output differs from ownership receipt",
  );

  const redact = (text: string) =>
    text
      .replaceAll(Redacted.value(config.apiToken), "[redacted]")
      .replaceAll(state.token, "[redacted]")
      .replaceAll(config.accountId, "[account]");

  const save = Effect.fnUntraced(function* (target: Target) {
    resources = {
      ...resources,
      targets: resources.targets.map((item) => (item.role === target.role ? target : item)),
    };
    yield* writeJson(path.join(output, "resources.json"), resources);
  });

  const api = Effect.fnUntraced(function* (route: string, body?: Schema.Json) {
    const request =
      body === undefined
        ? HttpClientRequest.get(
            `https://api.cloudflare.com/client/v4/accounts/${config.accountId}/${route}`,
          )
        : yield* HttpClientRequest.post(
            `https://api.cloudflare.com/client/v4/accounts/${config.accountId}/${route}`,
          ).pipe(HttpClientRequest.bodyJson(body));

    return yield* client.execute(request.pipe(HttpClientRequest.bearerToken(config.apiToken))).pipe(
      Effect.timeout("45 seconds"),
      Effect.mapError((error) => new BenchError({ message: redact(String(error)) })),
    );
  });

  const namespaces = Effect.fnUntraced(function* (name: string, known: readonly string[] = []) {
    const ids: string[] = [];

    for (let page = 1; page <= 100; page++) {
      const response = yield* api(`workers/durable_objects/namespaces?page=${page}&per_page=100`);

      yield* requireBench(response.status === 200, `Namespace listing failed: ${response.status}`);
      const result = yield* Schema.decodeUnknownEffect(NamespaceResponse)(yield* response.json);

      ids.push(
        ...result.result
          .filter((item) => item.script === name || known.includes(item.id))
          .map((item) => item.id),
      );
      if (result.result.length < 100) return ids;
    }

    return yield* new BenchError({ message: "Namespace listing exceeded bound" });
  });

  const alchemy = Effect.fnUntraced(function* (target: Target, action: "deploy" | "destroy") {
    let captured = "";
    const log = path.join(output, "deployments", target.role, `${target.generation}-${action}.log`);

    yield* Console.log(`${action} ${target.name} (${target.generation})`);
    yield* Effect.scoped(
      Effect.gen(function* () {
        const handle = yield* spawner.spawn(
          ChildProcess.make(
            "vp",
            ["exec", "alchemy", action, stack, "--stage", target.name, "--yes"],
            {
              cwd: resources.privateDirectory,
              stdin: "ignore",
              stdout: "pipe",
              stderr: "pipe",
              extendEnv: false,
              env: {
                PATH: executablePath,
                HOME: userHome,
                CI: "true",
                NO_COLOR: "1",
                TMPDIR: "/private/tmp",
                ALCHEMY_HOME: path.join(resources.privateDirectory, "auth"),
                CLOUDFLARE_ACCOUNT_ID: config.accountId,
                CLOUDFLARE_API_TOKEN: Redacted.value(config.apiToken),
                BENCH_ROLE: target.role,
                BENCH_WORKER_NAME: target.name,
                BENCH_BUNDLE: path.join(target.bundleDirectory, "worker.mjs"),
                BENCH_TOKEN: state.token,
                BENCH_GENERATION: target.generation,
              },
            },
          ),
        );

        yield* Stream.runForEach(handle.all, (bytes) =>
          Effect.sync(() => {
            captured += new TextDecoder().decode(bytes);
          }),
        );
        yield* requireBench(
          Number(yield* handle.exitCode) === 0,
          `Alchemy ${action} failed; inspect ${log}`,
        );
      }),
    ).pipe(
      Effect.timeout("6 minutes"),
      Effect.ensuring(
        Effect.gen(function* () {
          yield* fs.makeDirectory(path.dirname(log), { recursive: true });
          yield* fs.writeFileString(log, redact(captured));
        }).pipe(Effect.orDie),
      ),
    );
  });

  const inspect = Effect.fnUntraced(function* (target: Target) {
    const versionsResponse = yield* api(`workers/scripts/${target.name}/versions`);
    const versions = yield* Schema.decodeUnknownEffect(VersionList)(yield* versionsResponse.json);
    const version = versions.result.items[0];

    if (version === undefined)
      return yield* new BenchError({ message: "Missing deployed version" });
    const scriptsResponse = yield* api("workers/scripts");
    const scripts = yield* Schema.decodeUnknownEffect(ScriptList)(yield* scriptsResponse.json);
    const script = scripts.result.find((item) => item.id === target.name);

    if (script === undefined)
      return yield* new BenchError({ message: "Cannot find immutable Worker ID" });
    const startupResponse = yield* api(`workers/workers/${script.tag}/versions/${version.id}`);
    const startup = yield* Schema.decodeUnknownEffect(Startup)(yield* startupResponse.json);

    yield* requireBench(startup.result.limits.cpu_ms === 300_000, "Deployed CPU limit differs");
    const response = yield* api(`workers/scripts/${target.name}`);

    yield* requireBench(response.status === 200, "Cannot verify uploaded modules");
    const content = yield* response.arrayBuffer;
    const contentType = response.headers["content-type"] ?? "";

    const form = yield* Effect.tryPromise({
      try: () => new Response(content, { headers: { "content-type": contentType } }).formData(),
      catch: (cause) =>
        new BenchError({ message: `Cannot decode uploaded module multipart: ${String(cause)}` }),
    });

    const modules = [];

    for (const [field, value] of form.entries()) {
      const name = typeof value === "string" ? field : value.name;

      if (name !== "bench.mjs" && name !== "worker.mjs") continue;

      const bytes =
        typeof value === "string"
          ? new TextEncoder().encode(value)
          : new Uint8Array(
              yield* Effect.tryPromise({
                try: () => value.arrayBuffer(),
                catch: (cause) => new BenchError({ message: String(cause) }),
              }),
            );

      const remoteSha256 = yield* sha256(bytes);

      const localSha256 = yield* sha256(
        yield* fs.readFile(path.join(target.bundleDirectory, name)),
      );

      yield* requireBench(remoteSha256 === localSha256, `Uploaded ${name} differs from build`);
      modules.push({ name, bytes: bytes.length, sha256: remoteSha256, matchesLocal: true });
    }
    yield* requireBench(modules.length === 2, "Expected unchanged bench.mjs plus wrapper module");
    yield* writeJson(
      path.join(output, "deployments", target.role, `${target.generation}-verified.json`),
      {
        workerName: target.name,
        workerId: script.tag,
        generation: target.generation,
        version: startup.result,
        modules,
      },
    );

    return startup.result;
  });

  const deploy = Effect.fnUntraced(function* (role: Target["role"], generation: string) {
    const current = resources.targets.find((target) => target.role === role);

    if (current === undefined) return yield* new BenchError({ message: `Missing target ${role}` });
    const target = { ...current, generation, cleanupRequired: true, cleanupComplete: false };

    if (!current.cleanupRequired) {
      const response = yield* api(`workers/scripts/${target.name}`);

      yield* requireBench(
        response.status === 404,
        `Refusing preexisting or unverified Worker ${target.name}: ${response.status}`,
      );
    }
    yield* requireBench(
      yield* fs.exists(path.join(target.bundleDirectory, "worker.mjs")),
      `Missing ${role} bundle`,
    );
    yield* Effect.uninterruptible(save(target));
    yield* alchemy(target, "deploy");
    const ids = yield* namespaces(target.name);

    yield* requireBench(
      ids.length === (role === "tardie" ? 20 : 10),
      `${target.name}: unexpected namespace count ${ids.length}`,
    );
    const updated = { ...target, namespaces: ids };

    yield* save(updated);
    const versionResponse = yield* api(`workers/scripts/${target.name}/versions`);

    yield* requireBench(versionResponse.status === 200, "Cannot read deployed versions");
    const versions = yield* versionResponse.json;

    yield* fs.makeDirectory(path.join(output, "deployments", role), { recursive: true });
    yield* fs.writeFileString(
      path.join(output, "deployments", role, `${generation}-versions.json`),
      redact(JSON.stringify(versions, null, 2)) + "\n",
    );
    const hashes = [];

    for (const name of ["bench.mjs", "worker.mjs"]) {
      const bytes = yield* fs.readFile(path.join(target.bundleDirectory, name));

      hashes.push({ name, bytes: bytes.length, sha256: yield* sha256(bytes) });
    }
    yield* writeJson(path.join(output, "deployments", role, `${generation}-identity.json`), {
      name: target.name,
      role,
      generation,
      namespaces: ids,
      modules: hashes,
      cpuLimitMs: 300_000,
      compatibilityDate: "2026-08-18",
      locationHint: "wnam",
    });
    yield* inspect(updated);

    return updated;
  });

  const domainResponse = yield* api("workers/subdomain");

  yield* requireBench(domainResponse.status === 200, "Cannot read Workers subdomain");

  const domain = (yield* Schema.decodeUnknownEffect(SubdomainResponse)(yield* domainResponse.json))
    .result.subdomain;

  const call = Effect.fnUntraced(function* (target: Target, route: string, body?: Schema.Json) {
    const url = `https://${target.name}.${domain}.workers.dev${route}`;

    const request =
      body === undefined
        ? HttpClientRequest.get(url)
        : yield* HttpClientRequest.post(url).pipe(HttpClientRequest.bodyJson(body));

    for (let attempt = 0; attempt < 40; attempt++) {
      const started = yield* Clock.currentTimeMillis;

      const response = yield* client
        .execute(
          request.pipe(
            HttpClientRequest.bearerToken(state.token),
            HttpClientRequest.setHeader("x-bench-generation", target.generation),
          ),
        )
        .pipe(Effect.timeout("8 minutes"));

      if (response.status !== 200) {
        const text = yield* response.text;

        // This exact response is emitted by the ingress guard before any DO
        // access. Never retry a transport failure or an ambiguous DO response.
        if (response.status === 412 && text === "generation mismatch") {
          yield* writeJson(
            path.join(output, "rollout-rejections", `${target.role}-${started}.json`),
            {
              role: target.role,
              generation: target.generation,
              route,
              attempt,
              started,
              ended: yield* Clock.currentTimeMillis,
              status: response.status,
              cfRay: response.headers["cf-ray"],
              workloadExecuted: false,
            },
          );
          if (attempt < 39) {
            yield* Effect.sleep("3 seconds");
            continue;
          }
        }

        return yield* new BenchError({
          message: redact(`${target.role}${route}: HTTP ${response.status} ${text.slice(0, 4000)}`),
        });
      }
      const value = yield* response.json;
      const ended = yield* Clock.currentTimeMillis;

      return {
        value,
        started,
        ended,
        clientWallMs: ended - started,
        cfRay: response.headers["cf-ray"],
      };
    }

    return yield* new BenchError({ message: "Rollout did not reach the requested generation" });
  });

  const query = Effect.fnUntraced(function* (query: Schema.Json) {
    const response = yield* api("workers/observability/telemetry/query", query);

    yield* requireBench(response.status === 200, `Telemetry HTTP ${response.status}`);

    return yield* response.json;
  });

  const cleanup = Effect.fnUntraced(function* () {
    const { targets, coverage } = yield* readCleanupCoverage(resources);
    const checks = [];

    yield* writeJson(path.join(output, "cleanup.json"), {
      complete: false,
      privateStateRemoved: false,
      coverage,
      checkedAt: yield* Clock.currentTimeMillis,
      targets: [],
    });

    for (const planned of targets) {
      const { target, knownNamespaceIds, retiredReceiptFiles } = planned;
      const destroyed = yield* alchemy(target, "destroy").pipe(Effect.result);
      const exists = yield* api(`workers/scripts/${target.name}`);
      const remaining = yield* namespaces(target.name, knownNamespaceIds);
      const complete = exists.status === 404 && remaining.length === 0;

      checks.push({
        name: target.name,
        role: target.role,
        alchemy: destroyed._tag,
        workerStatus: exists.status,
        knownNamespaceIds,
        currentNamespaceCount: planned.currentNamespaceCount,
        retiredNamespaceCount: planned.retiredNamespaceCount,
        retiredReceiptFiles,
        namespaceIdsRemaining: remaining,
        complete,
      });
      yield* save({ ...target, cleanupComplete: complete });
      yield* writeJson(path.join(output, "cleanup.json"), {
        complete: false,
        privateStateRemoved: false,
        coverage,
        checkedAt: yield* Clock.currentTimeMillis,
        targets: checks,
      });
    }

    const complete =
      checks.length === coverage.workerCount &&
      new Set(checks.flatMap((item) => item.knownNamespaceIds)).size === coverage.namespaceCount &&
      new Set(checks.flatMap((item) => item.retiredReceiptFiles)).size ===
        coverage.retiredReceiptCount &&
      checks.every((item) => item.complete);

    if (complete) yield* fs.remove(resources.privateDirectory, { recursive: true });
    yield* writeJson(path.join(output, "cleanup.json"), {
      complete,
      privateStateRemoved: complete,
      coverage,
      checkedAt: yield* Clock.currentTimeMillis,
      targets: checks,
    });
    yield* requireBench(complete, "CLEANUP INCOMPLETE: keep private state and rerun cleanup");
    yield* Console.log("Cleanup verified through Cloudflare API: Workers and namespaces absent");
  });

  const retire = Effect.fnUntraced(function* (role: Target["role"]) {
    const target = resources.targets.find((item) => item.role === role);

    if (target === undefined) return yield* new BenchError({ message: `Missing target ${role}` });
    yield* alchemy(target, "destroy");
    const response = yield* api(`workers/scripts/${target.name}`);
    const remaining = yield* namespaces(target.name, target.namespaces);
    const complete = response.status === 404 && remaining.length === 0;

    yield* writeJson(
      path.join(output, "retired", `${role}-${yield* Clock.currentTimeMillis}.json`),
      {
        name: target.name,
        role,
        workerStatus: response.status,
        knownNamespaceIds: target.namespaces,
        namespaceIdsRemaining: remaining,
        complete,
      },
    );
    yield* requireBench(complete, `Retirement incomplete for ${role}`);
    yield* save({ ...target, cleanupComplete: true });
    yield* Console.log(`Verified retirement of ${role} Worker and namespaces`);
  });

  return {
    targets: () => resources.targets,
    deploy,
    call,
    query,
    cleanup,
    redact,
    inspect,
    retire,
  };
});
