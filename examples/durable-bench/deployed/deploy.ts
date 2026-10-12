import { basename, join } from "node:path";

import { Clock, Console, Effect, FileSystem, Schedule, Schema } from "effect";

import { build } from "./build.ts";
import { Cloudflare, request } from "./cloudflare.ts";
import { Health, WorkerBuild } from "./model.ts";
import {
  BenchError,
  directory,
  execute,
  hash,
  nonce,
  prefixLock,
  read,
  redact,
  repository,
  save,
  stateDirectory,
} from "./platform.ts";

/** The only persistent state is private Alchemy state and enough ownership data to destroy it. */
export const deployments = Effect.gen(function* () {
  const cloud = yield* Cloudflare;
  const fs = yield* FileSystem.FileSystem;
  const privateDirectory = stateDirectory(cloud.prefix);

  const Name = Schema.String.check(Schema.isPattern(new RegExp(`^${cloud.prefix}-[a-z0-9-]+$`)));
  const Workers = Schema.NonEmptyArray(Name).check(Schema.isUnique());

  const ownedWorkers = (name: string) =>
    Schema.NonEmptyArray(
      Schema.String.check(Schema.isPattern(new RegExp(`^${name}(?:-[a-z0-9-]+)?$`))),
    ).check(Schema.isUnique());

  const Stack = Schema.Struct({
    name: Name,
    kind: Schema.Literals(["infrastructure", "target"]),
    bundle: Schema.String,
    build: Schema.String,
    cpu: Schema.Boolean,
    workers: Schema.optionalKey(Workers),
    workerBuilds: Schema.optionalKey(Schema.Record(Name, WorkerBuild)),
    namespaceLayout: Schema.optionalKey(Schema.Literal("selected-target")),
  });

  type Stack = typeof Stack.Type;

  const State = Schema.Struct({
    version: Schema.Literal(1),
    accountId: Schema.String,
    token: Schema.String,
    infrastructurePrefix: Schema.String.check(
      Schema.isPattern(new RegExp(`^${cloud.prefix}-shared-[a-f0-9]{8}$`)),
    ),
    infrastructure: Schema.optionalKey(Stack),
    infrastructureDeployed: Schema.optionalKey(Schema.Boolean),
    targets: Schema.Record(Schema.String, Stack),
  });

  const stateFile = join(privateDirectory, "state.json");
  let state: typeof State.Type;

  if (yield* fs.exists(stateFile)) {
    state = yield* read(stateFile, State);
    if (state.accountId !== cloud.accountId)
      return yield* new BenchError({
        message: "Credentials select a different account from the private deployment state.",
      });
  } else {
    const existing = yield* cloud.resources();

    if (!existing.verified)
      return yield* new BenchError({
        message: `${cloud.prefix} resources exist without local ownership state. Restore the private state before deploying or tearing down.`,
      });
    state = {
      version: 1,
      accountId: cloud.accountId,
      token: nonce() + nonce(),
      infrastructurePrefix: cloud.prefix + "-shared-" + nonce().slice(0, 8),
      targets: {},
    };
  }
  yield* fs.makeDirectory(privateDirectory, { recursive: true, mode: 0o700 });
  yield* fs.chmod(privateDirectory, 0o700);
  yield* fs.remove(join(privateDirectory, "node_modules"), { force: true });
  yield* fs.symlink(join(repository, "node_modules"), join(privateDirectory, "node_modules"));
  yield* fs.writeFileString(
    join(privateDirectory, "package.json"),
    '{"private":true,"type":"module"}\n',
    { mode: 0o600 },
  );
  yield* save(stateFile, state);
  const url = (name: string) => `https://${name}.${cloud.subdomain}.workers.dev`;
  const driver = url(state.infrastructurePrefix + "-driver");
  const provider = url(state.infrastructurePrefix + "-provider") + "/v1";
  const secrets = [cloud.accountId, cloud.apiToken, state.token, cloud.accountName];

  const alchemy = Effect.fnUntraced(
    function* (stack: Stack, action: "deploy" | "destroy", force = false) {
      if (action === "deploy") {
        // Coordinate headroom checks and allocation across this machine's bench prefixes.
        yield* prefixLock("durable-bench-account-" + hash(cloud.accountId).slice(0, 16)).pipe(
          Effect.tapError((error) =>
            error._tag === "BenchError" && error.message.startsWith("Another controller owns")
              ? Console.error(
                  "Waiting for another bench deployment to release the account allocation lock…",
                )
              : Effect.void,
          ),
        );

        const headroom = yield* cloud.namespaceHeadroom(
          stack.kind === "target" ? (stack.workers ?? [stack.name]) : [],
        );

        yield* Console.error(
          `Namespace headroom: ${headroom.existing} existing, ${headroom.additional} additional, ${headroom.free} free after deployment.`,
        );
      }
      const startedMs = yield* Clock.currentTimeMillis;

      const result = yield* execute(
        [
          "exec",
          "alchemy",
          action,
          join(directory, "stack.ts"),
          "--stage",
          stack.name,
          "--yes",
          ...(force ? ["--force"] : []),
        ],
        privateDirectory,
        {
          CI: "true",
          NO_COLOR: "1",
          ALCHEMY_HOME: join(privateDirectory, "auth"),
          CLOUDFLARE_ACCOUNT_ID: cloud.accountId,
          CLOUDFLARE_API_TOKEN: cloud.apiToken,
          DURABLE_BENCH_KIND: stack.kind,
          DURABLE_BENCH_PREFIX: cloud.prefix,
          DURABLE_BENCH_INFRA_PREFIX: state.infrastructurePrefix,
          DURABLE_BENCH_NAME: stack.name,
          DURABLE_BENCH_WORKERS: JSON.stringify(stack.workers ?? [stack.name]),
          DURABLE_BENCH_WORKER_BUILDS: JSON.stringify(stack.workerBuilds ?? {}),
          DURABLE_BENCH_BUNDLE: stack.bundle,
          DURABLE_BENCH_BUILD: stack.build,
          DURABLE_BENCH_TOKEN: state.token,
          DURABLE_BENCH_CPU: String(stack.cpu),
          DURABLE_BENCH_PROVIDER: provider,
          DURABLE_BENCH_SUBDOMAIN: cloud.subdomain,
        },
      );

      for (const line of result.output.split("\n")) {
        const marker = line.indexOf("BENCH_DEPLOY_HTTP ");

        if (marker >= 0) yield* Console.error(line.slice(marker));
      }
      yield* Console.error(
        `BENCH_DEPLOY ${JSON.stringify({ kind: stack.kind, action, retry: force, elapsedMs: (yield* Clock.currentTimeMillis) - startedMs })}`,
      );
      if (result.code !== 0)
        return yield* new BenchError({
          message: `Alchemy ${action} failed (${result.code}): ${redact(result.output, secrets).slice(-3000)}`,
        });
    },
    Effect.scoped,
    Effect.retry({
      times: 60,
      schedule: Schedule.spaced("5 seconds"),
      while: (error) =>
        error._tag === "BenchError" &&
        error.message.startsWith("Another controller owns the durable-bench-account-"),
    }),
  );

  const ready = Effect.fnUntraced(function* (
    endpoint: string,
    expected: string,
    fromDriver = false,
    role = "target",
  ) {
    let consecutive = 0;
    let healthySince: number | undefined;
    const startedMs = yield* Clock.currentTimeMillis;

    for (let attempt = 0; (yield* Clock.currentTimeMillis) - startedMs < 45_000; attempt++) {
      const health = yield* request(
        fromDriver ? driver + "/ready" : endpoint + "/health?probe=" + nonce(),
        state.token,
        Health,
        fromDriver ? { targetUrl: endpoint } : undefined,
      ).pipe(Effect.timeout("10 seconds"), Effect.result);

      const now = yield* Clock.currentTimeMillis;
      const healthy = health._tag === "Success" && health.success.value.build === expected;

      consecutive = healthy ? consecutive + 1 : 0;
      healthySince = healthy ? (healthySince ?? now) : undefined;
      // A burst of successful probes can still precede stale routing on the next request.
      // Require sustained success, polling throughout the window instead of sleeping blindly.
      if (consecutive >= 3 && healthySince !== undefined && now - healthySince >= 5000) {
        yield* Console.error(
          `BENCH_READY ${JSON.stringify({ role, fromDriver, attempts: attempt + 1, elapsedMs: (yield* Clock.currentTimeMillis) - startedMs })}`,
        );

        return;
      }
      yield* Effect.sleep("250 millis");
    }

    yield* Console.error(
      `BENCH_READY ${JSON.stringify({ role, fromDriver, incomplete: true, elapsedMs: (yield* Clock.currentTimeMillis) - startedMs })}`,
    );

    return yield* new BenchError({
      message: `${role} Worker health did not propagate${fromDriver ? " through the driver" : ""}. No benchmark input was sent.`,
    });
  });

  const readyInfrastructure = Effect.fnUntraced(function* (revision: string) {
    yield* Effect.all(
      [
        ready(driver, revision, false, "driver"),
        ready(url(state.infrastructurePrefix + "-provider"), revision, true, "provider"),
        ...(state.infrastructure?.cpu
          ? [ready(url(state.infrastructurePrefix + "-cpu"), revision, false, "CPU collector")]
          : []),
      ],
      { concurrency: 2, discard: true },
    );
  });

  const readyTargets = (
    workers: readonly string[],
    revision: string,
    workerBuilds: Readonly<Record<string, WorkerBuild>> = {},
  ) =>
    Effect.forEach(
      workers,
      (name) =>
        Effect.all(
          [
            ready(url(name), workerBuilds[name]?.build ?? revision),
            ready(url(name), workerBuilds[name]?.build ?? revision, true),
          ],
          {
            concurrency: 2,
            discard: true,
          },
        ),
      { concurrency: workers.length, discard: true },
    );

  const infrastructureBuild = Effect.fnUntraced(function* () {
    if (state.infrastructure?.kind !== "infrastructure" || !state.infrastructureDeployed)
      return yield* new BenchError({ message: "Shared infrastructure is not deployed." });

    return state.infrastructure.build;
  });

  const infrastructure = Effect.fnUntraced(function* (cpu: boolean) {
    const output = join(privateDirectory, "infrastructure");
    const driverBuild = yield* build(output, "driver");
    const providerBuild = yield* build(output, "provider");
    const tailBuild = cpu ? yield* build(output, "tail") : undefined;

    const revision = hash(
      driverBuild.sha256 +
        providerBuild.sha256 +
        (tailBuild?.sha256 ?? "") +
        hash(yield* fs.readFile(join(directory, "stack.ts"))),
    );

    let reused = state.infrastructure?.build === revision && state.infrastructureDeployed === true;

    const infrastructure: Stack = {
      name: cloud.prefix + "-infrastructure",
      kind: "infrastructure",
      bundle: output,
      build: revision,
      cpu,
    };

    if (!reused) {
      state = { ...state, infrastructure, infrastructureDeployed: false };
      yield* save(stateFile, state);
      yield* Console.error("Deploying shared driver and mock provider…");
      yield* alchemy(infrastructure, "deploy");
    }

    const health = readyInfrastructure(revision);

    yield* health.pipe(
      Effect.catchTag("BenchError", () =>
        Effect.gen(function* () {
          // A fresh workers.dev route can remain on Cloudflare's 404/placeholder after upload.
          // Reconcile deployment once, before sending any benchmark input; never retry a turn.
          reused = false;
          state = { ...state, infrastructure, infrastructureDeployed: false };
          yield* save(stateFile, state);
          yield* Console.error(
            "Shared endpoint unavailable; reconciling its Alchemy deployment once…",
          );
          yield* alchemy(infrastructure, "deploy", true);
          yield* health;
        }),
      ),
    );
    state = { ...state, infrastructureDeployed: true };
    yield* save(stateFile, state);

    return reused;
  });

  const targets = Effect.fnUntraced(function* (
    name: string,
    bundle: string,
    revision: string,
    cpu: boolean,
    workers: readonly string[],
    persistent = false,
    workerBuilds: Readonly<Record<string, WorkerBuild>> = {},
  ) {
    yield* Schema.decodeUnknownEffect(Name)(name);
    const names = yield* Schema.decodeUnknownEffect(ownedWorkers(name))(workers);

    yield* Schema.decodeUnknownEffect(Schema.Record(Name, WorkerBuild))(workerBuilds);
    if (Object.keys(workerBuilds).some((worker) => !names.includes(worker)))
      return yield* new BenchError({ message: "Worker build assignment is outside its pool." });

    // One retained pool per prefix. Retire legacy four-binding pools before allocating replacements.
    for (const old of Object.values(state.targets))
      if (
        old.name.startsWith(cloud.prefix + "-pool-") &&
        (old.name !== name || old.namespaceLayout !== "selected-target")
      ) {
        yield* Console.error("Retiring the previous owned Worker pool before deployment…");
        yield* destroy(old.name);
      }
    const retained = join(privateDirectory, name, "bundles", revision);

    if (persistent) {
      yield* fs.makeDirectory(retained, { recursive: true });
      yield* fs.copyFile(bundle, join(retained, basename(bundle)));
      yield* fs.copyFile(bundle + ".map", join(retained, basename(bundle) + ".map"));
    }

    const retainedBuilds: Record<string, WorkerBuild> = {};
    const copied = new Map([[revision, persistent ? join(retained, basename(bundle)) : bundle]]);

    for (const [worker, assigned] of Object.entries(workerBuilds)) {
      let file = copied.get(assigned.build);

      if (!file) {
        const destination = join(privateDirectory, name, "bundles", assigned.build);

        file = persistent ? join(destination, basename(assigned.bundle)) : assigned.bundle;
        if (persistent) {
          yield* fs.makeDirectory(destination, { recursive: true });
          yield* fs.copyFile(assigned.bundle, file);
          yield* fs.copyFile(assigned.bundle + ".map", file + ".map");
        }
        copied.set(assigned.build, file);
      }
      retainedBuilds[worker] = { bundle: file, build: assigned.build };
    }

    const stack: Stack = {
      name,
      kind: "target",
      bundle: persistent ? join(retained, basename(bundle)) : bundle,
      build: revision,
      cpu,
      workers: names,
      workerBuilds: retainedBuilds,
      namespaceLayout: "selected-target",
    };

    state = { ...state, targets: { ...state.targets, [name]: stack } };
    yield* save(stateFile, state);
    yield* Console.error(`Uploading ${names.length} target Worker(s)…`);
    yield* alchemy(stack, "deploy");

    const health = readyTargets(names, revision, retainedBuilds);

    yield* health.pipe(
      Effect.catchTag("BenchError", () =>
        Effect.gen(function* () {
          yield* Console.error(
            "Target endpoint unavailable; reconciling its Alchemy deployment once…",
          );
          yield* alchemy(stack, "deploy", true);
          yield* health;
        }),
      ),
    );

    const endpoints: Readonly<Record<string, string>> = Object.fromEntries(
      names.map((worker) => [worker, url(worker)]),
    );

    return endpoints;
  });

  const target = Effect.fnUntraced(function* (
    name: string,
    bundle: string,
    revision: string,
    cpu: boolean,
  ) {
    yield* targets(name, bundle, revision, cpu, [name]);

    return url(name);
  });

  const attach = Effect.fnUntraced(function* (
    name: string,
    expectedTargetBuild: string,
    expectedInfrastructureBuild: string,
  ) {
    yield* Schema.decodeUnknownEffect(Name)(name);
    const stack = state.targets[name];
    const targetHash = expectedTargetBuild.slice(0, 64);

    if (
      !stack ||
      stack.name !== name ||
      stack.kind !== "target" ||
      stack.build !== expectedTargetBuild ||
      !/^[a-f0-9]{64}$/.test(targetHash) ||
      expectedTargetBuild !== `${targetHash}-${name}-e0`
    )
      return yield* new BenchError({
        message: "Resume target does not match the owned deployment.",
      });

    if (hash(yield* fs.readFile(stack.bundle)) !== targetHash)
      return yield* new BenchError({
        message: "Retained target bundle differs from the resume build.",
      });

    const revision = yield* infrastructureBuild();

    if (revision !== expectedInfrastructureBuild)
      return yield* new BenchError({
        message: "Shared infrastructure differs from the resume build.",
      });

    const names = yield* Schema.decodeUnknownEffect(ownedWorkers(name))(stack.workers ?? [name]);

    yield* readyInfrastructure(revision);
    yield* readyTargets(names, expectedTargetBuild);

    const endpoints: Readonly<Record<string, string>> = Object.fromEntries(
      names.map((worker) => [worker, url(worker)]),
    );

    return { endpoints, infrastructureBuild: revision };
  });

  const verifyCleanup = Effect.fnUntraced(function* (name?: string) {
    for (let attempt = 0; attempt < 30; attempt++) {
      const remaining = yield* cloud.resources(name, state.infrastructure?.cpu ?? false);

      if (remaining.verified || attempt === 29) return remaining;
      yield* Effect.sleep("2 seconds");
    }

    return yield* new BenchError({ message: "Cleanup verification did not complete" });
  });

  const destroy = Effect.fnUntraced(function* (name: string) {
    const stack = state.targets[name];

    if (stack) yield* alchemy(stack, "destroy");

    const remaining = yield* verifyCleanup(name);

    if (!remaining.verified)
      return yield* new BenchError({
        message: "Target cleanup could not be verified. Keep private state and run --teardown.",
      });
    const targets = { ...state.targets };

    delete targets[name];

    state = { ...state, targets };
    yield* save(stateFile, state);
    yield* fs.remove(join(privateDirectory, name), { recursive: true, force: true });

    return remaining;
  });

  const teardown = Effect.gen(function* () {
    for (const name of Object.keys(state.targets)) yield* destroy(name);
    if (state.infrastructure) yield* alchemy(state.infrastructure, "destroy");
    const remaining = yield* verifyCleanup();

    if (!remaining.verified)
      return yield* new BenchError({
        message: `${cloud.prefix} resources remain in this account; cleanup is not verified.`,
      });
    yield* fs.remove(privateDirectory, { recursive: true });

    return remaining;
  });

  return {
    token: state.token,
    driver,
    tail: url(state.infrastructurePrefix + "-cpu"),
    infrastructure,
    infrastructureBuild,
    target,
    targets,
    attach,
    destroy,
    teardown,
  };
});
