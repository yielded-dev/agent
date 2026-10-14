import { basename, join } from "node:path";

import { Clock, Console, Effect, FileSystem, Schema } from "effect";

import { build } from "./build.ts";
import { Cloudflare, request } from "./cloudflare.ts";
import { Health, WorkerBuild } from "./model.ts";
import {
  BenchError,
  directory,
  execute,
  hash,
  nonce,
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
      return yield* BenchError.make({
        message: "Credentials select a different account from the private deployment state.",
      });
  } else {
    const existing = yield* cloud.resources();

    if (!existing.verified)
      return yield* BenchError.make({
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

  const uploads = new Map<string, number>();

  const alchemy = Effect.fnUntraced(function* (
    stack: Stack,
    action: "deploy" | "destroy",
    force = false,
  ) {
    if (action === "deploy") {
      yield* Effect.gen(function* () {
        for (;;) {
          const headroom = yield* cloud.namespaceHeadroom(
            stack.kind === "target"
              ? (stack.workers ?? [stack.name])
              : [state.infrastructurePrefix + "-driver"],
          );

          if (headroom.projectedOwned > 40)
            return yield* BenchError.make({
              message:
                "This prefix would exceed 40 Durable Object namespaces; split the cells or reduce Objects.",
            });
          if (headroom.free >= 100) {
            yield* Console.error(
              `Namespace headroom: ${headroom.existing} existing, ${headroom.additional} additional, ${headroom.free} free after deployment; ${headroom.projectedOwned} owned.`,
            );

            return;
          }
          yield* Console.error(
            "Waiting 60 seconds for at least 100 spare namespaces after deployment…",
          );
          yield* Effect.sleep("60 seconds");
        }
      }).pipe(
        Effect.timeoutOrElse({
          duration: "30 minutes",
          orElse: () =>
            BenchError.make({ message: "Namespace headroom remained unavailable for 30 minutes." }),
        }),
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

      if (marker >= 0) {
        const event = yield* Schema.decodeEffect(
          Schema.fromJsonString(
            Schema.Struct({
              category: Schema.String,
              worker: Schema.optionalKey(Schema.String),
              ok: Schema.optionalKey(Schema.Boolean),
              startedMs: Schema.Number,
              elapsedMs: Schema.Number,
            }),
          ),
        )(line.slice(marker + "BENCH_DEPLOY_HTTP ".length));

        if (event.category === "upload" && event.ok && event.worker)
          uploads.set(event.worker, event.startedMs + event.elapsedMs);
        yield* Console.error(line.slice(marker));
      }
    }
    yield* Console.error(
      `BENCH_DEPLOY ${JSON.stringify({ kind: stack.kind, action, retry: force, elapsedMs: (yield* Clock.currentTimeMillis) - startedMs })}`,
    );
    if (result.code !== 0)
      return yield* BenchError.make({
        message: `Alchemy ${action} failed (${result.code}): ${redact(result.output, secrets).slice(-3000)}`,
      });
  }, Effect.scoped);

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

        return { readyMs: now, probes: attempt + 1 };
      }
      yield* Effect.sleep("250 millis");
    }

    yield* Console.error(
      `BENCH_READY ${JSON.stringify({ role, fromDriver, incomplete: true, elapsedMs: (yield* Clock.currentTimeMillis) - startedMs })}`,
    );

    return yield* BenchError.make({
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
    workerBuilds: Readonly<Record<string, WorkerBuild>>,
  ) =>
    Effect.forEach(
      workers,
      Effect.fnUntraced(function* (name) {
        const assigned = workerBuilds[name];

        if (!assigned)
          return yield* BenchError.make({ message: "Missing Worker readiness assignment." });

        const probes = yield* Effect.all(
          [ready(url(name), assigned.build), ready(url(name), assigned.build, true)],
          { concurrency: 2 },
        );

        return {
          worker: name,
          build: assigned.build,
          readyMs: Math.max(...probes.map((probe) => probe.readyMs)),
          probes: probes.reduce((sum, probe) => sum + probe.probes, 0),
        };
      }),
      { concurrency: workers.length },
    );

  const infrastructureBuild = Effect.fnUntraced(function* () {
    if (state.infrastructure?.kind !== "infrastructure" || !state.infrastructureDeployed)
      return yield* BenchError.make({ message: "Shared infrastructure is not deployed." });

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

          return yield* health;
        }),
      ),
    );
    state = { ...state, infrastructureDeployed: true };
    yield* save(stateFile, state);

    return reused;
  });

  const targets = Effect.fnUntraced(function* (
    name: string,
    cpu: boolean,
    workers: readonly string[],
    workerBuilds: Readonly<Record<string, WorkerBuild>>,
    persistent = false,
  ) {
    yield* Schema.decodeEffect(Name)(name);
    const names = yield* Schema.decodeUnknownEffect(ownedWorkers(name))(workers);

    yield* Schema.decodeEffect(Schema.Record(Name, WorkerBuild))(workerBuilds);
    if (
      Object.keys(workerBuilds).length !== names.length ||
      names.some((worker) => !workerBuilds[worker])
    )
      return yield* BenchError.make({
        message: "Every Worker in the pool requires exactly one explicit build assignment.",
      });

    // One retained pool per prefix. Retire legacy four-binding pools before allocating replacements.
    for (const old of Object.values(state.targets))
      if (
        old.name.startsWith(cloud.prefix + "-pool-") &&
        (old.name !== name || old.namespaceLayout !== "selected-target")
      ) {
        yield* Console.error("Retiring the previous owned Worker pool before deployment…");
        yield* destroy(old.name);
      }
    const retainedBuilds: Record<string, WorkerBuild> = {};
    const copied = new Map<string, string>();

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
      bundle: "explicit-worker-builds",
      build: hash(
        JSON.stringify(
          Object.entries(workerBuilds)
            .map(([worker, assigned]) => [worker, assigned.build])
            .sort(),
        ),
      ),
      cpu,
      workers: names,
      workerBuilds: retainedBuilds,
      namespaceLayout: "selected-target",
    };

    state = { ...state, targets: { ...state.targets, [name]: stack } };
    yield* save(stateFile, state);
    yield* Console.error(`Uploading ${names.length} target Worker(s)…`);
    yield* alchemy(stack, "deploy");

    const health = readyTargets(names, retainedBuilds);

    const readyEvents = yield* health.pipe(
      Effect.catchTag("BenchError", () =>
        Effect.gen(function* () {
          yield* Console.error(
            "Target endpoint unavailable; reconciling its Alchemy deployment once…",
          );
          yield* alchemy(stack, "deploy", true);

          return yield* health;
        }),
      ),
    );

    const endpoints: Readonly<Record<string, string>> = Object.fromEntries(
      names.map((worker) => [worker, url(worker)]),
    );

    const events = yield* Effect.forEach(
      readyEvents,
      Effect.fnUntraced(function* (event) {
        const uploadedMs = uploads.get(event.worker);

        if (uploadedMs === undefined)
          return yield* BenchError.make({ message: "Missing target upload chronology." });

        return { ...event, uploadedMs };
      }),
    );

    return { endpoints, events };
  });

  const attach = Effect.fnUntraced(function* (
    name: string,
    expectedBuilds: Readonly<Record<string, WorkerBuild>>,
    expectedInfrastructureBuild: string,
  ) {
    yield* Schema.decodeEffect(Name)(name);
    const stack = state.targets[name];

    if (
      !stack ||
      stack.name !== name ||
      stack.kind !== "target" ||
      !stack.workerBuilds ||
      Object.keys(stack.workerBuilds).length !== Object.keys(expectedBuilds).length
    )
      return yield* BenchError.make({
        message: "Resume target assignments differ from the owned deployment.",
      });
    for (const [worker, expected] of Object.entries(expectedBuilds)) {
      const assigned = stack.workerBuilds[worker];
      const targetHash = expected.build.slice(0, 64);

      if (
        !assigned ||
        assigned.build !== expected.build ||
        !/^[a-f0-9]{64}$/.test(targetHash) ||
        expected.build !== `${targetHash}-${name}-e0` ||
        hash(yield* fs.readFile(assigned.bundle)) !== targetHash
      )
        return yield* BenchError.make({
          message: "Retained target differs from the resume assignment.",
        });
    }

    const revision = yield* infrastructureBuild();

    if (revision !== expectedInfrastructureBuild)
      return yield* BenchError.make({
        message: "Shared infrastructure differs from the resume build.",
      });

    const names = yield* Schema.decodeUnknownEffect(ownedWorkers(name))(stack.workers ?? [name]);

    yield* readyInfrastructure(revision);
    yield* readyTargets(names, expectedBuilds);

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

    return yield* BenchError.make({ message: "Cleanup verification did not complete" });
  });

  const destroy = Effect.fnUntraced(function* (name: string) {
    const stack = state.targets[name];

    if (stack) yield* alchemy(stack, "destroy");

    const remaining = yield* verifyCleanup(name);

    if (!remaining.verified)
      return yield* BenchError.make({
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
      return yield* BenchError.make({
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
    targets,
    attach,
    destroy,
    teardown,
  };
});
