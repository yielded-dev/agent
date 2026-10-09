import { join } from "node:path";

import { Console, Effect, FileSystem, Schema } from "effect";

import { build } from "./build.ts";
import { Cloudflare, request } from "./cloudflare.ts";
import { Health } from "./model.ts";
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

  const Stack = Schema.Struct({
    name: Schema.String.check(Schema.isPattern(new RegExp(`^${cloud.prefix}-[a-z0-9-]+$`))),
    kind: Schema.Literals(["infrastructure", "target"]),
    bundle: Schema.String,
    build: Schema.String,
    cpu: Schema.Boolean,
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

  const alchemy = Effect.fnUntraced(function* (
    stack: Stack,
    action: "deploy" | "destroy",
    force = false,
  ) {
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
        DURABLE_BENCH_BUNDLE: stack.bundle,
        DURABLE_BENCH_BUILD: stack.build,
        DURABLE_BENCH_TOKEN: state.token,
        DURABLE_BENCH_CPU: String(stack.cpu),
        DURABLE_BENCH_PROVIDER: provider,
        DURABLE_BENCH_SUBDOMAIN: cloud.subdomain,
      },
    );

    if (result.code !== 0)
      return yield* new BenchError({
        message: `Alchemy ${action} failed (${result.code}): ${redact(result.output, secrets).slice(-3000)}`,
      });
  });

  const ready = Effect.fnUntraced(function* (
    endpoint: string,
    expected: string,
    fromDriver = false,
    role = "target",
  ) {
    let consecutive = 0;

    for (let attempt = 0; attempt < 30; attempt++) {
      const health = yield* request(
        fromDriver ? driver + "/ready" : endpoint + "/health?probe=" + nonce(),
        state.token,
        Health,
        fromDriver ? { targetUrl: endpoint } : undefined,
      ).pipe(Effect.timeout("10 seconds"), Effect.result);

      consecutive =
        health._tag === "Success" && health.success.value.build === expected ? consecutive + 1 : 0;
      if (consecutive === 3) return;
      yield* Effect.sleep("2 seconds");
    }

    return yield* new BenchError({
      message: `${role} Worker health did not propagate${fromDriver ? " through the driver" : ""}. No benchmark input was sent.`,
    });
  });

  const infrastructure = Effect.gen(function* () {
    const output = join(privateDirectory, "infrastructure");
    const driverBuild = yield* build(output, "driver");
    const providerBuild = yield* build(output, "provider");

    const revision = hash(
      driverBuild.sha256 +
        providerBuild.sha256 +
        hash(yield* fs.readFile(join(directory, "stack.ts"))),
    );

    let reused = state.infrastructure?.build === revision && state.infrastructureDeployed === true;

    const infrastructure: Stack = {
      name: cloud.prefix + "-infrastructure",
      kind: "infrastructure",
      bundle: output,
      build: revision,
      cpu: false,
    };

    if (!reused) {
      state = { ...state, infrastructure, infrastructureDeployed: false };
      yield* save(stateFile, state);
      yield* Console.error("Deploying shared driver and mock provider…");
      yield* alchemy(infrastructure, "deploy");
    }

    const health = Effect.gen(function* () {
      yield* ready(driver, revision, false, "driver");
      yield* ready(url(state.infrastructurePrefix + "-provider"), revision, true, "provider");
    });

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

  const target = Effect.fnUntraced(function* (
    name: string,
    bundle: string,
    revision: string,
    cpu: boolean,
  ) {
    const stack: Stack = { name, kind: "target", bundle, build: revision, cpu };

    state = { ...state, targets: { ...state.targets, [name]: stack } };
    yield* save(stateFile, state);
    yield* Console.error("Deploying target Worker…");
    yield* alchemy(stack, "deploy");

    const health = Effect.gen(function* () {
      yield* ready(url(name), revision);
      yield* ready(url(name), revision, true);
    });

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

    return url(name);
  });

  const destroy = Effect.fnUntraced(function* (name: string) {
    const stack = state.targets[name];

    if (stack) yield* alchemy(stack, "destroy");
    const remaining = yield* cloud.resources(name);

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
    const remaining = yield* cloud.resources();

    if (!remaining.verified)
      return yield* new BenchError({
        message: `${cloud.prefix} resources remain in this account; cleanup is not verified.`,
      });
    yield* fs.remove(privateDirectory, { recursive: true });

    return remaining;
  });

  return { token: state.token, driver, infrastructure, target, destroy, teardown };
});
