import { join } from "node:path";

import {
  Cause,
  Clock,
  Console,
  DateTime,
  Deferred,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Semaphore,
  Schema,
  Stream,
} from "effect";

import {
  history,
  MEASURED_TOOLS,
  next,
  payload,
  turn,
  type Message,
  type Turn,
} from "../src/plan.ts";
import { checkCost, planCost, reserveCost } from "./budget.ts";
import { build, ensureVendor } from "./build.ts";
import {
  Cloudflare,
  measureRequest,
  measureResult,
  request,
  requestEvents,
  retryRequest,
} from "./cloudflare.ts";
import { deployments } from "./deploy.ts";
import { checkFixture } from "./fixture-check.ts";
import { prepareFixtures } from "./fixtures.ts";
import {
  BuildEvent,
  HistoryCheckpoint,
  Options,
  Result,
  type Profile,
  type Sample,
} from "./model.ts";
import {
  BenchError,
  git,
  hash,
  nonce,
  read,
  redact,
  save,
  stateDirectory,
  workspace,
} from "./platform.ts";
import { prepareProfile, profileBatches } from "./profile.ts";
import { median, retirementFailures, sampleCpu, table, writeReport } from "./report.ts";
import { tailCpu } from "./tail-cpu.ts";
import { PREAMBLE, responseText } from "./text.ts";
import {
  type TransientObjectTransport,
  ColdResult,
  expectedSeed,
  ImportResult,
  Identity,
  ObjectIdentity,
  MaintenanceStatus,
  RouteReadiness,
  measurementPaths,
  RestoreAck,
  RestoreChunk,
  type Query,
} from "./worker/protocol.ts";
import type { UsageSnapshot } from "./worker/usage.ts";

const COLD_PROPAGATION_DELAY = "180 seconds";

const costInputs = Effect.fnUntraced(function* (options: Options) {
  if (options.buildHistory) return { prepared: undefined, plan: planCost(options, [], new Map()) };
  if (options.fixtureDirectory === undefined)
    return yield* BenchError.make({
      message: "Comparison requires --fixture-directory with saved, shared fixtures.",
    });

  const prepared = yield* prepareFixtures({
    targets: options.targets,
    sizes: options.sizes,
    directory: options.fixtureDirectory,
    ...(options.restoreBytes === undefined ? {} : { requestBytes: options.restoreBytes }),
  });

  return { prepared, plan: planCost(options, prepared.fixtures, prepared.writeEstimates) };
});

export const previewCost = Effect.fnUntraced(function* (options: Options) {
  const { plan } = yield* costInputs(options);

  yield* Console.log(JSON.stringify(plan, null, 2));
  yield* Console.error(
    "Admission estimate includes retry/recovery allowances; it is not an invoice. No deployment or budget reservation was made.",
  );
  yield* checkCost(options, plan);
}, Effect.scoped);

class IncarnationLost extends Schema.TaggedError<IncarnationLost>()("IncarnationLost", {
  message: Schema.String,
  identity: Identity,
}) {}

const equalCounts = (a: Readonly<Record<string, number>>, b: Readonly<Record<string, number>>) =>
  Object.keys(a).length === Object.keys(b).length &&
  Object.keys(a).every((key) => a[key] === b[key]);

// The reference transcript is independent of every framework and computed before timing.
const append = (
  messages: Message[],
  input: Turn,
  textStreaming = false,
  capture = true,
): string[] => {
  const hashes: string[] = [];

  messages.push({ role: "user", text: input.text });
  for (;;) {
    if (capture)
      hashes.push(
        hash(JSON.stringify(messages.map((m) => [m.role, m.text, m.calls ?? []]))).slice(0, 16),
      );
    const step = next(messages);

    if ("answer" in step) {
      messages.push({ role: "assistant", text: responseText(step.answer, textStreaming) });

      return hashes;
    }
    if (textStreaming && messages.at(-1)?.role === "user")
      messages.push({ role: "assistant", text: PREAMBLE });
    messages.push(
      { role: "assistant", text: "", calls: [step.call] },
      { role: "tool", text: payload(step.call) },
    );
  }
};

export const run = Effect.fnUntraced(function* (options: Options, resumed?: Result) {
  const snapshotProfile = !options.buildHistory && options.profiles.length > 0;

  if (snapshotProfile && (!options.baseline?.trim() || !options.candidate?.trim()))
    return yield* BenchError.make({
      message:
        "Snapshot profiling requires baseline and candidate refs (CLI defaults: origin/main and HEAD).",
    });

  const started = yield* Clock.currentTimeMillis;
  const seed = options.seed ?? nonce();
  const { prepared: costFixtures, plan: costPlan } = yield* costInputs(options);

  yield* checkCost(options, costPlan);

  const shuffle = <A>(values: readonly A[], phase: string): A[] =>
    values
      .map((value, index) => ({ value, order: hash(JSON.stringify([seed, phase, index])) }))
      .sort((a, b) => a.order.localeCompare(b.order))
      .map(({ value }) => value);

  const cloud = yield* Cloudflare;

  const deploy = yield* deployments;
  const fs = yield* FileSystem.FileSystem;
  const privateDirectory = stateDirectory(cloud.prefix);
  const runName = resumed?.run ?? `${cloud.prefix}-${started.toString(36)}-${nonce().slice(0, 8)}`;

  const persistent =
    !options.buildHistory &&
    !options.rigorous &&
    options.profiles.length === 0 &&
    !options.targets.includes("tardie");

  const targetStack = persistent
    ? `${cloud.prefix}-pool-${hash(JSON.stringify([options.targets, options.sizes, options.ttft, options.objects, options.cpu])).slice(0, 8)}`
    : runName;

  const output = join(workspace, "results", runName);
  const lock = yield* Semaphore.make(1);
  const withProfileBatch = yield* profileBatches(options.profiles.length);

  let result: Result = resumed
    ? {
        ...resumed,
        complete: false,
        resumeCount: (resumed.resumeCount ?? 0) + 1,
      }
    : {
        version: 4,
        run: runName,
        revision: yield* git(["rev-parse", "HEAD"]),
        dirty: (yield* git(["status", "--porcelain"])).length > 0,
        startedAt: DateTime.formatIso(DateTime.makeUnsafe(started)),
        wallMs: 0,
        infrastructureReused: false,
        resumeCount: 0,
        options: {
          ...options,
          seed,
          targets: [...options.targets],
          sizes: [...options.sizes],
          ttft: [...options.ttft],
          profiles: [...options.profiles],
        },
        builds: [],
        versions: {},
        histories: [],
        fixtures: [],
        samples: [],
        readiness: [],
        failures: [],
        profiles: [],
        kept: options.keep,
        ...(persistent ? { workerPool: targetStack } : {}),
        complete: false,
        ...(snapshotProfile
          ? {
              diagnostic: {
                kind: "snapshot-cpu",
                epoch: 0,
                complete: false,
                coverage: "unverified",
              } as const,
            }
          : {}),
      };

  const message = (cause: Cause.Cause<unknown>) =>
    redact(
      Cause.prettyErrors(cause)
        .map((e) => e.message)
        .join("; "),
      [cloud.accountId, cloud.apiToken, deploy.token, cloud.accountName],
    );

  const update = (change: (value: Result) => Result) =>
    lock.withPermit(
      Effect.gen(function* () {
        result = change(result);
        yield* save(output + ".json", result);
      }),
    );

  const updateDiagnostic = (fields: Partial<NonNullable<Result["diagnostic"]>>) =>
    update((value) =>
      value.diagnostic ? { ...value, diagnostic: { ...value.diagnostic, ...fields } } : value,
    );

  let phaseStarted = started;
  let phaseName = "preparation";

  const recordUsage = Effect.fnUntraced(function* (
    object: string,
    data: {
      readonly identity?: { readonly incarnation: string };
      readonly storageUsage?: typeof UsageSnapshot.Type;
    },
  ) {
    if (!options.writes) return;

    const row = {
      phase: phaseName,
      object,
      incarnation: data.identity?.incarnation ?? "unknown",
      at: yield* Clock.currentTimeMillis,
      usage: data.storageUsage ?? { unavailable: "Response did not include native write counters" },
    };

    yield* update((value) => ({
      ...value,
      storageUsage: [
        // Keep the latest known count and unavailable reason independently.
        ...(value.storageUsage ?? []).filter(
          (old) =>
            old.object !== row.object ||
            old.phase !== row.phase ||
            old.incarnation !== row.incarnation ||
            "unavailable" in old.usage !== "unavailable" in row.usage,
        ),
        row,
      ],
    }));
  });

  const phase = Effect.fnUntraced(function* (next: string) {
    const now = yield* Clock.currentTimeMillis;

    result = {
      ...result,
      phases: [...(result.phases ?? []), { phase: phaseName, ms: now - phaseStarted }],
    };
    phaseStarted = now;
    phaseName = next;
  });

  let targetWorkers: readonly string[] = [runName];
  const targetOwners = new Map<string, Pick<Query, "target" | "object">>();

  yield* Console.error(
    `Preparing ${options.buildHistory ? "deployed history builds" : "fixtures"}…`,
  );
  yield* update((value) => value);

  const finish = Effect.fnUntraced(function* <A, E>(exit: Exit.Exit<A, E>) {
    const preserved = options.buildHistory && Exit.isFailure(exit);

    if (Exit.isFailure(exit)) {
      result = {
        ...result,
        kept: preserved || options.keep,
        failures: Cause.hasInterruptsOnly(exit.cause)
          ? result.failures
          : [...result.failures, message(exit.cause)],
        samples: result.samples.map((row) =>
          row.status === "running"
            ? {
                ...row,
                status: "failed",
                error: "Controller interrupted; outcome unknown, input will not be replayed.",
              }
            : row,
        ),
        histories: result.histories.map((row) =>
          row.status === "running"
            ? {
                ...row,
                status: preserved ? "paused" : "failed",
              }
            : row,
        ),
      };
    }
    if (
      options.cpu &&
      result.samples.some(
        (sample) => sample.status === "ok" && (sample.state === "cold" || sample.state === "warm"),
      )
    ) {
      yield* Console.error("Collecting optional invocation CPU…");
      yield* Effect.sleep("15 seconds");

      let completeCpu = false;

      for (let attempt = 0; attempt < 6; attempt++) {
        yield* Effect.forEach(
          targetWorkers,
          (worker) => {
            const owner = targetOwners.get(worker);

            return owner === undefined
              ? Effect.fail(BenchError.make({ message: "CPU worker has no Object owner" }))
              : tailCpu(deploy.tail, deploy.token, owner.object, owner.target);
          },
          {
            concurrency: 3,
          },
        ).pipe(
          Effect.matchCauseEffect({
            onFailure: (cause) =>
              Effect.sync(() => {
                result = { ...result, failures: [...result.failures, message(cause)] };
              }),
            onSuccess: (data) =>
              Effect.sync(() => {
                result = {
                  ...result,
                  cpu: data.flat(),
                };
              }),
          }),
        );
        const samples = sampleCpu(result);

        completeCpu = samples.length > 0 && samples.every((sample) => sample.cpuMs !== undefined);
        if (completeCpu) break;
        if (attempt < 5) {
          yield* Console.error("Waiting for complete Tail invocation CPU…");
          yield* Effect.sleep("15 seconds");
        }
      }
      if (!completeCpu)
        result = {
          ...result,
          failures: [
            ...result.failures,
            "Tail CPU attribution is incomplete; refusing partial pairs",
          ],
        };
    }
    if (!snapshotProfile)
      result = { ...result, failures: [...result.failures, ...retirementFailures(result)] };
    yield* phase("teardown");
    if (!options.keep && !preserved && !persistent) {
      yield* Console.error("Destroying target stack and verifying cleanup…");
      yield* deploy.destroy(targetStack).pipe(
        Effect.matchCauseEffect({
          onFailure: (cause) =>
            Effect.sync(() => {
              result = { ...result, failures: [...result.failures, message(cause)] };
            }),
          onSuccess: (cleanup) =>
            Effect.sync(() => {
              result = { ...result, cleanup };
            }),
        }),
      );
    }
    yield* phase("report");

    const completed =
      Exit.isSuccess(exit) &&
      result.failures.length === 0 &&
      (options.keep || persistent || result.cleanup?.verified === true);

    result = {
      ...result,
      kept: options.keep || preserved || persistent,
      wallMs: (yield* Clock.currentTimeMillis) - Date.parse(result.startedAt),
      complete: !snapshotProfile && completed,
      ...(result.diagnostic
        ? {
            diagnostic: {
              ...result.diagnostic,
              complete:
                completed && result.profiles.length === 1 && result.diagnostic.after !== undefined,
            },
          }
        : {}),
    };
    yield* save(output + ".json", result);

    const report = options.writes
      ? writeReport(result)
      : result.diagnostic
        ? `Snapshot CPU diagnostic: ${result.diagnostic.complete ? "complete" : "incomplete"}.\nSelected Object: ${result.diagnostic.query?.object ?? "not selected"}; sample: ${result.diagnostic.query?.sample ?? "not started"}.\nCoverage: ${result.diagnostic.coverage}. Matching module tokens do not prove profiler attachment survived Object abort; inspect constructor frames.\nNo timing comparison or inferential tables.\n`
        : table(result);

    yield* fs.writeFileString(output + ".md", report);
    yield* Console.log(report);
    yield* Console.error(
      `Wall time ${(result.wallMs / 1000).toFixed(1)} s; infrastructure ${result.infrastructureReused ? "reused" : "deployed"}. Results: results/${runName}.json`,
    );
    if (persistent)
      yield* Console.error(
        "Worker pool retained; --teardown destroys and verifies all resources for this prefix.",
      );
    for (const profile of result.profiles) yield* Console.error(`Profile: ${profile.file}`);
    if (preserved)
      yield* Console.error(
        `Build retained. Resume with --resume ${runName}; unknown in-flight batches will be retired.`,
      );
    if (!result.complete && !result.diagnostic?.complete)
      return yield* BenchError.make({
        message: "Run incomplete; failures and cleanup status are recorded in the result JSON.",
      });
  });

  yield* Effect.gen(function* () {
    yield* update((value) => ({ ...value, costPlan }));
    if (!resumed) {
      if (
        (yield* git([
          "status",
          "--porcelain",
          "--",
          "examples/durable-bench",
          "package.json",
          "bun.lock",
          "patches",
        ])).length > 0
      )
        return yield* BenchError.make({
          message:
            "Commit harness and dependency inputs before comparison; both arms use frozen commits.",
        });
      yield* ensureVendor;
    }
    const versions: Record<string, string> = {};

    for (const file of resumed
      ? []
      : [
          "../../packages/effect-agent/package.json",
          "../../node_modules/effect/package.json",
          "third-party/node_modules/@earendil-works/pi-durable/package.json",
          "third-party/node_modules/@earendil-works/pi-ai/package.json",
          "third-party/node_modules/@earendil-works/chord/package.json",
          "third-party/node_modules/tardie/package.json",
        ]) {
      const metadata = yield* read(
        join(workspace, file),
        Schema.Struct({ name: Schema.String, version: Schema.String }),
      );

      versions[metadata.name] = metadata.version;
    }

    type Label = Sample["build"];
    const labels: readonly Label[] = options.baseline ? ["baseline", "candidate"] : ["working"];
    const builds = new Map<string, Effect.Success<ReturnType<typeof build>>>();
    const refs = new Map<Label, string>();

    const ordered = Effect.fnUntraced(function* <A>(
      values: readonly A[],
      phase: string,
      key: (value: A) => string,
    ) {
      const items = shuffle(values, phase);

      yield* update((value) => ({
        ...value,
        orders: [...(value.orders ?? []), { phase, items: items.map(key) }],
      }));

      return items;
    });

    // Resolve both inputs before either build. A worktree path is never an implicit snapshot.
    for (const label of labels) {
      const ref =
        label === "baseline"
          ? options.baseline
          : label === "candidate"
            ? options.candidate
            : "HEAD";

      const revision = yield* git([
        "rev-parse",
        "--verify",
        "--end-of-options",
        `${ref ?? "HEAD"}^{commit}`,
      ]);

      refs.set(label, revision);
    }
    if (!resumed) {
      const materialization = yield* Effect.forEach(
        [...refs],
        Effect.fnUntraced(function* ([label, revision]) {
          return {
            label,
            revision,
            tree: yield* git(["rev-parse", `${revision}^{tree}`]),
            kind: "commit-archive" as const,
          };
        }),
      );

      yield* update((value) => ({ ...value, materialization }));
    }
    const jobs = labels.flatMap((label) => options.targets.map((target) => ({ label, target })));
    const jobKey = ({ label, target }: (typeof jobs)[number]) => `${label}/${target}`;

    yield* phase("build");
    for (const job of yield* ordered(jobs, "build-target", jobKey)) {
      const { label, target } = job;
      const folder = join(privateDirectory, runName, label, target);

      if (resumed) {
        const previous = resumed.builds.find(
          (item) => item.label === label && item.target === target,
        );

        if (!previous || !previous.harnessRevision)
          return yield* BenchError.make({ message: "Missing preserved target build." });
        builds.set(jobKey(job), {
          ...previous,
          harnessRevision: previous.harnessRevision,
          dependencyCacheHit: previous.dependencyCacheHit ?? false,
          file: join(folder, "target.mjs"),
        });
        continue;
      }
      const compiled = yield* build(folder, "target", refs.get(label), target);

      if (compiled.harnessRevision !== result.revision)
        return yield* BenchError.make({ message: "Harness revision changed during the build." });

      if (options.profiles.length > 0) {
        const retained = join(output, label, target);

        yield* fs.makeDirectory(retained, { recursive: true, mode: 0o700 });
        yield* fs.copyFile(compiled.file, join(retained, "target.mjs"));
        yield* fs.copyFile(compiled.file + ".map", join(retained, "target.mjs.map"));
      }
      builds.set(jobKey(job), compiled);
      yield* update((value) => ({
        ...value,
        builds: [
          ...value.builds,
          {
            label,
            target,
            revision: compiled.revision,
            sha256: compiled.sha256,
            harnessRevision: compiled.harnessRevision,
            dependencyCacheHit: compiled.dependencyCacheHit,
          },
        ],
      }));
    }

    const requireIdenticalControl = Effect.fnUntraced(function* (artifacts: typeof builds) {
      if (options.targets.includes("pi") && labels.length > 1) {
        const controls = labels.map((label) => artifacts.get(`${label}/pi`));

        if (
          controls.some((item) => !item) ||
          new Set(controls.map((item) => item?.sha256)).size !== 1
        )
          return yield* BenchError.make({
            message: "Pi executable hashes differ across arms; deployment refused.",
          });
      }
    });

    yield* requireIdenticalControl(builds);

    yield* phase("local fixture");
    const preparedByBuild = new Map<Label, Effect.Success<ReturnType<typeof prepareFixtures>>>();
    const restoreBuilds = new Map<string, Effect.Success<ReturnType<typeof build>>>();

    if (!options.buildHistory) {
      if (options.fixtureDirectory === undefined)
        return yield* BenchError.make({
          message:
            "Comparison requires --fixture-directory with pre-existing shared fixtures; no arm may generate the other's history.",
        });

      const prepared = costFixtures;

      if (!prepared) return yield* BenchError.make({ message: "Missing costed fixtures." });

      for (const label of labels) preparedByBuild.set(label, prepared);
      // The floor restores its ephemeral Prompt through the measurement implementation.
      for (const job of yield* ordered(
        jobs.filter((job) => job.target !== "effect-ai-floor"),
        "build-restore",
        jobKey,
      ))
        restoreBuilds.set(
          jobKey(job),
          yield* build(
            join(privateDirectory, runName, `${job.label}-restore`, job.target),
            "restore",
            refs.get(job.label),
            job.target,
          ),
        );
      yield* requireIdenticalControl(restoreBuilds);
      // No source-code equality heuristic: execute each arm's own supported opener.
      for (const job of yield* ordered(
        jobs.filter((job) => job.target === "yielded"),
        "fixture-validation",
        jobKey,
      )) {
        const compiled = restoreBuilds.get(jobKey(job));

        if (!compiled)
          return yield* BenchError.make({ message: "Missing native fixture preflight build." });
        for (const fixture of prepared.fixtures.filter((fixture) => fixture.target === "yielded")) {
          const compatibility = yield* checkFixture(
            compiled,
            fixture,
            prepared.chunks.get(`yielded-${fixture.history}`) ?? [],
          );

          yield* update((value) => ({
            ...value,
            fixtureChecks: [
              ...(value.fixtureChecks ?? []),
              {
                label: job.label,
                history: fixture.history,
                formatKey: prepared.formatKey,
                build: compiled.sha256,
                ...compatibility,
              },
            ],
          }));
        }
      }
    }
    const firstFixtures = preparedByBuild.values().next().value?.fixtures ?? [];

    // Every revision must name the same physical Object; no Thread-ID rewriting is permitted.
    for (const prepared of preparedByBuild.values())
      for (const fixture of prepared.fixtures)
        if (
          firstFixtures.find(
            (item) => item.target === fixture.target && item.history === fixture.history,
          )?.threadId !== fixture.threadId
        )
          return yield* BenchError.make({
            message: "Paired fixtures must retain the same Thread identity",
          });

    yield* update((value) => ({
      ...value,
      versions: resumed ? value.versions : versions,
      fixtures: [...preparedByBuild].flatMap(([label, prepared]) =>
        prepared.fixtures.map(({ target, history, fingerprint, tables, mode }) => ({
          label,
          formatKey: prepared.formatKey,
          target,
          history,
          fingerprint,
          tables,
          mode,
        })),
      ),
    }));
    yield* phase("infrastructure");
    const reservation = yield* reserveCost(cloud.accountId, runName, options, costPlan);

    yield* update((value) => ({ ...value, reservation }));
    yield* Console.error(
      `Reserved ${reservation.rows.toLocaleString("en-US")} estimated row writes; ${reservation.rollingRows.toLocaleString("en-US")}/${reservation.limit.toLocaleString("en-US")} in the shared rolling 24h budget.`,
    );
    if (!resumed) {
      const reused = yield* deploy.infrastructure(options.cpu);
      const infrastructureBuild = yield* deploy.infrastructureBuild();

      yield* update((value) => ({ ...value, infrastructureReused: reused, infrastructureBuild }));
    }

    yield* phase("reference transcript");

    const epochs: readonly Label[] = options.rigorous
      ? hash(seed + "/epochs").charAt(0) < "8"
        ? ["baseline", "candidate", "candidate", "baseline"]
        : ["candidate", "baseline", "baseline", "candidate"]
      : options.baseline
        ? ["baseline", "candidate"]
        : ["working"];

    const turnsPerEpoch =
      options.repeats + (options.buildHistory ? 1 : 1 + (options.coldSamples ?? 2));

    const coldPosition = (index: number) => index === 1 || index >= options.repeats + 2;

    const sampleName = (epoch: number, index: number, attempt = 0) =>
      options.buildHistory
        ? `m${index}`
        : `${attempt === 0 ? "e" : "r"}${epoch}${attempt > 1 ? `a${attempt}` : ""}-t${index}`;

    const references = new Map<number, Map<string, string[]>>();
    const seedReferences = new Map<number, Map<number, string>>();
    const seededTranscripts = new Map<number, readonly Message[]>();

    for (const size of options.sizes) {
      let messages: Message[] = [];

      const seeds = new Map<number, string>();

      for (const [index, input] of history(0, size).entries()) {
        const checkpoint = (options.buildHistory && (index + 1) % 50 === 0) || index + 1 === size;
        const hashes = append(messages, input, false, checkpoint);
        const digest = hashes.at(-1);

        if (digest) seeds.set(index + 1, digest);
      }
      if (expectedSeed[size] !== undefined && seeds.get(size) !== expectedSeed[size])
        return yield* BenchError.make({
          message: "Independent history plan no longer matches the existing seed fingerprint.",
        });
      seedReferences.set(size, seeds);
      const expected = new Map<string, string[]>();

      const seededMessages = [...messages];

      seededTranscripts.set(size, seededMessages);

      for (let epoch = 0; epoch < epochs.length; epoch++) {
        if (!options.buildHistory && options.restoreEachPass === true)
          messages = [...seededMessages];
        for (let index = 0; index < turnsPerEpoch; index++) {
          const sample = sampleName(epoch, index);

          expected.set(
            sample,
            append(messages, turn(sample, MEASURED_TOOLS), options.textStreaming),
          );
        }
      }
      references.set(size, expected);
    }

    const cohorts = options.sizes.flatMap((size) =>
      options.ttft.flatMap((ttftMs) =>
        Array.from({ length: options.objects }, (_, object) =>
          options.targets.map((target): Omit<Query, "expectedBuild"> => ({
            target,
            object: `${runName}-h${size}-d${ttftMs}-o${object}`,
            ...(options.buildHistory
              ? {}
              : {
                  storageObject:
                    firstFixtures.find(
                      (fixture) => fixture.target === target && fixture.history === size,
                    )?.threadId ?? "main",
                }),
            history: size,
            ttftMs,
            chunkDelayMs: ttftMs === 400 ? 10 : 0,
            textStreaming: options.textStreaming,
            sample: "import",
          })),
        ).flat(),
      ),
    );

    const failedObjects = new Set<string>();
    const measuredObjects = new Set<string>();

    const objectKey = (cohort: Pick<Query, "target" | "object">) =>
      `${cohort.target}/${cohort.object}`;

    const workerName = (cohort: Pick<Query, "target" | "object">) =>
      `${persistent ? cohort.object.replace(runName, targetStack) : cohort.object}-${cohort.target}`;

    for (const cohort of cohorts)
      targetOwners.set(workerName(cohort), { target: cohort.target, object: cohort.object });

    const counterbalanced = Boolean(options.baseline && !options.buildHistory);
    const firstLabels = new Map<string, "baseline" | "candidate">();

    if (counterbalanced) {
      for (const size of options.sizes)
        for (const ttft of options.ttft)
          for (const target of options.targets) {
            const cell = yield* ordered(
              cohorts.filter(
                (cohort) =>
                  cohort.history === size && cohort.ttftMs === ttft && cohort.target === target,
              ),
              `assignment/${size}/${ttft}/${options.isolateCold ? "paired-targets" : target}`,
              objectKey,
            );

            for (const [index, cohort] of cell.entries())
              firstLabels.set(
                objectKey(cohort),
                index < Math.ceil(cell.length / 2) ? "baseline" : "candidate",
              );
          }
    }

    if (resumed) {
      for (const cohort of cohorts) {
        const rows = result.samples.filter((row) => objectKey(row) === objectKey(cohort));

        if (rows.length === turnsPerEpoch && rows.every((row) => row.status === "ok"))
          measuredObjects.add(objectKey(cohort));
        else if (rows.length > 0) {
          failedObjects.add(objectKey(cohort));
          yield* update((value) => ({
            ...value,
            failures: [
              ...value.failures,
              `${cohort.target}/${cohort.history}: partial measurement retired on resume; no input was replayed.`,
            ],
          }));
        }
        if (
          result.histories.some(
            (row) => objectKey(row) === objectKey(cohort) && row.status === "failed",
          )
        )
          failedObjects.add(objectKey(cohort));
      }
    }

    const isolateEpochs = new Map<string, number>();

    const poolCohorts = cohorts;
    const retriedPasses = new Set<string>();

    const measureEpoch = Effect.fnUntraced(function* (
      epoch: number,
      defaultLabel: Label,
      cohorts: typeof poolCohorts,
      attempt: number,
      replayBefore: ReadonlyMap<string, number>,
    ) {
      const active = new Set(cohorts.map(objectKey));
      const retryFrom = new Map<string, number>();

      // Rebuild the acknowledged prefix with fresh IDs after restoring the fixture.
      // These reference transcripts are computed before any replacement is timed.
      if (attempt > 0)
        for (const size of new Set(cohorts.map((cohort) => cohort.history))) {
          const seeded = seededTranscripts.get(size);
          const expected = references.get(size);

          if (!seeded || !expected)
            return yield* BenchError.make({ message: "Missing recovery reference transcript." });
          const messages = [...seeded];

          for (let index = 0; index < turnsPerEpoch; index++) {
            const sample = sampleName(epoch, index, attempt);

            expected.set(
              sample,
              append(messages, turn(sample, MEASURED_TOOLS), options.textStreaming),
            );
          }
        }

      // Every comparison pass restores the same source bytes before its arm opens them.
      const restorePass =
        !options.buildHistory && (epoch === 0 || options.restoreEachPass === true);

      const recoveryAvailable =
        !snapshotProfile &&
        restorePass &&
        (epoch === epochs.length - 1 || options.restoreEachPass === true);

      const clearBeforeRestore = persistent || epoch > 0 || attempt > 0;

      const assignment = Effect.fnUntraced(function* (cohort: Pick<Query, "target" | "object">) {
        const first = firstLabels.get(objectKey(cohort));

        const label =
          first === undefined
            ? defaultLabel
            : epoch === 0 || (options.rigorous && epoch === 3)
              ? first
              : first === "baseline"
                ? "candidate"
                : "baseline";

        const compiled = builds.get(`${label}/${cohort.target}`);

        if (!compiled) return yield* BenchError.make({ message: "Missing target build." });

        return {
          label,
          compiled,
          restoreBuild: restoreBuilds.get(`${label}/${cohort.target}`),
          fixtures: preparedByBuild.get(label)?.fixtures ?? [],
          chunks: preparedByBuild.get(label)?.chunks ?? new Map<string, string[]>(),
        };
      });

      let restoring = restorePass;
      let coldGeneration = 0;

      const deploymentFor = Effect.fnUntraced(function* (cohort: Pick<Query, "target" | "object">) {
        const { compiled, restoreBuild } = yield* assignment(cohort);

        const bundle =
          restoring && active.has(objectKey(cohort)) && restoreBuild ? restoreBuild : compiled;

        return {
          bundle: bundle.file,
          build: `${bundle.sha256}-${runName}-e${epoch}${attempt > 0 ? `-a${attempt}` : ""}${coldGeneration === 0 ? "" : `-c${coldGeneration}`}`,
        };
      });

      const expectedFor = (cohort: Pick<Query, "target" | "object">) =>
        deploymentFor(cohort).pipe(Effect.map((deployment) => deployment.build));

      const assignments = Effect.fnUntraced(function* (items: typeof poolCohorts) {
        return Object.fromEntries(
          yield* Effect.forEach(
            items,
            Effect.fnUntraced(function* (cohort) {
              return [workerName(cohort), yield* deploymentFor(cohort)] as const;
            }),
          ),
        );
      });

      const deployPass = Effect.fnUntraced(function* (probe = true) {
        const items = yield* ordered(
          poolCohorts,
          `deploy/${epoch}/${attempt}/${restoring ? "restore" : "target"}`,
          objectKey,
        );

        const deployed = yield* deploy.targets(
          targetStack,
          options.cpu,
          items.map(workerName),
          yield* assignments(items),
          persistent,
          probe,
          options.writes ?? false,
        );

        yield* update((value) => ({
          ...value,
          deployments: [...(value.deployments ?? []), ...deployed.events],
        }));

        return deployed.endpoints;
      });

      yield* phase("deploy");

      let endpoints = resumed
        ? (yield* deploy.attach(
            runName,
            yield* assignments(poolCohorts),
            resumed.infrastructureBuild ?? "",
          )).endpoints
        : yield* deployPass();

      targetWorkers = Object.keys(endpoints);

      const endpointFor = Effect.fnUntraced(function* (cohort: Pick<Query, "target" | "object">) {
        const endpoint = endpoints[workerName(cohort)];

        if (!endpoint) return yield* BenchError.make({ message: "Missing sample Worker endpoint" });

        return endpoint;
      });

      const url = Effect.fnUntraced(function* (path: string, query: Omit<Query, "expectedBuild">) {
        const address = new URL(path, yield* endpointFor(query));

        for (const [key, value] of Object.entries(query))
          address.searchParams.set(key, String(value));
        address.searchParams.set("expectedBuild", yield* expectedFor(query));

        return address;
      });

      // Worker health does not establish the Object's build. Poll read-only identity
      // before acknowledged setup resets; imports and inputs always get one attempt.
      const resetForBuild = Effect.fnUntraced(
        function* <E = never, R = never>(
          cohort: Omit<Query, "expectedBuild">,
          beforeReset?: Effect.Effect<void, E, R>,
        ) {
          const expectedBuild = yield* expectedFor(cohort);
          const started = yield* Clock.currentTimeMillis;
          const probeUrl = new URL("/measure", deploy.driver);

          probeUrl.searchParams.set(
            "readiness",
            JSON.stringify({
              query: { ...cohort, expectedBuild },
              targetUrl: yield* endpointFor(cohort),
            }),
          );
          let healthySince: number | undefined;

          for (let attempt = 1; ; attempt++) {
            const remaining = 180_000 - ((yield* Clock.currentTimeMillis) - started);

            if (remaining <= 0)
              return yield* BenchError.make({
                message: "Timed routes did not propagate before the cold reset; no input was sent.",
              });

            const probe = yield* request(
              probeUrl,
              deploy.token,
              RouteReadiness,
              undefined,
              Math.min(15_000, remaining),
            ).pipe(
              Effect.map(({ value }) => ({ value, error: undefined })),
              Effect.catchCause((cause) =>
                Effect.succeed({ value: undefined, error: message(cause) }),
              ),
            );

            const now = yield* Clock.currentTimeMillis;
            const routes = probe.value?.routes ?? [];
            const paths = measurementPaths(cohort.target);

            const ready =
              probe.value?.build === result.infrastructureBuild &&
              routes.length === paths.length &&
              paths.every((path) =>
                routes.some(
                  (route) =>
                    route.path === path && route.status === 200 && route.build === expectedBuild,
                ),
              );

            healthySince = ready ? (healthySince ?? now) : undefined;
            yield* update((value) => ({
              ...value,
              readiness: [
                ...value.readiness,
                {
                  target: cohort.target,
                  object: cohort.object,
                  epoch,
                  expectedBuild,
                  atMs: now,
                  phase: "routes",
                  outcome: ready
                    ? "ready"
                    : probe.value === undefined
                      ? "unavailable"
                      : "wrong-build",
                  ...(probe.value === undefined ? {} : { driverBuild: probe.value.build, routes }),
                  ...(probe.error === undefined ? {} : { error: probe.error }),
                  attempt,
                  waitMs: now - started,
                },
              ],
            }));
            if (healthySince !== undefined && now - healthySince >= 5000) break;
            if (!ready)
              yield* Console.error(
                `Timed routes pending: ${cohort.target}/${cohort.object}, attempt ${attempt}, ${now - started} ms`,
              );
            yield* Effect.sleep("2 seconds");
          }

          for (let attempt = 1; attempt <= 90; attempt++) {
            const remaining = 180_000 - ((yield* Clock.currentTimeMillis) - started);

            if (remaining <= 0) break;

            // Alchemy's fresh-pool placeholder has no Object fetch handler even after
            // the Worker router is healthy. Probe without resetting or importing state.
            const probe = yield* request(
              yield* url("/identity", cohort),
              deploy.token,
              ObjectIdentity,
              undefined,
              remaining,
            ).pipe(
              Effect.catchTag("RequestError", (error) =>
                error.message.startsWith("Worker /identity returned HTTP 500;") &&
                error.message.includes('"error":"Handler does not export a fetch() function."')
                  ? Effect.succeed(undefined)
                  : Effect.fail(error),
              ),
            );

            const identity = probe?.value;

            const outcome =
              identity === undefined
                ? "placeholder"
                : identity.identity.build === expectedBuild &&
                    (identity.directory === undefined || identity.directory.build === expectedBuild)
                  ? "ready"
                  : "wrong-build";

            const probeWaitMs = (yield* Clock.currentTimeMillis) - started;

            yield* update((value) => ({
              ...value,
              readiness: [
                ...value.readiness,
                {
                  target: cohort.target,
                  object: cohort.object,
                  epoch,
                  expectedBuild,
                  ...(identity === undefined
                    ? {}
                    : { objectBuild: identity.identity.build, identity: identity.identity }),
                  ...(identity?.directory === undefined
                    ? {}
                    : {
                        directoryBuild: identity.directory.build,
                        directoryIdentity: identity.directory,
                      }),
                  atMs: started + probeWaitMs,
                  phase: "identity",
                  outcome,
                  attempt,
                  waitMs: probeWaitMs,
                },
              ],
            }));
            if (outcome !== "ready") {
              yield* Console.error(
                `Object identity pending: ${cohort.target}/${cohort.object}, attempt ${attempt}, ${outcome}, ${probeWaitMs} ms`,
              );
              if (outcome === "placeholder") {
                yield* Effect.sleep("2 seconds");
                continue;
              }
              // A live old incarnation may retain its build indefinitely. Once fetch
              // exists, use the existing acknowledged cold reset to retire it, then
              // probe the replacement. Never reset a placeholder or retry an error.
            }
            const resetRemaining = 180_000 - probeWaitMs;

            if (resetRemaining <= 0) break;

            if (beforeReset) {
              if (outcome !== "ready")
                return yield* BenchError.make({
                  message: "Profiling requires the prepared Object's expected build.",
                });
              yield* beforeReset;
            }

            const reset = (yield* request(
              yield* url("/cold", cohort),
              deploy.token,
              ColdResult,
              {},
              resetRemaining,
            ).pipe((operation) => (beforeReset ? operation : retryRequest(operation)))).value;

            if (!reset.ok || !reset.threadAborted || reset.directoryAborted === false)
              return yield* BenchError.make({ message: "Cold abort was not acknowledged." });
            const waitMs = (yield* Clock.currentTimeMillis) - started;

            yield* update((value) => ({
              ...value,
              readiness: [
                ...value.readiness,
                {
                  target: cohort.target,
                  object: cohort.object,
                  epoch,
                  expectedBuild,
                  objectBuild: reset.before.build,
                  identity: reset.before,
                  atMs: started + waitMs,
                  phase: "reset",
                  ...(reset.directoryBefore === undefined
                    ? {}
                    : {
                        directoryBuild: reset.directoryBefore.build,
                        directoryIdentity: reset.directoryBefore,
                      }),
                  attempt,
                  waitMs,
                },
              ],
            }));
            if (
              outcome === "ready" &&
              reset.before.build === expectedBuild &&
              (reset.directoryBefore === undefined || reset.directoryBefore.build === expectedBuild)
            )
              // Observation attaches to the new incarnation before the submission timer starts.
              return reset;
            if (beforeReset)
              return yield* BenchError.make({
                message: "Profiled cold reset changed builds; capture will not be retried.",
              });
            yield* Effect.sleep("2 seconds");
          }

          return yield* BenchError.make({
            message: "Object build did not propagate before measurement; no input was sent.",
          });
        },
        Effect.timeoutOrElse({
          duration: "3 minutes",
          orElse: () =>
            BenchError.make({
              message: "Cold setup timed out before measurement; no input was sent.",
            }),
        }),
      );

      yield* phase("deployed seed");
      // Seeding and its count scans must not contend with timed turns, including the first A/B pass.
      if (epoch === 0 || restorePass) {
        yield* Console.error(`Seeding and verifying ${cohorts.length} Objects…`);
        yield* Effect.forEach(
          yield* ordered(
            cohorts.filter(
              (cohort) =>
                !failedObjects.has(objectKey(cohort)) && !measuredObjects.has(objectKey(cohort)),
            ),
            `restore/${epoch}/${attempt}`,
            objectKey,
          ),
          (cohort) =>
            Effect.gen(function* () {
              const { label, fixtures, chunks } = yield* assignment(cohort);
              const expectedBuild = yield* expectedFor(cohort);

              if (options.buildHistory) {
                const previous = result.histories.find(
                  (row) => objectKey(row) === objectKey(cohort),
                );

                if (!previous) {
                  yield* resetForBuild(cohort);
                  yield* update((value) => ({
                    ...value,
                    histories: [
                      ...value.histories,
                      {
                        target: cohort.target,
                        object: cohort.object,
                        history: cohort.history,
                        ttftMs: cohort.ttftMs,
                        status: "running",
                        completed: 0,
                        batches: [],
                      },
                    ],
                  }));
                } else {
                  const checkpoint = (yield* request(
                    deploy.driver + "/history-checkpoint",
                    deploy.token,
                    HistoryCheckpoint,
                    {
                      query: { ...cohort, expectedBuild },
                      targetUrl: yield* endpointFor(cohort),
                    },
                  )).value;

                  const batch = checkpoint.batch;

                  if (
                    checkpoint.identity.build !== expectedBuild ||
                    checkpoint.inFlight ||
                    checkpoint.completed < previous.completed ||
                    checkpoint.completed > cohort.history ||
                    (checkpoint.completed !== cohort.history && checkpoint.completed % 50 !== 0) ||
                    (checkpoint.completed > 0 &&
                      (!batch ||
                        batch.to !== checkpoint.completed ||
                        batch.identity.build !== expectedBuild ||
                        batch.fingerprint !==
                          seedReferences.get(cohort.history)?.get(checkpoint.completed) ||
                        (previous.startedMs !== undefined &&
                          batch.startedMs !== previous.startedMs) ||
                        (checkpoint.completed > previous.completed &&
                          batch.from !== previous.completed)))
                  )
                    return yield* BenchError.make({
                      message:
                        "Object is not at a verified history checkpoint; it is retired without replaying input.",
                    });

                  yield* update((value) => ({
                    ...value,
                    histories: value.histories.map((row) =>
                      objectKey(row) !== objectKey(cohort)
                        ? row
                        : {
                            ...row,
                            status: checkpoint.completed === cohort.history ? "ok" : "running",
                            completed: checkpoint.completed,
                            resumed:
                              checkpoint.completed < cohort.history ||
                              checkpoint.completed > previous.completed,
                            ...(batch ? { startedMs: batch.startedMs } : {}),
                            ...(batch && checkpoint.completed > previous.completed
                              ? {
                                  driverMs: checkpoint.driverMs,
                                  batches: [
                                    ...row.batches,
                                    {
                                      _tag: "Batch",
                                      result: batch,
                                      driverMs: checkpoint.driverMs,
                                      colo: null,
                                    },
                                  ],
                                }
                              : {}),
                          },
                    ),
                  }));
                }

                const profileAfter = options.profileAfter;

                const capture =
                  profileAfter === undefined
                    ? undefined
                    : yield* prepareProfile({
                        worker: workerName(cohort),
                        endpoint: yield* url("/profile-target", cohort),
                        token: deploy.token,
                        query: { ...cohort, expectedBuild },
                        output,
                        sourceMap: join(output, label, cohort.target, "target.mjs.map"),
                      });

                const reached = yield* Deferred.make<void>();
                let profileStarted = false;

                const profiling =
                  capture && profileAfter !== undefined
                    ? yield* Deferred.await(reached).pipe(
                        Effect.flatMap(() =>
                          Effect.forEach(
                            options.profiles,
                            (type) =>
                              capture(type, 10_000).pipe(
                                Effect.matchCauseEffect({
                                  onFailure: (cause) =>
                                    update((value) => ({
                                      ...value,
                                      failures: [
                                        ...value.failures,
                                        `${cohort.target}/${cohort.history} history ${type} profile: ${message(cause)}`,
                                      ],
                                    })),
                                  onSuccess: (profile) =>
                                    update((value) => ({
                                      ...value,
                                      profiles: [
                                        ...value.profiles,
                                        {
                                          ...profile,
                                          afterHistoryTurns: profileAfter,
                                          completedAtEnd:
                                            value.histories.find(
                                              (row) => objectKey(row) === objectKey(cohort),
                                            )?.completed ?? 0,
                                        },
                                      ],
                                    })),
                                }),
                              ),
                            { concurrency: 2, discard: true },
                          ),
                        ),
                        Effect.forkScoped,
                      )
                    : undefined;

                yield* Effect.gen(function* () {
                  let progress = result.histories.find(
                    (row) => objectKey(row) === objectKey(cohort),
                  );

                  while (progress && progress.completed < cohort.history) {
                    const from = progress.completed;

                    yield* requestEvents(
                      deploy.driver + "/build-history",
                      deploy.token,
                      BuildEvent,
                      {
                        query: { ...cohort, expectedBuild },
                        targetUrl: yield* endpointFor(cohort),
                        from,
                        ...(progress.startedMs === undefined
                          ? {}
                          : { startedMs: progress.startedMs }),
                      },
                    ).pipe(
                      Stream.runForEach((event) =>
                        Effect.gen(function* () {
                          if (event._tag === "Failed") {
                            yield* update((value) => ({
                              ...value,
                              histories: value.histories.map((row) =>
                                objectKey(row) === objectKey(cohort)
                                  ? {
                                      ...row,
                                      driverMs: event.driverMs,
                                    }
                                  : row,
                              ),
                            }));

                            return yield* BenchError.make({ message: event.error });
                          }
                          const batch = event.result;

                          yield* recordUsage(`${cohort.target}/${cohort.object}`, batch);

                          if (
                            batch.from !== from ||
                            batch.to !== Math.min(from + 50, cohort.history) ||
                            batch.identity.build !== expectedBuild ||
                            batch.fingerprint !== seedReferences.get(cohort.history)?.get(batch.to)
                          )
                            return yield* BenchError.make({
                              message: `History fingerprint or build mismatch at turn ${batch.to}.`,
                            });
                          yield* update((value) => ({
                            ...value,
                            histories: value.histories.map((row) =>
                              objectKey(row) === objectKey(cohort)
                                ? {
                                    ...row,
                                    status: batch.to === cohort.history ? "ok" : "running",
                                    completed: batch.to,
                                    startedMs: batch.startedMs,
                                    driverMs: event.driverMs,
                                    batches: [...row.batches, event],
                                  }
                                : row,
                            ),
                          }));
                          yield* Console.error(
                            `${cohort.target}/${cohort.history}/${cohort.object.split("-").at(-1)} built ${batch.to}/${cohort.history}`,
                          );
                          if (profiling && batch.to === options.profileAfter) {
                            profileStarted = true;
                            yield* Deferred.succeed(reached, undefined);
                          }
                        }),
                      ),
                      // A graceful interrupt waits only for this batch and its saved acknowledgement.
                      Effect.uninterruptible,
                    );
                    progress = result.histories.find((row) => objectKey(row) === objectKey(cohort));
                    if (!progress || progress.completed <= from)
                      return yield* BenchError.make({
                        message: "History batch ended without a verified acknowledgement.",
                      });
                    yield* Effect.yieldNow;
                  }
                }).pipe(
                  Effect.onExit(() =>
                    profiling && profileStarted ? Fiber.join(profiling) : Effect.void,
                  ),
                );
                if (
                  !result.histories.some(
                    (row) => objectKey(row) === objectKey(cohort) && row.status === "ok",
                  )
                )
                  return yield* BenchError.make({
                    message: "History build ended before its final batch.",
                  });
                // Release the seeded runtime while the remaining Objects are built.
                yield* resetForBuild(cohort);

                return;
              }

              const fixture = fixtures.find(
                (item) => item.target === cohort.target && item.history === cohort.history,
              );

              if (!fixture) return yield* BenchError.make({ message: "Missing fixture." });

              const restoreStarted = yield* Clock.currentTimeMillis;

              let restoreAttempt = 0;
              let restoreRequests = 0;

              yield* Effect.gen(function* () {
                const clear = clearBeforeRestore || restoreAttempt > 0;

                restoreAttempt++;
                if (clear) {
                  yield* resetForBuild(cohort);
                  restoreRequests++;

                  const cleared = yield* request(
                    yield* url("/restore/clear", cohort),
                    deploy.token,
                    RestoreAck,
                    {},
                  );

                  yield* recordUsage(`${cohort.target}/${cohort.object}`, cleared.value);
                  // A new incarnation separates unmetered deleteAll from import accounting.
                  if (options.writes) yield* resetForBuild(cohort);
                }

                restoreRequests++;

                const start = yield* request(
                  yield* url("/restore/start", cohort),
                  deploy.token,
                  RestoreAck,
                  fixture,
                );

                yield* recordUsage(`${cohort.target}/${cohort.object}`, start.value);

                if (
                  fixture.actor &&
                  !equalCounts(
                    start.value.directoryTables ?? {},
                    Object.fromEntries(
                      fixture.actor.tables.map((table) => [table.name, table.rows.length]),
                    ),
                  )
                )
                  return yield* BenchError.make({
                    message: "Restored Actor table counts differ from source",
                  });
                for (const path of chunks.get(`${cohort.target}-${cohort.history}`) ?? []) {
                  restoreRequests++;

                  const restored = yield* request(
                    yield* url("/restore/rows", cohort),
                    deploy.token,
                    RestoreAck,
                    yield* read(path, RestoreChunk),
                  );

                  yield* recordUsage(`${cohort.target}/${cohort.object}`, restored.value);
                }

                restoreRequests++;

                const seeded = (yield* request(
                  yield* url("/restore/finalize", cohort),
                  deploy.token,
                  ImportResult,
                  {},
                  "10 minutes",
                )).value;

                yield* recordUsage(`${cohort.target}/${cohort.object}`, seeded);

                if (seeded.identity.build !== expectedBuild)
                  return yield* BenchError.make({
                    message: "Seeded Object reported the wrong build.",
                  });
                if (
                  seeded.fingerprint !== fixture.fingerprint ||
                  !equalCounts(seeded.tables, fixture.tables)
                )
                  return yield* BenchError.make({
                    message:
                      "Deployed seed fingerprint or complete table counts differ from the local fixture.",
                  });
              }).pipe(retryRequest);

              const restoreMs = (yield* Clock.currentTimeMillis) - restoreStarted;

              yield* update((value) => ({
                ...value,
                restores: [
                  ...(value.restores ?? []),
                  {
                    object: cohort.object,
                    target: cohort.target,
                    history: cohort.history,
                    epoch,
                    build: label,
                    requests: restoreRequests,
                    attempts: restoreAttempt,
                    startedMs: restoreStarted,
                    endedMs: restoreStarted + restoreMs,
                    ms: restoreMs,
                  },
                ],
              }));
            }).pipe(
              Effect.scoped,
              (batch) => (options.profileAfter === undefined ? batch : withProfileBatch(batch)),
              Effect.catchCause((cause) =>
                Effect.gen(function* () {
                  if (Cause.hasInterrupts(cause)) return yield* Effect.failCause(cause);
                  failedObjects.add(objectKey(cohort));
                  yield* update((value) => ({
                    ...value,
                    histories: value.histories.map((row) =>
                      objectKey(row) === objectKey(cohort)
                        ? {
                            ...row,
                            status: "failed",
                            error: message(cause),
                          }
                        : row,
                    ),
                    failures: [
                      ...value.failures,
                      `${cohort.target}/${cohort.history}/${cohort.ttftMs} seed: ${message(cause)}`,
                    ],
                  }));
                  if (!options.buildHistory) return yield* Effect.failCause(cause);
                }),
              ),
            ),
          { concurrency: options.buildConcurrency, discard: true },
        );
      }

      yield* phase("measurement");
      if (restorePass) {
        yield* phase("deploy");
        restoring = false;
        endpoints = yield* deployPass();
        // Allow the measurement build to propagate before preparation readiness/reset.
        if (options.isolateCold || snapshotProfile) yield* Effect.sleep(COLD_PROPAGATION_DELAY);
        yield* phase("measurement");
      }
      const isolateOwners = new Map<string, Set<string>>();

      for (const row of result.histories)
        for (const batch of row.batches) {
          const owners = isolateOwners.get(batch.result.identity.isolate) ?? new Set<string>();

          owners.add(objectKey(row));
          isolateOwners.set(batch.result.identity.isolate, owners);
        }

      const shared = new Set(
        [...isolateOwners.values()]
          .filter((owners) => owners.size > 1)
          .flatMap((owners) => [...owners]),
      );

      if (shared.size) {
        for (const key of shared) failedObjects.add(key);
        yield* update((value) => ({
          ...value,
          histories: value.histories.map((row) =>
            shared.has(objectKey(row))
              ? {
                  ...row,
                  status: "failed",
                  error: "Worker isolate was shared with another build cohort; timing rejected.",
                }
              : row,
          ),
          failures: [
            ...value.failures,
            ...[...shared].map((key) => `${key}: shared build isolate; timing rejected.`),
          ],
        }));
      }

      yield* Console.error(
        snapshotProfile
          ? "Preparing snapshot CPU diagnostic; only the selected candidate Object's first cold turn will run."
          : `Measuring ${counterbalanced ? "counterbalanced AB/BA" : defaultLabel}, pass ${epoch + 1}/${epochs.length}: ${cohorts.length} Objects, concurrency ${options.concurrency}…`,
      );

      const measurements = yield* Effect.forEach(
        cohorts.filter(
          (cohort) =>
            !failedObjects.has(objectKey(cohort)) && !measuredObjects.has(objectKey(cohort)),
        ),
        Effect.fnUntraced(function* (cohort) {
          const resumeAt = replayBefore.get(objectKey(cohort)) ?? 0;
          let currentSample: Sample | undefined;

          const recover = Effect.fnUntraced(function* (
            error: IncarnationLost | TransientObjectTransport,
          ) {
            const sample = currentSample;
            const key = `${objectKey(cohort)}/${epoch}`;

            // One complete recovery per Object/pass is funded by the write reservation.
            // Never retry against advanced storage or repeatedly replay growing prefixes.
            if (
              !recoveryAvailable ||
              !sample ||
              sample.state === "recovery" ||
              retriedPasses.has(key)
            )
              return yield* BenchError.make({
                message: error.message + "; automatic sample recovery unavailable or exhausted.",
              });
            retriedPasses.add(key);
            retryFrom.set(objectKey(cohort), sample.repeat);
            const reason = error._tag === "IncarnationLost" ? "incarnation-lost" : error.reason;
            const replacementSample = sampleName(epoch, sample.repeat, attempt + 1);

            yield* update((value) => ({
              ...value,
              samples: value.samples.map((row) =>
                objectKey(row) === objectKey(cohort) && row.sample === sample.sample
                  ? {
                      ...row,
                      status: "invalidated",
                      error: error.message,
                      ...(error._tag === "IncarnationLost"
                        ? { invalidatedIdentity: error.identity }
                        : {}),
                    }
                  : row,
              ),
              recoveries: [
                ...(value.recoveries ?? []),
                {
                  target: cohort.target,
                  object: cohort.object,
                  epoch,
                  repeat: sample.repeat,
                  sample: sample.sample,
                  replacementSample,
                  build: sample.build,
                  reason,
                  error: error.message,
                },
              ],
            }));
            yield* Console.error(
              `${cohort.target}/${cohort.history}/${cohort.ttftMs} ${sample.sample}: ${reason}; restoring and rebuilding ${sample.repeat} acknowledged turns before one replacement.`,
            );
          });

          const { label } = yield* assignment(cohort);
          let reset: typeof ColdResult.Type | undefined;
          let incarnation = "";
          let directoryIncarnation: string | undefined;

          const firstAfterReset = (index: number) =>
            index === 0 ||
            (!options.buildHistory && cohort.target !== "effect-ai-floor" && coldPosition(index));

          const prepare = Effect.fnUntraced(function* <E = never, R = never>(
            index: number,
            beforeReset?: Effect.Effect<void, E, R>,
          ) {
            if (firstAfterReset(index)) reset = yield* resetForBuild(cohort, beforeReset);
          });

          const measure = (index: number, wave: number, peers: readonly string[]) =>
            Effect.gen(function* () {
              const expectedBuild = yield* expectedFor(cohort);

              if (reset === undefined)
                return yield* BenchError.make({
                  message: "Measurement must follow acknowledged readiness/reset.",
                });

              const acknowledgedReset = reset;

              const query = {
                ...cohort,
                sample: sampleName(epoch, index, attempt),
                expectedBuild,
              };

              const state =
                !options.buildHistory && index === 0
                  ? "preparation"
                  : cohort.target === "effect-ai-floor" && coldPosition(index)
                    ? "warmup"
                    : firstAfterReset(index)
                      ? "cold"
                      : "warm";

              const driverRequestId = `${query.target}/${query.object}/${epoch}/${query.sample}`;

              const sample: Sample = {
                ...query,
                driverRequestId,
                controllerQueuedMs: yield* Clock.currentTimeMillis,
                wave,
                peers,
                build: label,
                ...(counterbalanced
                  ? {
                      order:
                        firstLabels.get(objectKey(cohort)) === "baseline"
                          ? ("AB" as const)
                          : ("BA" as const),
                    }
                  : {}),
                epoch,
                repeat: index,
                state: index < resumeAt ? "recovery" : state,
                status: "running",
                ...(firstAfterReset(index) ? { coldBoundary: acknowledgedReset } : {}),
              };

              currentSample = sample;

              const matches = (row: Sample) =>
                row.target === query.target &&
                row.object === query.object &&
                row.sample === query.sample;

              yield* update((value) => ({ ...value, samples: [...value.samples, sample] }));
              yield* Effect.gen(function* () {
                const before = yield* Clock.currentTimeMillis;

                const expectedDriverBuild = result.infrastructureBuild;

                if (expectedDriverBuild === undefined)
                  return yield* BenchError.make({ message: "Driver build was not recorded." });

                const received = yield* measureRequest(deploy.driver + "/measure", deploy.token, {
                  requestId: driverRequestId,
                  query,
                  targetUrl: yield* endpointFor(cohort),
                  observeText: !options.buildHistory,
                  expectedDriverBuild,
                  cold: options.buildHistory && index === 0,
                  isolateCold: options.isolateCold === true && state === "cold",
                  quiescent:
                    snapshotProfile || state === "preparation" || index === turnsPerEpoch - 1,
                  resetBeforeMetrics:
                    options.verifyIncarnationRecovery === true &&
                    attempt === 0 &&
                    epoch === 0 &&
                    index === 2 &&
                    cohort === poolCohorts.find((item) => item.target === cohort.target),
                });

                const after = yield* Clock.currentTimeMillis;

                const recovery = received.recovery;

                if (recovery !== undefined)
                  yield* update((value) => ({
                    ...value,
                    polledRecoveries: [
                      ...(value.polledRecoveries ?? []),
                      {
                        ...recovery,
                        target: query.target,
                        object: query.object,
                        history: query.history,
                        ttftMs: query.ttftMs,
                        build: label,
                        epoch,
                        sample: query.sample,
                        outcome: received.outcome.body.ok ? "completed" : "failed",
                      },
                    ],
                  }));

                const measured = {
                  value: yield* measureResult(received.outcome, query.sample),
                  colo: received.colo,
                };

                const firstText = measured.value.firstText;

                const expectedText = [
                  responseText(`done after ${MEASURED_TOOLS} lookups`, query.textStreaming),
                  ...(query.textStreaming && measured.value.firstTextSource !== "settlementRecord"
                    ? [PREAMBLE]
                    : []),
                ];

                if (
                  !options.buildHistory &&
                  query.target !== "tardie" &&
                  (measured.value.firstTextMs === null ||
                    measured.value.firstTextMs < 0 ||
                    !firstText?.trim() ||
                    !expectedText.some((text) => text.startsWith(firstText)))
                )
                  return yield* BenchError.make({
                    message:
                      "First visible text did not match the submitted turn's assistant reply.",
                  });

                const metrics = measured.value.metrics;

                yield* recordUsage(`${cohort.target}/${cohort.object}`, metrics);

                if (!metrics.ok) {
                  if (
                    metrics.reason === "incarnation-lost" &&
                    metrics.identity.build === expectedBuild &&
                    metrics.identity.firstEntry &&
                    metrics.requestedSample === query.sample
                  )
                    return yield* IncarnationLost.make({
                      message: `Metrics lost with Object incarnation: ${JSON.stringify(metrics)}`,
                      identity: metrics.identity,
                    });

                  return yield* BenchError.make({
                    message: `Metrics mismatch: ${JSON.stringify(metrics)}`,
                  });
                }

                const priorEpoch = isolateEpochs.get(metrics.identity.isolate);

                if (priorEpoch !== undefined && priorEpoch !== epoch)
                  return yield* BenchError.make({
                    message: "Worker isolate survived a revision pass; timing rejected.",
                  });
                isolateEpochs.set(metrics.identity.isolate, epoch);

                const expected = references.get(query.history)?.get(query.sample);

                if (
                  [...(isolateOwners.get(metrics.identity.isolate) ?? [])].some(
                    (owner) => owner !== objectKey(cohort),
                  )
                )
                  return yield* BenchError.make({
                    message: "Measured isolate also hosted another build cohort; timing rejected.",
                  });

                isolateOwners.set(metrics.identity.isolate, new Set([objectKey(cohort)]));

                if (
                  metrics.query.expectedBuild !== expectedBuild ||
                  metrics.identity.build !== expectedBuild ||
                  (metrics.directory !== undefined && metrics.directory.build !== expectedBuild) ||
                  metrics.calls.some(
                    (call) =>
                      call.receipt?.expectedBuild !== expectedBuild ||
                      call.receipt.objectBuild !== expectedBuild,
                  )
                )
                  return yield* BenchError.make({
                    message:
                      "Object or provider receipt build mismatch; sample is invalid and input will not be replayed.",
                  });
                if (!firstAfterReset(index) && incarnation !== metrics.identity.incarnation)
                  return yield* IncarnationLost.make({
                    message: "Warm Object incarnation changed; the sample prefix must be restored.",
                    identity: metrics.identity,
                  });
                if (
                  !expected ||
                  metrics.calls.length !== 9 ||
                  metrics.calls.some(
                    (call, i) =>
                      call.status !== 200 ||
                      call.error !== undefined ||
                      call.endMs === undefined ||
                      call.fingerprint !== expected[i] ||
                      call.receipt?.fingerprint !== expected[i],
                  )
                )
                  return yield* BenchError.make({
                    message:
                      "Model-visible transcript or provider completion differed from the reference.",
                  });

                const coldVerified =
                  firstAfterReset(index) &&
                  metrics.identity.incarnation !== acknowledgedReset.before.incarnation &&
                  metrics.identity.firstEntry &&
                  metrics.identity.priorAlarms === 0 &&
                  (!options.isolateCold ||
                    state !== "cold" ||
                    (metrics.identity.isolate !== acknowledgedReset.before.isolate &&
                      metrics.identity.incarnation ===
                        measured.value.coldObjectIdentity?.incarnation)) &&
                  (!(cohort.target === "yielded" || cohort.target === "pi") ||
                    (acknowledgedReset.maintenanceAfter?.logical === null &&
                      acknowledgedReset.maintenanceAfter.native === null)) &&
                  (!options.buildHistory ||
                    metrics.identity.incarnation === measured.value.primed?.incarnation);

                const residentVerified =
                  !firstAfterReset(index) && incarnation === metrics.identity.incarnation;

                const actorVerified =
                  cohort.target !== "tardie" ||
                  (firstAfterReset(index)
                    ? metrics.directory?.incarnation !==
                        acknowledgedReset.directoryBefore?.incarnation &&
                      metrics.directory?.firstEntry === true &&
                      metrics.directory.priorAlarms === 0
                    : metrics.directory?.incarnation === directoryIncarnation);

                if (!(firstAfterReset(index) ? coldVerified : residentVerified) || !actorVerified)
                  return yield* BenchError.make({
                    message:
                      "Object residency changed; sample is not valid for its cold/warm label.",
                  });
                incarnation = metrics.identity.incarnation;
                directoryIncarnation = metrics.directory?.incarnation;

                if (snapshotProfile && index === 1) {
                  const turnCompletedMs = yield* Clock.currentTimeMillis;

                  yield* updateDiagnostic({
                    after: metrics.identity,
                    turnCompletedMs,
                    coverage:
                      result.diagnostic?.before?.isolate === metrics.identity.isolate &&
                      result.diagnostic?.before?.incarnation ===
                        acknowledgedReset.before.incarnation
                        ? "same-isolate-unverified"
                        : "cold-work-not-captured",
                  });
                }

                const gaps = metrics.calls
                  .slice(1)
                  .map((call, i) => call.startMs - (metrics.calls[i]?.endMs ?? NaN));

                const lastEnd = metrics.calls.at(-1)?.endMs;
                const objectToFirstModelMs = metrics.calls[0]?.sinceEntryMs;

                yield* update((value) => ({
                  ...value,
                  samples: value.samples.map((row) =>
                    matches(row)
                      ? {
                          ...sample,
                          status: "ok",
                          ...(measured.value.workerStart === undefined
                            ? {}
                            : {
                                workerStart: measured.value.workerStart,
                                coldObjectIdentity: measured.value.coldObjectIdentity,
                                workerStartMs: measured.value.workerStartMs,
                                coldStartToFirstTextMs: measured.value.coldStartToFirstTextMs,
                                coldStartToCompletionMs: measured.value.coldStartToCompletionMs,
                              }),
                          driverMs: measured.value.driverMs,
                          firstTextMs: measured.value.firstTextMs,
                          ...(measured.value.firstText === undefined
                            ? {}
                            : { firstText: measured.value.firstText }),
                          ...(measured.value.firstTextSource === undefined
                            ? {}
                            : { firstTextSource: measured.value.firstTextSource }),
                          observationMs: measured.value.observationMs,
                          settlementRecordRead: measured.value.settlementRecordRead,
                          ...(measured.value.observerToCompletionMs === undefined
                            ? {}
                            : { observerToCompletionMs: measured.value.observerToCompletionMs }),
                          ...(measured.value.observerToFirstTextMs === undefined
                            ? {}
                            : { observerToFirstTextMs: measured.value.observerToFirstTextMs }),
                          constructorAndProbeMs: measured.value.constructorAndProbeMs,
                          primeMs: measured.value.primeMs,
                          bytes: metrics.bytes,
                          ...(state === "preparation" ? { preparationTables: metrics.tables } : {}),
                          providerColos: [
                            ...new Set(
                              metrics.calls.flatMap((call) =>
                                call.receipt?.colo ? [call.receipt.colo] : [],
                              ),
                            ),
                          ],
                          ...(objectToFirstModelMs === undefined ? {} : { objectToFirstModelMs }),
                          ...(measured.value.admissionMs === undefined
                            ? {}
                            : { admissionMs: measured.value.admissionMs }),
                          gapMs: median(gaps),
                          ...(lastEnd === undefined
                            ? {}
                            : { lastResponseToClientMs: measured.value.observedMs - lastEnd }),
                          controllerStartedMs: before,
                          controllerEndedMs: after,
                          ...(measured.value.driverIdentity
                            ? { driverIdentity: measured.value.driverIdentity }
                            : {}),
                          controllerMs: after - before,
                          controllerColo: measured.colo,
                          colo: measured.value.colo,
                          fingerprintVerified: true,
                          coldVerified,
                          residentVerified,
                          buildVerified: true,
                          objectBuild: metrics.identity.build,
                          isolate: metrics.identity.isolate,
                          incarnation: metrics.identity.incarnation,
                          ...(metrics.directoryUsed === undefined
                            ? {}
                            : { directoryUsed: metrics.directoryUsed }),
                          providerCalls: metrics.calls,
                          ...(metrics.cpuInvocationIds === undefined
                            ? {}
                            : { cpuInvocationIds: metrics.cpuInvocationIds }),
                          ...(metrics.phases === undefined ? {} : { objectPhases: metrics.phases }),
                          ...(metrics.calls[0]?.firstTokenMs === undefined
                            ? {}
                            : {
                                providerFirstTokenMs:
                                  metrics.calls[0].sinceEntryMs +
                                  metrics.calls[0].firstTokenMs -
                                  metrics.calls[0].startMs,
                              }),
                          ...(metrics.phases?.firstTextForwardedMs === undefined
                            ? {}
                            : {
                                firstTextForwardedMs:
                                  metrics.phases.firstTextForwardedMs - metrics.phases.entryMs,
                              }),
                          ...(metrics.phases?.settlementMs === undefined
                            ? {}
                            : {
                                settlementMs: metrics.phases.settlementMs - metrics.phases.entryMs,
                              }),
                          fingerprints: metrics.calls.map((call) => call.fingerprint),
                        }
                      : row,
                  ),
                }));
              }).pipe(
                Effect.tapCause((cause) =>
                  update((value) => ({
                    ...value,
                    samples: value.samples.map((row) =>
                      matches(row) ? { ...row, status: "failed", error: message(cause) } : row,
                    ),
                  })),
                ),
              );
            }).pipe(
              Effect.catchTag("IncarnationLost", recover),
              Effect.catchTag("TransientObjectTransport", recover),
              Effect.catchTag("UnknownOutcome", (error) =>
                Effect.gen(function* () {
                  const sample = currentSample;

                  if (!sample || options.buildHistory || snapshotProfile) return yield* error;
                  failedObjects.add(objectKey(cohort));
                  yield* update((value) => ({
                    ...value,
                    retirements: [
                      ...(value.retirements ?? []),
                      {
                        target: cohort.target,
                        object: cohort.object,
                        history: cohort.history,
                        ttftMs: cohort.ttftMs,
                        build: sample.build,
                        epoch,
                        sample: sample.sample,
                        error: message(Cause.fail(error)),
                      },
                    ],
                  }));
                  yield* Console.error(
                    `${cohort.target}/${cohort.history}/${cohort.ttftMs}/${sample.build}/epoch${epoch}: retiring the complete Object pair after an unknown input outcome; no resubmission.`,
                  );
                }),
              ),
              Effect.catchCause((cause) =>
                Effect.gen(function* () {
                  if (Cause.hasInterrupts(cause)) return yield* Effect.failCause(cause);
                  yield* update((value) => ({
                    ...value,
                    failures: [
                      ...value.failures,
                      `${cohort.target}/${cohort.history}/${cohort.ttftMs}: ${message(cause)}`,
                    ],
                  }));
                  failedObjects.add(objectKey(cohort));
                  if (!options.buildHistory) return yield* Effect.failCause(cause);
                }),
              ),
            );

          return { cohort, label, prepare, measure };
        }),
      );

      // Each wave contains paired arms of the same target/cell where both survive.
      // Readiness completes for the whole wave before any input; all turns finish
      // before the next wave, so completion speed cannot choose the next peer mix.
      for (let index = 0; index < turnsPerEpoch; index++) {
        if (snapshotProfile && index === 1) {
          const selected = measurements.find((item) => item.label === "candidate");

          if (!selected)
            return yield* BenchError.make({
              message: "Snapshot profiling requires a prepared candidate Object.",
            });
          const { cohort, label } = selected;

          const query = {
            ...cohort,
            sample: sampleName(epoch, index),
            expectedBuild: yield* expectedFor(cohort),
          };

          yield* update((value) => ({
            ...value,
            ...(value.diagnostic ? { diagnostic: { ...value.diagnostic, query } } : {}),
            orders: [
              ...(value.orders ?? []),
              { phase: `measure/${epoch}/${attempt}/${index}`, items: [objectKey(cohort)] },
            ],
          }));

          yield* Effect.gen(function* () {
            const issued = yield* Deferred.make<
              { versionId: string; requestedMs: number },
              BenchError
            >();

            let profiling: Fiber.Fiber<Profile, BenchError> | undefined;

            // POST to these read-only routes disables request()'s automatic GET retries.
            const readIdentity = request(
              yield* url("/identity", cohort),
              deploy.token,
              ObjectIdentity,
              {},
              "5 seconds",
            ).pipe(Effect.map(({ value }) => value.identity));

            const quiescent = Effect.gen(function* () {
              for (;;) {
                const status = (yield* request(
                  yield* url("/maintenance-status", cohort),
                  deploy.token,
                  MaintenanceStatus,
                  {},
                  "5 seconds",
                )).value;

                if (status.logical === null && status.native === null) break;
                yield* Effect.sleep("100 millis");
              }
            });

            // Discovery traffic precedes the reset deadline and the capture itself.
            const discovery = yield* Effect.gen(function* () {
              yield* quiescent;
              const startedMs = yield* Clock.currentTimeMillis;
              const started = yield* Clock.monotonicTimeNanos;
              const first = yield* readIdentity;

              if (first.build !== query.expectedBuild)
                return yield* BenchError.make({ message: "Profiling discovery build mismatch." });
              let last = first;
              let requests = 1;

              for (;;) {
                const remaining =
                  60_000 - Number((yield* Clock.monotonicTimeNanos) - started) / 1_000_000;

                if (remaining <= 0) break;
                yield* Effect.sleep(Math.min(1000, remaining));
                if ((yield* Clock.monotonicTimeNanos) - started >= 60_000_000_000n) break;
                last = yield* readIdentity;
                requests++;
                if (
                  last.build !== first.build ||
                  last.isolate !== first.isolate ||
                  last.incarnation !== first.incarnation
                )
                  return yield* BenchError.make({
                    message: "Profiling Object changed during discovery traffic.",
                  });
              }

              return {
                durationMs: 60_000,
                startedMs,
                completedMs: yield* Clock.currentTimeMillis,
                requests,
                first,
                last,
              };
            }).pipe(
              Effect.timeoutOrElse({
                duration: "75 seconds",
                orElse: () => BenchError.make({ message: "Profiling discovery timed out." }),
              }),
            );

            yield* updateDiagnostic({ discovery });

            const beforeReset = Effect.gen(function* () {
              yield* quiescent;
              const before = yield* readIdentity;

              if (
                before.build !== query.expectedBuild ||
                before.isolate !== discovery.last.isolate ||
                before.incarnation !== discovery.last.incarnation
              )
                return yield* BenchError.make({
                  message: "Profiling Object changed identity before capture.",
                });
              yield* updateDiagnostic({ before });

              const capture = yield* prepareProfile({
                worker: workerName(cohort),
                endpoint: yield* url("/profile-target", query),
                token: deploy.token,
                query,
                output,
                sourceMap: join(output, label, cohort.target, "target.mjs.map"),
                onIssued: (value) => {
                  Deferred.doneUnsafe(issued, Effect.succeed(value));
                },
              });

              profiling = yield* capture("cpu", 50_000).pipe(
                Effect.timeout("90 seconds"),
                Effect.tap((profile) =>
                  update((value) => ({ ...value, profiles: [...value.profiles, profile] })),
                ),
                Effect.mapError((error) =>
                  BenchError.make({
                    message: `Snapshot CPU capture failed: ${message(Cause.fail(error))}`,
                    cause: error,
                  }),
                ),
                Effect.tapCause((cause) =>
                  Deferred.fail(issued, BenchError.make({ message: message(cause) })),
                ),
                Effect.forkScoped,
              );
              const receipt = yield* Deferred.await(issued);

              yield* updateDiagnostic(receipt);
              // The API has no attachment acknowledgement; allow transit, then
              // require reconstruction frames before claiming capture coverage.
              yield* Effect.sleep("1 second");
            });

            yield* Effect.gen(function* () {
              yield* selected.prepare(index, beforeReset);
              const resetCompletedMs = yield* Clock.currentTimeMillis;

              yield* updateDiagnostic({ resetCompletedMs });
              yield* selected.measure(index, 0, [objectKey(cohort)]);
            }).pipe(Effect.onExit(() => (profiling ? Fiber.join(profiling) : Effect.void)));
          }).pipe(Effect.scoped, withProfileBatch);
          break;
        }

        const activeMeasurements = measurements.filter(
          ({ cohort }) =>
            !failedObjects.has(objectKey(cohort)) && !retryFrom.has(objectKey(cohort)),
        );

        const freshVersion = options.isolateCold === true && coldPosition(index);

        if (freshVersion) {
          yield* Effect.forEach(activeMeasurements, (item) => item.prepare(index), {
            concurrency: options.concurrency,
            discard: true,
          });
          coldGeneration++;
          endpoints = yield* deployPass(false);
          // Fixed propagation allowance; any stale first response fails without input retry.
          yield* Effect.sleep(COLD_PROPAGATION_DELAY);
        }

        const groups = new Map<string, typeof activeMeasurements>();

        for (const item of activeMeasurements) {
          const key = options.isolateCold
            ? `${item.cohort.history}/${item.cohort.ttftMs}/${item.cohort.object}`
            : `${item.cohort.target}/${item.cohort.history}/${item.cohort.ttftMs}`;

          const group = groups.get(key) ?? [];

          group.push(item);
          groups.set(key, group);
        }

        const cells = shuffle([...groups], `wave-cells/${epoch}/${attempt}/${index}`).map(
          ([cell, items]) => {
            const arms = options.isolateCold
              ? options.targets.map((target) =>
                  items.filter((item) => item.cohort.target === target),
                )
              : labels.map((label) =>
                  shuffle(
                    items.filter((item) => item.label === label),
                    `wave-arm/${epoch}/${attempt}/${index}/${cell}/${label}`,
                  ),
                );

            const paired: (typeof items)[] = [];

            for (let offset = 0; arms.some((arm) => offset < arm.length); offset++)
              paired.push(
                shuffle(
                  arms.flatMap((arm) => {
                    const item = arm[offset];

                    return item ? [item] : [];
                  }),
                  `wave-pair/${epoch}/${attempt}/${index}/${cell}/${offset}`,
                ),
              );

            return paired;
          },
        );

        const waves: (typeof activeMeasurements)[] = [];

        for (let offset = 0; cells.some((cell) => offset < cell.length); offset++)
          for (const cell of cells) {
            const pair = cell[offset];

            if (!pair) continue;
            const wave = waves.at(-1);

            if (wave && wave.length + pair.length <= options.concurrency) wave.push(...pair);
            else waves.push(pair);
          }
        const order = waves.flat();

        yield* update((value) => ({
          ...value,
          orders: [
            ...(value.orders ?? []),
            {
              phase: `measure/${epoch}/${attempt}/${index}`,
              items: order.map(({ cohort }) => objectKey(cohort)),
            },
          ],
        }));
        for (const [waveIndex, wave] of waves.entries()) {
          const peers = wave.map(({ cohort }) => objectKey(cohort));

          if (!freshVersion)
            yield* Effect.forEach(wave, (item) => item.prepare(index), {
              concurrency: options.concurrency,
              discard: true,
            });
          yield* Effect.forEach(wave, (item) => item.measure(index, waveIndex, peers), {
            concurrency: options.concurrency,
            discard: true,
          });
        }
      }
      // Only after the last wave: retire observers before collecting completed Tail events.
      if (options.cpu)
        yield* Effect.forEach(
          measurements.filter(
            ({ cohort }) =>
              !failedObjects.has(objectKey(cohort)) && !retryFrom.has(objectKey(cohort)),
          ),
          ({ cohort }) => resetForBuild(cohort),
          { concurrency: options.concurrency, discard: true },
        );

      return retryFrom;
    });

    for (const [epoch, defaultLabel] of epochs.entries()) {
      let pending = poolCohorts.filter((cohort) => !failedObjects.has(objectKey(cohort)));
      let from: ReadonlyMap<string, number> = new Map();

      for (let attempt = 0; pending.length > 0; attempt++) {
        from = yield* measureEpoch(epoch, defaultLabel, pending, attempt, from);
        pending = pending.filter((cohort) => from.has(objectKey(cohort)));
      }
      if (snapshotProfile) break;
    }
  }).pipe(Effect.onExit(finish));
}, Effect.scoped);

export const resume = Effect.fnUntraced(function* (name: string) {
  const cloud = yield* Cloudflare;

  if (!name.startsWith(cloud.prefix + "-") || !/^[a-z0-9-]+$/.test(name))
    return yield* BenchError.make({ message: "Resume requires a run name owned by this prefix." });
  const previous = yield* read(join(workspace, "results", name + ".json"), Result);
  const options = yield* Schema.decodeUnknownEffect(Options)(previous.options);

  if (
    previous.run !== name ||
    previous.complete ||
    previous.cleanup?.verified ||
    !previous.infrastructureBuild ||
    !options.buildHistory ||
    options.rigorous ||
    options.profiles.length > 0
  )
    return yield* BenchError.make({
      message: "Only a preserved, unprofiled history-build run can resume.",
    });

  return yield* run(options, previous);
});

export const teardown = Effect.gen(function* () {
  const cloud = yield* Cloudflare;

  const deploy = yield* deployments;
  const cleanup = yield* deploy.teardown;

  const result = {
    checkedAt: DateTime.formatIso(yield* DateTime.now),
    ...cleanup,
  };

  yield* save(join(workspace, "results", "cleanup.json"), result);
  yield* Console.log(
    `Cleanup verified: no ${cloud.prefix} Workers, Durable Object namespaces or CPU buckets remain.`,
  );
}).pipe(Effect.scoped);
