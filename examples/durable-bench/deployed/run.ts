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
import { build, ensureVendor } from "./build.ts";
import { Cloudflare, request, requestEvents } from "./cloudflare.ts";
import { cpu } from "./cpu.ts";
import { deployments } from "./deploy.ts";
import { prepareFixtures } from "./fixtures.ts";
import { BuildEvent, MeasureResponse, type Options, type Result, type Sample } from "./model.ts";
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
import { median, table } from "./report.ts";
import { PREAMBLE, responseText } from "./text.ts";
import { ColdResult, expectedSeed, ImportResult, Metrics, type Query } from "./worker/protocol.ts";

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

const shuffle = <A>(values: readonly A[]): A[] =>
  values
    .map((value) => ({ value, order: nonce() }))
    .sort((a, b) => a.order.localeCompare(b.order))
    .map(({ value }) => value);

export const run = Effect.fnUntraced(function* (options: Options) {
  const started = yield* Clock.currentTimeMillis;
  const cloud = yield* Cloudflare;
  const deploy = yield* deployments;
  const fs = yield* FileSystem.FileSystem;
  const privateDirectory = stateDirectory(cloud.prefix);
  const runName = `${cloud.prefix}-${started.toString(36)}-${nonce().slice(0, 8)}`;
  const output = join(workspace, "results", runName);
  const lock = yield* Semaphore.make(1);
  const withProfileBatch = yield* profileBatches(options.profiles.length);

  let result: Result = {
    version: 3,
    run: runName,
    revision: yield* git(["rev-parse", "HEAD"]),
    dirty: (yield* git(["status", "--porcelain"])).length > 0,
    startedAt: DateTime.formatIso(DateTime.makeUnsafe(started)),
    wallMs: 0,
    infrastructureReused: false,
    options: {
      ...options,
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
    complete: false,
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

  let targetStarted = false;
  let telemetryFrom = started;

  yield* Console.error(
    `Account: ${cloud.accountName}. Preparing ${options.buildHistory ? "deployed history builds" : "fixtures"}…`,
  );
  yield* update((value) => value);

  const finish = (exit: Exit.Exit<unknown, unknown>) =>
    Effect.gen(function* () {
      if (Exit.isFailure(exit)) {
        result = {
          ...result,
          failures: result.failures.length ? result.failures : [message(exit.cause)],
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
                  status: "failed",
                  error:
                    "Controller interrupted; history outcome unknown and will not be replayed.",
                }
              : row,
          ),
        };
      }
      if (options.cpu && targetStarted) {
        yield* Console.error("Collecting optional invocation CPU…");
        yield* Effect.sleep("15 seconds");
        const upper = yield* Clock.currentTimeMillis;

        yield* cpu(runName, telemetryFrom, upper).pipe(
          Effect.matchCauseEffect({
            onFailure: (cause) =>
              Effect.sync(() => {
                result = { ...result, failures: [...result.failures, message(cause)] };
              }),
            onSuccess: (data) =>
              Effect.sync(() => {
                result = {
                  ...result,
                  cpu: data.invocations,
                  unmatchedCpuMarkers: data.unmatchedMarkers,
                };
              }),
          }),
        );
      }
      if (!options.keep) {
        yield* Console.error("Destroying target stack and verifying cleanup…");
        yield* deploy.destroy(runName).pipe(
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
      result = {
        ...result,
        wallMs: (yield* Clock.currentTimeMillis) - started,
        complete:
          result.failures.length === 0 && (options.keep || result.cleanup?.verified === true),
      };
      yield* save(output + ".json", result);
      yield* fs.writeFileString(output + ".md", table(result));
      yield* Console.log(table(result));
      yield* Console.error(
        `Wall time ${(result.wallMs / 1000).toFixed(1)} s; infrastructure ${result.infrastructureReused ? "reused" : "deployed"}. Results: results/${runName}.json`,
      );
      for (const profile of result.profiles) yield* Console.error(`Profile: ${profile.file}`);
      if (!result.complete)
        return yield* new BenchError({
          message: "Run incomplete; failures and cleanup status are recorded in the result JSON.",
        });
    });

  yield* Effect.gen(function* () {
    yield* ensureVendor;
    const versions: Record<string, string> = {};

    for (const file of [
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

    const fixtures = options.buildHistory
      ? []
      : yield* prepareFixtures({ targets: options.targets, sizes: options.sizes });

    yield* update((value) => ({
      ...value,
      versions,
      fixtures: fixtures.map(({ target, history, fingerprint, tables, mode, fallbackReason }) => ({
        target,
        history,
        fingerprint,
        tables,
        mode,
        ...(fallbackReason === undefined ? {} : { fallbackReason }),
      })),
    }));
    const reused = yield* deploy.infrastructure;

    yield* update((value) => ({ ...value, infrastructureReused: reused }));
    type Label = Sample["build"];
    const labels: readonly Label[] = options.rigorous ? ["baseline", "candidate"] : ["working"];
    const builds = new Map<Label, Effect.Success<ReturnType<typeof build>>>();

    for (const label of labels) {
      const ref =
        label === "baseline"
          ? options.baseline
          : label === "candidate"
            ? options.candidate
            : undefined;

      const compiled = yield* build(join(privateDirectory, runName, label), "target", ref);

      if (options.profiles.length > 0) {
        const retained = join(output, label);

        yield* fs.makeDirectory(retained, { recursive: true, mode: 0o700 });
        yield* fs.copyFile(compiled.file, join(retained, "target.mjs"));
        yield* fs.copyFile(compiled.file + ".map", join(retained, "target.mjs.map"));
      }
      builds.set(label, compiled);
      yield* update((value) => ({
        ...value,
        builds: [...value.builds, { label, revision: compiled.revision, sha256: compiled.sha256 }],
      }));
    }

    const epochs: readonly Label[] = options.rigorous
      ? nonce().charAt(0) < "8"
        ? ["baseline", "candidate", "candidate", "baseline"]
        : ["candidate", "baseline", "baseline", "candidate"]
      : ["working"];

    const turnsPerEpoch = options.repeats + (options.buildHistory ? 1 : options.cold ? 2 : 1);

    const sampleName = (epoch: number, index: number) =>
      options.buildHistory ? `m${index}` : `e${epoch}-t${index}`;

    const references = new Map<number, Map<string, string[]>>();
    const seedReferences = new Map<number, Map<number, string>>();

    for (const size of options.sizes) {
      const messages: Message[] = [];

      const seeds = new Map<number, string>();

      for (const [index, input] of history(0, size).entries()) {
        const checkpoint = (index + 1) % 50 === 0 || index + 1 === size;
        const hashes = append(messages, input, false, checkpoint);
        const digest = hashes.at(-1);

        if (digest) seeds.set(index + 1, digest);
      }
      if (expectedSeed[size] !== undefined && seeds.get(size) !== expectedSeed[size])
        return yield* new BenchError({
          message: "Independent history plan no longer matches the existing seed fingerprint.",
        });
      seedReferences.set(size, seeds);
      const expected = new Map<string, string[]>();

      for (let epoch = 0; epoch < epochs.length; epoch++)
        for (let index = 0; index < turnsPerEpoch; index++) {
          const sample = sampleName(epoch, index);

          expected.set(
            sample,
            append(messages, turn(sample, MEASURED_TOOLS), options.textStreaming),
          );
        }
      references.set(size, expected);
    }

    const cohorts = options.sizes.flatMap((size) =>
      options.ttft.flatMap((ttftMs) =>
        Array.from({ length: options.objects }, (_, object) =>
          options.targets.map((target): Omit<Query, "expectedBuild"> => ({
            target,
            object: `${runName}-h${size}-d${ttftMs}-o${object}`,
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

    const objectKey = (cohort: Pick<Query, "target" | "object">) =>
      `${cohort.target}/${cohort.object}`;

    for (const [epoch, label] of epochs.entries()) {
      const compiled = builds.get(label);

      if (!compiled) return yield* new BenchError({ message: "Missing target build." });
      telemetryFrom = Math.min(telemetryFrom, yield* Clock.currentTimeMillis);
      targetStarted = true;
      const expectedBuild = compiled.sha256 + `-e${epoch}`;

      const endpoint = yield* deploy.target(runName, compiled.file, expectedBuild, options.cpu);

      const url = (path: string, query: Omit<Query, "expectedBuild">) => {
        const address = new URL(path, endpoint);

        for (const [key, value] of Object.entries(query))
          address.searchParams.set(key, String(value));
        address.searchParams.set("expectedBuild", expectedBuild);

        return address;
      };

      // Worker health does not establish the Object's build. Retry only acknowledged
      // setup resets while it propagates; imports and inputs always get one attempt.
      const resetForBuild = Effect.fnUntraced(function* (cohort: Omit<Query, "expectedBuild">) {
        const started = yield* Clock.currentTimeMillis;

        for (let attempt = 1; attempt <= 90; attempt++) {
          const remaining = 180_000 - ((yield* Clock.currentTimeMillis) - started);

          if (remaining <= 0) break;

          const reset = (yield* request(
            url("/cold", cohort),
            deploy.token,
            ColdResult,
            {},
            remaining,
          )).value;

          if (!reset.ok || !reset.threadAborted || reset.directoryAborted === false)
            return yield* new BenchError({ message: "Cold abort was not acknowledged." });
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
                ...(reset.directoryBefore === undefined
                  ? {}
                  : { directoryBuild: reset.directoryBefore.build }),
                attempt,
                waitMs,
              },
            ],
          }));
          if (
            reset.before.build === expectedBuild &&
            (reset.directoryBefore === undefined || reset.directoryBefore.build === expectedBuild)
          )
            // Observation attaches to the new incarnation before the submission timer starts.
            return reset;
          yield* Effect.sleep("2 seconds");
        }

        return yield* new BenchError({
          message: "Object build did not propagate before measurement; no input was sent.",
        });
      });

      // Seeding and its count scans must not contend with timed turns, including the first A/B pass.
      if (epoch === 0) {
        yield* Console.error(`Seeding and verifying ${cohorts.length} Objects…`);
        yield* Effect.forEach(
          shuffle(cohorts),
          (cohort) =>
            Effect.gen(function* () {
              if (options.buildHistory) {
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
                yield* requestEvents(deploy.driver + "/build-history", deploy.token, BuildEvent, {
                  query: { ...cohort, expectedBuild },
                  targetUrl: endpoint,
                }).pipe(
                  Stream.runForEach((event) =>
                    Effect.gen(function* () {
                      if (event._tag === "Failed") {
                        yield* update((value) => ({
                          ...value,
                          histories: value.histories.map((row) =>
                            objectKey(row) === objectKey(cohort)
                              ? {
                                  ...row,
                                  completed: event.completed,
                                  driverMs: event.driverMs,
                                }
                              : row,
                          ),
                        }));

                        return yield* new BenchError({ message: event.error });
                      }
                      const batch = event.result;

                      if (
                        batch.identity.build !== expectedBuild ||
                        batch.fingerprint !== seedReferences.get(cohort.history)?.get(batch.to)
                      )
                        return yield* new BenchError({
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
                                driverMs: event.driverMs,
                                batches: [...row.batches, event],
                              }
                            : row,
                        ),
                      }));
                      yield* Console.error(
                        `${cohort.target}/${cohort.history}/${cohort.object.split("-").at(-1)} built ${batch.to}/${cohort.history}`,
                      );
                    }),
                  ),
                  Effect.timeout("2 hours"),
                );
                if (
                  !result.histories.some(
                    (row) => objectKey(row) === objectKey(cohort) && row.status === "ok",
                  )
                )
                  return yield* new BenchError({
                    message: "History build ended before its final batch.",
                  });
                // Release the seeded runtime while the remaining Objects are built.
                yield* resetForBuild(cohort);

                return;
              }

              const fixture = fixtures.find(
                (item) => item.target === cohort.target && item.history === cohort.history,
              );

              if (!fixture) return yield* new BenchError({ message: "Missing fixture." });

              const seeded = (yield* request(
                url("/import", cohort),
                deploy.token,
                ImportResult,
                fixture,
                "10 minutes",
              )).value;

              if (seeded.identity.build !== expectedBuild)
                return yield* new BenchError({
                  message: "Seeded Object reported the wrong build.",
                });
              if (
                seeded.fingerprint !== fixture.fingerprint ||
                !equalCounts(seeded.tables, fixture.tables) ||
                (fixture.actor &&
                  !equalCounts(
                    seeded.directoryTables ?? {},
                    Object.fromEntries(fixture.actor.tables.map((t) => [t.name, t.rows.length])),
                  ))
              )
                return yield* new BenchError({
                  message:
                    "Deployed seed fingerprint or complete table counts differ from the local fixture.",
                });
            }).pipe(
              Effect.catchCause((cause) =>
                Effect.gen(function* () {
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
          { concurrency: options.concurrency, discard: true },
        );
      }

      yield* Console.error(
        `Measuring ${label}, pass ${epoch + 1}/${epochs.length}: ${cohorts.length} Objects, concurrency ${options.concurrency}…`,
      );
      yield* Effect.forEach(
        shuffle(cohorts.filter((cohort) => !failedObjects.has(objectKey(cohort)))),
        (cohort) => {
          const profiled =
            options.profiles.length > 0 &&
            cohort.object.endsWith("-o0") &&
            cohort.target !== "tardie";

          return Effect.gen(function* () {
            const capture = profiled
              ? yield* prepareProfile({
                  worker: runName,
                  endpoint: url("/profile-target", cohort),
                  token: deploy.token,
                  query: { ...cohort, expectedBuild },
                  output,
                  sourceMap: join(output, label, "target.mjs.map"),
                })
              : undefined;

            const warmed = yield* Deferred.make<number>();

            const profiling = capture
              ? yield* Deferred.await(warmed).pipe(
                  Effect.flatMap((warmupMs) =>
                    Effect.forEach(
                      options.profiles,
                      (type) =>
                        capture(
                          type,
                          Math.min(
                            50_000,
                            Math.max(5000, Math.ceil(warmupMs * options.repeats * 1.25)),
                          ),
                        ).pipe(
                          Effect.matchCauseEffect({
                            onFailure: (cause) =>
                              update((value) => ({
                                ...value,
                                failures: [
                                  ...value.failures,
                                  `${cohort.target}/${cohort.history}/${cohort.ttftMs} ${type} profile: ${message(cause)}`,
                                ],
                              })),
                            onSuccess: (profile) =>
                              update((value) => ({
                                ...value,
                                profiles: [...value.profiles, profile],
                              })),
                          }),
                        ),
                      { concurrency: 2, discard: true },
                    ),
                  ),
                  Effect.forkScoped,
                )
              : undefined;

            // Discard constructor caches after import, and restart the same stored Object after every build change.
            const reset = yield* resetForBuild(cohort);
            let incarnation = "";
            let directoryIncarnation: string | undefined;

            for (let index = 0; index < turnsPerEpoch; index++) {
              const query = { ...cohort, sample: sampleName(epoch, index), expectedBuild };

              const state =
                options.cold && index === 0
                  ? "cold"
                  : !options.buildHistory && index < (options.cold ? 2 : 1)
                    ? "warmup"
                    : "warm";

              const sample: Sample = {
                ...query,
                build: label,
                epoch,
                repeat: index,
                state,
                status: "running",
              };

              const matches = (row: Sample) =>
                row.target === query.target &&
                row.object === query.object &&
                row.sample === query.sample;

              yield* update((value) => ({ ...value, samples: [...value.samples, sample] }));
              yield* Effect.gen(function* () {
                const before = yield* Clock.currentTimeMillis;

                const measured = yield* request(
                  deploy.driver + "/measure",
                  deploy.token,
                  MeasureResponse,
                  {
                    query,
                    targetUrl: endpoint,
                    observeText: !options.buildHistory,
                    cold: options.buildHistory && index === 0,
                  },
                );

                const after = yield* Clock.currentTimeMillis;
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
                  return yield* new BenchError({
                    message:
                      "First visible text did not match the submitted turn's assistant reply.",
                  });

                const metrics = (yield* request(url("/metrics", query), deploy.token, Metrics))
                  .value;

                const expected = references.get(query.history)?.get(query.sample);

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
                  return yield* new BenchError({
                    message:
                      "Object or provider receipt build mismatch; sample is invalid and input will not be replayed.",
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
                  return yield* new BenchError({
                    message:
                      "Model-visible transcript or provider completion differed from the reference.",
                  });

                const coldVerified =
                  index === 0 &&
                  metrics.identity.incarnation !== reset.before.incarnation &&
                  metrics.identity.firstEntry &&
                  metrics.identity.priorAlarms === 0 &&
                  (!options.buildHistory ||
                    metrics.identity.incarnation === measured.value.primed?.incarnation);

                const residentVerified = index > 0 && incarnation === metrics.identity.incarnation;

                const actorVerified =
                  cohort.target !== "tardie" ||
                  (index === 0
                    ? metrics.directory?.incarnation !== reset.directoryBefore?.incarnation &&
                      metrics.directory?.firstEntry === true &&
                      metrics.directory.priorAlarms === 0
                    : metrics.directory?.incarnation === directoryIncarnation);

                if (!(index === 0 ? coldVerified : residentVerified) || !actorVerified)
                  return yield* new BenchError({
                    message:
                      "Object residency changed; sample is not valid for its cold/warm label.",
                  });
                incarnation = metrics.identity.incarnation;
                directoryIncarnation = metrics.directory?.incarnation;

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
                          driverMs: measured.value.driverMs,
                          firstTextMs: measured.value.firstTextMs,
                          ...(measured.value.firstText === undefined
                            ? {}
                            : { firstText: measured.value.firstText }),
                          ...(measured.value.firstTextSource === undefined
                            ? {}
                            : { firstTextSource: measured.value.firstTextSource }),
                          observationMs: measured.value.observationMs,
                          constructorAndProbeMs: measured.value.constructorAndProbeMs,
                          primeMs: measured.value.primeMs,
                          bytes: metrics.bytes,
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
                          controllerMs: after - before,
                          controllerColo: measured.colo,
                          colo: measured.value.colo,
                          fingerprintVerified: true,
                          coldVerified,
                          residentVerified,
                          buildVerified: true,
                          objectBuild: metrics.identity.build,
                          ...(metrics.directoryUsed === undefined
                            ? {}
                            : { directoryUsed: metrics.directoryUsed }),
                          fingerprints: metrics.calls.map((call) => call.fingerprint),
                        }
                      : row,
                  ),
                }));
                if (state === "warmup") yield* Deferred.succeed(warmed, measured.value.driverMs);
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
            }
            // Joining and writing profiles are outside the driver's per-turn timer.
            if (profiling) yield* Fiber.join(profiling);
          }).pipe(
            Effect.scoped,
            (batch) => (profiled ? withProfileBatch(batch) : batch),
            Effect.catchCause((cause) =>
              Effect.gen(function* () {
                yield* update((value) => ({
                  ...value,
                  failures: [
                    ...value.failures,
                    `${cohort.target}/${cohort.history}/${cohort.ttftMs}: ${message(cause)}`,
                  ],
                }));
                if (!options.buildHistory) return yield* Effect.failCause(cause);
              }),
            ),
          );
        },
        { concurrency: options.concurrency, discard: true },
      );
    }
  }).pipe(Effect.onExit(finish));
}, Effect.scoped);

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
    `Cleanup verified in ${cloud.accountName}: no ${cloud.prefix} Workers or Durable Object namespaces remain.`,
  );
});
