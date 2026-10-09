import { join } from "node:path";

import { Cause, Clock, Console, DateTime, Effect, Exit, FileSystem, Semaphore } from "effect";

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
import { connect, request } from "./cloudflare.ts";
import { cpu } from "./cpu.ts";
import { deployments } from "./deploy.ts";
import { prepareFixtures } from "./fixtures.ts";
import { MeasureResponse, type Options, type Result, type Sample } from "./model.ts";
import {
  BenchError,
  git,
  hash,
  nonce,
  privateDirectory,
  redact,
  save,
  workspace,
} from "./platform.ts";
import { median, table } from "./report.ts";
import { ColdResult, ImportResult, Metrics, type Query } from "./worker/protocol.ts";

const equalCounts = (a: Readonly<Record<string, number>>, b: Readonly<Record<string, number>>) =>
  Object.keys(a).length === Object.keys(b).length &&
  Object.keys(a).every((key) => a[key] === b[key]);

// The reference transcript is independent of every framework and computed before timing.
const append = (messages: Message[], input: Turn): string[] => {
  const hashes: string[] = [];

  messages.push({ role: "user", text: input.text });
  for (;;) {
    hashes.push(
      hash(JSON.stringify(messages.map((m) => [m.role, m.text, m.calls ?? []]))).slice(0, 16),
    );
    const step = next(messages);

    if ("answer" in step) {
      messages.push({ role: "assistant", text: step.answer });

      return hashes;
    }
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
  const cloud = yield* connect;
  const deploy = yield* deployments(cloud);
  const fs = yield* FileSystem.FileSystem;
  const runName = `durable-bench-${started.toString(36)}-${nonce().slice(0, 8)}`;
  const output = join(workspace, "results", runName);
  const lock = yield* Semaphore.make(1);

  let result: Result = {
    version: 1,
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
    },
    builds: [],
    fixtures: [],
    samples: [],
    failures: [],
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

  yield* Console.error(`Account: ${cloud.accountName}. Preparing fixtures…`);
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
        };
      }
      if (options.cpu && targetStarted) {
        yield* Console.error("Collecting optional invocation CPU…");
        yield* Effect.sleep("15 seconds");
        const upper = yield* Clock.currentTimeMillis;

        yield* cpu(cloud, runName, telemetryFrom, upper).pipe(
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
      if (!result.complete)
        return yield* new BenchError({
          message: "Run incomplete; failures and cleanup status are recorded in the result JSON.",
        });
    });

  yield* Effect.gen(function* () {
    yield* ensureVendor;
    const fixtures = yield* prepareFixtures({ targets: options.targets, sizes: options.sizes });

    yield* update((value) => ({
      ...value,
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

      const compiled = yield* build(join(privateDirectory, runName, label), "worker/index.ts", ref);

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

    const turnsPerEpoch = options.repeats + (options.cold ? 2 : 1);
    const references = new Map<number, Map<string, string[]>>();

    for (const size of options.sizes) {
      const messages: Message[] = [];

      for (const input of history(0, size)) append(messages, input);
      const expected = new Map<string, string[]>();

      for (let epoch = 0; epoch < epochs.length; epoch++)
        for (let index = 0; index < turnsPerEpoch; index++) {
          const sample = `e${epoch}-t${index}`;

          expected.set(sample, append(messages, turn(sample, MEASURED_TOOLS)));
        }
      references.set(size, expected);
    }

    const cohorts = options.sizes.flatMap((size) =>
      options.ttft.flatMap((ttftMs) =>
        Array.from({ length: options.objects }, (_, object) =>
          options.targets.map((target): Query => ({
            target,
            object: `${runName}-h${size}-d${ttftMs}-o${object}`,
            history: size,
            ttftMs,
            chunkDelayMs: ttftMs === 400 ? 10 : 0,
            sample: "import",
          })),
        ).flat(),
      ),
    );

    for (const [epoch, label] of epochs.entries()) {
      const compiled = builds.get(label);

      if (!compiled) return yield* new BenchError({ message: "Missing target build." });
      telemetryFrom = Math.min(telemetryFrom, yield* Clock.currentTimeMillis);
      targetStarted = true;

      const endpoint = yield* deploy.target(
        runName,
        compiled.file,
        compiled.sha256 + `-e${epoch}`,
        options.cpu,
      );

      const url = (path: string, query: Query) => {
        const address = new URL(path, endpoint);

        for (const [key, value] of Object.entries(query))
          address.searchParams.set(key, String(value));

        return address;
      };

      // Seeding and its count scans must not contend with timed turns, including the first A/B pass.
      if (epoch === 0) {
        yield* Console.error(`Seeding and verifying ${cohorts.length} Objects…`);
        yield* Effect.forEach(
          shuffle(cohorts),
          (cohort) =>
            Effect.gen(function* () {
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
              Effect.tapCause((cause) =>
                update((value) => ({
                  ...value,
                  failures: [
                    ...value.failures,
                    `${cohort.target}/${cohort.history}/${cohort.ttftMs} seed: ${message(cause)}`,
                  ],
                })),
              ),
            ),
          { concurrency: options.concurrency, discard: true },
        );
      }

      yield* Console.error(
        `Measuring ${label}, pass ${epoch + 1}/${epochs.length}: ${cohorts.length} Objects, concurrency ${options.concurrency}…`,
      );
      yield* Effect.forEach(
        shuffle(cohorts),
        (cohort) =>
          Effect.gen(function* () {
            // Discard constructor caches after import, and restart the same stored Object after every build change.
            const reset = (yield* request(url("/cold", cohort), deploy.token, ColdResult, {}))
              .value;

            if (!reset.ok || !reset.threadAborted || reset.directoryAborted === false)
              return yield* new BenchError({ message: "Cold abort was not acknowledged." });
            let incarnation = "";
            let directoryIncarnation: string | undefined;

            for (let index = 0; index < turnsPerEpoch; index++) {
              const query = { ...cohort, sample: `e${epoch}-t${index}` };

              const state =
                options.cold && index === 0
                  ? "cold"
                  : index < (options.cold ? 2 : 1)
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
                  { query, targetUrl: endpoint },
                );

                const after = yield* Clock.currentTimeMillis;

                const metrics = (yield* request(url("/metrics", query), deploy.token, Metrics))
                  .value;

                const expected = references.get(query.history)?.get(query.sample);

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
                  metrics.identity.priorAlarms === 0;

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

                yield* update((value) => ({
                  ...value,
                  samples: value.samples.map((row) =>
                    matches(row)
                      ? {
                          ...sample,
                          status: "ok",
                          driverMs: measured.value.driverMs,
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
            }
          }).pipe(
            Effect.tapCause((cause) =>
              update((value) => ({
                ...value,
                failures: [
                  ...value.failures,
                  `${cohort.target}/${cohort.history}/${cohort.ttftMs}: ${message(cause)}`,
                ],
              })),
            ),
          ),
        { concurrency: options.concurrency, discard: true },
      );
    }
  }).pipe(Effect.onExit(finish));
}, Effect.scoped);

export const teardown = Effect.gen(function* () {
  const cloud = yield* connect;
  const deploy = yield* deployments(cloud);
  const cleanup = yield* deploy.teardown;

  const result = {
    checkedAt: DateTime.formatIso(yield* DateTime.now),
    ...cleanup,
  };

  yield* save(join(workspace, "results", "cleanup.json"), result);
  yield* Console.log(
    `Cleanup verified in ${cloud.accountName}: no durable-bench Workers or Durable Object namespaces remain.`,
  );
});
