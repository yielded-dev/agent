import { dirname, join } from "node:path";

import {
  Cause,
  Clock,
  Console,
  DateTime,
  Effect,
  Exit,
  FileSystem,
  Schema,
  Semaphore,
} from "effect";

import {
  history,
  MEASURED_TOOLS,
  next,
  payload,
  turn,
  type Message,
  type Turn,
} from "../../src/plan.ts";
import { build, ensureVendor } from "../build.ts";
import { Cloudflare, request } from "../cloudflare.ts";
import { cpu } from "../cpu.ts";
import { deployments } from "../deploy.ts";
import { prepareFixtures } from "../fixtures.ts";
import {
  Health,
  IsolatedTarget,
  MeasureResponse,
  type BuildLabel,
  type Options,
  type Result,
  type Sample,
  type Upload,
} from "../model.ts";
import {
  BenchError,
  git,
  hash,
  nonce,
  privateDirectory,
  redact,
  save,
  workspace,
} from "../platform.ts";
import { median, table } from "../report.ts";
import {
  ColdResult,
  Identity,
  ImportResult,
  Metrics,
  PaddingResult,
  type Query,
} from "./protocol.ts";

type Built = Effect.Success<ReturnType<typeof build>>;
type Live = { readonly worker: string; readonly endpoint: string; readonly expectedBuild: string };

const key = (query: Query) => `${query.target}/${query.object}`;

const address = (live: Live, path: string, query: Query) => {
  const url = new URL(path, live.endpoint);

  for (const [name, value] of Object.entries(query)) url.searchParams.set(name, String(value));

  return url;
};

const equalCounts = (a: Readonly<Record<string, number>>, b: Readonly<Record<string, number>>) =>
  Object.keys(a).length === Object.keys(b).length &&
  Object.keys(a).every((name) => a[name] === b[name]);

// Same framework-independent transcript as the original controller; no measured turn is replayed.
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

/** Task-local controller: old-code reset -> new code bytes -> health-only readiness -> turns. */
export const run = Effect.fnUntraced(function* (options: Options) {
  const targets = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(IsolatedTarget))(
    options.targets,
  ).pipe(
    Effect.mapError(
      () => new BenchError({ message: "Isolated mode supports only yielded and pi." }),
    ),
  );

  if (options.rigorous && (!options.baseline || !options.candidate))
    return yield* new BenchError({
      message: "Rigorous isolated runs require both framework refs.",
    });

  const started = yield* Clock.currentTimeMillis;
  const cloud = yield* Cloudflare;
  const deploy = yield* deployments;
  const fs = yield* FileSystem.FileSystem;
  const lock = yield* Semaphore.make(1);
  const runName = `cold-storage-fresh-${started.toString(36)}-${nonce().slice(0, 8)}`;
  const output = join(workspace, "results", runName);
  const labels: readonly BuildLabel[] = options.rigorous ? ["baseline", "candidate"] : ["working"];

  const order =
    options.storageProbe === "none"
      ? (options.order ?? (nonce().charAt(0) < "8" ? "ABBA" : "BAAB"))
      : "AABB";

  const sequence: readonly BuildLabel[] = options.rigorous
    ? order === "AABB"
      ? ["baseline", "baseline", "candidate", "candidate"]
      : order === "ABBA"
        ? ["baseline", "candidate", "candidate", "baseline"]
        : ["candidate", "baseline", "baseline", "candidate"]
    : ["working"];

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
      isolate: true,
      order,
      targets: [...targets],
      sizes: [...options.sizes],
      ttft: [...options.ttft],
      initialNamespaceReadiness: {
        propagationWaitMs: 30000,
        probesPerRound: options.concurrency,
        consecutiveRounds: 3,
        measuredObjectsTouched: false,
      },
      measuredBuildReadiness: {
        propagationWaitMs: 60000,
        ingresses: ["controller", "driver"],
        consecutiveRounds: 10,
        measuredObjectsTouched: false,
      },
      paddingPreparation: {
        when: "label changes",
        paddingBytesMeasuredAt: "preparation",
        currentDatabaseBytesMeasuredAfterEveryTurn: true,
      },
    },
    sequence: [...sequence],
    builds: [],
    fixtures: [],
    uploads: [],
    resets: [],
    samples: [],
    failures: [],
    kept: options.keep,
    complete: false,
  };

  const message = (cause: Cause.Cause<unknown>) =>
    redact(
      Cause.prettyErrors(cause)
        .map((error) => error.message)
        .join("; "),
      [cloud.accountId, cloud.apiToken, cloud.accountName, cloud.subdomain, deploy.token],
    );

  const update = (change: (value: Result) => Result) =>
    lock.withPermit(
      Effect.gen(function* () {
        result = change(result);
        yield* save(output + ".json", result);
      }),
    );

  const failure = (text: string) =>
    update((value) => ({ ...value, failures: [...value.failures, text] }));

  const attempted = new Map<IsolatedTarget, string>();
  const live = new Map<string, Live>();
  const poisoned = new Set<string>();

  const finish = (exit: Exit.Exit<unknown, unknown>) =>
    Effect.gen(function* () {
      if (Exit.isFailure(exit)) {
        const error = message(exit.cause);

        result = {
          ...result,
          failures: result.failures.includes(error) ? result.failures : [...result.failures, error],
          samples: result.samples.map((row) =>
            row.status === "running"
              ? {
                  ...row,
                  status: "failed",
                  outcome: row.outcome ?? "unknown",
                  error: "Controller interrupted; input will not be replayed.",
                }
              : row,
          ),
          resets: result.resets?.map((row) =>
            row.status === "running"
              ? { ...row, status: "failed", error: "Reset acknowledgement unavailable." }
              : row,
          ),
          uploads: result.uploads?.map((row) =>
            row.status === "uploading"
              ? { ...row, status: "failed", error: "Upload/readiness did not complete." }
              : row,
          ),
        };
      }
      if (options.cpu && attempted.size > 0) {
        yield* Console.error(
          "Collecting invocation CPU for both isolated Workers, including aborts…",
        );
        yield* Effect.sleep("15 seconds");
        const upper = yield* Clock.currentTimeMillis;

        for (const [target, worker] of attempted) {
          const collected = yield* cpu(worker, started, upper).pipe(Effect.exit);

          if (collected._tag === "Failure")
            result = { ...result, failures: [...result.failures, message(collected.cause)] };
          else
            result = {
              ...result,
              cpu: [
                ...(result.cpu ?? []),
                ...collected.value.invocations.map((row) => ({
                  ...row,
                  worker,
                  target: row.target ?? target,
                })),
              ],
              unmatchedCpuMarkers:
                (result.unmatchedCpuMarkers ?? 0) + collected.value.unmatchedMarkers,
            };
        }
      }
      if (!options.keep && attempted.size > 0) {
        yield* Console.error("Destroying this run's isolated target stacks and verifying both…");
        let verified = true;
        const workers: string[] = [];
        const namespaces: string[] = [];

        // Sequential writes preserve the shared deployment state; never call global teardown here.
        for (const worker of attempted.values()) {
          const removed = yield* deploy.destroy(worker).pipe(Effect.exit);

          if (removed._tag === "Failure") {
            verified = false;
            workers.push(worker);
            result = { ...result, failures: [...result.failures, message(removed.cause)] };
          } else {
            verified &&= removed.value.verified;
            workers.push(...removed.value.workers);
            namespaces.push(...removed.value.namespaces);
          }
        }
        result = { ...result, cleanup: { verified, workers, namespaces } };
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
        `Results: results/${runName}.json; wall ${(result.wallMs / 1000).toFixed(1)} s.`,
      );
      if (!result.complete)
        return yield* new BenchError({
          message:
            "Isolated run incomplete; failures and cleanup evidence are retained in its result JSON.",
        });
    });

  yield* update((value) => value);
  yield* Effect.gen(function* () {
    yield* Console.error(
      "Preparing the verified 50/250 fixtures; measured turns have not started…",
    );
    yield* ensureVendor;

    const fixtures = (yield* prepareFixtures({ targets, sizes: options.sizes })).map((fixture) => {
      if (fixture.target !== "yielded") return fixture;
      const { archive: _archive, ...source } = fixture;

      return {
        ...source,
        mode: "replay" as const,
        fallbackReason:
          fixture.fallbackReason ?? "Isolate-start seeds through public submit/await replay.",
      };
    });

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
    const infrastructureReused = yield* deploy.infrastructure;

    yield* update((value) => ({ ...value, infrastructureReused }));
    const yieldedBuilds = new Map<BuildLabel, Built>();
    let piBuild: Built | undefined;

    if (targets.includes("yielded"))
      for (const label of labels) {
        const ref =
          label === "baseline"
            ? options.baseline
            : label === "candidate"
              ? options.candidate
              : undefined;

        const existing =
          options.storageProbe === "none" ? undefined : yieldedBuilds.get("baseline");

        const compiled =
          existing ??
          (yield* build(
            join(privateDirectory, runName, "build", "yielded", label),
            "yielded",
            ref,
          ));

        yieldedBuilds.set(label, compiled);
        yield* update((value) => ({
          ...value,
          builds: [
            ...value.builds,
            { label, target: "yielded", revision: compiled.revision, sha256: compiled.sha256 },
          ],
        }));
      }
    if (targets.includes("pi")) {
      const compiled = yield* build(join(privateDirectory, runName, "build", "pi"), "pi");

      piBuild = compiled;
      yield* update((value) => ({
        ...value,
        builds: [
          ...value.builds,
          { label: "control", target: "pi", revision: compiled.revision, sha256: compiled.sha256 },
        ],
      }));
    }

    const cohorts: Query[] = options.sizes.flatMap((size) =>
      options.ttft.flatMap((ttftMs) =>
        Array.from({ length: options.objects }, (_, object) =>
          targets.map((target): Query => ({
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

    const turnsPerEpoch = options.repeats + 2;
    const seedBytes = new Map<string, number>();

    const storageGroup = (cohort: Query) =>
      options.storageProbe === "none"
        ? undefined
        : /[02468]$/.test(cohort.object)
          ? "grown"
          : "control";

    const references = new Map<number, Map<string, string[]>>();

    for (const size of options.sizes) {
      const messages: Message[] = [];

      for (const input of history(0, size)) append(messages, input);
      const expected = new Map<string, string[]>();

      for (let epoch = 0; epoch < sequence.length; epoch++)
        for (let index = 0; index < turnsPerEpoch; index++) {
          const sample = `e${epoch}-t${index}`;

          expected.set(sample, append(messages, turn(sample, MEASURED_TOOLS)));
        }
      references.set(size, expected);
    }

    const upload = Effect.fnUntraced(function* (
      target: IsolatedTarget,
      label: BuildLabel,
      phase: "seed" | "epoch",
      epoch?: number,
    ) {
      const compiled = target === "pi" ? piBuild : yieldedBuilds.get(label);

      if (!compiled) return yield* new BenchError({ message: "Missing isolated target build." });
      const worker = `${runName}-${target}`;
      const suffix = epoch === undefined ? "seed" : `e${epoch}`;
      const expectedBuild = `${runName}-${target}-${label}-${suffix}-${compiled.sha256}`;
      const file = join(privateDirectory, runName, "uploads", target, suffix, "worker.mjs");
      // Different module bytes even for the adjacent B/B (or A/A) pass, with no global random work.
      const code = `// cold-storage-fresh code version: ${expectedBuild}\n${yield* fs.readFileString(compiled.file)}`;

      yield* fs.makeDirectory(dirname(file), { recursive: true, mode: 0o700 });
      yield* fs.writeFileString(file, code, { mode: 0o600 });
      const previousBuild = live.get(target)?.expectedBuild;

      const row: Upload = {
        target,
        worker,
        phase,
        label,
        ...(epoch === undefined ? {} : { epoch }),
        expectedBuild,
        ...(previousBuild === undefined ? {} : { previousBuild }),
        sourceSha256: compiled.sha256,
        uploadedSha256: hash(code),
        resetCount:
          phase === "seed" ? 0 : cohorts.filter((cohort) => cohort.target === target).length,
        status: "uploading",
      };

      attempted.set(target, worker);
      yield* update((value) => ({ ...value, uploads: [...(value.uploads ?? []), row] }));
      yield* Effect.gen(function* () {
        const endpoint = yield* deploy.target(
          worker,
          file,
          expectedBuild,
          options.cpu,
          target,
          false,
        );

        let consecutive = 0;
        let health: typeof Health.Type | undefined;
        let last = "No matching health response.";
        const readinessFailures: NonNullable<Upload["readinessFailures"]>[number][] = [];
        const requiredRounds = phase === "seed" ? 3 : 10;

        // New Worker routes and their Object bindings propagate separately. These disposable
        // identity probes are used only for the untimed seed version, never a measured epoch.
        // A previous build served measured pi requests after three matching health rounds.
        // Wait before stateless readiness; never probe or retry a measured Object here.
        yield* Effect.sleep(phase === "seed" ? "30 seconds" : "60 seconds");

        for (let attempt = 0; attempt < 30; attempt++) {
          const [controller, probe] = yield* Effect.all(
            [
              request(
                endpoint + `/health?probe=${attempt}`,
                deploy.token,
                Health,
                undefined,
                "10 seconds",
                expectedBuild,
              ).pipe(Effect.exit),
              request(
                deploy.driver + "/ready",
                deploy.token,
                Health,
                { targetUrl: endpoint, expectedBuild },
                "10 seconds",
                expectedBuild,
              ).pipe(Effect.exit),
            ],
            { concurrency: 2 },
          );

          const objectProbes =
            phase === "seed"
              ? yield* Effect.forEach(
                  Array.from({ length: options.concurrency }, (_, index) => index),
                  (index) =>
                    request(
                      address({ worker, endpoint, expectedBuild }, "/identity", {
                        target,
                        object: `${runName}-readiness-${target}-${attempt}-${index}`,
                        sample: "readiness",
                        history: 50,
                        ttftMs: 0,
                        chunkDelayMs: 0,
                      }),
                      deploy.token,
                      Identity,
                      undefined,
                      "10 seconds",
                      expectedBuild,
                    ).pipe(Effect.exit),
                  { concurrency: options.concurrency },
                )
              : [];

          const objectsReady = objectProbes.every(
            (outcome) =>
              outcome._tag === "Success" && outcome.value.value.isolate?.build === expectedBuild,
          );

          for (const outcome of objectProbes) {
            if (
              outcome._tag === "Failure" ||
              outcome.value.value.isolate?.build !== expectedBuild
            ) {
              readinessFailures.push({
                ingress: "object",
                attempt,
                error:
                  outcome._tag === "Failure"
                    ? message(outcome.cause)
                    : "Probe Object still reports an older build.",
              });
              yield* update((value) => ({
                ...value,
                uploads: value.uploads?.map((item) =>
                  item.expectedBuild === expectedBuild
                    ? { ...item, readinessFailures: [...readinessFailures] }
                    : item,
                ),
              }));
            }
          }

          for (const [ingress, outcome] of [
            ["controller", controller],
            ["driver", probe],
          ] as const) {
            if (outcome._tag === "Failure" || outcome.value.value.build !== expectedBuild) {
              readinessFailures.push({
                ingress,
                attempt,
                error:
                  outcome._tag === "Failure"
                    ? message(outcome.cause)
                    : "Health still reports an older build.",
              });
              yield* update((value) => ({
                ...value,
                uploads: value.uploads?.map((item) =>
                  item.expectedBuild === expectedBuild
                    ? { ...item, readinessFailures: [...readinessFailures] }
                    : item,
                ),
              }));
            }
          }

          if (
            controller._tag === "Success" &&
            controller.value.value.build === expectedBuild &&
            probe._tag === "Success" &&
            probe.value.value.build === expectedBuild &&
            objectsReady
          ) {
            health = probe.value.value;
            consecutive++;
            if (consecutive === requiredRounds) break;
          } else {
            consecutive = 0;
            last =
              probe._tag === "Failure"
                ? message(probe.cause)
                : "Health still reports an older build.";
          }
          yield* Effect.sleep("1 second");
        }
        if (consecutive !== requiredRounds || health === undefined)
          return yield* new BenchError({
            message: `Target build readiness failed; no Object lookup was requested. ${last}`,
          });

        live.set(target, { worker, endpoint, expectedBuild });
        yield* update((value) => ({
          ...value,
          uploads: value.uploads?.map((item) =>
            item.expectedBuild === expectedBuild ? { ...item, status: "ready", health } : item,
          ),
        }));
      }).pipe(
        Effect.tapCause((cause) =>
          update((value) => ({
            ...value,
            uploads: value.uploads?.map((item) =>
              item.expectedBuild === expectedBuild
                ? { ...item, status: "failed", error: message(cause) }
                : item,
            ),
          })),
        ),
      );
    });

    // The seed upload is never measured. Yielded uses its public replay path; pi imports native rows.
    for (const target of targets)
      yield* upload(target, options.rigorous ? "baseline" : "working", "seed");
    yield* Console.error(
      `Seeding and checking complete fixture counts on ${cohorts.length} Objects…`,
    );
    let seedFailed = false;
    let seededCount = 0;

    yield* Effect.forEach(
      shuffle(cohorts),
      (cohort) =>
        Effect.gen(function* () {
          if (seedFailed)
            return yield* failure(
              `${key(cohort)} seed: not sent after an earlier fixture setup failure.`,
            );
          const current = live.get(cohort.target);

          const fixture = fixtures.find(
            (item) => item.target === cohort.target && item.history === cohort.history,
          );

          if (!current || !fixture)
            return yield* new BenchError({ message: "Missing seed deployment or fixture." });

          const seeded = (yield* request(
            address(current, "/import", cohort),
            deploy.token,
            ImportResult,
            fixture,
            "10 minutes",
            current.expectedBuild,
          )).value;

          seedBytes.set(key(cohort), seeded.bytes);
          if (
            seeded.target !== cohort.target ||
            seeded.history !== cohort.history ||
            seeded.mode !== fixture.mode ||
            seeded.identity.isolate?.build !== current.expectedBuild ||
            seeded.fingerprint !== fixture.fingerprint ||
            !equalCounts(seeded.tables, fixture.tables)
          )
            return yield* new BenchError({
              message:
                "Seed build, replay mode, transcript fingerprint, or complete table counts differ from the fixture.",
            });
          seededCount++;
          if (seededCount % 10 === 0 || seededCount === cohorts.length)
            yield* Console.error(
              `Verified fixture setup on ${seededCount}/${cohorts.length} Objects.`,
            );
        }).pipe(
          Effect.catchCause((cause) => {
            seedFailed = true;

            return failure(`${key(cohort)} seed: ${message(cause)}`);
          }),
        ),
      { concurrency: options.concurrency, discard: true },
    );
    if (seedFailed)
      return yield* new BenchError({ message: "Seeding failed; no measured epoch was uploaded." });

    const paddingByObject = new Map<string, PaddingResult>();

    for (const [epoch, label] of sequence.entries()) {
      const resets = new Map<string, { readonly build: string; readonly response: ColdResult }>();
      let resetFailed = false;

      // Prepare every database before old-code resets and uploads. Never touch an Object
      // between its fresh upload and the measured first turn. Repeated labels retain their
      // existing padding; measured turns append canonical facts without changing that table.
      if (options.storageProbe !== "none" && (epoch === 0 || sequence[epoch - 1] !== label)) {
        yield* Console.error(`Preparing ${label} storage on old builds before all resets…`);
        yield* Effect.forEach(
          cohorts,
          (cohort) =>
            Effect.gen(function* () {
              const current = live.get(cohort.target);

              if (!current)
                return yield* new BenchError({
                  message: "Missing old build for storage preparation.",
                });

              const prepared = (yield* request(
                address(current, "/storage", cohort),
                deploy.token,
                PaddingResult,
                {
                  mib:
                    label === "candidate" && storageGroup(cohort) === "grown"
                      ? options.paddingMiB
                      : 0,
                  read: options.storageProbe === "touched",
                },
                "3 minutes",
                current.expectedBuild,
              )).value;

              paddingByObject.set(key(cohort), prepared);
            }),
          { concurrency: options.concurrency, discard: true },
        );
        yield* Effect.sleep("2 seconds");
      }

      yield* Console.error(
        `Pass ${epoch + 1}/${sequence.length}: acknowledging all resets on OLD builds…`,
      );
      yield* Effect.forEach(
        shuffle(cohorts),
        (cohort) =>
          Effect.gen(function* () {
            const current = live.get(cohort.target);
            const query = { ...cohort, sample: `reset-e${epoch}` };

            if (!current)
              return yield* new BenchError({ message: "Missing old deployment for reset." });
            yield* update((value) => ({
              ...value,
              resets: [
                ...(value.resets ?? []),
                { ...query, epoch, expectedBuild: current.expectedBuild, status: "running" },
              ],
            }));
            yield* Effect.gen(function* () {
              const response = (yield* request(
                address(current, "/cold", query),
                deploy.token,
                ColdResult,
                {},
                "3 minutes",
                current.expectedBuild,
              )).value;

              yield* update((value) => ({
                ...value,
                resets: value.resets?.map((row) =>
                  row.epoch === epoch && key(row) === key(query) ? { ...row, response } : row,
                ),
              }));
              if (
                !response.ok ||
                !response.threadAborted ||
                response.directoryAborted === false ||
                response.before.isolate?.build !== current.expectedBuild
              )
                return yield* new BenchError({
                  message:
                    "Old-build Object abort or Object build was not acknowledged; refusing the next upload.",
                });
              resets.set(key(cohort), { build: current.expectedBuild, response });
              yield* update((value) => ({
                ...value,
                resets: value.resets?.map((row) =>
                  row.epoch === epoch && key(row) === key(query) ? { ...row, status: "ok" } : row,
                ),
              }));
            }).pipe(
              Effect.tapCause((cause) =>
                update((value) => ({
                  ...value,
                  resets: value.resets?.map((row) =>
                    row.epoch === epoch && key(row) === key(query)
                      ? { ...row, status: "failed", error: message(cause) }
                      : row,
                  ),
                })),
              ),
            );
          }).pipe(
            Effect.catchCause((cause) => {
              resetFailed = true;

              return failure(`${key(cohort)} reset e${epoch}: ${message(cause)}`);
            }),
          ),
        { concurrency: options.concurrency, discard: true },
      );
      if (resetFailed || resets.size !== cohorts.length)
        return yield* new BenchError({
          message:
            "Not every old-build Object reset was acknowledged; no next-epoch upload was attempted.",
        });

      // No /cold request occurs after either new upload. All subsequent Object work is measured.
      for (const target of targets) yield* upload(target, label, "epoch", epoch);
      yield* Console.error(
        `Measuring ${label} pass ${epoch + 1}: ${cohorts.length} Objects, concurrency ${options.concurrency}…`,
      );
      yield* Effect.forEach(
        shuffle(cohorts),
        (cohort) =>
          Effect.gen(function* () {
            const current = live.get(cohort.target);
            const reset = resets.get(key(cohort));

            if (!current || !reset)
              return yield* new BenchError({ message: "Missing upload/reset evidence." });
            let anchor: Identity | undefined;

            for (let index = 0; index < turnsPerEpoch; index++) {
              const query = { ...cohort, sample: `e${epoch}-t${index}` };

              const sample: Sample = {
                ...query,
                build: label,
                epoch,
                repeat: index,
                state: index === 0 ? "fresh-first-turn" : index === 1 ? "warmup" : "warm",
                status: poisoned.has(key(cohort)) ? "skipped" : "running",
                ...(poisoned.has(key(cohort))
                  ? {
                      outcome: "not-sent" as const,
                      error:
                        "An earlier turn failed; later inputs to this Object are not sent or replayed.",
                    }
                  : {}),
                worker: current.worker,
                expectedBuild: current.expectedBuild,
                resetBuild: reset.build,
                resetBefore: reset.response.before,
                seedBytes: seedBytes.get(key(cohort)),
                ...(storageGroup(cohort) === undefined
                  ? {}
                  : { storageGroup: storageGroup(cohort) }),
                ...(paddingByObject.get(key(cohort)) === undefined
                  ? {}
                  : { padding: paddingByObject.get(key(cohort)) }),
              };

              const amend = (change: Partial<Sample>) =>
                update((value) => ({
                  ...value,
                  samples: value.samples.map((row) =>
                    key(row) === key(query) && row.sample === query.sample
                      ? { ...row, ...change }
                      : row,
                  ),
                }));

              yield* update((value) => ({ ...value, samples: [...value.samples, sample] }));
              if (sample.status === "skipped") continue;

              yield* Effect.gen(function* () {
                const before = yield* Clock.currentTimeMillis;

                const measured = yield* request(
                  deploy.driver + "/measure",
                  deploy.token,
                  MeasureResponse,
                  { query, targetUrl: current.endpoint, expectedBuild: current.expectedBuild },
                  "3 minutes",
                  current.expectedBuild,
                );

                const after = yield* Clock.currentTimeMillis;

                yield* amend({
                  outcome: "completed",
                  driverMs: measured.value.driverMs,
                  ...(measured.value.admissionMs === undefined
                    ? {}
                    : { admissionMs: measured.value.admissionMs }),
                  controllerMs: after - before,
                  controllerColo: measured.colo,
                  colo: measured.value.colo,
                });

                const metrics = (yield* request(
                  address(current, "/metrics", query),
                  deploy.token,
                  Metrics,
                  undefined,
                  "3 minutes",
                  current.expectedBuild,
                )).value;

                yield* amend({
                  identity: metrics.identity,
                  fingerprints: metrics.calls.map((call) => call.fingerprint),
                  databaseBytes: metrics.bytes,
                  ...(metrics.storage === undefined ? {} : { storage: metrics.storage }),
                });
                const expected = references.get(query.history)?.get(query.sample);
                const actual = metrics.query;

                if (
                  actual.target !== query.target ||
                  actual.object !== query.object ||
                  actual.sample !== query.sample ||
                  actual.history !== query.history ||
                  actual.ttftMs !== query.ttftMs ||
                  actual.chunkDelayMs !== query.chunkDelayMs ||
                  !expected ||
                  metrics.calls.length !== MEASURED_TOOLS + 1 ||
                  metrics.calls.some(
                    (call, i) =>
                      call.call !== i ||
                      call.status !== 200 ||
                      call.error !== undefined ||
                      call.endMs === undefined ||
                      call.fingerprint !== expected[i] ||
                      call.receipt === undefined ||
                      call.receipt.fingerprint !== expected[i] ||
                      call.receipt.sample !== query.sample ||
                      call.receipt.object !== query.object ||
                      call.receipt.call !== i,
                  )
                )
                  return yield* new BenchError({
                    message:
                      "Model-visible transcript, provider receipt/completion, or metrics query differed from the reference.",
                  });

                const identity = metrics.identity;
                const objectIsolate = identity.isolate;

                const buildVerified =
                  objectIsolate?.build === current.expectedBuild &&
                  identity.workerIsolate?.build === current.expectedBuild;

                const reasons: string[] = [];

                if (objectIsolate?.build !== current.expectedBuild)
                  reasons.push("Object build mismatch or missing");
                if (identity.workerIsolate?.build !== current.expectedBuild)
                  reasons.push("Routing Worker build mismatch or missing");

                const residentVerified =
                  index > 0 &&
                  anchor !== undefined &&
                  identity.incarnation === anchor.incarnation &&
                  objectIsolate !== undefined &&
                  objectIsolate.id === anchor.isolate?.id &&
                  !identity.firstEntry;

                if (index === 0) {
                  anchor = identity;
                  if (reset.build === current.expectedBuild)
                    reasons.push("Epoch build did not change after reset");
                  if (identity.incarnation === reset.response.before.incarnation)
                    reasons.push("Object incarnation unchanged after old-build reset");
                  if (!identity.firstEntry)
                    reasons.push("Object already entered before measured turn");
                  if (identity.priorAlarms !== 0)
                    reasons.push("Object alarm preceded measured turn");
                  if (!objectIsolate || objectIsolate.id === reset.response.before.isolate?.id)
                    reasons.push("Object isolate UUID unchanged or missing");
                  if (objectIsolate?.durableObjectConstructors !== 1)
                    reasons.push("Object isolate constructor count is not one");
                  if (objectIsolate?.statelessFetches !== 0)
                    reasons.push("Object isolate had earlier stateless fetches");
                } else if (!residentVerified)
                  reasons.push("Object incarnation or isolate changed within the epoch");

                const lastEnd = metrics.calls.at(-1)?.endMs;

                const gaps = metrics.calls
                  .slice(1)
                  .map((call, i) => call.startMs - (metrics.calls[i]?.endMs ?? NaN));

                yield* amend({
                  status: reasons.length === 0 ? "ok" : "excluded",
                  fingerprintVerified: true,
                  buildVerified,
                  freshVerified: index === 0 && reasons.length === 0,
                  residentVerified,
                  exclusionReasons: reasons,
                  gapMs: median(gaps),
                  ...(lastEnd === undefined
                    ? {}
                    : { lastResponseToClientMs: measured.value.observedMs - lastEnd }),
                });
              }).pipe(
                Effect.catchCause((cause) =>
                  Effect.gen(function* () {
                    poisoned.add(key(cohort));
                    const error = message(cause);

                    yield* update((value) => ({
                      ...value,
                      samples: value.samples.map((row) =>
                        key(row) === key(query) && row.sample === query.sample
                          ? { ...row, status: "failed", outcome: row.outcome ?? "unknown", error }
                          : row,
                      ),
                      failures: [...value.failures, `${key(query)}/${query.sample}: ${error}`],
                    }));
                  }),
                ),
              );
            }
          }),
        { concurrency: options.concurrency, discard: true },
      );
    }
  }).pipe(Effect.onExit(finish));
}, Effect.scoped);
