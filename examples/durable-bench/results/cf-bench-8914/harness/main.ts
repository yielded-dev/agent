import { NodeCrypto, NodeRuntime, NodeServices } from "@effect/platform-node";
import { Clock, Console, Effect, FileSystem, Layer, Path, Schema } from "effect";
import { Command, Flag } from "effect/cli";
import { FetchHttpClient, HttpClientError } from "effect/http";

import {
  sanitizeReplayCpuTelemetry,
  telemetryFor,
} from "../../../../../tooling/context-continuity-eval/src/replay-cpu-results.ts";
import { history, turn } from "../../../src/plan.ts";
import {
  type RoleWindow,
  type Role,
  BenchError,
  CohortWindow,
  FINGERPRINTS,
  FIXTURES,
  Identity,
  measurementSamples,
  PHASES,
  Receipt,
  ReferenceObservation,
  ReferenceReceipt,
  ROLES,
  SIZES,
  Sample,
  Seed,
  SerialSeedReceipt,
  Stats,
  WatchdogSnapshot,
  type Target,
} from "./contracts.ts";
import { initialize, openDeployment, readJson, requireBench, writeJson } from "./deployment.ts";
import { validateSeedProof } from "./seed-proof.ts";
import { constructors } from "./telemetry.ts";

const refsOf = (row: typeof Sample.Type) =>
  row.reference === undefined
    ? []
    : [
        row.reference.value.thread,
        ...(row.reference.value.actor === undefined ? [] : [row.reference.value.actor]),
      ];

const median = (values: readonly number[]) => {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  const right = sorted[middle];
  const left = sorted[middle - 1];

  return right === undefined
    ? null
    : sorted.length % 2 === 0 && left !== undefined
      ? (left + right) / 2
      : right;
};

const command = Command.make(
  "cf-bench-8914",
  {
    action: Flag.Literals("action", [
      "init",
      "deploy",
      "seed",
      "seed-proof",
      "measure",
      "collect",
      "inspect",
      "diagnose",
      "reference",
      "logs",
      "retire",
      "outcomes",
      "cleanup",
      "run",
      "dry-run",
    ]).pipe(Flag.withDefault("dry-run")),
    output: Flag.String("output-dir"),
    bundles: Flag.String("bundles-dir").pipe(
      Flag.withDefault("/private/tmp/cf-bench-8914-bundles"),
    ),
    role: Flag.Literals("role", ["all", ...ROLES]).pipe(Flag.withDefault("all")),
    size: Flag.Literals("size", ["all", "50", "250", "1000"]).pipe(Flag.withDefault("all")),
    sample: Flag.Literals("sample", ["all", "0", "1", "2", "3"]).pipe(Flag.withDefault("all")),
    key: Flag.String("key").pipe(Flag.withDefault("250_0")),
    skipVerified: Flag.Boolean("skip-verified").pipe(Flag.withDefault(false)),
    resumeMeasure: Flag.Boolean("resume-measure").pipe(Flag.withDefault(false)),
    deferTelemetry: Flag.Boolean("defer-telemetry").pipe(Flag.withDefault(false)),
    tardieIngressBatch: Flag.Boolean("tardie-ingress-batch").pipe(Flag.withDefault(true)),
    generation: Flag.String("generation").pipe(Flag.withDefault("seed")),
    resumeAt: Flag.Int("resume-at").pipe(Flag.withDefault(0)),
    batch: Flag.Int("batch").pipe(
      Flag.withDefault(10),
      Flag.withSchema(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 50 }))),
    ),
  },
  Effect.fnUntraced(function* ({
    action,
    output: outputInput,
    bundles,
    role,
    size: selectedSize,
    sample: selectedSample,
    key,
    skipVerified,
    resumeMeasure,
    deferTelemetry,
    tardieIngressBatch,
    generation,
    resumeAt,
    batch: batchSize,
  }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const output = path.resolve(outputInput);
    const roles: readonly Role[] = role === "all" ? ROLES : [role];
    const sizes = SIZES.filter((size) => selectedSize === "all" || String(size) === selectedSize);

    const sampleNumbers = [0, 1, 2, 3].filter(
      (sample) => selectedSample === "all" || String(sample) === selectedSample,
    );

    if (action === "dry-run") {
      yield* Console.log(
        JSON.stringify(
          {
            roles,
            sizes,
            samples: sampleNumbers,
            turns: PHASES.length,
            toolCallsPerTurn: 8,
            primaryMetric: "client-observed and Cloudflare Durable Object invocation wall latency",
            breakdown:
              "DO CPU; DO wall minus CPU; client wall minus DO wall; observed alarm overlap",
            normalisation:
              "Fixed plain-JS reference on the same Object before each turn, with separate Tardie Actor reference; cold reference precedes the final reset",
            unitOfAnalysis:
              "three distinct seeded Objects per role/size; paired Object medians and identical-code control spread",
            locationHint: "wnam",
            cpuLimitMs: 300_000,
            bundle: false,
            output,
            bundles,
            cold: "first workload in a fresh DO incarnation after readiness and ctx.abort over seeded storage; constructor log must match the cold RPC trace; includes open and turn, excludes module readiness, upload startup, and reset",
            cleanup:
              "Alchemy destroy, then independent API verification of Worker and namespace absence",
          },
          null,
          2,
        ),
      );

      return;
    }
    if (action === "init") return yield* initialize(output, path.resolve(bundles));
    const deployment = yield* openDeployment(output);

    if (action === "cleanup") return yield* deployment.cleanup();

    const targetFor = (role: Role) => {
      const target = deployment.targets().find((item) => item.role === role);

      return target === undefined
        ? Effect.fail(new BenchError({ message: `Missing configured role ${role}` }))
        : Effect.succeed(target);
    };

    if (action === "retire") {
      yield* requireBench(role !== "all", "Retire requires a single role");
      for (const item of roles) yield* deployment.retire(item);

      return;
    }
    if (action === "logs") {
      const to = yield* Clock.currentTimeMillis;

      for (const item of roles) {
        const target = yield* targetFor(item);

        const query: Schema.Json = {
          queryId: `${target.name}-constructor-preview`,
          dry: true,
          view: "events",
          limit: 2000,
          timeframe: { from: to - 600_000, to },
          parameters: {
            filterCombination: "and",
            filters: [
              { key: "$workers.scriptName", operation: "eq", type: "string", value: target.name },
            ],
          },
        };

        const result = yield* constructors(yield* deployment.query(query));

        yield* writeJson(path.join(output, `constructor-preview-${item}.json`), {
          query,
          ...result,
        });
        yield* Console.log(
          `${item}: ${result.rows.length} constructor identities from ${result.totalLogs} logs`,
        );
      }

      return;
    }
    if (action === "diagnose") {
      const rows = [];

      for (const item of roles)
        for (const method of ["identity", "stats", ...(item === "tardie" ? ["diagnostic"] : [])]) {
          const response = yield* deployment
            .call(yield* targetFor(item), `/${method}?key=${key}`)
            .pipe(
              Effect.flatMap((result) =>
                method === "diagnostic"
                  ? Schema.decodeUnknownEffect(WatchdogSnapshot)(result.value).pipe(
                      Effect.map((value) => ({ ...result, value })),
                    )
                  : Effect.succeed(result),
              ),
              Effect.timeout("25 seconds"),
              Effect.result,
            );

          const row =
            response._tag === "Success"
              ? { role: item, method, key, result: response.success }
              : { role: item, method, key, error: deployment.redact(String(response.failure)) };

          rows.push(row);
          yield* Console.log(JSON.stringify(row));
        }
      yield* writeJson(
        path.join(output, `diagnostic-${role}-${key}-${yield* Clock.currentTimeMillis}.json`),
        rows,
      );

      return;
    }

    const proveSeedPrefix = Effect.fnUntraced(function* (
      target: Target,
      key: string,
      count: number,
    ) {
      yield* requireBench(
        target.role === "pinned" || target.role === "tardie",
        "Prefix proof is scoped to the two lost-acknowledgement fixtures",
      );
      if (target.role !== "pinned" && target.role !== "tardie")
        return yield* new BenchError({ message: "Unsupported proof role" });
      for (let attempt = 0; attempt < 40; attempt++) {
        const identity = yield* Schema.decodeUnknownEffect(Identity)(
          (yield* deployment.call(target, `/identity?key=${key}`)).value,
        );

        if (identity.generation === target.generation && identity.protocol === "cold-v3-reference")
          break;
        const reset = yield* deployment.call(target, `/restart?key=${key}`, {});

        yield* writeJson(
          path.join(output, "seed-proofs", `${target.role}-${key}-reset-${reset.started}.json`),
          { identity, reset },
        );
        yield* requireBench(
          attempt < 39,
          "Prefix proof Object did not reach the requested version",
        );
        yield* Effect.sleep("3 seconds");
      }
      const evidence = yield* deployment.call(target, `/seedProof?key=${key}`);

      const directory = path.join(
        output,
        "seed-proofs",
        `${target.role}-${key}-${evidence.started}`,
      );

      yield* writeJson(path.join(directory, "raw.json"), evidence);

      const identity = yield* Schema.decodeUnknownEffect(Schema.Struct({ identity: Identity }))(
        evidence.value,
      );

      yield* requireBench(
        identity.identity.generation === target.generation,
        "Prefix proof used an old DO generation",
      );
      const proof = yield* validateSeedProof(evidence.value, target.role, count);

      yield* writeJson(path.join(directory, "decision.json"), {
        ...proof,
        identity: identity.identity,
        generation: target.generation,
        key,
        directory,
      });
      yield* Console.log(
        `${target.role}/${key}: persisted completed prefix ${count} verified without replay`,
      );

      return { ...proof, directory };
    });

    if (action === "seed-proof") {
      yield* requireBench(
        role !== "all" && resumeAt > 0,
        "Prefix proof requires a single role and positive --resume-at",
      );
      for (const item of roles) yield* proveSeedPrefix(yield* targetFor(item), key, resumeAt);

      return;
    }

    const seedPath = (role: Role, size: number, sample: number) =>
      path.join(output, "seed", role, `${size}-${sample}.json`);

    const cohortDirectory = (size: number, sample: number) =>
      path.join(output, "cohorts", `${size}-${sample}`);

    const ready = Effect.fnUntraced(function* (target: Target) {
      for (let attempt = 0; attempt < 20; attempt++) {
        const health = yield* deployment.call(target, "/health").pipe(Effect.result);

        if (health._tag === "Success") return;
        if (attempt === 19) return yield* health.failure;
        yield* Effect.sleep("3 seconds");
      }
    });

    const deployInitial = Effect.fnUntraced(function* () {
      for (const item of roles) {
        const target = yield* deployment.deploy(item, generation);

        yield* ready(target);
      }
    });

    const reference = Effect.fnUntraced(function* (target: Target, key: string, phase: string) {
      // Tardie's Actor has its own placement. Calibrate it separately, then make
      // the same single-Object HTTP reference request immediately before the turn.
      const actor =
        target.role === "tardie"
          ? yield* deployment.call(target, `/${phase}?key=${key}&object=actor`, {})
          : undefined;

      const result = yield* deployment.call(target, `/${phase}?key=${key}`, {});
      const thread = yield* Schema.decodeUnknownEffect(ReferenceReceipt)(result.value);

      const actorReceipt =
        actor === undefined
          ? undefined
          : yield* Schema.decodeUnknownEffect(ReferenceReceipt)(actor.value);

      yield* requireBench(
        [thread, ...(actorReceipt === undefined ? [] : [actorReceipt])].every(
          (value) =>
            value.phase === phase &&
            value.generation === target.generation &&
            value.protocol === "cold-v3-reference",
        ),
        "Reference returned the wrong phase or deployment identity",
      );

      return ReferenceObservation.make({
        started: result.started,
        ended: result.ended,
        clientWallMs: result.clientWallMs,
        ...(result.cfRay === undefined ? {} : { cfRay: result.cfRay }),
        value: { thread, ...(actorReceipt === undefined ? {} : { actor: actorReceipt }) },
        ...(actor === undefined
          ? {}
          : {
              actorTiming: {
                started: actor.started,
                ended: actor.ended,
                clientWallMs: actor.clientWallMs,
                ...(actor.cfRay === undefined ? {} : { cfRay: actor.cfRay }),
              },
            }),
      });
    });

    const prepareCold = Effect.fnUntraced(function* (
      target: Target,
      key: string,
      directory: string,
    ) {
      const probes = [];

      for (let attempt = 0; attempt < 40; attempt++) {
        const result = yield* Effect.gen(function* () {
          const health = yield* deployment.call(target, "/health");

          const identity = yield* Schema.decodeUnknownEffect(Identity)(
            (yield* deployment.call(target, `/identity?key=${key}`)).value,
          );

          if (
            target.role === "tardie" ||
            identity.generation !== target.generation ||
            identity.protocol !== "cold-v3-reference"
          ) {
            // The Actor can retain an older incarnation after the Thread has
            // reached the new version. Reset both before reference preparation.
            const reset = yield* deployment.call(target, `/restart?key=${key}`, {});

            yield* writeJson(path.join(directory, `${target.role}-stale-reset-${attempt}.json`), {
              identity,
              reset,
            });
          }
          yield* requireBench(
            identity.generation === target.generation && identity.protocol === "cold-v3-reference",
            "DO has not reached the guarded measurement version",
          );

          // Warm the reference function once outside the recorded reference,
          // then reset. Cold still includes a fresh benchmark Object constructor.
          const prime = yield* reference(target, key, "referencePrime");
          const coldReference = yield* reference(target, key, "referenceCold");
          const reset = yield* deployment.call(target, `/restart?key=${key}`, {});

          const value = yield* Schema.decodeUnknownEffect(
            Schema.Struct({
              objects: Schema.Array(
                Schema.Struct({
                  ...Identity.fields,
                  kind: Schema.Literals(["thread", "actor"]),
                  restarted: Schema.Literal(true),
                }),
              ),
            }),
          )(reset.value);

          const refs = [
            coldReference.value.thread,
            ...(coldReference.value.actor === undefined ? [] : [coldReference.value.actor]),
          ];

          yield* requireBench(
            value.objects.length === refs.length &&
              value.objects.every((object) =>
                refs.some(
                  (ref) =>
                    ref.objectId === object.objectId &&
                    ref.runtimeId === object.runtimeId &&
                    ref.version === object.version &&
                    ref.generation === target.generation,
                ),
              ),
            "Reset did not confirm the reference Objects on the measurement version",
          );
          yield* writeJson(path.join(directory, `${target.role}-cold-reference.json`), {
            prime,
            reference: coldReference,
            reset,
          });

          return { health, identity: coldReference.value.thread, reference: coldReference };
        }).pipe(Effect.result);

        // Only readiness, storage-free reference work and explicit resets can
        // repeat here. No workload input has been admitted at this boundary.
        probes.push(
          result._tag === "Success"
            ? { attempt, ...result.success }
            : { attempt, error: deployment.redact(String(result.failure)) },
        );
        yield* writeJson(path.join(directory, `${target.role}-cold-preparation.json`), probes);
        if (result._tag === "Success") return result.success;
        if (attempt === 39) return yield* result.failure;
        yield* Effect.sleep("3 seconds");
      }

      return yield* new BenchError({ message: "Cold preparation did not complete" });
    });

    if (action === "reference") {
      yield* requireBench(
        roles.length === 1,
        "Reference validation requires one role and a quarantined key",
      );
      const target = yield* targetFor(roles[0] ?? "pi");
      const from = (yield* Clock.currentTimeMillis) - 2000;
      const directory = path.join(output, "reference-validation", `${target.role}-${from}`);
      const prepared = yield* prepareCold(target, key, directory);
      const to = (yield* Clock.currentTimeMillis) + 2000;

      const query: Schema.Json = {
        queryId: `${target.name}-reference-${from}`,
        dry: true,
        view: "events",
        limit: 2000,
        timeframe: { from, to },
        parameters: {
          filterCombination: "and",
          filters: [
            { key: "$workers.scriptName", operation: "eq", type: "string", value: target.name },
            { key: "$workers.cpuTimeMs", operation: "exists", type: "number" },
          ],
        },
      };

      for (let poll = 0; poll < 12; poll++) {
        const events = yield* sanitizeReplayCpuTelemetry(yield* deployment.query(query));

        const matches = telemetryFor(
          events,
          prepared.reference.value.thread.objectId,
          "referenceCold",
        );

        yield* writeJson(path.join(directory, "telemetry.json"), { query, poll, events });
        const event = matches[0];

        if (
          matches.length === 1 &&
          typeof event?.cpuTimeMs === "number" &&
          typeof event.wallTimeMs === "number"
        ) {
          yield* requireBench(
            event.cpuTimeMs >= 20,
            "Reference CPU is too close to telemetry quantisation; increase the fixed work before measurement",
          );
          yield* Console.log(
            `Deployed reference: ${event.cpuTimeMs}ms CPU, ${event.wallTimeMs}ms DO wall; checksum verified`,
          );

          return;
        }
        if (poll < 11) yield* Effect.sleep("5 seconds");
      }

      return yield* new BenchError({
        message: "Reference telemetry not available; validation retained",
      });
    }

    const seed = Effect.fnUntraced(function* () {
      yield* requireBench(
        50 % batchSize === 0,
        "Seed batch size must divide 50, the original bench's restart interval",
      );
      yield* requireBench(
        !roles.includes("tardie") || !tardieIngressBatch || batchSize <= 10,
        "Tardie's ingress seed batch is bounded to ten original per-turn RPCs",
      );
      for (const item of roles) {
        const target = yield* targetFor(item);

        for (const size of sizes) {
          for (const sample of sampleNumbers) {
            if (selectedSample === "all" && !measurementSamples(size).includes(sample)) continue;
            yield* requireBench(
              FIXTURES.some((fixture) => fixture.size === size && fixture.sample === sample),
              `No provisioned fixture ${size}/${sample}`,
            );
            const key = `${size}_${sample}`;

            if (yield* fs.exists(seedPath(item, size, sample))) {
              yield* requireBench(skipVerified, `Seed already verified for ${item}/${key}`);
              const previous = yield* readJson(seedPath(item, size, sample), Seed);

              const current = yield* Schema.decodeUnknownEffect(Identity)(
                (yield* deployment.call(target, `/identity?key=${key}`)).value,
              );

              yield* requireBench(
                current.objectId === previous.identity.objectId &&
                  previous.fingerprint === FINGERPRINTS[size],
                `Cannot reuse mismatched verified fixture ${item}/${key}`,
              );
              yield* Console.log(`${item}/${key}: retained verified fixture`);
              continue;
            }
            let fingerprint = "";

            const batches: unknown[] =
              resumeAt === 0
                ? []
                : [
                    ...(yield* readJson(
                      path.join(output, "seed", item, `${size}-${sample}-batches.json`),
                      Schema.Array(Schema.Unknown),
                    )),
                  ];

            const restarts: unknown[] =
              resumeAt === 0 ||
              !(yield* fs.exists(
                path.join(output, "seed", item, `${size}-${sample}-restarts.json`),
              ))
                ? []
                : [
                    ...(yield* readJson(
                      path.join(output, "seed", item, `${size}-${sample}-restarts.json`),
                      Schema.Array(Schema.Unknown),
                    )),
                  ];

            const quiescence: unknown[] =
              resumeAt > 0 &&
              (yield* fs.exists(
                path.join(output, "seed", item, `${size}-${sample}-quiescence.json`),
              ))
                ? [
                    ...(yield* readJson(
                      path.join(output, "seed", item, `${size}-${sample}-quiescence.json`),
                      Schema.Array(Schema.Unknown),
                    )),
                  ]
                : [];

            if (resumeAt > 0) {
              yield* requireBench(
                roles.length === 1 &&
                  sizes.length === 1 &&
                  sampleNumbers.length === 1 &&
                  item !== "pi" &&
                  resumeAt <= size,
                "Resume is scoped to one fixture after a transport interruption",
              );

              const last = yield* Schema.decodeUnknownEffect(
                Schema.Struct({
                  to: Schema.Number,
                  value: Schema.Union([Schema.String, SerialSeedReceipt]),
                }),
              )(batches.at(-1));

              yield* requireBench(
                last.to <= resumeAt,
                "Resume cannot replay an acknowledged batch",
              );

              const recovered =
                last.to < resumeAt || item === "tardie"
                  ? yield* proveSeedPrefix(target, key, resumeAt)
                  : undefined;

              if (item === "tardie") {
                const diagnostic = yield* Schema.decodeUnknownEffect(WatchdogSnapshot)(
                  (yield* deployment.call(target, `/diagnostic?key=${key}`)).value,
                );

                yield* writeJson(
                  path.join(
                    output,
                    "seed",
                    item,
                    `${size}-${sample}-resume-quiescence-${resumeAt}.json`,
                  ),
                  diagnostic,
                );
                yield* requireBench(
                  diagnostic.alarm === null && Object.keys(diagnostic.watchdog).length === 0,
                  "Tardie continuation found pending native background work",
                );
              }

              yield* requireBench(
                item !== "tardie" || recovered !== undefined,
                "Tardie continuation requires persisted terminal proof",
              );
              yield* requireBench(
                recovered === undefined || resumeAt < size,
                "Recovered prefix must be followed by a new seed turn so its full transcript fingerprint is observed",
              );

              const old = yield* readJson(
                path.join(output, "deployments", item, "seed-verified.json"),
                Schema.Struct({ version: Schema.Struct({ id: Schema.String }) }),
              );

              const current = yield* readJson(
                path.join(output, "deployments", item, `${target.generation}-verified.json`),
                Schema.Struct({
                  workerName: Schema.String,
                  generation: Schema.String,
                  version: Schema.Struct({ id: Schema.String }),
                }),
              );

              const identity = yield* Schema.decodeUnknownEffect(Identity)(
                (yield* deployment.call(target, `/identity?key=${key}`)).value,
              );

              const stats = yield* Schema.decodeUnknownEffect(Stats)(
                (yield* deployment.call(target, `/stats?key=${key}`)).value,
              );

              const safe =
                identity.version !== old.version.id &&
                current.workerName === target.name &&
                current.generation === target.generation &&
                identity.version === current.version.id &&
                identity.generation === target.generation &&
                (recovered === undefined
                  ? identity.generation.startsWith("seed-resume-")
                  : recovered.completed === resumeAt &&
                    recovered.role === item &&
                    recovered.identity.objectId === identity.objectId &&
                    recovered.identity.actorObjectId === identity.actorObjectId &&
                    recovered.identity.generation === identity.generation &&
                    recovered.identity.version === identity.version) &&
                (item === "tardie"
                  ? stats.tables.experimental_messages === resumeAt && recovered !== undefined
                  : stats.tables.effect_agent_submissions === resumeAt &&
                    stats.tables.effect_agent_attempts === resumeAt &&
                    stats.tables.effect_agent_submission_ownership === 0 &&
                    stats.tables.effect_agent_work_entries === 0);

              yield* writeJson(
                path.join(output, "seed", item, `${size}-${sample}-resume-${resumeAt}.json`),
                {
                  oldVersion: old.version.id,
                  currentVersion: current.version.id,
                  identity,
                  stats,
                  resumeAt,
                  acknowledgedThrough: last.to,
                  safe,
                  ...(recovered === undefined ? {} : { recovered }),
                  basis:
                    recovered === undefined
                      ? "All acknowledged seed turns completed before their responses. Fresh-version counters must prove that no later admission or attempt exists; otherwise refuse to resume."
                      : "Read-only persisted input and successful terminal facts prove the exact resume prefix on the owned generation and version. Acknowledgements retain their original status; continue only with the next unseen input, then verify the full seed fingerprint.",
                },
              );
              yield* requireBench(
                safe,
                `${item}/${key}: ambiguous durable progress, refusing to replay or skip any turn`,
              );
              fingerprint = typeof last.value === "string" ? last.value : last.value.fingerprint;
            } else {
              yield* requireBench(resumeAt === 0, "Invalid negative resume offset");
              yield* deployment.call(target, `/setup?key=${key}`, {});
            }

            for (let from = resumeAt; from < size;) {
              const to = Math.min(from + batchSize, size, Math.ceil((from + 1) / 50) * 50);
              const inputs = history(from, to);

              const batch = yield* deployment
                .call(
                  target,
                  `/${item === "tardie" && tardieIngressBatch ? "seedSerial" : "seed"}?key=${key}`,
                  inputs.map((input) => ({ ...input })),
                )
                .pipe(
                  Effect.tapError((error) =>
                    writeJson(
                      path.join(output, "seed", item, `${size}-${sample}-failure-${from}.json`),
                      {
                        role: item,
                        size,
                        sample,
                        from,
                        to,
                        error: deployment.redact(String(error)),
                      },
                    ),
                  ),
                );

              if (item === "tardie" && tardieIngressBatch) {
                const value = yield* Schema.decodeUnknownEffect(SerialSeedReceipt)(batch.value);

                yield* requireBench(
                  value.steps.length === inputs.length &&
                    value.steps.every((step, index) => step.id === inputs[index]?.id),
                  "Serial seed receipts do not match the requested plan",
                );
                fingerprint = value.fingerprint;
              } else fingerprint = yield* Schema.decodeUnknownEffect(Schema.String)(batch.value);
              batches.push({ from, to, ...batch });
              yield* writeJson(
                path.join(output, "seed", item, `${size}-${sample}-batches.json`),
                batches,
              );

              if (item === "tardie") {
                let quiet = false;
                let blocked = false;
                const checks = [];

                for (let poll = 0; poll < 120; poll++) {
                  // Only this read-only post-batch GET may retry a transport failure.
                  const observation = yield* deployment.call(target, `/diagnostic?key=${key}`).pipe(
                    Effect.retry({
                      times: 2,
                      while: (error) =>
                        HttpClientError.isHttpClientError(error) &&
                        error.request.method === "GET" &&
                        error.reason._tag === "TransportError",
                    }),
                  );

                  const value = yield* Schema.decodeUnknownEffect(WatchdogSnapshot)(
                    observation.value,
                  );

                  checks.push({ ...observation, value });
                  blocked = Object.values(value.watchdog).some(
                    (entry) => entry.status === "blocked",
                  );
                  if (blocked) break;
                  if (value.alarm === null && Object.keys(value.watchdog).length === 0) {
                    quiet = true;
                    break;
                  }
                  yield* Effect.sleep("250 millis");
                }
                quiescence.push({ afterTurn: to, quiet, checks });
                yield* writeJson(
                  path.join(output, "seed", item, `${size}-${sample}-quiescence.json`),
                  quiescence,
                );
                yield* requireBench(
                  !blocked,
                  `${item}/${key}: watchdog blocked after seed turn ${to}`,
                );
                yield* requireBench(
                  quiet,
                  `${item}/${key}: native background work did not quiesce after seed turn ${to}`,
                );
              }

              if (to % 50 === 0 || to === size) {
                restarts.push({
                  afterTurn: to,
                  result: yield* deployment.call(target, `/restart?key=${key}`, {}),
                });
                yield* writeJson(
                  path.join(output, "seed", item, `${size}-${sample}-restarts.json`),
                  restarts,
                );
              }
              from = to;
            }
            yield* requireBench(
              fingerprint === FINGERPRINTS[size],
              `${item}/${key}: fingerprint mismatch ${fingerprint}`,
            );

            const stats = yield* Schema.decodeUnknownEffect(Stats)(
              (yield* deployment.call(target, `/stats?key=${key}`)).value,
            );

            const identity = yield* Schema.decodeUnknownEffect(Identity)(
              (yield* deployment.call(target, `/identity?key=${key}`)).value,
            );

            yield* writeJson(
              seedPath(item, size, sample),
              Seed.make({ role: item, size, sample, fingerprint, stats, identity }),
            );
            // seed.ts also disposes the separate Miniflare instance used to inspect storage.
            restarts.push({
              afterTurn: size,
              result: yield* deployment.call(target, `/restart?key=${key}`, {}),
            });
            yield* writeJson(
              path.join(output, "seed", item, `${size}-${sample}-restarts.json`),
              restarts,
            );
            yield* Console.log(
              `${item} ${size} sample ${sample + 1}: fingerprint ${fingerprint}, ${stats.bytes} storage bytes`,
            );
          }
        }
      }
    });

    const collectCohort = Effect.fnUntraced(function* (size: number, sample: number) {
      const directory = cohortDirectory(size, sample);
      const data = yield* readJson(path.join(directory, "samples.json"), Schema.Array(Sample));

      const window = yield* readJson(path.join(directory, "window.json"), CohortWindow);

      const combined = [];
      const cohortCpu = [];
      const collectionStatus = [];

      for (const item of roles) {
        const target = yield* targetFor(item);
        const expected = data.filter((row) => row.role === item);
        const roleWindow = window.roleWindows[item];

        if (roleWindow === undefined)
          return yield* new BenchError({ message: `Missing ${item} observation window` });
        const timeframe = { from: roleWindow.from, to: roleWindow.to };

        const query: Schema.Json = {
          queryId: `${target.name}-${size}-${sample}`,
          dry: true,
          view: "events",
          limit: 2000,
          timeframe,
          parameters: {
            filterCombination: "and",
            filters: [
              { key: "$workers.scriptName", operation: "eq", type: "string", value: target.name },
              { key: "$workers.cpuTimeMs", operation: "exists", type: "number" },
            ],
          },
        };

        const constructorQuery: Schema.Json = {
          queryId: `${target.name}-${size}-${sample}-constructors`,
          dry: true,
          view: "events",
          limit: 2000,
          timeframe,
          parameters: {
            filterCombination: "and",
            filters: [
              { key: "$workers.scriptName", operation: "eq", type: "string", value: target.name },
            ],
          },
        };

        let complete = false;
        let previousIds = "";

        for (let poll = 0; poll < 7; poll++) {
          const [events, logs] = yield* Effect.all(
            [
              deployment.query(query).pipe(Effect.flatMap(sanitizeReplayCpuTelemetry)),
              deployment.query(constructorQuery).pipe(Effect.flatMap(constructors)),
            ],
            { concurrency: 2 },
          );

          yield* writeJson(path.join(directory, `${item}-telemetry.json`), {
            query,
            polledAt: yield* Clock.currentTimeMillis,
            poll,
            events,
          });
          yield* writeJson(path.join(directory, `${item}-constructors.json`), {
            query: constructorQuery,
            polledAt: yield* Clock.currentTimeMillis,
            poll,
            ...logs,
          });
          complete = expected.every((row) => {
            const matches = telemetryFor(events, row.receipt.objectId, row.phase);
            const refs = refsOf(row);

            const calibrated = refs.every((ref) => {
              const matches = telemetryFor(events, ref.objectId, ref.phase);
              const event = matches[0];

              return (
                matches.length === 1 &&
                event?.scriptVersion?.id === ref.version &&
                typeof event.cpuTimeMs === "number" &&
                event.cpuTimeMs > 0 &&
                typeof event.wallTimeMs === "number" &&
                event.wallTimeMs > 0 &&
                event.outcome === "ok" &&
                !event.truncated
              );
            });

            return (
              calibrated &&
              matches.length === 1 &&
              (row.phase !== "cold" ||
                logs.rows.some(
                  (log) =>
                    log.construction.runtimeId === row.receipt.runtimeId &&
                    log.construction.kind === "thread" &&
                    log.construction.version === row.receipt.version &&
                    log.traceId === matches[0]?.traceId,
                ))
            );
          });

          const ids = events
            .map((event) => event.id)
            .sort()
            .join(":");

          const stable = previousIds === ids;

          previousIds = ids;
          if (!complete || !stable) {
            complete = false;
            if (poll < 6) {
              yield* Effect.sleep("10 seconds");
              continue;
            }
          }
          const missing = [];

          for (const row of expected) {
            const matches = telemetryFor(events, row.receipt.objectId, row.phase);
            const event = matches[0];

            if (event === undefined || matches.length !== 1) {
              missing.push({
                phase: row.phase,
                matches: matches.length,
                reason: "main RPC telemetry missing or duplicated",
              });
              continue;
            }

            const related = events.filter(
              (other) =>
                other.metadataType === "cf-worker-event" && other.traceId === event.traceId,
            );

            const objects = related.filter((other) => other.executionModel === "durableObject");
            const ingress = related.filter((other) => other.executionModel === "stateless");

            const coldConstruction =
              row.phase !== "cold" ||
              logs.rows.some(
                (log) =>
                  log.construction.runtimeId === row.receipt.runtimeId &&
                  log.construction.kind === "thread" &&
                  log.construction.version === row.receipt.version &&
                  log.traceId === event.traceId,
              );

            if (ingress.length !== 1) {
              missing.push({
                phase: row.phase,
                reason:
                  "supporting ingress telemetry missing or duplicated; unique versioned Object RPC retained",
                ingress: ingress.length,
              });
            }
            if (!coldConstruction) {
              missing.push({
                phase: row.phase,
                reason: "cold constructor evidence missing",
                ingress: ingress.length,
                coldConstruction,
              });
              continue;
            }

            yield* requireBench(
              event.scriptVersion?.id === row.receipt.version && event.scriptName === target.name,
              `${item}: invocation build identity mismatch`,
            );
            yield* requireBench(
              related.every((other) => !other.truncated),
              `${item}: truncated invocation`,
            );
            const cpuTimeMs = yield* Schema.decodeUnknownEffect(Schema.Number)(event.cpuTimeMs);

            const objectCpu = yield* Effect.forEach(objects, (other) =>
              Schema.decodeUnknownEffect(Schema.Number)(other.cpuTimeMs),
            );

            const referenceInvocations = refsOf(row).flatMap((ref) => {
              const matches = telemetryFor(events, ref.objectId, ref.phase);
              const event = matches[0];

              return matches.length === 1 &&
                event?.scriptVersion?.id === ref.version &&
                event.outcome === "ok" &&
                !event.truncated
                ? [event]
                : [];
            });

            const mainReference = referenceInvocations.find(
              (ref) => ref.durableObjectId === row.receipt.objectId,
            );

            const refCpu = mainReference?.cpuTimeMs;
            const referenceCpuTimeMs = typeof refCpu === "number" && refCpu > 0 ? refCpu : null;
            const refWall = mainReference?.wallTimeMs;
            const referenceDoWallMs = typeof refWall === "number" && refWall > 0 ? refWall : null;
            const referenceClientWallMs = row.reference?.clientWallMs ?? null;

            if (
              row.reference !== undefined &&
              (referenceCpuTimeMs === null ||
                referenceDoWallMs === null ||
                referenceInvocations.length !== refsOf(row).length)
            ) {
              missing.push({
                phase: row.phase,
                reason: "reference telemetry missing, zero, or invalid",
              });
            }
            let normalizedDoTotalCpuRatio: number | null = 0;

            for (const object of objects) {
              const ref = referenceInvocations.find(
                (ref) => ref.durableObjectId === object.durableObjectId,
              );

              if (
                typeof object.cpuTimeMs !== "number" ||
                typeof ref?.cpuTimeMs !== "number" ||
                ref.cpuTimeMs <= 0
              ) {
                normalizedDoTotalCpuRatio = null;
                break;
              }
              normalizedDoTotalCpuRatio += object.cpuTimeMs / ref.cpuTimeMs;
            }

            const ownedIds = [
              row.receipt.objectId,
              ...(row.receipt.actorObjectId === undefined ? [] : [row.receipt.actorObjectId]),
            ];

            const doWall = typeof event.wallTimeMs === "number" ? event.wallTimeMs : null;
            const start = Number(event.timestamp);
            const end = start + (doWall ?? 0);

            const observedAlarmOverlaps = events.flatMap((alarm) => {
              if (
                alarm.metadataType !== "cf-worker-event" ||
                alarm.executionModel !== "durableObject" ||
                alarm.eventType !== "alarm" ||
                typeof alarm.durableObjectId !== "string" ||
                !ownedIds.includes(alarm.durableObjectId) ||
                typeof alarm.wallTimeMs !== "number" ||
                !Number.isFinite(start) ||
                doWall === null
              )
                return [];
              const alarmStart = Number(alarm.timestamp);

              const overlapMs =
                Math.min(end, alarmStart + alarm.wallTimeMs) - Math.max(start, alarmStart);

              return Number.isFinite(overlapMs) && overlapMs > 0 ? [{ ...alarm, overlapMs }] : [];
            });

            combined.push({
              ...row,
              invocationId: event.id,
              traceId: event.traceId,
              cpuTimeMs,
              doTotalCpuTimeMs: objectCpu.reduce((a, b) => a + b, 0),
              doInvocationWallMs: event.wallTimeMs,
              doWallMinusCpuMs: doWall === null ? null : doWall - cpuTimeMs,
              clientWallMinusDoWallMs: doWall === null ? null : row.clientWallMs - doWall,
              referenceInvocations,
              referenceCpuTimeMs,
              referenceDoWallMs,
              referenceClientWallMs,
              normalizedCpuRatio:
                referenceCpuTimeMs === null ? null : cpuTimeMs / referenceCpuTimeMs,
              normalizedDoWallRatio:
                referenceDoWallMs === null || doWall === null ? null : doWall / referenceDoWallMs,
              normalizedClientWallRatio:
                referenceClientWallMs === null || referenceClientWallMs <= 0
                  ? null
                  : row.clientWallMs / referenceClientWallMs,
              normalizedDoTotalCpuRatio,
              observedAlarmOverlap: observedAlarmOverlaps.length > 0,
              observedAlarmOverlaps,
              outcome: event.outcome,
              supportingDoInvocations: objects.filter((other) => other.id !== event.id),
              ingress,
              allOutcomes: related.map((other) => other.outcome),
            });
          }
          const cold = expected.find((row) => row.phase === "cold");

          if (cold !== undefined) {
            const objectIds = [
              cold.receipt.objectId,
              ...(cold.receipt.actorObjectId === undefined ? [] : [cold.receipt.actorObjectId]),
            ];

            // Audits read storage and the last model-visible transcript immediately
            // after turn 10. They must not enter lifecycle workload CPU totals.
            const auditTraces = new Set(
              events
                .filter((event) =>
                  event.rpcMethods.some(
                    (method) =>
                      [
                        "stats",
                        "bytes",
                        "fingerprint",
                        "identity",
                        "restart",
                        "diagnostic",
                      ].includes(method) || method.startsWith("reference"),
                  ),
                )
                .map((event) => event.traceId),
            );

            const coldEvent = telemetryFor(events, cold.receipt.objectId, "cold")[0];

            const ownedInvocations = events.filter(
              (event) =>
                event.metadataType === "cf-worker-event" &&
                event.executionModel === "durableObject" &&
                event.durableObjectId !== null &&
                event.durableObjectId !== undefined &&
                objectIds.includes(event.durableObjectId) &&
                !auditTraces.has(event.traceId) &&
                event.scriptVersion?.id === cold.receipt.version,
            );

            const invocations = ownedInvocations.filter(
              (event) =>
                Number(event.timestamp) >= Number(coldEvent?.timestamp ?? roleWindow.workloadFrom),
            );

            const cpus = yield* Effect.forEach(invocations, (event) =>
              Schema.decodeUnknownEffect(Schema.Number)(event.cpuTimeMs),
            );

            const measuredTraces = new Set(
              combined.filter((row) => row.role === item).map((row) => row.traceId),
            );

            const extra = invocations.filter((event) => !measuredTraces.has(event.traceId));

            const extraCpus = yield* Effect.forEach(extra, (event) =>
              Schema.decodeUnknownEffect(Schema.Number)(event.cpuTimeMs),
            );

            const objectReferences = objectIds.map((objectId) => {
              const referenceEvents = expected.flatMap((row) =>
                refsOf(row)
                  .filter((ref) => ref.objectId === objectId)
                  .flatMap((ref) => {
                    const matches = telemetryFor(events, ref.objectId, ref.phase);

                    return matches.length === 1 ? matches : [];
                  }),
              );

              const values = referenceEvents.flatMap((event) =>
                typeof event.cpuTimeMs === "number" &&
                event.cpuTimeMs > 0 &&
                event.outcome === "ok" &&
                !event.truncated
                  ? [event.cpuTimeMs]
                  : [],
              );

              const referenceCpuMedianMs = median(values);

              const observedCpuTimeMs = invocations
                .filter((event) => event.durableObjectId === objectId)
                .reduce((sum, event) => sum + (event.cpuTimeMs ?? 0), 0);

              return {
                objectId,
                referenceEvents,
                count: values.length,
                referenceCpuMedianMs,
                observedCpuTimeMs,
                normalizedObservedCpuRatio:
                  referenceCpuMedianMs === null ? null : observedCpuTimeMs / referenceCpuMedianMs,
              };
            });

            const normalizedTenTurnMeanCpuRatio =
              complete &&
              expected.length === 10 &&
              objectReferences.every(
                (ref) => ref.count === 10 && ref.normalizedObservedCpuRatio !== null,
              )
                ? objectReferences.reduce(
                    (sum, ref) => sum + (ref.normalizedObservedCpuRatio ?? 0),
                    0,
                  ) / 10
                : null;

            cohortCpu.push({
              role: item,
              size,
              sample,
              objectIds,
              version: cold.receipt.version,
              window: roleWindow,
              telemetryComplete: complete,
              missing,
              objectReferences,
              normalizedTenTurnMeanCpuRatio,
              sumObservedDoCpuTimeMs: cpus.reduce((a, b) => a + b, 0),
              extraTraceCpuTimeMs: extraCpus.reduce((a, b) => a + b, 0),
              invocations,
              extraTraceInvocations: extra,
              coldConstruction: logs.rows.find(
                (log) => log.construction.runtimeId === cold.receipt.runtimeId,
              ),
              preColdInvocations: ownedInvocations.filter(
                (event) =>
                  Number(event.timestamp) < Number(coldEvent?.timestamp) &&
                  event.traceId !== coldEvent?.traceId,
              ),
            });
          }
          collectionStatus.push({
            role: item,
            complete,
            expected: expected.length,
            matched: combined.filter((row) => row.role === item).length,
            missing,
            poll,
          });
          break;
        }
        // Persist each role independently. Missing telemetry must remain visible,
        // without erasing successful measurements or retrying a consumed fixture.
        yield* writeJson(path.join(directory, "measured.json"), combined);
        yield* writeJson(path.join(directory, "cohort-cpu.json"), cohortCpu);
        yield* writeJson(path.join(directory, "collection-status.json"), collectionStatus);
        if (!complete)
          yield* Console.log(
            `INCOMPLETE telemetry ${item}/${size}/${sample}; raw receipts retained`,
          );
      }
      yield* Console.log(
        `Telemetry ${size} sample ${sample + 1}: ${combined.length} measurements matched`,
      );

      return collectionStatus.every((status) => status.complete);
    });

    const measure = Effect.fnUntraced(function* () {
      const failedCohorts = [];

      for (const sample of sampleNumbers) {
        for (const size of sizes) {
          if (selectedSample === "all" && !measurementSamples(size).includes(sample)) continue;
          yield* requireBench(
            measurementSamples(size).includes(sample),
            `Fixture ${size}/${sample} is quarantined or outside the measurement plan`,
          );
          const directory = cohortDirectory(size, sample);

          const existing = yield* fs.exists(path.join(directory, "samples.json"));

          yield* requireBench(
            !existing || resumeMeasure,
            `Refusing to repeat measured cohort ${size}/${sample}; explicit continuation required`,
          );

          const ordinal = measurementSamples(size).indexOf(sample);

          const order: readonly Role[] =
            ordinal % 2 === 0 ? ROLES : ["control", "head", "base", "tardie", "pi", "pinned"];

          const rotated = order.filter((item) => roles.includes(item));

          const seeds = new Map<Role, typeof Seed.Type>();

          for (const item of rotated)
            seeds.set(item, yield* readJson(seedPath(item, size, sample), Seed));
          const started = yield* Clock.currentTimeMillis;

          if (!existing)
            yield* writeJson(path.join(directory, "order.json"), {
              roles: rotated,
              size,
              sample,
              started,
            });

          const samples: Array<typeof Sample.Type> = existing
            ? [...(yield* readJson(path.join(directory, "samples.json"), Schema.Array(Sample)))]
            : [];

          const attempts: unknown[] =
            existing && (yield* fs.exists(path.join(directory, "attempts.json")))
              ? [
                  ...(yield* readJson(
                    path.join(directory, "attempts.json"),
                    Schema.Array(Schema.Unknown),
                  )),
                ]
              : [];

          const failures: Array<{
            role: Role;
            size: number;
            sample: number;
            phase: string;
            error: string;
          }> =
            existing && (yield* fs.exists(path.join(directory, "failures.json")))
              ? [
                  ...(yield* readJson(
                    path.join(directory, "failures.json"),
                    Schema.Array(
                      Schema.Struct({
                        role: Schema.Literals(ROLES),
                        size: Schema.Number,
                        sample: Schema.Number,
                        phase: Schema.String,
                        error: Schema.String,
                      }),
                    ),
                  )),
                ]
              : [];

          const cold = new Map<Role, typeof Identity.Type>();

          const previousWindow = existing
            ? yield* readJson(path.join(directory, "window.json"), CohortWindow)
            : undefined;

          const from = previousWindow?.from ?? started - 2000;

          const roleWindows: Record<string, typeof RoleWindow.Type> = {
            ...previousWindow?.roleWindows,
          };

          const audits: Array<{ role: Role; stats: typeof Stats.Type; fingerprint: string }> =
            existing && (yield* fs.exists(path.join(directory, "audits.json")))
              ? [
                  ...(yield* readJson(
                    path.join(directory, "audits.json"),
                    Schema.Array(
                      Schema.Struct({
                        role: Schema.Literals(ROLES),
                        stats: Stats,
                        fingerprint: Schema.String,
                      }),
                    ),
                  )),
                ]
              : [];

          if (existing)
            yield* writeJson(path.join(directory, `continuation-${started}.json`), {
              started,
              roles: rotated,
              completedRoles: audits.map((audit) => audit.role),
              retainedReceipts: samples.length,
            });

          yield* writeJson(path.join(directory, "window.json"), {
            from,
            to: from + 3_600_000,
            roleWindows,
            alarmTailMs: 35_000,
          });
          yield* writeJson(path.join(directory, "samples.json"), samples);
          // Each sample is the original consecutive ten-turn workload. Alternate
          // baseline/head/control samples, rather than inserting other targets
          // (or a stalled target's request) between an Object's warm turns.
          for (const item of rotated) {
            const retained = samples.filter((row) => row.role === item);

            const completed =
              retained.length === PHASES.length &&
              PHASES.every((phase) => retained.filter((row) => row.phase === phase).length === 1) &&
              audits.some((audit) => audit.role === item) &&
              !failures.some((failure) => failure.role === item);

            if (completed) {
              yield* Console.log(
                `${item}/${size}/${sample}: retained complete sample; no workload replay`,
              );
              continue;
            }
            yield* requireBench(
              retained.length === 0 &&
                !attempts.some(
                  (attempt) =>
                    typeof attempt === "object" &&
                    attempt !== null &&
                    "role" in attempt &&
                    attempt.role === item,
                ) &&
                !failures.some((failure) => failure.role === item),
              `${item}/${size}/${sample}: partial or ambiguous workload cannot be resumed or replayed`,
            );
            const deploymentFrom = (yield* Clock.currentTimeMillis) - 2000;

            yield* deployment.deploy(item, `measure-${size}-${sample}`);
            const target = yield* targetFor(item);
            const prepared = yield* prepareCold(target, `${size}_${sample}`, directory);
            const workloadFrom = yield* Clock.currentTimeMillis;

            for (const [index, phase] of PHASES.entries()) {
              if (failures.some((failure) => failure.role === item)) continue;

              const attempt = yield* Effect.gen(function* () {
                const observedReference =
                  phase === "cold"
                    ? prepared.reference
                    : yield* reference(target, `${size}_${sample}`, `referenceWarm${index}`);

                attempts.push({ role: item, size, sample, phase, reference: observedReference });
                yield* writeJson(path.join(directory, "attempts.json"), attempts);

                const result = yield* deployment.call(target, `/${phase}?key=${size}_${sample}`, {
                  ...turn(`m${index}`, 8),
                });

                // Preserve the raw receipt even when a subsequent identity check rejects it.
                attempts.push({ role: item, size, sample, phase, ...result });
                yield* writeJson(path.join(directory, "attempts.json"), attempts);

                const receipt = yield* Schema.decodeUnknownEffect(Receipt)(result.value);
                const previous = seeds.get(item);

                yield* requireBench(
                  previous !== undefined &&
                    receipt.objectId === previous.identity.objectId &&
                    receipt.phase === phase &&
                    receipt.generation === target.generation &&
                    receipt.protocol === "cold-v3-reference" &&
                    observedReference.value.thread.objectId === receipt.objectId &&
                    observedReference.value.thread.version === receipt.version,
                  `${item}: wrong Object, phase, or deployment generation`,
                );
                if (phase === "cold") {
                  yield* requireBench(
                    receipt.runtimeId !== prepared.identity.runtimeId &&
                      receipt.runtimeId !== previous?.identity.runtimeId &&
                      receipt.version === prepared.identity.version &&
                      receipt.version !== previous?.identity.version,
                    `${item}: fresh Object on the confirmed version not observed`,
                  );
                  cold.set(item, receipt);
                } else {
                  const initial = cold.get(item);

                  yield* requireBench(
                    receipt.moduleId === initial?.moduleId &&
                      receipt.runtimeId === initial?.runtimeId &&
                      receipt.version === initial?.version &&
                      observedReference.value.thread.runtimeId === receipt.runtimeId,
                    `${item}: warm incarnation changed`,
                  );
                }
                samples.push(
                  Sample.make({
                    role: item,
                    size,
                    sample,
                    phase,
                    started: result.started,
                    ended: result.ended,
                    clientWallMs: result.clientWallMs,
                    ...(result.cfRay === undefined ? {} : { cfRay: result.cfRay }),
                    receipt,
                    reference: observedReference,
                  }),
                );
                yield* writeJson(path.join(directory, "samples.json"), samples);
              }).pipe(Effect.result);

              if (attempt._tag === "Failure") {
                const failure = {
                  role: item,
                  size,
                  sample,
                  phase,
                  error: deployment.redact(String(attempt.failure)),
                };

                failures.push(failure);
                yield* writeJson(path.join(directory, "failures.json"), failures);
                yield* Console.log(`FAILED ${item}/${size}/${sample}/${phase}: ${failure.error}`);
              }
            }
            const workloadTo = yield* Clock.currentTimeMillis;

            roleWindows[item] = {
              from: deploymentFrom,
              to: workloadTo + 37_000,
              workloadFrom,
              workloadTo,
            };
            yield* writeJson(path.join(directory, "window.json"), {
              from,
              to: Math.max(...Object.values(roleWindows).map((item) => item.to)),
              roleWindows,
              alarmTailMs: 35_000,
            });
            if (failures.some((failure) => failure.role === item)) continue;

            const stats = yield* Schema.decodeUnknownEffect(Stats)(
              (yield* deployment.call(target, `/stats?key=${size}_${sample}`)).value,
            );

            const fingerprint = yield* Schema.decodeUnknownEffect(Schema.String)(
              (yield* deployment.call(target, `/fingerprint?key=${size}_${sample}`)).value,
            );

            audits.push({ role: item, stats, fingerprint });
            yield* writeJson(path.join(directory, "audits.json"), audits);
            yield* Console.log(
              `${item}/${size}/${sample}: ten turns complete, transcript ${fingerprint}`,
            );
          }

          // Every role gets the same 35s alarm tail (+2s timestamp tolerance),
          // even though later roles are deployed and measured sequentially.
          const waitMs =
            Math.max(...Object.values(roleWindows).map((item) => item.to)) -
            (yield* Clock.currentTimeMillis);

          if (waitMs > 0) yield* Effect.sleep(waitMs);
          yield* requireBench(
            new Set(audits.map((audit) => JSON.stringify(audit.fingerprint))).size === 1,
            `Final transcripts differ at ${size}/${sample}`,
          );
          const telemetryComplete = deferTelemetry ? null : yield* collectCohort(size, sample);

          if (failures.length > 0 || telemetryComplete === false)
            failedCohorts.push({ size, sample, failures, telemetryComplete });
        }
      }
      yield* writeJson(
        path.join(output, `measurement-status-${selectedSize}-${selectedSample}.json`),
        {
          complete: failedCohorts.length === 0 && !deferTelemetry,
          telemetryCollectionDeferred: deferTelemetry,
          failedCohorts,
        },
      );
      yield* requireBench(
        failedCohorts.length === 0,
        "Measured failures retained; see measurement-status and cohort failures",
      );
    });

    if (action === "deploy") return yield* deployInitial();
    if (action === "inspect") {
      for (const item of roles) yield* deployment.inspect(yield* targetFor(item));

      return;
    }

    const outcomes = Effect.fnUntraced(function* () {
      const to = (yield* Clock.currentTimeMillis) + 60_000;
      const rows = [];

      for (const item of roles) {
        const target = yield* targetFor(item);

        if (!target.cleanupRequired) continue;
        for (const outcome of ["exceededCpu", "exceededMemory"]) {
          const query: Schema.Json = {
            queryId: `${target.name}-${outcome}`,
            dry: true,
            view: "events",
            limit: 2000,
            timeframe: { from: to - 86_400_000, to },
            parameters: {
              filterCombination: "and",
              filters: [
                { key: "$workers.scriptName", operation: "eq", type: "string", value: target.name },
                { key: "$workers.outcome", operation: "eq", type: "string", value: outcome },
              ],
            },
          };

          const events = yield* sanitizeReplayCpuTelemetry(yield* deployment.query(query));

          rows.push({ role: item, outcome, query, events });
        }
      }
      yield* writeJson(path.join(output, "resource-outcomes.json"), rows);
      yield* Console.log(
        `Resource-limit outcome invocations: ${rows.reduce((sum, row) => sum + row.events.length, 0)}`,
      );
    });

    if (action === "outcomes") return yield* outcomes();
    if (action === "seed") return yield* seed();
    if (action === "measure") return yield* measure();
    if (action === "collect") {
      for (const sample of sampleNumbers)
        for (const size of sizes) {
          if (yield* fs.exists(path.join(cohortDirectory(size, sample), "samples.json")))
            yield* collectCohort(size, sample);
        }

      return;
    }
    yield* Effect.gen(function* () {
      yield* deployInitial();
      yield* seed();
      yield* measure();
      yield* outcomes();
    }).pipe(Effect.ensuring(deployment.cleanup().pipe(Effect.orDie)));
  }),
);

if (import.meta.main)
  command.pipe(
    Command.run({ version: "1.0.0" }),
    Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer, FetchHttpClient.layer)),
    NodeRuntime.runMain,
  );
