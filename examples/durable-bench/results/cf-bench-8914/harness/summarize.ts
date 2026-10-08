import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Clock, Console, Effect, FileSystem, Path, Schema } from "effect";
import { Command, Flag } from "effect/cli";

import type { ReferenceObservation } from "./contracts.ts";
import { PHASES, ROLES, Role, RoleWindow, Sample, Seed, Stats } from "./contracts.ts";

const sizes = [50, 250, 1000] as const;
const samplesFor = (size: number) => (size === 50 ? [1, 2, 3] : [0, 1, 2]);
const samplesBySize = Object.fromEntries(sizes.map((size) => [size, samplesFor(size)]));

const maybeNumber = Schema.optionalKey(Schema.NullOr(Schema.Finite));
const maybeString = Schema.optionalKey(Schema.NullOr(Schema.String));

const Coordinates = {
  role: Role,
  size: Schema.Literals(sizes),
  sample: Schema.Literals([0, 1, 2, 3]),
};

const SavedSample = Schema.Struct({
  ...Sample.fields,
  ...Coordinates,
  phase: Schema.Literals(PHASES),
});

const Measured = Schema.Struct({
  ...SavedSample.fields,
  cpuTimeMs: maybeNumber,
  doTotalCpuTimeMs: maybeNumber,
  doInvocationWallMs: maybeNumber,
  doWallMinusCpuMs: maybeNumber,
  clientWallMinusDoWallMs: maybeNumber,
  referenceCpuTimeMs: maybeNumber,
  referenceDoWallMs: maybeNumber,
  referenceClientWallMs: maybeNumber,
  normalizedClientWallRatio: maybeNumber,
  normalizedDoWallRatio: maybeNumber,
  normalizedCpuRatio: maybeNumber,
  normalizedDoTotalCpuRatio: maybeNumber,
  observedAlarmOverlap: Schema.optionalKey(Schema.NullOr(Schema.Boolean)),
  observedAlarmOverlaps: Schema.optionalKey(
    Schema.NullOr(Schema.Array(Schema.Record(Schema.String, Schema.Json))),
  ),
  referenceEvidence: Schema.optionalKey(Schema.Json),
  referenceInvocations: Schema.optionalKey(Schema.Array(Schema.Record(Schema.String, Schema.Json))),
  outcome: maybeString,
  allOutcomes: Schema.optionalKey(Schema.NullOr(Schema.Array(Schema.NullOr(Schema.String)))),
});

const Failure = Schema.Struct({
  ...Coordinates,
  phase: Schema.Literals(PHASES),
  error: Schema.String,
});

const Audit = Schema.Struct({ role: Role, stats: Stats, fingerprint: Schema.String });

const Invocation = Schema.Struct({
  id: maybeString,
  eventType: maybeString,
  cpuTimeMs: maybeNumber,
  outcome: maybeString,
});

const ReferenceTimingEvent = Schema.Struct({
  durableObjectId: Schema.String,
  cpuTimeMs: maybeNumber,
  wallTimeMs: maybeNumber,
  outcome: maybeString,
});

const MissingTelemetry = Schema.Struct({
  phase: Schema.String,
  reason: Schema.String,
  matches: maybeNumber,
  ingress: maybeNumber,
  coldConstruction: Schema.optionalKey(Schema.Boolean),
});

const CollectionStatus = Schema.Struct({
  role: Role,
  complete: Schema.Boolean,
  expected: Schema.Finite,
  matched: Schema.Finite,
  missing: Schema.Array(MissingTelemetry),
  poll: Schema.Finite,
});

const CohortCpu = Schema.Struct({
  ...Coordinates,
  objectIds: Schema.optionalKey(Schema.Array(Schema.String)),
  window: Schema.optionalKey(RoleWindow),
  telemetryComplete: Schema.optionalKey(Schema.Boolean),
  missing: Schema.optionalKey(Schema.Array(MissingTelemetry)),
  extraTraceCpuTimeMs: maybeNumber,
  sumObservedDoCpuTimeMs: maybeNumber,
  normalizedTenTurnMeanCpuRatio: maybeNumber,
  referenceEvidence: Schema.optionalKey(Schema.Json),
  objectReferences: Schema.optionalKey(Schema.Array(Schema.Record(Schema.String, Schema.Json))),
  invocations: Schema.optionalKey(Schema.NullOr(Schema.Array(Invocation))),
  extraTraceInvocations: Schema.optionalKey(Schema.NullOr(Schema.Array(Invocation))),
});

const FileIdentity = Schema.Struct({
  sha256: Schema.String,
  rawBytes: Schema.Finite,
  gzipBytes: Schema.Finite,
});

const Build = Schema.Struct({
  label: Role,
  target: Schema.String,
  repositoryCommit: Schema.String,
  effect: Schema.Struct({ version: Schema.String, runtimeSha256: Schema.String }),
  targetPackage: Schema.optionalKey(
    Schema.NullOr(Schema.Struct({ name: Schema.String, version: Schema.String })),
  ),
  bench: FileIdentity,
  wrapper: FileIdentity,
});

const Deployment = Schema.Struct({
  generation: Schema.String,
  version: Schema.Struct({ id: Schema.String, startup_time_ms: maybeNumber }),
  modules: Schema.Array(
    Schema.Struct({ name: Schema.String, sha256: Schema.String, matchesLocal: Schema.Boolean }),
  ),
});

const metrics = [
  "clientWallMs",
  "doInvocationWallMs",
  "cpuTimeMs",
  "doWallMinusCpuMs",
  "clientWallMinusDoWallMs",
  "normalizedClientWallRatio",
  "normalizedDoWallRatio",
  "receipt.insideWallMs",
  "doTotalCpuTimeMs",
  "normalizedCpuRatio",
  "normalizedDoTotalCpuRatio",
  "referenceClientWallMs",
  "referenceDoWallMs",
  "referenceCpuTimeMs",
] as const;

type Metric = (typeof metrics)[number];
type Value = number | null;
type Values = Record<Metric, Value>;

const minimumWarmTurns = (metric: Metric) =>
  metric === "clientWallMs" ||
  metric === "normalizedClientWallRatio" ||
  metric === "referenceClientWallMs" ||
  metric === "receipt.insideWallMs"
    ? 9
    : 7;

const metricUnits: Record<Metric, string> = {
  cpuTimeMs: "ms",
  doTotalCpuTimeMs: "ms",
  "receipt.insideWallMs": "ms",
  clientWallMs: "ms",
  doInvocationWallMs: "ms",
  doWallMinusCpuMs: "ms",
  clientWallMinusDoWallMs: "ms",
  referenceCpuTimeMs: "ms reference CPU",
  referenceDoWallMs: "ms reference DO wall",
  referenceClientWallMs: "ms reference client wall",
  normalizedClientWallRatio: "ms client wall / ms reference client wall",
  normalizedDoWallRatio: "ms DO wall / ms reference DO wall",
  normalizedCpuRatio: "ms CPU / ms reference CPU",
  normalizedDoTotalCpuRatio: "ms CPU / ms reference CPU",
};

const cohortMetrics = ["tenTurnMeanCpuTimeMs", "normalizedTenTurnMeanCpuRatio"] as const;
const referenceMetrics = ["clientWallMs", "doWallMs", "cpuTimeMs"] as const;

const pairs = [
  ["head", "base"],
  ["control", "base"],
  ["head", "control"],
  ["head", "pi"],
  ["head", "tardie"],
  ["pinned", "pi"],
  ["pinned", "tardie"],
] as const;

const finite = (value: number | null | undefined): Value =>
  typeof value === "number" && Number.isFinite(value) ? value : null;

/** Linear interpolation at (n - 1) * p; the caller supplies ascending values. */
const quantile = (sorted: readonly number[], p: number): Value => {
  if (sorted.length === 0) return null;
  const position = (sorted.length - 1) * p;
  const lower = sorted[Math.floor(position)];
  const upper = sorted[Math.ceil(position)];
  const weight = position - Math.floor(position);

  return lower === undefined || upper === undefined ? null : (1 - weight) * lower + weight * upper;
};

const distribution = (values: readonly Value[]) => {
  const sorted = values.filter((value): value is number => value !== null).sort((a, b) => a - b);

  return {
    status: sorted.length === values.length ? "complete" : "incomplete",
    availability:
      sorted.length === 0
        ? "unavailable"
        : sorted.length === values.length
          ? "available"
          : "partial",
    n: sorted.length,
    expectedN: values.length,
    missingN: values.length - sorted.length,
    min: quantile(sorted, 0),
    p25: quantile(sorted, 0.25),
    p50: quantile(sorted, 0.5),
    p75: quantile(sorted, 0.75),
    p90: quantile(sorted, 0.9),
    p95: quantile(sorted, 0.95),
    p99: quantile(sorted, 0.99),
    max: quantile(sorted, 1),
  };
};

const total = (values: readonly Value[]) => {
  const available = values.filter((value): value is number => value !== null);
  const observedTotal = available.reduce((sum, value) => sum + value, 0);

  return {
    status: available.length === values.length ? "complete" : "incomplete",
    availability:
      available.length === values.length
        ? "available"
        : available.length === 0
          ? "unavailable"
          : "partial",
    n: available.length,
    expectedN: values.length,
    total: available.length === values.length ? observedTotal : null,
    observedTotal,
  };
};

const ratio = (numerator: Value, denominator: Value) => {
  const reason =
    numerator === null || denominator === null
      ? "missing_or_incomplete_operand"
      : denominator === 0
        ? "zero_denominator"
        : Number.isFinite(numerator / denominator)
          ? null
          : "nonfinite_ratio";

  return {
    numerator,
    denominator,
    ratio:
      reason === null && numerator !== null && denominator !== null
        ? numerator / denominator
        : null,
    reason,
  };
};

const countBy = (values: readonly string[]) => {
  const counts: Record<string, number> = {};

  for (const value of values) counts[value] = (counts[value] ?? 0) + 1;

  return counts;
};

const invocationTotals = (events: ReadonlyArray<typeof Invocation.Type>) => {
  const alarms = events.filter((event) => event.eventType === "alarm");

  const unknownEventTypes = events.filter(
    (event) => event.eventType === null || event.eventType === undefined,
  ).length;

  const alarmCpu = total(alarms.map((event) => finite(event.cpuTimeMs)));

  return {
    count: events.length,
    cpuTimeMs: total(events.map((event) => finite(event.cpuTimeMs))),
    outcomes: countBy(events.map((event) => event.outcome ?? "unknown")),
    unknownEventTypes,
    alarms: {
      count: alarms.length,
      countStatus: unknownEventTypes === 0 ? "complete" : "incomplete",
      cpuTimeMs: {
        ...alarmCpu,
        status: unknownEventTypes === 0 ? alarmCpu.status : "incomplete",
        availability:
          unknownEventTypes === 0
            ? alarmCpu.availability
            : alarmCpu.n === 0
              ? "unavailable"
              : "partial",
        total: unknownEventTypes === 0 ? alarmCpu.total : null,
      },
      status:
        unknownEventTypes === 0 && alarms.every((event) => finite(event.cpuTimeMs) !== null)
          ? "complete"
          : "incomplete",
    },
  };
};

/** Preserve partial event CPU sums as observed totals, with separate Object/event coverage. */
const eventCpuTotal = (objects: ReadonlyArray<ReturnType<typeof total> | null>) => {
  const available = objects.filter((value) => value !== null);
  const observedTotal = available.reduce((sum, value) => sum + value.observedTotal, 0);
  const n = available.reduce((sum, value) => sum + value.n, 0);
  const expectedN = available.reduce((sum, value) => sum + value.expectedN, 0);

  const complete =
    available.length === objects.length && available.every((value) => value.status === "complete");

  return {
    status: complete ? "complete" : "incomplete",
    availability: complete
      ? "available"
      : available.some((value) => value.availability !== "unavailable")
        ? "partial"
        : "unavailable",
    n,
    expectedN,
    nObjects: available.length,
    expectedObjects: objects.length,
    total: complete ? observedTotal : null,
    observedTotal,
  };
};

type Issue = { file: string; kind: string; detail: string; row?: number };
type Observation = {
  role: Role;
  size: number;
  sample: number;
  phase: string;
  status: string;
  reasons: string[];
  source: "measured" | "samples" | "missing";
  rpcCompleted: boolean;
  workloadStatus: "completed" | "failed" | "unavailable";
  primaryClientSource: "samples.json" | null;
  httpColo: string | null;
  protocol: string | null;
  reference: typeof ReferenceObservation.Type | null;
  referenceEvidence: Schema.Json | null;
  referenceTimingEvents: readonly (typeof ReferenceTimingEvent.Type)[];
  observedAlarmOverlap: boolean | null;
  observedAlarmOverlaps: readonly { readonly [key: string]: Schema.Json }[] | null;
  outcome: string | null;
  allOutcomes: readonly (string | null)[];
  reportedValues: Values;
  values: Values;
};

const alarmOverlapSummary = (rows: readonly Observation[]) => ({
  expectedTurns: rows.length,
  observedOverlapTurns: rows.filter((row) => row.observedAlarmOverlap === true).length,
  noObservedOverlapTurns: rows.filter((row) => row.observedAlarmOverlap === false).length,
  unavailableTurns: rows.filter((row) => row.observedAlarmOverlap === null).length,
  observedEventEntries: total(rows.map((row) => row.observedAlarmOverlaps?.length ?? null)),
});

const httpColoSummary = (rows: readonly Observation[]) => {
  const accepted = rows.filter((row) => row.primaryClientSource === "samples.json");
  const colos = accepted.flatMap((row) => (row.httpColo === null ? [] : [row.httpColo]));

  return {
    expectedTurns: rows.length,
    acceptedReceiptTurns: accepted.length,
    withColoTurns: colos.length,
    unavailableColoTurns: accepted.length - colos.length,
    missingReceiptTurns: rows.length - accepted.length,
    histogram: countBy(colos.sort()),
  };
};

type ObjectResult = {
  role: Role;
  size: number;
  sample: number;
  status: string;
  httpRouting: ReturnType<typeof httpColoSummary>;
  metrics: Record<
    Metric,
    {
      cold: Value;
      warmMedian: Value;
      warm: ReturnType<typeof distribution>;
      minimumWarmTurns: number;
      warmMedianEligible: boolean;
    }
  >;
  startupTimeMs: Value;
  seedStorageBytes: Value;
  postStorageBytes: Value;
  seedFingerprint: string | null;
  postFingerprint: string | null;
  deploymentVersion: string | null;
  deploymentFile: string;
  deployment: typeof Deployment.Type | null;
  build: typeof Build.Type | null;
  buildManifestFiles: readonly string[];
  buildStatus: "matched" | "unavailable" | "unmatched" | "ambiguous" | "deployment_mismatch";
  collectionStatus: typeof CollectionStatus.Type | null;
  collectionComplete: boolean;
  cohortCpu: {
    objectIds: readonly string[] | null;
    window: typeof RoleWindow.Type | null;
    reportedTelemetryComplete: boolean | null;
    telemetryComplete: boolean;
    missing: readonly (typeof MissingTelemetry.Type)[] | null;
    allOwnedTotalIsLowerBound: boolean;
    extraTraceClassificationComplete: boolean;
    completedTurns: number;
    expectedTurns: number;
    tenTurnMeanCpuTimeMs: Value;
    normalizedTenTurnMeanCpuRatio: Value;
    reportedNormalizedTenTurnMeanCpuRatio: Value;
    referenceEvidence: Schema.Json | null;
    extraTraceCpuTimeMs: Value;
    sumObservedDoCpuTimeMs: Value;
    all: ReturnType<typeof invocationTotals> | null;
    extra: ReturnType<typeof invocationTotals> | null;
  } | null;
};

export const command = Command.make(
  "cf-bench-summarize",
  {
    outputDir: Flag.String("output-dir").pipe(
      Flag.withSchema(Schema.NonEmptyString),
      Flag.withDescription(
        "Run directory containing cohorts/, seed/, and deployments/; writes summary.json and tables.md here.",
      ),
    ),
  },
  Effect.fnUntraced(function* ({ outputDir }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const output = path.resolve(outputDir);
    const readStartedAt = yield* Clock.currentTimeMillis;
    const issues: Issue[] = [];
    const inputs: Array<{ file: string; status: string; rows?: number; validRows?: number }> = [];

    const read = Effect.fnUntraced(function* <A>(
      file: string,
      schema: Schema.Codec<A, unknown>,
      optional = false,
    ) {
      if (!(yield* fs.exists(file))) {
        inputs.push({ file, status: optional ? "optional_absent" : "missing" });
        if (!optional)
          issues.push({ file, kind: "missing_file", detail: "Expected input is absent" });

        return null;
      }

      const result = yield* fs
        .readFileString(file)
        .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(schema))), Effect.result);

      if (result._tag === "Failure") {
        inputs.push({ file, status: "invalid" });
        issues.push({ file, kind: "invalid_file", detail: String(result.failure) });

        return null;
      }
      inputs.push({ file, status: "read" });

      return result.success;
    });

    const rows = Effect.fnUntraced(function* <A>(
      file: string,
      schema: Schema.Codec<A, unknown>,
      optional = false,
    ) {
      const raw = yield* read(file, Schema.Array(Schema.Unknown), optional);
      const accepted: A[] = [];

      if (raw === null) return accepted;
      for (const [index, value] of raw.entries()) {
        const decoded = yield* Schema.decodeUnknownEffect(schema)(value).pipe(Effect.result);

        if (decoded._tag === "Success") accepted.push(decoded.success);
        else
          issues.push({ file, row: index, kind: "invalid_row", detail: String(decoded.failure) });
      }
      inputs.push({
        file,
        status: accepted.length === raw.length ? "rows_valid" : "rows_incomplete",
        rows: raw.length,
        validRows: accepted.length,
      });

      return accepted;
    });

    const unique = <A>(matches: readonly A[], file: string, key: string): A | null => {
      if (matches.length === 1) return matches[0] ?? null;
      issues.push({
        file,
        kind: matches.length === 0 ? "missing_row" : "duplicate_row",
        detail: `${key}: ${matches.length} rows`,
      });

      return null;
    };

    const builds: Array<{
      role: Role;
      file: string;
      source: "current" | "archive";
      status: string;
      build: typeof Build.Type | null;
    }> = [];

    const buildsDirectory = path.resolve(output, "../builds");

    const archives = (yield* fs.exists(buildsDirectory))
      ? (yield* fs.readDirectory(buildsDirectory))
          .filter((name) => /^wrapper-v\d+$/.test(name))
          .sort()
      : [];

    for (const role of ROLES) {
      const files = [path.join(buildsDirectory, `${role}.json`)];

      for (const archive of archives)
        for (const file of [
          path.join(buildsDirectory, archive, role, "manifest.json"),
          path.join(buildsDirectory, archive, `${role}.json`),
        ])
          if (yield* fs.exists(file)) files.push(file);

      for (const [index, file] of files.entries()) {
        const build = yield* read(file, Build, index === 0);

        if (build !== null && build.label !== role)
          issues.push({
            file,
            kind: "identity_mismatch",
            detail: `Expected label ${role}, got ${build.label}`,
          });
        builds.push({
          role,
          file,
          source: index === 0 ? "current" : "archive",
          status: build !== null && build.label === role ? "complete" : "incomplete",
          build,
        });
      }
    }
    const observations: Observation[] = [];
    const objects: ObjectResult[] = [];
    const recordedFailures: Array<typeof Failure.Type> = [];

    for (const size of sizes)
      for (const sample of samplesFor(size)) {
        const directory = path.join(output, "cohorts", `${size}-${sample}`);
        const measuredFile = path.join(directory, "measured.json");
        const measured = yield* rows(measuredFile, Measured);
        const samplesFile = path.join(directory, "samples.json");
        const savedSamples = yield* rows(samplesFile, SavedSample, true);
        const failures = yield* rows(path.join(directory, "failures.json"), Failure, true);

        recordedFailures.push(...failures);
        for (const failure of failures)
          issues.push({
            file: path.join(directory, "failures.json"),
            kind: "recorded_failure",
            detail: `${failure.role}/${failure.phase}: ${failure.error}`,
          });
        const auditFile = path.join(directory, "audits.json");
        const audits = yield* rows(auditFile, Audit);
        const cpuFile = path.join(directory, "cohort-cpu.json");
        const cpuRows = yield* rows(cpuFile, CohortCpu);
        const collectionFile = path.join(directory, "collection-status.json");
        const collectionRows = yield* rows(collectionFile, CollectionStatus);

        for (const row of [...measured, ...savedSamples, ...cpuRows, ...failures]) {
          if (row.size !== size || row.sample !== sample)
            issues.push({
              file: directory,
              kind: "coordinate_mismatch",
              detail: `${row.role}/${row.size}/${row.sample}`,
            });
        }
        for (const role of ROLES) {
          const own: Observation[] = [];

          for (const phase of PHASES) {
            const matches = measured.filter(
              (row) =>
                row.role === role &&
                row.size === size &&
                row.sample === sample &&
                row.phase === phase,
            );

            const measuredRow =
              matches.length === 0 ? null : unique(matches, measuredFile, `${role}/${phase}`);

            const savedMatches = savedSamples.filter(
              (row) =>
                row.role === role &&
                row.size === size &&
                row.sample === sample &&
                row.phase === phase,
            );

            const savedRow =
              savedMatches.length === 0
                ? null
                : unique(savedMatches, samplesFile, `${role}/${phase}`);

            const row = savedRow ?? measuredRow;
            const reasons: string[] = [];

            if (row === null) {
              reasons.push("missing_row");
              issues.push({ file: directory, kind: "missing_row", detail: `${role}/${phase}` });
            }
            if (matches.length > 1) reasons.push("duplicate_telemetry_row");
            if (savedMatches.length > 1) reasons.push("duplicate_raw_receipt");

            const failure = failures.find(
              (item) =>
                item.role === role &&
                item.size === size &&
                item.sample === sample &&
                item.phase === phase,
            );

            if (failure !== undefined) reasons.push("recorded_failure");
            const outcomes = [measuredRow?.outcome, ...(measuredRow?.allOutcomes ?? [])];

            const unsuccessfulTelemetry = outcomes.some(
              (outcome) => outcome !== undefined && outcome !== null && outcome !== "ok",
            );

            if (unsuccessfulTelemetry) reasons.push("unsuccessful_outcome");
            if (row !== null && row.receipt.phase !== phase) reasons.push("receipt_phase_mismatch");

            const rawReceiptUsable =
              savedRow !== null && savedRow.receipt.phase === phase && failure === undefined;

            const telemetryUsable =
              measuredRow !== null &&
              measuredRow.receipt.phase === phase &&
              failure === undefined &&
              !unsuccessfulTelemetry;

            const rpcCompleted = rawReceiptUsable || telemetryUsable;
            const rawClientWallMs = rawReceiptUsable ? finite(savedRow.clientWallMs) : null;

            const rawReferenceClientWallMs = rawReceiptUsable
              ? finite(savedRow.reference?.clientWallMs)
              : null;

            const normalizedClientWallRatio =
              rawClientWallMs !== null &&
              rawReferenceClientWallMs !== null &&
              rawReferenceClientWallMs > 0
                ? finite(rawClientWallMs / rawReferenceClientWallMs)
                : null;

            if (!rawReceiptUsable) reasons.push("primary_client_receipt_unavailable");

            if (measuredRow === null) reasons.push("telemetry_unavailable");
            else if (
              measuredRow.outcome !== "ok" ||
              !measuredRow.allOutcomes?.length ||
              measuredRow.allOutcomes.some((outcome) => outcome !== "ok")
            )
              reasons.push("telemetry_outcome_incomplete");

            const reportedValues: Values = {
              cpuTimeMs: finite(measuredRow?.cpuTimeMs),
              doTotalCpuTimeMs: finite(measuredRow?.doTotalCpuTimeMs),
              "receipt.insideWallMs": finite(row?.receipt.insideWallMs),
              clientWallMs: finite(row?.clientWallMs),
              doInvocationWallMs: finite(measuredRow?.doInvocationWallMs),
              doWallMinusCpuMs: finite(measuredRow?.doWallMinusCpuMs),
              clientWallMinusDoWallMs: finite(measuredRow?.clientWallMinusDoWallMs),
              referenceCpuTimeMs: finite(measuredRow?.referenceCpuTimeMs),
              referenceDoWallMs: finite(measuredRow?.referenceDoWallMs),
              referenceClientWallMs: finite(measuredRow?.referenceClientWallMs),
              normalizedClientWallRatio: finite(measuredRow?.normalizedClientWallRatio),
              normalizedDoWallRatio: finite(measuredRow?.normalizedDoWallRatio),
              normalizedCpuRatio: finite(measuredRow?.normalizedCpuRatio),
              normalizedDoTotalCpuRatio: finite(measuredRow?.normalizedDoTotalCpuRatio),
            };

            const values: Values = telemetryUsable
              ? { ...reportedValues }
              : {
                  cpuTimeMs: null,
                  doTotalCpuTimeMs: null,
                  "receipt.insideWallMs": null,
                  clientWallMs: null,
                  doInvocationWallMs: null,
                  doWallMinusCpuMs: null,
                  clientWallMinusDoWallMs: null,
                  referenceCpuTimeMs: null,
                  referenceDoWallMs: null,
                  referenceClientWallMs: null,
                  normalizedClientWallRatio: null,
                  normalizedDoWallRatio: null,
                  normalizedCpuRatio: null,
                  normalizedDoTotalCpuRatio: null,
                };

            values.clientWallMs = rawClientWallMs;
            values.referenceClientWallMs = rawReferenceClientWallMs;
            values.normalizedClientWallRatio = normalizedClientWallRatio;
            values["receipt.insideWallMs"] = rawReceiptUsable
              ? finite(savedRow.receipt.insideWallMs)
              : telemetryUsable
                ? finite(measuredRow.receipt.insideWallMs)
                : null;

            if (rpcCompleted)
              for (const metric of metrics)
                if (values[metric] === null) reasons.push(`missing_metric:${metric}`);
            // Decode reference diagnostics separately: malformed reference evidence must not
            // discard a completed workload receipt or its available workload telemetry.
            const referenceTimingEvents: Array<typeof ReferenceTimingEvent.Type> = [];

            for (const event of measuredRow?.referenceInvocations ?? []) {
              const decoded = yield* Schema.decodeUnknownEffect(ReferenceTimingEvent)(event).pipe(
                Effect.result,
              );

              if (decoded._tag === "Success") referenceTimingEvents.push(decoded.success);
              else
                issues.push({
                  file: measuredFile,
                  kind: "invalid_reference_timing",
                  detail: `${role}/${phase}: ${String(decoded.failure)}`,
                });
            }
            own.push({
              role,
              size,
              sample,
              phase,
              status: reasons.length === 0 ? "complete" : "incomplete",
              reasons,
              source: savedRow !== null ? "samples" : measuredRow !== null ? "measured" : "missing",
              rpcCompleted,
              workloadStatus: rpcCompleted
                ? "completed"
                : failure !== undefined || unsuccessfulTelemetry
                  ? "failed"
                  : "unavailable",
              primaryClientSource: rawReceiptUsable ? "samples.json" : null,
              httpColo: rawReceiptUsable
                ? (savedRow.cfRay
                    ?.trim()
                    .match(/^[\da-f]+-([a-z]{3})$/i)?.[1]
                    ?.toUpperCase() ?? null)
                : null,
              protocol: row?.receipt.protocol ?? null,
              reference: row?.reference ?? savedRow?.reference ?? null,
              referenceEvidence:
                measuredRow?.referenceEvidence ??
                (measuredRow?.referenceInvocations === undefined
                  ? null
                  : { referenceInvocations: measuredRow.referenceInvocations }),
              referenceTimingEvents,
              observedAlarmOverlap: measuredRow?.observedAlarmOverlap ?? null,
              observedAlarmOverlaps: measuredRow?.observedAlarmOverlaps ?? null,
              outcome: measuredRow?.outcome ?? null,
              allOutcomes: measuredRow?.allOutcomes ?? [],
              reportedValues,
              values,
            });
          }
          observations.push(...own);

          const metricResults = Object.fromEntries(
            metrics.map((metric) => {
              const warm = distribution(
                own.filter((row) => row.phase !== "cold").map((row) => row.values[metric]),
              );

              const requiredTurns = minimumWarmTurns(metric);

              return [
                metric,
                {
                  cold: own.find((row) => row.phase === "cold")?.values[metric] ?? null,
                  warm,
                  minimumWarmTurns: requiredTurns,
                  warmMedianEligible: warm.n >= requiredTurns,
                  warmMedian: warm.n >= requiredTurns ? warm.p50 : null,
                },
              ];
            }),
          ) as ObjectResult["metrics"];

          const seedFile = path.join(output, "seed", role, `${size}-${sample}.json`);
          const seed = yield* read(seedFile, Seed);

          const seedMatches =
            seed !== null && seed.role === role && seed.size === size && seed.sample === sample;

          if (seed !== null && !seedMatches)
            issues.push({
              file: seedFile,
              kind: "coordinate_mismatch",
              detail: `${role}/${size}/${sample}`,
            });

          const audit = unique(
            audits.filter((row) => row.role === role),
            auditFile,
            role,
          );

          const generation = `measure-${size}-${sample}`;

          const deploymentFile = path.join(
            output,
            "deployments",
            role,
            `${generation}-verified.json`,
          );

          const deployment = yield* read(deploymentFile, Deployment);

          const receipts = [...savedSamples, ...measured]
            .filter((row) => row.role === role && row.size === size && row.sample === sample)
            .map((row) => row.receipt);

          const deploymentMatches =
            deployment !== null &&
            deployment.generation === generation &&
            receipts.every(
              (receipt) =>
                receipt.version === deployment.version.id &&
                receipt.generation === deployment.generation,
            );

          if (deployment !== null && !deploymentMatches)
            issues.push({
              file: deploymentFile,
              kind: "deployment_identity_mismatch",
              detail: `${role}/${generation}: deployment generation or receipt version/generation differs`,
            });

          const benchModules = deployment?.modules.filter((module) => module.name === "bench.mjs");

          const wrapperModules = deployment?.modules.filter(
            (module) => module.name === "worker.mjs",
          );

          const benchSha256 = benchModules?.length === 1 ? benchModules[0]?.sha256 : undefined;

          const wrapperSha256 =
            wrapperModules?.length === 1 ? wrapperModules[0]?.sha256 : undefined;

          const matchingBuilds = deploymentMatches
            ? builds.filter(
                (entry) =>
                  entry.role === role &&
                  entry.status === "complete" &&
                  entry.build?.bench.sha256 === benchSha256 &&
                  entry.build?.wrapper.sha256 === wrapperSha256,
              )
            : [];

          // Identical current/archive copies are one identity. Conflicting metadata for
          // the same module bytes stays unresolved instead of choosing the newest file.
          const identities = new Set(matchingBuilds.map((entry) => JSON.stringify(entry.build)));
          const build = identities.size === 1 ? (matchingBuilds[0]?.build ?? null) : null;

          const buildStatus: ObjectResult["buildStatus"] =
            deployment === null
              ? "unavailable"
              : !deploymentMatches
                ? "deployment_mismatch"
                : build !== null
                  ? "matched"
                  : identities.size > 1
                    ? "ambiguous"
                    : "unmatched";

          if (deployment !== null && buildStatus !== "matched")
            issues.push({
              file: deploymentFile,
              kind: "build_provenance_unresolved",
              detail: `${role}/${generation}: ${buildStatus}; bench=${benchSha256 ?? "unavailable"}; wrapper=${wrapperSha256 ?? "unavailable"}`,
            });

          const cpu = unique(
            cpuRows.filter(
              (row) => row.role === role && row.size === size && row.sample === sample,
            ),
            cpuFile,
            role,
          );

          const collectionStatus = unique(
            collectionRows.filter((row) => row.role === role),
            collectionFile,
            role,
          );

          const collectionComplete =
            collectionStatus !== null &&
            collectionStatus.complete &&
            collectionStatus.expected === PHASES.length &&
            collectionStatus.matched === PHASES.length &&
            collectionStatus.missing.length === 0;

          const telemetryComplete =
            cpu?.telemetryComplete === true && cpu.missing?.length === 0 && collectionComplete;

          if (!telemetryComplete)
            issues.push({
              file: collectionFile,
              kind: "telemetry_incomplete",
              detail: `${role}/${size}/${sample}: full ten-turn collection not confirmed`,
            });

          const startupTimeMs = deploymentMatches
            ? finite(deployment.version.startup_time_ms)
            : null;

          const seedStorageBytes = seedMatches ? finite(seed.stats.bytes) : null;
          const postStorageBytes = audit === null ? null : finite(audit.stats.bytes);

          const all =
            cpu?.invocations === undefined || cpu.invocations === null
              ? null
              : invocationTotals(cpu.invocations);

          const extra =
            cpu?.extraTraceInvocations === undefined || cpu.extraTraceInvocations === null
              ? null
              : invocationTotals(cpu.extraTraceInvocations);

          const sumObservedDoCpuTimeMs =
            finite(cpu?.sumObservedDoCpuTimeMs) ?? all?.cpuTimeMs.total ?? null;

          const completedTurns = own.filter((row) => row.rpcCompleted).length;

          const fullCohort =
            telemetryComplete &&
            completedTurns === PHASES.length &&
            all?.cpuTimeMs.status === "complete";

          const cohortCpu =
            cpu === null
              ? null
              : {
                  objectIds: cpu.objectIds ?? null,
                  window: cpu.window ?? null,
                  reportedTelemetryComplete: cpu.telemetryComplete ?? null,
                  telemetryComplete,
                  missing: cpu.missing ?? null,
                  allOwnedTotalIsLowerBound:
                    !telemetryComplete || all?.cpuTimeMs.status !== "complete",
                  extraTraceClassificationComplete: telemetryComplete,
                  completedTurns,
                  expectedTurns: PHASES.length,
                  tenTurnMeanCpuTimeMs:
                    fullCohort && sumObservedDoCpuTimeMs !== null
                      ? sumObservedDoCpuTimeMs / PHASES.length
                      : null,
                  normalizedTenTurnMeanCpuRatio: fullCohort
                    ? finite(cpu.normalizedTenTurnMeanCpuRatio)
                    : null,
                  reportedNormalizedTenTurnMeanCpuRatio: finite(cpu.normalizedTenTurnMeanCpuRatio),
                  referenceEvidence:
                    cpu.referenceEvidence ??
                    (cpu.objectReferences === undefined
                      ? null
                      : { objectReferences: cpu.objectReferences }),
                  extraTraceCpuTimeMs:
                    finite(cpu.extraTraceCpuTimeMs) ?? extra?.cpuTimeMs.total ?? null,
                  sumObservedDoCpuTimeMs,
                  all,
                  extra,
                };

          objects.push({
            role,
            size,
            sample,
            status:
              own.every((row) => row.status === "complete") &&
              buildStatus === "matched" &&
              startupTimeMs !== null &&
              seedStorageBytes !== null &&
              postStorageBytes !== null &&
              cohortCpu !== null &&
              telemetryComplete &&
              cohortCpu.extraTraceCpuTimeMs !== null &&
              cohortCpu.sumObservedDoCpuTimeMs !== null &&
              cohortCpu.normalizedTenTurnMeanCpuRatio !== null &&
              cohortCpu.all?.cpuTimeMs.status === "complete" &&
              cohortCpu.extra?.cpuTimeMs.status === "complete" &&
              cohortCpu.all?.alarms.status === "complete" &&
              cohortCpu.extra?.alarms.status === "complete"
                ? "complete"
                : "incomplete",
            metrics: metricResults,
            httpRouting: httpColoSummary(own),
            startupTimeMs,
            seedStorageBytes,
            postStorageBytes,
            seedFingerprint: seedMatches ? seed.fingerprint : null,
            postFingerprint: audit?.fingerprint ?? null,
            deploymentVersion: deploymentMatches ? deployment.version.id : null,
            deploymentFile,
            deployment,
            build,
            buildManifestFiles: matchingBuilds.map((entry) => entry.file),
            buildStatus,
            collectionStatus,
            collectionComplete,
            cohortCpu,
          });
        }
      }

    const groups = sizes.flatMap((size) =>
      ROLES.map((role) => {
        const own = objects.filter((row) => row.role === role && row.size === size);
        const deployedBuilds: Array<typeof Build.Type> = [];

        for (const object of own)
          if (
            object.build !== null &&
            !deployedBuilds.some(
              (build) =>
                build.bench.sha256 === object.build?.bench.sha256 &&
                build.wrapper.sha256 === object.build?.wrapper.sha256,
            )
          )
            deployedBuilds.push(object.build);
        const allTurns = observations.filter((row) => row.role === role && row.size === size);

        const turns = allTurns.filter((row) => row.phase !== "cold");

        const nTelemetryComplete = own.filter(
          (row) => row.cohortCpu?.telemetryComplete === true,
        ).length;

        const observed = <A extends ReturnType<typeof total>>(value: A, lowerBound = true) => {
          const complete = value.status === "complete" && nTelemetryComplete === own.length;

          return {
            ...value,
            status: complete ? "complete" : "incomplete",
            availability:
              value.availability === "unavailable"
                ? "unavailable"
                : complete
                  ? "available"
                  : "partial",
            total: complete ? value.total : null,
            lowerBound: !complete && lowerBound,
            nTelemetryComplete,
            expectedTelemetryObjects: own.length,
          };
        };

        return {
          role,
          size,
          builds: deployedBuilds,
          buildProvenance: {
            matchedObjects: own.filter((row) => row.buildStatus === "matched").length,
            expectedObjects: own.length,
          },
          deploymentVersions: own.map((row) => ({
            sample: row.sample,
            version: row.deploymentVersion,
            generation: row.deployment?.generation ?? null,
            modules: row.deployment?.modules ?? null,
            buildStatus: row.buildStatus,
            buildManifestFiles: row.buildManifestFiles,
          })),
          samples: samplesFor(size),
          status: own.every((row) => row.status === "complete") ? "complete" : "incomplete",
          nObjectsComplete: own.filter((row) => row.status === "complete").length,
          expectedObjects: 3,
          workload: {
            completedTurns: allTurns.filter((row) => row.rpcCompleted).length,
            expectedTurns: allTurns.length,
            statuses: countBy(allTurns.map((row) => row.workloadStatus)),
          },
          httpRouting: httpColoSummary(allTurns),
          observedAlarmOverlap: {
            cold: alarmOverlapSummary(allTurns.filter((row) => row.phase === "cold")),
            warm: alarmOverlapSummary(turns),
          },
          collection: {
            completeObjects: own.filter((row) => row.collectionComplete).length,
            expectedObjects: own.length,
            expectedRpcRows: total(own.map((row) => row.collectionStatus?.expected ?? null)),
            matchedRpcRows: total(own.map((row) => row.collectionStatus?.matched ?? null)),
            missingEvidenceRows: total(
              own.map((row) => row.collectionStatus?.missing.length ?? null),
            ),
          },
          metrics: metrics.map((metric) => ({
            metric,
            unit: metricUnits[metric],
            minimumWarmTurns: minimumWarmTurns(metric),
            warmObjectCoverage: own.map((row) => ({
              sample: row.sample,
              observedTurns: row.metrics[metric].warm.n,
              expectedTurns: row.metrics[metric].warm.expectedN,
              eligible: row.metrics[metric].warmMedianEligible,
              coverageStatus: row.metrics[metric].warm.status,
            })),
            eligibleWarmObjects: own.filter((row) => row.metrics[metric].warmMedianEligible).length,
            cold: distribution(own.map((row) => row.metrics[metric].cold)),
            primaryWarm: distribution(own.map((row) => row.metrics[metric].warmMedian)),
            pooledWarm: distribution(turns.map((row) => row.values[metric])),
          })),
          startupTimeMs: distribution(own.map((row) => row.startupTimeMs)),
          seedStorageBytes: distribution(own.map((row) => row.seedStorageBytes)),
          postStorageBytes: distribution(own.map((row) => row.postStorageBytes)),
          cohortCpu: {
            tenTurnMeanCpuTimeMs: distribution(
              own.map((row) => row.cohortCpu?.tenTurnMeanCpuTimeMs ?? null),
            ),
            normalizedTenTurnMeanCpuRatio: distribution(
              own.map((row) => row.cohortCpu?.normalizedTenTurnMeanCpuRatio ?? null),
            ),
            sumObservedDoCpuTimeMs: observed(
              total(own.map((row) => row.cohortCpu?.sumObservedDoCpuTimeMs ?? null)),
            ),
            extraTraceCpuTimeMs: observed(
              total(own.map((row) => row.cohortCpu?.extraTraceCpuTimeMs ?? null)),
              false,
            ),
            allDoInvocationCount: observed(
              total(own.map((row) => row.cohortCpu?.all?.count ?? null)),
            ),
            allDoEventCpuTimeMs: observed(
              eventCpuTotal(own.map((row) => row.cohortCpu?.all?.cpuTimeMs ?? null)),
            ),
            extraTraceInvocationCount: observed(
              total(own.map((row) => row.cohortCpu?.extra?.count ?? null)),
              false,
            ),
            extraTraceEventCpuTimeMs: observed(
              eventCpuTotal(own.map((row) => row.cohortCpu?.extra?.cpuTimeMs ?? null)),
              false,
            ),
            extraTraceAlarmCount: observed(
              total(
                own.map((row) =>
                  row.cohortCpu?.extra?.alarms.countStatus === "complete"
                    ? row.cohortCpu.extra.alarms.count
                    : null,
                ),
              ),
              false,
            ),
            extraTraceAlarmCpuTimeMs: observed(
              eventCpuTotal(own.map((row) => row.cohortCpu?.extra?.alarms.cpuTimeMs ?? null)),
              false,
            ),
            allDoAlarmCount: observed(
              total(
                own.map((row) =>
                  row.cohortCpu?.all?.alarms.countStatus === "complete"
                    ? row.cohortCpu.all.alarms.count
                    : null,
                ),
              ),
            ),
            allDoAlarmCpuTimeMs: observed(
              eventCpuTotal(own.map((row) => row.cohortCpu?.all?.alarms.cpuTimeMs ?? null)),
            ),
          },
        };
      }),
    );

    const comparisons = sizes.flatMap((size) =>
      pairs.flatMap(([numerator, denominator]) =>
        metrics.map((metric) => {
          const perObject = samplesFor(size).map((sample) => {
            const left = objects.find(
              (row) => row.size === size && row.sample === sample && row.role === numerator,
            )?.metrics[metric];

            const right = objects.find(
              (row) => row.size === size && row.sample === sample && row.role === denominator,
            )?.metrics[metric];

            const control = objects.find(
              (row) => row.size === size && row.sample === sample && row.role === "control",
            )?.metrics[metric];

            const base = objects.find(
              (row) => row.size === size && row.sample === sample && row.role === "base",
            )?.metrics[metric];

            return {
              sample,
              cold: ratio(left?.cold ?? null, right?.cold ?? null),
              warm: ratio(left?.warmMedian ?? null, right?.warmMedian ?? null),
              controlCold: ratio(control?.cold ?? null, base?.cold ?? null),
              controlWarm: ratio(control?.warmMedian ?? null, base?.warmMedian ?? null),
              warmObservedTurns: {
                numerator: left?.warm.n ?? 0,
                denominator: right?.warm.n ?? 0,
                control: control?.warm.n ?? 0,
                base: base?.warm.n ?? 0,
                minimum: minimumWarmTurns(metric),
                expected: 9,
              },
            };
          });

          const pooledWarm = samplesFor(size).flatMap((sample) =>
            PHASES.filter((phase) => phase !== "cold").map((phase) => {
              const left = observations.find(
                (row) =>
                  row.size === size &&
                  row.sample === sample &&
                  row.phase === phase &&
                  row.role === numerator,
              );

              const right = observations.find(
                (row) =>
                  row.size === size &&
                  row.sample === sample &&
                  row.phase === phase &&
                  row.role === denominator,
              );

              const control = observations.find(
                (row) =>
                  row.size === size &&
                  row.sample === sample &&
                  row.phase === phase &&
                  row.role === "control",
              );

              const base = observations.find(
                (row) =>
                  row.size === size &&
                  row.sample === sample &&
                  row.phase === phase &&
                  row.role === "base",
              );

              return {
                sample,
                phase,
                ...ratio(left?.values[metric] ?? null, right?.values[metric] ?? null),
                control: ratio(control?.values[metric] ?? null, base?.values[metric] ?? null),
              };
            }),
          );

          const cold = distribution(perObject.map((row) => row.cold.ratio));
          const primaryWarm = distribution(perObject.map((row) => row.warm.ratio));
          const pooledPairedWarm = distribution(pooledWarm.map((row) => row.ratio));

          const controlSpread = {
            pair: "control/base",
            cold: distribution(
              perObject.map((row) => (row.cold.ratio === null ? null : row.controlCold.ratio)),
            ),
            primaryWarm: distribution(
              perObject.map((row) => (row.warm.ratio === null ? null : row.controlWarm.ratio)),
            ),
            pooledPairedWarm: distribution(
              pooledWarm.map((row) => (row.ratio === null ? null : row.control.ratio)),
            ),
          };

          return {
            size,
            numerator,
            denominator,
            metric,
            sourceUnit: metricUnits[metric],
            unit: "numerator / denominator",
            perObject,
            pooledWarm,
            cold,
            primaryWarm,
            pooledPairedWarm,
            controlSpread,
            eligibleWarmObjectPairs: primaryWarm.n,
            eligibleWarmObjectPairsWithControl: controlSpread.primaryWarm.n,
            status:
              cold.status === "complete" &&
              primaryWarm.status === "complete" &&
              pooledPairedWarm.status === "complete" &&
              controlSpread.cold.status === "complete" &&
              controlSpread.primaryWarm.status === "complete" &&
              controlSpread.pooledPairedWarm.status === "complete"
                ? "complete"
                : "incomplete",
          };
        }),
      ),
    );

    const cohortComparisons = sizes.flatMap((size) =>
      pairs.flatMap(([numerator, denominator]) =>
        cohortMetrics.map((metric) => {
          const perObject = samplesFor(size).map((sample) => {
            const value = (role: Role) =>
              objects.find((row) => row.size === size && row.sample === sample && row.role === role)
                ?.cohortCpu?.[metric] ?? null;

            return {
              sample,
              ...ratio(value(numerator), value(denominator)),
              control: ratio(value("control"), value("base")),
            };
          });

          const ratios = distribution(perObject.map((row) => row.ratio));

          const controlSpread = distribution(
            perObject.map((row) => (row.ratio === null ? null : row.control.ratio)),
          );

          return {
            size,
            numerator,
            denominator,
            metric,
            sourceUnit: metric === "tenTurnMeanCpuTimeMs" ? "ms" : "ms CPU / ms reference CPU",
            unit: "numerator / denominator",
            perObject,
            ratios,
            controlPair: "control/base",
            controlSpread,
            status:
              ratios.status === "complete" && controlSpread.status === "complete"
                ? "complete"
                : "incomplete",
          };
        }),
      ),
    );

    const referenceEvolution = objects.flatMap((object) => {
      const own = observations.filter(
        (row) =>
          row.role === object.role && row.size === object.size && row.sample === object.sample,
      );

      const kinds: readonly ("thread" | "actor")[] =
        object.role === "tardie" ? ["thread", "actor"] : ["thread"];

      return kinds.map((kind) => {
        const phases = own.map((row) => {
          const receipt = row.reference?.value[kind];

          const matches = row.referenceTimingEvents.filter(
            (event) => event.durableObjectId === receipt?.objectId,
          );

          const event = matches.length === 1 && matches[0]?.outcome === "ok" ? matches[0] : null;

          return {
            phase: row.phase,
            referencePhase: receipt?.phase ?? null,
            objectId: receipt?.objectId ?? null,
            referenceRuntimeId: receipt?.runtimeId ?? null,
            matchedReferenceEvents: matches.length,
            clientSource: row.primaryClientSource,
            values: {
              clientWallMs:
                row.primaryClientSource === null
                  ? null
                  : finite(
                      kind === "thread"
                        ? row.reference?.clientWallMs
                        : row.reference?.actorTiming?.clientWallMs,
                    ),
              doWallMs:
                kind === "thread"
                  ? row.reportedValues.referenceDoWallMs
                  : finite(event?.wallTimeMs),
              cpuTimeMs:
                kind === "thread"
                  ? row.reportedValues.referenceCpuTimeMs
                  : finite(event?.cpuTimeMs),
            },
          };
        });

        const objectIds = [...new Set(phases.flatMap((row) => row.objectId ?? []))];

        return {
          role: object.role,
          size: object.size,
          sample: object.sample,
          kind,
          objectIds,
          phases,
          metrics: referenceMetrics.map((metric) => ({
            metric,
            all: distribution(phases.map((row) => row.values[metric])),
            warm: distribution(
              phases.filter((row) => row.phase !== "cold").map((row) => row.values[metric]),
            ),
          })),
        };
      });
    });

    const status =
      issues.length === 0 &&
      groups.every((group) => group.status === "complete") &&
      comparisons.every((row) => row.status === "complete") &&
      cohortComparisons.every((row) => row.status === "complete")
        ? "complete"
        : "incomplete";

    const methodology = {
      primaryOutcomes:
        "Latency is primary: clientWallMs and doInvocationWallMs lead cold and warm reporting for every role/size/build. CPU, DO wall minus CPU, and client wall minus DO wall follow as breakdown diagnostics. Effect improvement claims require latency evidence; CPU changes alone do not establish a latency improvement.",
      primaryClient:
        "Primary clientWallMs comes from valid raw samples.json receipts, independently of measured.json or CF logs. normalizedClientWallRatio is computed from that raw clientWallMs divided by the raw Sample.reference.clientWallMs when finite and positive. Both client medians require all nine successful warm receipts; the normalized median additionally requires nine valid HTTP references. A telemetry failure or dropped log does not erase a valid client receipt.",
      httpRouting:
        "HTTP colo histograms use the three-letter CF-Ray response suffix from accepted workload turn receipts in samples.json, independently of DO telemetry. Each sample identifies one Object cluster; group totals aggregate its planned samples. Missing or unparseable headers remain unavailable, separately from missing accepted receipts. Reference RPCs, preparation, rejected requests and historical attempts are excluded. CF-Ray identifies the HTTP request-processing data center, not the Durable Object's physical location or host. Different codes across paired Objects expose a client-latency routing confound; matching codes do not establish identical paths or placement. These diagnostics do not change metric eligibility, matching or ratios. [Cloudflare CF-Ray documentation](https://developers.cloudflare.com/fundamentals/reference/http-headers/#cf-ray).",
      quantiles:
        "Ascending values; linear interpolation at (n-1)*p. Null quantiles when n=0. Cold, warm-turn, and Object-median tables display median [p25, p75].",
      acceptedSamples:
        "Planned sizes are 50,250,1000. Size 50 uses sample IDs 1,2,3; sizes 250 and 1000 use 0,1,2. Rollout pilot 50-0 is excluded regardless of its current location. Every planned role/size is included even without fixtures.",
      unmeasuredSizes:
        "Size 3500 was not provisioned and remains unmeasured; it is excluded from planned coverage and tables.",
      buildIdentity:
        "Each Object's verified deployment resolves its role manifest by both deployed bench.mjs and worker.mjs SHA-256, using current builds/<role>.json and archived builds/wrapper-v*/<role>/manifest.json (also accepting wrapper-v*/<role>.json). Identical manifest copies share one identity; conflicting selected metadata for the same module hashes remains unresolved. Receipt version/generation must match the saved deployment. Historical matchesLocal flags are retained as recorded, never recomputed against the latest wrapper. Unresolved provenance is explicit and does not discard timing observations. Group build columns show the resolved deployed set, which can contain multiple wrapper hashes. Pi uses its targetPackage version (pi-durable 1.0.4) and no Effect runtime; its manifest Effect version is build-CLI provenance only. Other roles display their resolved manifest Effect version. summary.json retains selected manifest fields and each Object's deployment generation, version and module identities; full fixture/reference hashes remain in the original current or archived manifests.",
      primaryWarm:
        "Object-level cohort/control robustness summary (stored as primaryWarm): predeclared minimum of seven out of nine observed successful finite warm turns for DO/CPU metrics, normalized DO/CPU metrics, and telemetry-dependent residuals/reference timings. Below seven yields null. Client/HTTP-reference and receipt-only metrics require all nine. Exact observed n, eligibility, and the threshold are stored per Object/metric. An eligible seven- or eight-turn median retains incomplete telemetry coverage.",
      cold: "Three cold turns per role/size. Readiness and the fixed JavaScript reference prime the module before the final Thread (and Tardie Actor) ctx.abort. Cold requires a fresh DO runtimeId and a constructor log on the cold RPC trace. Module/isolate survival across the abort is not guaranteed, so module freshness is not guaranteed. Matched rows carry collector validation; receipt-only fallback retains timings with unavailable telemetry evidence explicitly marked.",
      pooledWarm:
        "README warm-turn summary: pool the nine warm turns from each of three Objects, matching bench/results.ts median(group.flatMap(line => line.turn.slice(1))). Report pooled median and IQR endpoints (p25, p75); available valid turns retained with n/expectedN.",
      independentUnits:
        "The design targets three distinct Object clusters per role/size. Inference has n=3 only when all three Objects (and, for comparisons, matching control pairs) are eligible for that metric; otherwise use the reported smaller eligible count. A Tardie Thread plus its Actor belongs to one cluster. Up to 27 pooled warm turns are repeated observations within at most three clusters, a modest independent sample. No formal confidence intervals are reported.",
      pairs:
        "Numerator/denominator matched by size/sample; warm robustness compares eligible per-Object medians, retaining each side's exact observed turn n, cold compares single turns. Eligibility follows each metric's predeclared nine- or seven-turn minimum. Pooled paired-turn ratios additionally match phase and remain repeated-turn diagnostics. Zero denominators yield null with an explicit reason.",
      controlSpread:
        "Every raw and normalized comparison carries control/base ratios from the same available size/sample/phase slots. Control spread reports median, p25/p75, min/max, and coverage. Missing control evidence marks the comparison incomplete without discarding the target ratio. A control/base row shows its own spread, not a separate replication.",
      normalization:
        "Client normalization uses raw HTTP receipts and references, even when CF telemetry is missing. The collector supplies normalizedDoWallRatio = doInvocationWallMs / referenceDoWallMs, normalizedCpuRatio = main CPU / referenceCpuTimeMs, and normalizedDoTotalCpuRatio = sum(each DO CPU / that DO reference CPU). Tardie's Actor reference is a separate earlier HTTP RPC; the Thread reference immediately precedes every turn with the same single-loop HTTP path for every role. For cold, the reference precedes final ctx.abort. Fixed JavaScript work is identical on Threads and Actors. Object medians and pooled summaries operate on per-turn ratios. Reference receipts, referenceInvocations/objectReferences evidence, and all three reference timings are retained. Missing references leave normalized values unavailable without discarding raw rows.",
      normalizationCaveat:
        "Normalization is a diagnostic ratio, not a guaranteed correction for machine cost. Identical fixed JavaScript work does not establish that reference cost scales with workload cost across roles, builds, or turns. Differences in reference timing can change normalized rankings; raw latency, normalized latency, and matched controls must remain visible together. JIT tier and module placement are possible hypotheses, not established explanations. Control agreement alone does not validate a universal calibration.",
      referenceEvolution:
        "Reference evolution lists cold, then warm1 through warm9, for each planned Object. These are repeated reference timings within at most three Object clusters per role/size, not ten independent calibration replicates. Tardie's Actor is listed separately but belongs to the Thread's cluster; its HTTP reference precedes the Thread HTTP reference. Client values use raw samples.json reference receipts independently of CF telemetry. Thread DO/CPU values use collector reference fields; Actor values require one matching successful reference invocation. Missing values remain unavailable. Warm reference median [p25, p75] is descriptive over the available references with exact n, without a completeness or calibration claim. Full Object IDs and per-phase reference runtime IDs are retained in summary.json; cold reference runtime IDs precede the final abort.",
      latencyBreakdown:
        "doWallMinusCpuMs = doInvocationWallMs - cpuTimeMs; clientWallMinusDoWallMs = clientWallMs - doInvocationWallMs. These are collector-reported timing differences, with signs preserved, not causal allocations to a particular queue, network, or runtime component.",
      observedAlarmOverlap:
        "observedAlarmOverlap and observedAlarmOverlaps retain the collector's DO telemetry interval-intersection evidence per turn. False means no overlap was observed in available telemetry, not proof that alarms were absent. Missing flags/evidence remain unavailable. Event entries may repeat across intersected turns and are not distinct alarm counts; overlap observations do not establish causation.",
      normalizedCohortMean:
        "normalizedTenTurnMeanCpuRatio is supplied by the collector as sum(each owned DO window-observed cohort CPU / median of that DO's ten reference CPU values) / 10. It has the same conservative telemetry/ten-turn gate as the raw cohort mean. The reported value and referenceEvidence are retained separately even when that gate fails. Passing the gate does not prove exhaustive CPU capture.",
      interpretation:
        "Per measurement steering, raw differences below roughly 30% across separate Workers require supporting normalized control evidence to resolve, but normalization is not sufficient by itself. Control interpretation must account for median bias as well as spread: use a conservative envelope around one including the full control/base range and its reciprocals, not IQR width alone. This report supplies descriptive values and control spread without automatically classifying an effect; three Object clusters remain a modest basis for inference.",
      failures:
        "Recorded workload failures and invalid/duplicate raw receipts exclude their client values. Duplicate, mismatched, or known non-ok telemetry excludes its telemetry values while valid raw client receipts survive. workloadStatus is separate from metric/telemetry coverage: completed receipts remain completed when logs are missing. Absent telemetry values remain null, never zero, and make coverage incomplete. Finite negative timing residuals are preserved.",
      cohortMean:
        "All-owned DO totals are window-observed CPU, summed across available Object windows. The separate per-Object total/10 diagnostic requires ten completed RPC receipts, cohort telemetryComplete=true, complete ten-row collection status, empty missing[] lists in both cohort and collection records, and CPU values for every saved event. This conservative gate suppresses raw and normalized cohort means even for ingress-only missing[] entries when all ten primary DO RPCs matched; it does not mark completed workloads failed or discard available RPC metrics or observed totals. A passed gate describes the saved evidence, never proof of exhaustive invocation capture. Incomplete totals remain observed lower bounds and are not promoted to complete per-turn CPU. Alarms are never allocated to cold or warm RPC metrics.",
      alarmTotals:
        "All roles include observed native alarms. Invocation CPU totals cover saved events across all owned DOs, including the Tardie Actor, and all outcomes. Alarm means eventType=alarm; absent event types mark alarm coverage incomplete. Counts remain available when CPU is missing. Collector bounds start at the cold RPC event timestamp (workloadFrom fallback if absent) through the role's 35s+2s tail, excluding prep and audit traces. These window-observed totals never establish exhaustive capture, even when telemetryComplete is true. Partial event CPU sums retain event/Object and telemetry coverage. When telemetry is incomplete, extra-trace classification is provisional: it is relative to matched RPC traces and can include unmatched RPC work, so extra totals are not labeled lower bounds for non-RPC work.",
      inputs:
        "Only the selected run directory and its sibling current/archived build manifests are read; historical workload attempts are not pooled. Archives supply build provenance only. This is a sequential snapshot of files that may still be arriving.",
    };

    const summary = {
      status,
      readStartedAt,
      readFinishedAt: yield* Clock.currentTimeMillis,
      methodology,
      sizes,
      unmeasuredSizes: [3500],
      samplesBySize,
      roles: ROLES,
      completeness: {
        expectedRows: observations.length,
        completedRpcRows: observations.filter((row) => row.rpcCompleted).length,
        workloadStatuses: countBy(observations.map((row) => row.workloadStatus)),
        primaryClientReceiptRows: observations.filter(
          (row) => row.primaryClientSource === "samples.json",
        ).length,
        rowStatuses: countBy(observations.map((row) => row.status)),
        rowReasons: countBy(observations.flatMap((row) => row.reasons)),
        issueCounts: countBy(issues.map((issue) => issue.kind)),
        recordedFailures: recordedFailures.length,
      },
      builds,
      groups,
      comparisons,
      cohortComparisons,
      referenceEvolution,
      objects,
      observations,
      recordedFailures,
      inputs,
      issues,
    };

    const number = (value: Value) =>
      value === null ? "unavailable" : Number(value.toPrecision(6)).toString();

    const cell = (value: ReturnType<typeof distribution>) =>
      `${number(value.p50)} (${value.n}/${value.expectedN})`;

    const medianIqrCell = (value: ReturnType<typeof distribution>) =>
      `${number(value.p50)} [${number(value.p25)}, ${number(value.p75)}] (${value.n}/${value.expectedN})`;

    const spreadCell = (value: ReturnType<typeof distribution>) =>
      `${medianIqrCell(value)}; range [${number(value.min)}, ${number(value.max)}]`;

    const sumCell = (
      value: ReturnType<typeof total> & {
        lowerBound?: boolean;
        nTelemetryComplete?: number;
        expectedTelemetryObjects?: number;
      },
    ) =>
      `${number(value.availability === "unavailable" ? null : (value.total ?? value.observedTotal))}${value.lowerBound && value.availability !== "unavailable" ? " lower bound" : ""} (${value.n}/${value.expectedN}${value.status === "incomplete" ? "; incomplete" : ""}${value.nTelemetryComplete === undefined ? "" : `; telemetry ${value.nTelemetryComplete}/${value.expectedTelemetryObjects}`})`;

    const eventSumCell = (value: ReturnType<typeof eventCpuTotal>) =>
      `${sumCell(value)} events; ${value.nObjects}/${value.expectedObjects} Objects`;

    const runtimeLabel = (role: Role, build: typeof Build.Type | null) =>
      build === null
        ? "unavailable"
        : role === "pi"
          ? `pi ${build.targetPackage?.version ?? "unavailable"} / no Effect`
          : `${role}@${build.effect.version}`;

    const buildCell = (group: (typeof groups)[number]) =>
      `${group.builds.length === 0 ? "unavailable" : group.builds.map((build) => `${runtimeLabel(group.role, build)} (${build.bench.sha256.slice(0, 8)}/${build.wrapper.sha256.slice(0, 8)})`).join("; ")}; ${group.buildProvenance.matchedObjects}/${group.buildProvenance.expectedObjects} deployments matched`;

    const lines = [
      `# cf-bench-8914 statistics`,
      "",
      `Status: **${status}**. Counts are observed/expected. Values marked incomplete describe available data only.`,
      "",
      methodology.primaryOutcomes,
      "",
      methodology.primaryClient,
      "",
      methodology.primaryWarm,
      "",
      "Quantiles use linear interpolation. The README warm-turn summary pools 27 warm turns and reports median [p25, p75]. Object medians summarize independent cohorts; paired Object-median ratios provide the robustness comparison. Ratios are numerator/denominator; undefined ratios remain missing.",
      "",
      methodology.independentUnits,
      "",
      methodology.interpretation,
      "",
      methodology.normalization,
      "",
      methodology.normalizationCaveat,
      "",
      methodology.latencyBreakdown,
      "",
      methodology.controlSpread,
      "",
      "Accepted sample IDs: size 50 = 1,2,3; sizes 250,1000 = 0,1,2. Pilot 50-0 and attempts/ are excluded. Primary client timings and HTTP normalization use raw receipts, independently of CF telemetry availability. Completed workload status is separate from incomplete telemetry coverage.",
      "",
      methodology.unmeasuredSizes,
      "",
      methodology.cold,
      "",
      methodology.buildIdentity,
      "",
      "Build columns show the resolved deployed set with abbreviated bench/wrapper hashes. Per-Object deployment modules, matched manifest paths, selected identity fields and provenance status are retained in summary.json. Full manifests, including fixture and reference hashes, remain in the run directory's sibling builds/ and wrapper-v*/ archives.",
      "",
    ];

    for (const metric of metrics) {
      lines.push(
        `## RPC ${metric} (${metricUnits[metric]})`,
        "",
        `Object-median eligibility: at least ${minimumWarmTurns(metric)} of 9 warm observations. Per-cluster counts below retain the sample ID and mark ineligible medians.`,
        "",
        "| Size | Role | Deployed build set | Cold median [p25, p75] (clusters/3) | README warm median [p25, p75] (turns/27) | Object-median median [p25, p75] (eligible clusters/3) | Warm n per cluster | Warm-turn p95 | Coverage |",
        "|---:|---|---|---:|---:|---:|---|---:|---|",
      );
      for (const group of groups) {
        const row = group.metrics.find((item) => item.metric === metric);

        if (row !== undefined)
          lines.push(
            `| ${group.size} | ${group.role} | ${buildCell(group)} | ${medianIqrCell(row.cold)} | ${medianIqrCell(row.pooledWarm)} | ${medianIqrCell(row.primaryWarm)} | ${row.warmObjectCoverage.map((value) => `${value.sample}:${value.observedTurns}/${value.expectedTurns}${value.eligible ? "" : " ineligible"}`).join("; ")} | ${number(row.pooledWarm.p95)} | ${[row.cold, row.primaryWarm, row.pooledWarm].every((value) => value.status === "complete") ? "complete" : "incomplete"} |`,
          );
      }
      lines.push(
        "",
        "| Size | Pair | Cold ratio median [IQR] (pairs/3) | Control/base cold spread | Object-median ratio median [IQR] (pairs/3) | Control/base Object spread | Paired-turn ratio median [IQR] (turn pairs/27; diagnostic) | Control/base turn spread | Status |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---|",
      );
      for (const row of comparisons.filter((item) => item.metric === metric))
        lines.push(
          `| ${row.size} | ${row.numerator}/${row.denominator} | ${medianIqrCell(row.cold)} | ${spreadCell(row.controlSpread.cold)} | ${medianIqrCell(row.primaryWarm)} | ${spreadCell(row.controlSpread.primaryWarm)} | ${medianIqrCell(row.pooledPairedWarm)} | ${spreadCell(row.controlSpread.pooledPairedWarm)} | ${row.status} |`,
        );
      lines.push("");
    }

    const coloCell = (routing: ReturnType<typeof httpColoSummary>) =>
      Object.entries(routing.histogram)
        .map(([colo, count]) => `${colo}:${count}`)
        .join(", ") || "unavailable";

    lines.push(
      "## HTTP processing colo by Object",
      "",
      methodology.httpRouting,
      "",
      "| Size | Role | Sample | CF-Ray colo histogram | Accepted receipts / planned turns | Accepted receipts without a usable colo |",
      "|---:|---|---|---|---:|---:|",
    );
    for (const group of groups) {
      for (const object of objects.filter(
        (row) => row.role === group.role && row.size === group.size,
      ))
        lines.push(
          `| ${object.size} | ${object.role} | ${object.sample} | ${coloCell(object.httpRouting)} | ${object.httpRouting.acceptedReceiptTurns}/${object.httpRouting.expectedTurns} | ${object.httpRouting.unavailableColoTurns} |`,
        );
      lines.push(
        `| ${group.size} | ${group.role} | all | ${coloCell(group.httpRouting)} | ${group.httpRouting.acceptedReceiptTurns}/${group.httpRouting.expectedTurns} | ${group.httpRouting.unavailableColoTurns} |`,
      );
    }
    lines.push("", "## Reference evolution per Object", "", methodology.referenceEvolution, "");
    for (const metric of referenceMetrics) {
      lines.push(
        `### Reference ${metric} (ms)`,
        "",
        `| Size | Role | Sample | DO kind | Object ID prefix | ${PHASES.join(" | ")} | Warm reference median [p25, p75] (n/9) | Observed refs / 10 |`,
        `|---:|---|---:|---|---|${PHASES.map(() => "---:").join("|")}|---:|---|`,
      );
      for (const row of referenceEvolution) {
        const stats = row.metrics.find((value) => value.metric === metric);

        if (stats !== undefined)
          lines.push(
            `| ${row.size} | ${row.role} | ${row.sample} | ${row.kind} | ${row.objectIds.length === 0 ? "unavailable" : row.objectIds.map((id) => id.slice(0, 12)).join(", ")} | ${row.phases.map((phase) => number(phase.values[metric])).join(" | ")} | ${medianIqrCell(stats.warm)} | ${stats.all.n}/${stats.all.expectedN}; ${stats.all.status} |`,
          );
      }
      lines.push("");
    }
    lines.push(
      "## Observed alarm overlaps",
      "",
      methodology.observedAlarmOverlap,
      "",
      "| Size | Role | Deployed build set | Phase | Observed overlap turns | No observed overlap turns | Unavailable flags | Expected turns | Observed event entries |",
      "|---:|---|---|---|---:|---:|---:|---:|---:|",
    );
    for (const group of groups)
      for (const phase of ["cold", "warm"] as const) {
        const row = group.observedAlarmOverlap[phase];

        lines.push(
          `| ${group.size} | ${group.role} | ${buildCell(group)} | ${phase} | ${row.observedOverlapTurns} | ${row.noObservedOverlapTurns} | ${row.unavailableTurns} | ${row.expectedTurns} | ${sumCell(row.observedEventEntries)} |`,
        );
      }
    lines.push("");
    lines.push(
      "## Startup and storage",
      "",
      "| Size | Role | Startup p50 ms (n/3) | Seed bytes p50 (n/3) | Post bytes p50 (n/3) |",
      "|---:|---|---:|---:|---:|",
    );
    for (const row of groups)
      lines.push(
        `| ${row.size} | ${row.role} | ${cell(row.startupTimeMs)} | ${cell(row.seedStorageBytes)} | ${cell(row.postStorageBytes)} |`,
      );
    lines.push(
      "",
      "## All-owned DO window-observed cohort CPU",
      "",
      methodology.cohortMean,
      "",
      methodology.alarmTotals,
      "",
      "Alarm columns overlap the all-DO/extra columns and must not be added again.",
      "",
      methodology.normalizedCohortMean,
      "",
      "| Size | Role | Observed cohort CPU Σ ms | Gated observed total/10 p50 ms (n/3) | Observed DO invocations Σ | Observed extra CPU Σ ms | Observed extra invocations Σ |",
      "|---:|---|---:|---:|---:|---:|---:|",
    );
    for (const row of groups)
      lines.push(
        `| ${row.size} | ${row.role} | ${sumCell(row.cohortCpu.sumObservedDoCpuTimeMs)} | ${cell(row.cohortCpu.tenTurnMeanCpuTimeMs)} | ${sumCell(row.cohortCpu.allDoInvocationCount)} | ${sumCell(row.cohortCpu.extraTraceCpuTimeMs)} | ${sumCell(row.cohortCpu.extraTraceInvocationCount)} |`,
      );
    lines.push(
      "",
      "| Size | Role | Normalized cohort mean median [p25, p75] (ms CPU / ms reference CPU; Objects/3) |",
      "|---:|---|---:|",
    );
    for (const row of groups)
      lines.push(
        `| ${row.size} | ${row.role} | ${medianIqrCell(row.cohortCpu.normalizedTenTurnMeanCpuRatio)} |`,
      );
    for (const metric of cohortMetrics) {
      lines.push(
        "",
        `### ${metric}: paired cohort ratios`,
        "",
        "| Size | Pair | Ratio median [p25, p75] (Object pairs/3) | Control/base spread | Status |",
        "|---:|---|---:|---:|---|",
      );
      for (const row of cohortComparisons.filter((item) => item.metric === metric))
        lines.push(
          `| ${row.size} | ${row.numerator}/${row.denominator} | ${medianIqrCell(row.ratios)} | ${spreadCell(row.controlSpread)} | ${row.status} |`,
        );
    }
    lines.push(
      "",
      "Event CPU sums retain available values when other events lack CPU telemetry. Coverage is events with CPU / saved events, plus Objects with event lists / expected Objects and complete telemetry / expected Objects; missing event lists are not interpreted as zero work. With partial telemetry, extra traces can include unmatched RPC work and remain provisional observed totals.",
      "",
      "| Size | Role | Observed all-DO event CPU Σ ms | Observed extra event CPU Σ ms | Observed alarms Σ count | Observed alarm CPU Σ ms | Observed extra alarms Σ count | Observed extra alarm CPU Σ ms |",
      "|---:|---|---:|---:|---:|---:|---:|---:|",
    );
    for (const row of groups)
      lines.push(
        `| ${row.size} | ${row.role} | ${eventSumCell(row.cohortCpu.allDoEventCpuTimeMs)} | ${eventSumCell(row.cohortCpu.extraTraceEventCpuTimeMs)} | ${sumCell(row.cohortCpu.allDoAlarmCount)} | ${eventSumCell(row.cohortCpu.allDoAlarmCpuTimeMs)} | ${sumCell(row.cohortCpu.extraTraceAlarmCount)} | ${eventSumCell(row.cohortCpu.extraTraceAlarmCpuTimeMs)} |`,
      );
    lines.push(
      "",
      "## Collection coverage",
      "",
      "Expected/matched counts below come from the collector; the design requires 30 RPC rows per role/size. The summary's complete-collection count also requires an empty missing[] list, so ingress-only gaps remain incomplete even with ten matched primary DO RPCs and collector complete=true. Missing evidence lists can also be empty when collection is incomplete (for example an unstable final poll). Neither case changes completed workload receipts, and complete collection is not proof of exhaustive alarm capture.",
      "",
      "| Size | Role | Complete collections (n/3) | Collector expected RPC Σ | Matched RPC Σ | Missing evidence rows Σ |",
      "|---:|---|---:|---:|---:|---:|",
    );
    for (const row of groups)
      lines.push(
        `| ${row.size} | ${row.role} | ${row.collection.completeObjects}/${row.collection.expectedObjects} | ${sumCell(row.collection.expectedRpcRows)} | ${sumCell(row.collection.matchedRpcRows)} | ${sumCell(row.collection.missingEvidenceRows)} |`,
      );
    lines.push(
      "",
      "## Resolved deployment manifests",
      "",
      "Only manifests resolved for saved deployments appear below. The full candidate inventory and unresolved per-Object provenance remain in summary.json; current and archive copies can identify the same deployed bytes.",
      "",
      "| Role | Target runtime | Bench/wrapper SHA prefixes | Bench raw/gzip bytes | Wrapper raw/gzip bytes | Manifest |",
      "|---|---|---|---:|---:|---|",
    );
    for (const row of builds.filter((entry) =>
      objects.some(
        (object) =>
          object.buildStatus === "matched" && object.buildManifestFiles.includes(entry.file),
      ),
    ))
      lines.push(
        `| ${row.role} | ${runtimeLabel(row.role, row.build)} | ${row.build === null ? "—" : `${row.build.bench.sha256.slice(0, 8)}/${row.build.wrapper.sha256.slice(0, 8)}`} | ${row.build === null ? "—" : `${row.build.bench.rawBytes}/${row.build.bench.gzipBytes}`} | ${row.build === null ? "—" : `${row.build.wrapper.rawBytes}/${row.build.wrapper.gzipBytes}`} | ${path.relative(output, row.file)} |`,
      );
    lines.push("", "## Completeness", "", "| Issue | Count |", "|---|---:|");
    for (const [kind, count] of Object.entries(summary.completeness.issueCounts))
      lines.push(`| ${kind} | ${count} |`);
    lines.push(
      "",
      `Recorded measurement failures: ${recordedFailures.length}. Row status counts: ${JSON.stringify(summary.completeness.rowStatuses)}.`,
      "",
      "Per-Object values, ratio operands, input status, missing/failed row reasons, invocation outcomes, and all quantiles are retained in summary.json.",
      "",
    );
    yield* fs.makeDirectory(output, { recursive: true });
    for (const [name, content] of [
      ["summary.json", JSON.stringify(summary, null, 2) + "\n"],
      ["tables.md", lines.join("\n")],
    ]) {
      yield* fs.writeFileString(path.join(output, `${name}.tmp`), content);
      yield* fs.rename(path.join(output, `${name}.tmp`), path.join(output, name));
    }
    yield* Console.log(
      `${status}: ${path.join(output, "summary.json")} and ${path.join(output, "tables.md")}`,
    );
  }),
).pipe(
  Command.withDescription(
    "Summarize saved Cloudflare benchmark artifacts without running workloads or contacting Cloudflare.",
  ),
);

if (import.meta.main)
  command.pipe(
    Command.run({ version: "1.0.0" }),
    Effect.provide(BunServices.layer),
    BunRuntime.runMain,
  );
