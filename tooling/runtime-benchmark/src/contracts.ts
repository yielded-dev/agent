import { Effect, Schema } from "effect";

export const FIXTURE_VERSION = "runtime-v4";

export class BenchmarkError extends Schema.TaggedError<BenchmarkError>()("BenchmarkError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export const check = Effect.fn("benchmark.check")(function* (condition: boolean, message: string) {
  if (!condition) return yield* BenchmarkError.make({ message });
});

/** Exact selectors retain fixture order; omission selects the unchanged full matrix. */
export const selectCaseNames = Effect.fn("benchmark.selectCaseNames")(function* (
  available: ReadonlyArray<string>,
  requested: ReadonlyArray<string> = [],
) {
  yield* check(new Set(requested).size === requested.length, "Duplicate --case selectors");
  const unknown = requested.filter((name) => !available.includes(name));

  yield* check(unknown.length === 0, `Unknown --case: ${unknown.join(", ")}; use --list-cases`);

  return requested.length === 0 ? available : available.filter((name) => requested.includes(name));
});

export const Profile = Schema.Literals(["smoke", "pr", "extended", "archive"]);
export type Profile = typeof Profile.Type;

export const Case = Schema.Struct({
  name: Schema.String,
  kind: Schema.Literals(["run", "stream", "tools", "durable", "recovery", "ledger"]),
  chunks: Schema.Natural,
  outputBytes: Schema.Natural,
  historyBytes: Schema.Natural,
  records: Schema.Natural,
  rounds: Schema.Natural,
});

export type Case = typeof Case.Type;

const workload = (
  name: string,
  kind: Case["kind"],
  options: Partial<Omit<Case, "name" | "kind">> = {},
): Case => ({
  name,
  kind,
  chunks: 1,
  outputBytes: 0,
  historyBytes: 0,
  records: 0,
  rounds: 0,
  ...options,
});

/** Defaults stay fixed; explicit PR selection also permits the existing 16-record fresh case. */
export const casesFor = (profile: Profile, includeSelectable = false): ReadonlyArray<Case> => [
  workload("small-run", "run"),
  workload("small-stream", "stream"),
  ...[1, 64, 1_024, 4_096].map((chunks) =>
    workload(`stream-64k-${chunks}`, "stream", { chunks, outputBytes: 65_536 }),
  ),
  ...[65_536, 1_048_576].map((historyBytes) =>
    workload(`history-${historyBytes}`, "run", { historyBytes }),
  ),
  workload("parallel-tools-8", "tools", { rounds: 1 }),
  workload("tool-rounds-4", "tools", { rounds: 4 }),
  ...[
    0,
    ...(profile === "smoke" ? [16] : [256, 2_048]),
    ...(profile === "extended" || profile === "archive" ? [8_192] : []),
    ...(profile === "archive" ? [100_000] : []),
  ].flatMap((records) => [
    workload(`durable-fresh-${records}`, "durable", { records }),
    workload(`checkpoint-recovery-${records}`, "recovery", { records }),
    workload(`settled-ledger-${records}`, "ledger", { records }),
  ]),
  ...(includeSelectable && profile === "pr"
    ? [workload("durable-fresh-16", "durable", { records: 16 })]
    : []),
];

/** Resident SQLite is a separate workload from reopening a host in ordinary A/B samples. */
export const steadyStateCases: ReadonlyArray<Case> = [
  workload("sqlite-tool-rounds-4", "durable", { rounds: 4, outputBytes: 32 }),
];

export const STEADY_STATE = {
  warmupOperations: 500,
  operations: 1_000,
  samplingIntervalMicros: 1_000,
} as const;

export const STEADY_STATE_MEASUREMENT = "resident-operation-v1" as const;

export const SteadyStateResult = Schema.Struct({
  case: Schema.Literal("sqlite-tool-rounds-4"),
  workload: Schema.Literal(
    "resident file-backed SQLite host; fresh Thread and Submission per operation",
  ),
  capture: Schema.Literal(
    "one continuous operation loop; imports, host acquisition, warmup, reporting and host disposal excluded",
  ),
  provider: Schema.Literal("native Effect LanguageModel with Stream.make; no inference or network"),
  warmupOperations: Schema.Natural,
  warmupMs: Schema.Finite,
  operations: Schema.Natural,
  operationMs: Schema.Finite,
  profileDurationMs: Schema.NullOr(Schema.Finite),
  samplingIntervalMicros: Schema.NullOr(Schema.Literal(STEADY_STATE.samplingIntervalMicros)),
  measurement: Schema.optionalKey(Schema.Literal(STEADY_STATE_MEASUREMENT)),
  operationSamplesMs: Schema.optionalKey(
    Schema.Array(Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))),
  ),
  modelCalls: Schema.Natural,
  modelFinalizers: Schema.Natural,
  toolCalls: Schema.Natural,
  toolFinalizers: Schema.Natural,
  canonicalCompletions: Schema.Natural,
  inputBytes: Schema.Literal(32),
  outputBytes: Schema.Literal(32),
  toolResultBytes: Schema.Literal(32),
  syntheticDelayMs: Schema.Literal(0),
});

export type SteadyStateResult = typeof SteadyStateResult.Type;

export const WorkerOptions = Schema.Struct({
  cold: Schema.Boolean,
  profile: Profile,
  mode: Schema.optionalKey(
    Schema.Literals(["comparison", "cpu-profile", "steady-state-profile", "steady-state"]),
  ),
  cpuProfile: Schema.optionalKey(Schema.String),
  cases: Schema.optionalKey(Schema.Array(Schema.String).check(Schema.isMinLength(1))),
  warmups: Schema.Natural.check(Schema.isLessThanOrEqualTo(20)),
  samples: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
  output: Schema.String,
});

export const SamplePhase = Schema.Literals(["setup", "checkpoint", "operation", "verification"]);
export type SamplePhase = typeof SamplePhase.Type;

export const SampleProgress = Schema.Struct({
  case: Schema.String,
  ordinal: Schema.Natural,
  warmup: Schema.Boolean,
  phase: SamplePhase,
  elapsedMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
});

export type SampleProgress = typeof SampleProgress.Type;

export const Sample = Schema.Struct({
  case: Schema.String,
  ordinal: Schema.Natural,
  warmup: Schema.Boolean,
  totalMs: Schema.Finite,
  attemptMs: Schema.Finite,
  setupMs: Schema.Finite,
  failurePhase: Schema.NullOr(SamplePhase),
  modelEntryMs: Schema.NullOr(Schema.Finite),
  checkpointCreationMs: Schema.NullOr(Schema.Finite),
  retainedPromptMessages: Schema.Natural,
  modelCalls: Schema.Natural,
  finalizers: Schema.Natural,
  toolCalls: Schema.Natural,
  outputBytes: Schema.Natural,
  status: Schema.Literals(["passed", "failed"]),
  failure: Schema.NullOr(Schema.String),
});

export type Sample = typeof Sample.Type;

export const WorkerReport = Schema.Struct({
  fixture: Schema.Literal(FIXTURE_VERSION),
  profile: Profile,
  mode: WorkerOptions.fields.mode,
  cases: WorkerOptions.fields.cases,
  runtime: Schema.String,
  platform: Schema.String,
  architecture: Schema.String,
  active: Schema.NullOr(SampleProgress),
  failure: Schema.NullOr(Schema.String),
  samples: Schema.Array(Sample),
  steadyState: Schema.optionalKey(SteadyStateResult),
});

export type WorkerReport = typeof WorkerReport.Type;

export const completeBatch = (
  report: WorkerReport,
  options: typeof WorkerOptions.Type,
): boolean => {
  if (options.mode === "steady-state-profile" || options.mode === "steady-state") {
    const result = report.steadyState;

    return (
      report.mode === options.mode &&
      report.profile === options.profile &&
      !options.cold &&
      options.cases?.length === 1 &&
      options.cases[0] === "sqlite-tool-rounds-4" &&
      report.cases?.length === 1 &&
      report.cases[0] === "sqlite-tool-rounds-4" &&
      report.active === null &&
      report.failure === null &&
      report.samples.length === 0 &&
      result !== undefined &&
      result.operations === STEADY_STATE.operations &&
      result.warmupOperations === STEADY_STATE.warmupOperations &&
      result.warmupMs > 0 &&
      result.operationMs > 0 &&
      (options.mode === "steady-state"
        ? options.cpuProfile === undefined &&
          result.profileDurationMs === null &&
          result.samplingIntervalMicros === null &&
          result.measurement === STEADY_STATE_MEASUREMENT &&
          result.operationSamplesMs?.length === STEADY_STATE.operations &&
          result.operationSamplesMs.every((ms) => Number.isFinite(ms) && ms >= 0)
        : options.cpuProfile !== undefined &&
          result.profileDurationMs !== null &&
          result.profileDurationMs > 0 &&
          result.samplingIntervalMicros === STEADY_STATE.samplingIntervalMicros &&
          result.measurement === undefined &&
          result.operationSamplesMs === undefined) &&
      result.modelCalls === result.operations * 5 &&
      result.modelFinalizers === result.modelCalls &&
      result.toolCalls === result.operations * 4 &&
      result.toolFinalizers === result.toolCalls &&
      result.canonicalCompletions === result.operations
    );
  }

  const available = options.cold
    ? casesFor(options.profile).slice(0, 1)
    : casesFor(options.profile, (options.cases?.length ?? 0) > 0);

  const names = options.cases ?? available.map(({ name }) => name);
  const workloads = available.filter(({ name }) => names.includes(name));

  if (
    workloads.length === 0 ||
    names.length !== workloads.length ||
    (report.mode ?? "comparison") !== (options.mode ?? "comparison") ||
    report.steadyState !== undefined ||
    (options.cases !== undefined &&
      (report.cases?.length !== names.length ||
        !workloads.every(({ name }, index) => report.cases?.[index] === name)))
  )
    return false;

  const expected = new Set(
    workloads.flatMap((workload) =>
      Array.from(
        { length: options.warmups + options.samples },
        (_, ordinal) => `${workload.name}:${ordinal}`,
      ),
    ),
  );

  if (
    report.profile !== options.profile ||
    report.samples.length !== expected.size ||
    report.active !== null ||
    report.failure !== null
  )
    return false;

  return report.samples.every(
    (sample) =>
      expected.delete(`${sample.case}:${sample.ordinal}`) &&
      sample.warmup === sample.ordinal < options.warmups &&
      sample.status === "passed" &&
      sample.failure === null &&
      sample.failurePhase === null &&
      sample.totalMs >= 0 &&
      sample.setupMs >= 0 &&
      sample.attemptMs >= sample.setupMs + sample.totalMs &&
      sample.modelEntryMs !== null &&
      sample.modelEntryMs >= 0 &&
      sample.modelEntryMs <= sample.totalMs &&
      sample.modelCalls > 0 &&
      sample.modelCalls === sample.finalizers,
  );
};

export const summary = (values: ReadonlyArray<number>) => {
  const sorted = [...values].sort((a, b) => a - b);

  const quantile = (p: number) => {
    const index = (sorted.length - 1) * p;
    const lower = sorted[Math.floor(index)] ?? 0;

    return lower + ((sorted[Math.ceil(index)] ?? lower) - lower) * (index % 1);
  };

  return {
    count: sorted.length,
    median: quantile(0.5),
    q1: quantile(0.25),
    q3: quantile(0.75),
    min: sorted[0] ?? 0,
    max: sorted.at(-1) ?? 0,
  };
};
