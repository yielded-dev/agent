import { Schema } from "effect";

export const SLUG = "cf-bench-8914";
export const SIZES = [50, 250, 1000] as const;
export const SAMPLES = [0, 1, 2] as const;

// Sample 0 at size 50 was consumed by the invalid rollout pilot. Preserve it
// and provision one fresh replacement; never replay a measured fixture.
export const FIXTURES = [
  ...SIZES.flatMap((size) => SAMPLES.map((sample) => ({ size, sample }))),
  { size: 50, sample: 3 },
] as const;

export const measurementSamples = (size: number): readonly number[] =>
  size === 50 ? [1, 2, 3] : SAMPLES;

export const ROLES = ["base", "head", "control", "pinned", "pi", "tardie"] as const;
export const Role = Schema.Literals(ROLES);
export type Role = typeof Role.Type;

export const PHASES = [
  "cold",
  "warm1",
  "warm2",
  "warm3",
  "warm4",
  "warm5",
  "warm6",
  "warm7",
  "warm8",
  "warm9",
] as const;

export const REFERENCE_ITERATIONS = 10_000_000;
export const REFERENCE_ALGORITHM = "mix32-v1";
export const REFERENCE_CHECKSUM = 623056721;

export const FINGERPRINTS: Readonly<Record<number, string>> = {
  50: "b017b487524e44a4",
  250: "dcea9f30b0917245",
  1000: "ac520308146f2a8f",
  3500: "0a8c8e4b0d9a0794",
};

export class BenchError extends Schema.TaggedError<BenchError>()("BenchError", {
  message: Schema.String,
}) {}

export const Identity = Schema.Struct({
  objectId: Schema.String,
  moduleId: Schema.String,
  runtimeId: Schema.String,
  version: Schema.String,
  generation: Schema.String,
  actorObjectId: Schema.optionalKey(Schema.String),
  protocol: Schema.optionalKey(Schema.String),
});

export const Receipt = Schema.Struct({
  ...Identity.fields,
  insideWallMs: Schema.Number,
  phase: Schema.String,
});

export const ReferenceReceipt = Schema.Struct({
  ...Identity.fields,
  phase: Schema.String,
  algorithm: Schema.Literal(REFERENCE_ALGORITHM),
  iterations: Schema.Literal(REFERENCE_ITERATIONS),
  checksum: Schema.Literal(REFERENCE_CHECKSUM),
});

export const ReferenceSet = Schema.Struct({
  thread: ReferenceReceipt,
  actor: Schema.optionalKey(ReferenceReceipt),
});

export const ReferenceObservation = Schema.Struct({
  started: Schema.Number,
  ended: Schema.Number,
  clientWallMs: Schema.Number,
  cfRay: Schema.optionalKey(Schema.String),
  value: ReferenceSet,
  actorTiming: Schema.optionalKey(
    Schema.Struct({
      started: Schema.Number,
      ended: Schema.Number,
      clientWallMs: Schema.Number,
      cfRay: Schema.optionalKey(Schema.String),
    }),
  ),
});

export const Stats = Schema.Struct({
  bytes: Schema.Number,
  tables: Schema.Record(Schema.String, Schema.Number),
  checkpoint: Schema.optionalKey(Schema.Number),
});

export const WatchdogSnapshot = Schema.Struct({
  alarm: Schema.NullOr(Schema.Number),
  watchdog: Schema.Record(
    Schema.String,
    Schema.Struct({
      target: Schema.Struct({
        actor: Schema.String,
        instance: Schema.String,
        thread: Schema.optionalKey(Schema.String),
      }),
      generation: Schema.Number,
      budgetGeneration: Schema.optionalKey(Schema.Number),
      progressCursor: Schema.Number,
      attempts: Schema.Number,
      consecutiveNoProgress: Schema.Number,
      nextWakeAt: Schema.NullOr(Schema.Number),
      status: Schema.Literals(["pending", "blocked"]),
      reason: Schema.optionalKey(Schema.String),
    }),
  ),
});

export const SerialSeedReceipt = Schema.Struct({
  fingerprint: Schema.String,
  steps: Schema.Array(
    Schema.Struct({ id: Schema.String, fingerprint: Schema.String, polls: Schema.Int }),
  ),
});

export const Target = Schema.Struct({
  role: Role,
  name: Schema.String,
  bundleDirectory: Schema.String,
  generation: Schema.String,
  namespaces: Schema.Array(Schema.String),
  cleanupRequired: Schema.Boolean,
  cleanupComplete: Schema.Boolean,
});

export type Target = typeof Target.Type;

export const Resources = Schema.Struct({
  run: Schema.String,
  accountDigest: Schema.String,
  privateDirectory: Schema.String,
  output: Schema.String,
  targets: Schema.Array(Target),
});

export const PrivateState = Schema.Struct({ token: Schema.String, accountDigest: Schema.String });

export const Sample = Schema.Struct({
  role: Role,
  size: Schema.Number,
  sample: Schema.Number,
  phase: Schema.String,
  started: Schema.Number,
  ended: Schema.Number,
  clientWallMs: Schema.Number,
  receipt: Receipt,
  cfRay: Schema.optionalKey(Schema.String),
  reference: Schema.optionalKey(ReferenceObservation),
});

export const Seed = Schema.Struct({
  role: Role,
  size: Schema.Number,
  sample: Schema.Number,
  fingerprint: Schema.String,
  stats: Stats,
  identity: Identity,
});

export const RoleWindow = Schema.Struct({
  from: Schema.Number,
  to: Schema.Number,
  workloadFrom: Schema.Number,
  workloadTo: Schema.Number,
});

export const CohortWindow = Schema.Struct({
  from: Schema.Number,
  to: Schema.Number,
  roleWindows: Schema.Record(Schema.String, RoleWindow),
  alarmTailMs: Schema.Literal(35_000),
});
