import { Schema } from "effect";

import { Query, Target } from "./worker/protocol.ts";

export const ProfileType = Schema.Literals(["cpu", "memory"]);

export const Profile = Schema.Struct({
  type: ProfileType,
  target: Query.fields.target,
  object: Query.fields.object,
  history: Query.fields.history,
  ttftMs: Query.fields.ttftMs,
  build: Query.fields.expectedBuild,
  durationMs: Schema.Int,
  file: Schema.String,
  sourceMap: Schema.String,
});

export type Profile = typeof Profile.Type;

export interface Options {
  readonly targets: readonly (typeof Target.Type)[];
  readonly sizes: readonly number[];
  readonly ttft: readonly (0 | 400)[];
  readonly textStreaming: boolean;
  readonly objects: number;
  readonly repeats: number;
  readonly concurrency: number;
  readonly cold: boolean;
  readonly cpu: boolean;
  readonly profiles: readonly (typeof ProfileType.Type)[];
  readonly keep: boolean;
  readonly rigorous: boolean;
  readonly baseline?: string;
  readonly candidate?: string;
}

export const Invocation = Schema.Struct({
  kind: Schema.String,
  outcome: Schema.String,
  cpuMs: Schema.NullOr(Schema.Number),
  wallMs: Schema.NullOr(Schema.Number),
  target: Schema.optionalKey(Target),
  object: Schema.optionalKey(Schema.String),
  sample: Schema.optionalKey(Schema.String),
});

export type Invocation = typeof Invocation.Type;

export const FirstTextSource = Schema.Literals(["watchText", "watchEvents", "settlementRecord"]);
export type FirstTextSource = typeof FirstTextSource.Type;

export const Sample = Schema.Struct({
  ...Query.fields,
  build: Schema.Literals(["working", "baseline", "candidate"]),
  epoch: Schema.Natural,
  repeat: Schema.Natural,
  state: Schema.Literals(["cold", "warmup", "warm"]),
  status: Schema.Literals(["running", "ok", "failed"]),
  driverMs: Schema.optionalKey(Schema.Number),
  firstTextMs: Schema.optionalKey(Schema.NullOr(Schema.Number)),
  firstText: Schema.optionalKey(Schema.String),
  firstTextSource: Schema.optionalKey(FirstTextSource),
  observationMs: Schema.optionalKey(Schema.NullOr(Schema.Number)),
  firstModelRequestMs: Schema.optionalKey(Schema.Number),
  admissionMs: Schema.optionalKey(Schema.Number),
  gapMs: Schema.optionalKey(Schema.Number),
  lastResponseToClientMs: Schema.optionalKey(Schema.Number),
  controllerMs: Schema.optionalKey(Schema.Number),
  colo: Schema.optionalKey(Schema.NullOr(Schema.String)),
  controllerColo: Schema.optionalKey(Schema.NullOr(Schema.String)),
  fingerprintVerified: Schema.optionalKey(Schema.Boolean),
  coldVerified: Schema.optionalKey(Schema.Boolean),
  residentVerified: Schema.optionalKey(Schema.Boolean),
  buildVerified: Schema.optionalKey(Schema.Boolean),
  objectBuild: Schema.optionalKey(Schema.String),
  directoryUsed: Schema.optionalKey(Schema.Boolean),
  fingerprints: Schema.optionalKey(Schema.Array(Schema.String)),
  error: Schema.optionalKey(Schema.String),
});

export type Sample = typeof Sample.Type;

export const Cleanup = Schema.Struct({
  verified: Schema.Boolean,
  workers: Schema.Array(Schema.String),
  namespaces: Schema.Array(Schema.String),
});

export type Cleanup = typeof Cleanup.Type;

export const Result = Schema.Struct({
  version: Schema.Literal(2),
  run: Schema.String,
  revision: Schema.String,
  dirty: Schema.Boolean,
  startedAt: Schema.String,
  wallMs: Schema.Number,
  infrastructureReused: Schema.Boolean,
  options: Schema.Json,
  builds: Schema.Array(
    Schema.Struct({ label: Schema.String, revision: Schema.String, sha256: Schema.String }),
  ),
  fixtures: Schema.Array(
    Schema.Struct({
      target: Target,
      history: Schema.Int,
      fingerprint: Schema.String,
      tables: Schema.Record(Schema.String, Schema.Natural),
      mode: Schema.Literals(["import", "replay"]),
      fallbackReason: Schema.optionalKey(Schema.String),
    }),
  ),
  samples: Schema.Array(Sample),
  readiness: Schema.Array(
    Schema.Struct({
      target: Target,
      object: Schema.String,
      epoch: Schema.Natural,
      expectedBuild: Schema.String,
      objectBuild: Schema.String,
      directoryBuild: Schema.optionalKey(Schema.String),
      attempt: Schema.Natural,
      waitMs: Schema.Number,
    }),
  ),
  failures: Schema.Array(Schema.String),
  profiles: Schema.Array(Profile),
  cpu: Schema.optionalKey(Schema.Array(Invocation)),
  unmatchedCpuMarkers: Schema.optionalKey(Schema.Natural),
  cleanup: Schema.optionalKey(Cleanup),
  kept: Schema.Boolean,
  complete: Schema.Boolean,
});

export type Result = typeof Result.Type;

export const MeasureRequest = Schema.Struct({ query: Query, targetUrl: Schema.String });

export const MeasureResponse = Schema.Struct({
  ok: Schema.Literal(true),
  driverMs: Schema.Number,
  startedMs: Schema.Number,
  observedMs: Schema.Number,
  firstTextMs: Schema.NullOr(Schema.Number),
  firstText: Schema.optionalKey(Schema.String),
  firstTextSource: Schema.optionalKey(FirstTextSource),
  observationMs: Schema.NullOr(Schema.Number),
  admissionMs: Schema.optionalKey(Schema.Number),
  colo: Schema.NullOr(Schema.String),
});

export const Health = Schema.Struct({ ok: Schema.Literal(true), build: Schema.String });
