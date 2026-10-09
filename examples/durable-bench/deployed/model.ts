import { Schema } from "effect";

import {
  ColdResult,
  Identity,
  IsolateState,
  PaddingResult,
  Query,
  StorageTrace,
  Target,
  Timeline,
  ProfileCapture,
} from "./worker/protocol.ts";

export const BuildLabel = Schema.Literals(["working", "baseline", "candidate"]);
export type BuildLabel = typeof BuildLabel.Type;
export const IsolatedTarget = Schema.Literals(["yielded", "pi", "bare"]);
export type IsolatedTarget = typeof IsolatedTarget.Type;

export interface Options {
  readonly targets: readonly (typeof Target.Type)[];
  readonly sizes: readonly number[];
  readonly ttft: readonly (0 | 400)[];
  readonly objects: number;
  readonly repeats: number;
  readonly concurrency: number;
  readonly cold: boolean;
  readonly cpu: boolean;
  readonly keep: boolean;
  readonly rigorous: boolean;
  readonly isolate?: boolean;
  readonly coldMode?: "object" | "fresh";
  readonly profile?: boolean;
  readonly order?: "ABBA" | "BAAB" | "AABB";
  readonly baseline?: string;
  readonly candidate?: string;
  readonly storageProbe: "none" | "untouched" | "touched";
  readonly paddingMiB: number;
}

export const Invocation = Schema.Struct({
  kind: Schema.String,
  outcome: Schema.String,
  cpuMs: Schema.NullOr(Schema.Number),
  wallMs: Schema.NullOr(Schema.Number),
  target: Schema.optionalKey(Target),
  object: Schema.optionalKey(Schema.String),
  sample: Schema.optionalKey(Schema.String),
  worker: Schema.optionalKey(Schema.String),
});

export type Invocation = typeof Invocation.Type;

export const Sample = Schema.Struct({
  ...Query.fields,
  build: BuildLabel,
  epoch: Schema.Natural,
  repeat: Schema.Natural,
  state: Schema.Literals(["cold", "fresh-first-turn", "warmup", "warm", "profile"]),
  status: Schema.Literals(["running", "ok", "excluded", "failed", "skipped"]),
  outcome: Schema.optionalKey(Schema.Literals(["completed", "unknown", "not-sent"])),
  worker: Schema.optionalKey(Schema.String),
  expectedBuild: Schema.optionalKey(Schema.String),
  resetBuild: Schema.optionalKey(Schema.String),
  resetBefore: Schema.optionalKey(Identity),
  identity: Schema.optionalKey(Identity),
  buildVerified: Schema.optionalKey(Schema.Boolean),
  freshVerified: Schema.optionalKey(Schema.Boolean),
  exclusionReasons: Schema.optionalKey(Schema.Array(Schema.String)),
  driverMs: Schema.optionalKey(Schema.Number),
  admissionMs: Schema.optionalKey(Schema.Number),
  startedMs: Schema.optionalKey(Schema.Number),
  firstModelMs: Schema.optionalKey(Schema.Number),
  firstDispatchIoMs: Schema.optionalKey(Schema.Number),
  providerColo: Schema.optionalKey(Schema.NullOr(Schema.String)),
  timeline: Schema.optionalKey(Timeline),
  routing: Schema.optionalKey(Timeline),
  gapMs: Schema.optionalKey(Schema.Number),
  lastResponseToClientMs: Schema.optionalKey(Schema.Number),
  controllerMs: Schema.optionalKey(Schema.Number),
  colo: Schema.optionalKey(Schema.NullOr(Schema.String)),
  controllerColo: Schema.optionalKey(Schema.NullOr(Schema.String)),
  fingerprintVerified: Schema.optionalKey(Schema.Boolean),
  coldVerified: Schema.optionalKey(Schema.Boolean),
  profileVerified: Schema.optionalKey(Schema.Boolean),
  residentVerified: Schema.optionalKey(Schema.Boolean),
  fingerprints: Schema.optionalKey(Schema.Array(Schema.String)),
  storage: Schema.optionalKey(StorageTrace),
  padding: Schema.optionalKey(PaddingResult),
  storageGroup: Schema.optionalKey(Schema.Literals(["grown", "control"])),
  seedBytes: Schema.optionalKey(Schema.Natural),
  databaseBytes: Schema.optionalKey(Schema.Natural),
  error: Schema.optionalKey(Schema.String),
});

export type Sample = typeof Sample.Type;

export const Health = Schema.Struct({
  ok: Schema.Literal(true),
  build: Schema.String,
  workerIsolate: Schema.optionalKey(IsolateState),
});

export const Upload = Schema.Struct({
  target: IsolatedTarget,
  worker: Schema.String,
  phase: Schema.Literals(["seed", "epoch"]),
  label: BuildLabel,
  epoch: Schema.optionalKey(Schema.Natural),
  expectedBuild: Schema.String,
  previousBuild: Schema.optionalKey(Schema.String),
  sourceSha256: Schema.String,
  uploadedSha256: Schema.String,
  resetCount: Schema.Natural,
  status: Schema.Literals(["uploading", "ready", "failed"]),
  health: Schema.optionalKey(Health),
  readinessFailures: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        ingress: Schema.Literals(["controller", "driver", "object"]),
        attempt: Schema.Natural,
        error: Schema.String,
      }),
    ),
  ),
  error: Schema.optionalKey(Schema.String),
});

export type Upload = typeof Upload.Type;

export const Reset = Schema.Struct({
  ...Query.fields,
  epoch: Schema.Natural,
  expectedBuild: Schema.String,
  status: Schema.Literals(["running", "ok", "failed"]),
  response: Schema.optionalKey(ColdResult),
  error: Schema.optionalKey(Schema.String),
});

export type Reset = typeof Reset.Type;

export const Cleanup = Schema.Struct({
  verified: Schema.Boolean,
  workers: Schema.Array(Schema.String),
  namespaces: Schema.Array(Schema.String),
});

export type Cleanup = typeof Cleanup.Type;

export const Result = Schema.Struct({
  version: Schema.Literal(1),
  run: Schema.String,
  revision: Schema.String,
  dirty: Schema.Boolean,
  startedAt: Schema.String,
  wallMs: Schema.Number,
  infrastructureReused: Schema.Boolean,
  options: Schema.Json,
  builds: Schema.Array(
    Schema.Struct({
      label: Schema.String,
      revision: Schema.String,
      sha256: Schema.String,
      target: Schema.optionalKey(IsolatedTarget),
    }),
  ),
  sequence: Schema.optionalKey(Schema.Array(BuildLabel)),
  uploads: Schema.optionalKey(Schema.Array(Upload)),
  resets: Schema.optionalKey(Schema.Array(Reset)),
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
  failures: Schema.Array(Schema.String),
  cpu: Schema.optionalKey(Schema.Array(Invocation)),
  unmatchedCpuMarkers: Schema.optionalKey(Schema.Natural),
  profiles: Schema.optionalKey(Schema.Array(ProfileCapture)),
  cleanup: Schema.optionalKey(Cleanup),
  kept: Schema.Boolean,
  complete: Schema.Boolean,
});

export type Result = typeof Result.Type;

export const MeasureRequest = Schema.Struct({
  query: Query,
  targetUrl: Schema.String,
  expectedBuild: Schema.optionalKey(Schema.NonEmptyString),
});

export const MeasureResponse = Schema.Struct({
  ok: Schema.Literal(true),
  driverMs: Schema.Number,
  observedMs: Schema.Number,
  admissionMs: Schema.optionalKey(Schema.Number),
  startedMs: Schema.optionalKey(Schema.Number),
  firstModelMs: Schema.optionalKey(Schema.Number),
  firstDispatchIoMs: Schema.optionalKey(Schema.Number),
  providerColo: Schema.optionalKey(Schema.NullOr(Schema.String)),
  timeline: Schema.optionalKey(Timeline),
  routing: Schema.optionalKey(Timeline),
  colo: Schema.NullOr(Schema.String),
});
