import { Schema } from "effect";

import {
  Counts,
  MetricsResult,
  ObjectPhases,
  ProviderCall,
  History,
  Identity,
  Query,
  RouteProbe,
  SeedCheckpoint,
  SeedResult,
  Target,
} from "./worker/protocol.ts";

export const ProfileType = Schema.Literals(["cpu", "memory"]);

export const Profile = Schema.Struct({
  type: ProfileType,
  target: Query.fields.target,
  object: Query.fields.object,
  history: Query.fields.history,
  ttftMs: Query.fields.ttftMs,
  build: Query.fields.expectedBuild,
  durationMs: Schema.Int,
  afterHistoryTurns: Schema.optionalKey(Schema.Natural),
  completedAtEnd: Schema.optionalKey(Schema.Natural),
  file: Schema.String,
  sourceMap: Schema.String,
});

export type Profile = typeof Profile.Type;

export const Options = Schema.Struct({
  verifyIncarnationRecovery: Schema.optionalKey(Schema.Boolean),
  fixtureDirectory: Schema.optionalKey(Schema.String),
  restoreBytes: Schema.optionalKey(Schema.Int),
  restoreEachPass: Schema.optionalKey(Schema.Boolean),
  buildHistory: Schema.Boolean,
  targets: Schema.Array(Target).check(Schema.isMinLength(1)),
  sizes: Schema.Array(History).check(Schema.isMinLength(1)),
  ttft: Schema.Array(Schema.Literals([0, 400])).check(Schema.isMinLength(1)),
  textStreaming: Schema.Boolean,
  objects: History,
  repeats: History,
  concurrency: History,
  buildConcurrency: History,
  cold: Schema.Boolean,
  cpu: Schema.Boolean,
  profiles: Schema.Array(ProfileType),
  profileAfter: Schema.optionalKey(Schema.Int),
  keep: Schema.Boolean,
  rigorous: Schema.Boolean,
  baseline: Schema.optionalKey(Schema.String),
  candidate: Schema.optionalKey(Schema.String),
});

export type Options = typeof Options.Type;

export const Invocation = Schema.Struct({
  invocationId: Schema.optionalKey(Schema.String),
  invocationIds: Schema.optionalKey(Schema.Array(Schema.String)),
  kinds: Schema.optionalKey(Schema.Array(Schema.String)),
  kind: Schema.String,
  invocationType: Schema.optionalKey(Schema.String),
  outcome: Schema.String,
  cpuMs: Schema.NullOr(Schema.Number),
  wallMs: Schema.NullOr(Schema.Number),
  target: Schema.optionalKey(Target),
  object: Schema.optionalKey(Schema.String),
  sample: Schema.optionalKey(Schema.String),
});

export type Invocation = typeof Invocation.Type;

export const FirstTextSource = Schema.Literals([
  "watchText",
  "watchEvents",
  "nativeStream",
  "settlementRecord",
]);

export type FirstTextSource = typeof FirstTextSource.Type;

export const WorkerBuild = Schema.Struct({ bundle: Schema.String, build: Schema.String });
export type WorkerBuild = typeof WorkerBuild.Type;

export const Sample = Schema.Struct({
  ...Query.fields,
  build: Schema.Literals(["working", "baseline", "candidate"]),
  epoch: Schema.Natural,
  order: Schema.optionalKey(Schema.Literals(["AB", "BA"])),
  repeat: Schema.Natural,
  state: Schema.Literals(["cold", "warmup", "warm", "preparation"]),
  preparationTables: Schema.optionalKey(Counts),
  status: Schema.Literals(["running", "ok", "failed", "invalidated"]),
  driverMs: Schema.optionalKey(Schema.Number),
  firstTextMs: Schema.optionalKey(Schema.NullOr(Schema.Number)),
  observerToCompletionMs: Schema.optionalKey(Schema.Number),
  observerToFirstTextMs: Schema.optionalKey(Schema.Number),
  firstText: Schema.optionalKey(Schema.String),
  firstTextSource: Schema.optionalKey(FirstTextSource),
  settlementRecordRead: Schema.optionalKey(Schema.Boolean),
  observationMs: Schema.optionalKey(Schema.NullOr(Schema.Number)),
  constructorAndProbeMs: Schema.optionalKey(Schema.Number),
  primeMs: Schema.optionalKey(Schema.Number),
  bytes: Schema.optionalKey(Schema.Natural),
  providerColos: Schema.optionalKey(Schema.Array(Schema.String)),
  objectToFirstModelMs: Schema.optionalKey(Schema.Number),
  providerCalls: Schema.optionalKey(Schema.Array(ProviderCall)),
  objectPhases: Schema.optionalKey(ObjectPhases),
  cpuInvocationIds: Schema.optionalKey(Schema.Array(Schema.String)),
  providerFirstTokenMs: Schema.optionalKey(Schema.Number),
  firstTextForwardedMs: Schema.optionalKey(Schema.Number),
  settlementMs: Schema.optionalKey(Schema.Number),
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
  isolate: Schema.optionalKey(Schema.String),
  incarnation: Schema.optionalKey(Schema.String),
  invalidatedIdentity: Schema.optionalKey(Identity),
  directoryUsed: Schema.optionalKey(Schema.Boolean),
  fingerprints: Schema.optionalKey(Schema.Array(Schema.String)),
  error: Schema.optionalKey(Schema.String),
});

export type Sample = typeof Sample.Type;

export const Cleanup = Schema.Struct({
  verified: Schema.Boolean,
  workers: Schema.Array(Schema.String),
  namespaces: Schema.Array(Schema.String),
  buckets: Schema.Array(Schema.String),
});

export type Cleanup = typeof Cleanup.Type;

export const BuildEvent = Schema.Union([
  Schema.TaggedStruct("Batch", {
    result: SeedResult,
    driverMs: Schema.Number,
    colo: Schema.NullOr(Schema.String),
  }),
  Schema.TaggedStruct("Failed", {
    completed: Schema.Natural,
    driverMs: Schema.Number,
    error: Schema.String,
    colo: Schema.NullOr(Schema.String),
  }),
]);

export type BuildEvent = typeof BuildEvent.Type;

export const HistoryBuild = Schema.Struct({
  target: Target,
  object: Schema.String,
  history: Schema.Int,
  ttftMs: Query.fields.ttftMs,
  status: Schema.Literals(["running", "paused", "ok", "failed"]),
  completed: Schema.Natural,
  startedMs: Schema.optionalKey(Schema.Number),
  resumed: Schema.optionalKey(Schema.Boolean),
  driverMs: Schema.optionalKey(Schema.Number),
  batches: Schema.Array(BuildEvent.members[0]),
  error: Schema.optionalKey(Schema.String),
});

export type HistoryBuild = typeof HistoryBuild.Type;

export const Result = Schema.Struct({
  version: Schema.Literal(4),
  run: Schema.String,
  revision: Schema.String,
  dirty: Schema.Boolean,
  startedAt: Schema.String,
  wallMs: Schema.Number,
  phases: Schema.optionalKey(
    Schema.Array(Schema.Struct({ phase: Schema.String, ms: Schema.Number })),
  ),
  infrastructureReused: Schema.Boolean,
  infrastructureBuild: Schema.optionalKey(Schema.String),
  resumeCount: Schema.optionalKey(Schema.Natural),
  options: Schema.Json,
  builds: Schema.Array(
    Schema.Struct({ label: Schema.String, revision: Schema.String, sha256: Schema.String }),
  ),
  versions: Schema.Record(Schema.String, Schema.String),
  restores: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        object: Schema.String,
        target: Target,
        history: Schema.Int,
        epoch: Schema.optionalKey(Schema.Natural),
        build: Schema.optionalKey(Schema.String),
        requests: Schema.Int,
        ms: Schema.Number,
      }),
    ),
  ),
  histories: Schema.Array(HistoryBuild),
  fixtures: Schema.Array(
    Schema.Struct({
      target: Target,
      history: Schema.Int,
      fingerprint: Schema.String,
      tables: Schema.Record(Schema.String, Schema.Natural),
      mode: Schema.Literals(["import", "replay"]),
      label: Schema.optionalKey(Schema.String),
      formatKey: Schema.optionalKey(Schema.String),
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
      objectBuild: Schema.optionalKey(Schema.String),
      directoryBuild: Schema.optionalKey(Schema.String),
      phase: Schema.optionalKey(Schema.Literals(["routes", "identity", "reset"])),
      outcome: Schema.optionalKey(
        Schema.Literals(["ready", "placeholder", "wrong-build", "unavailable"]),
      ),
      routes: Schema.optionalKey(Schema.Array(RouteProbe)),
      driverBuild: Schema.optionalKey(Schema.String),
      error: Schema.optionalKey(Schema.String),
      attempt: Schema.Natural,
      waitMs: Schema.Number,
    }),
  ),
  failures: Schema.Array(Schema.String),
  profiles: Schema.Array(Profile),
  cpu: Schema.optionalKey(Schema.Array(Invocation)),
  cleanup: Schema.optionalKey(Cleanup),
  kept: Schema.Boolean,
  workerPool: Schema.optionalKey(Schema.String),
  complete: Schema.Boolean,
});

export type Result = typeof Result.Type;

export const DriverRequest = Schema.Struct({ query: Query, targetUrl: Schema.String });

export const BuildRequest = Schema.Struct({
  ...DriverRequest.fields,
  from: Schema.Natural,
  startedMs: Schema.optionalKey(Schema.Number),
});

export const HistoryCheckpoint = Schema.Struct({
  ...SeedCheckpoint.fields,
  driverMs: Schema.Number,
});

export const MeasureRequest = Schema.Struct({
  expectedDriverBuild: Schema.String,
  ...DriverRequest.fields,
  observeText: Schema.Boolean,
  cold: Schema.Boolean,
  quiescent: Schema.Boolean,
  resetBeforeMetrics: Schema.optionalKey(Schema.Boolean),
});

export const MeasureResponse = Schema.Struct({
  metrics: MetricsResult,
  ok: Schema.Literal(true),
  driverMs: Schema.Number,
  observedMs: Schema.Number,
  firstTextMs: Schema.NullOr(Schema.Number),
  observerToCompletionMs: Schema.optionalKey(Schema.Number),
  observerToFirstTextMs: Schema.optionalKey(Schema.Number),
  firstText: Schema.optionalKey(Schema.String),
  firstTextSource: Schema.optionalKey(FirstTextSource),
  settlementRecordRead: Schema.Boolean,
  observationMs: Schema.NullOr(Schema.Number),
  constructorAndProbeMs: Schema.Number,
  primeMs: Schema.Number,
  primed: Schema.optionalKey(Identity),
  admissionMs: Schema.optionalKey(Schema.Number),
  colo: Schema.NullOr(Schema.String),
});

export const Health = Schema.Struct({ ok: Schema.Literal(true), build: Schema.String });
