import { Receipt } from "@yielded/agent/durable-agent-runtime";
import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/rpc";

export const Size = Schema.Literals([200, 20_000]);

export const Variant = Schema.Literals([
  "native",
  "fetch",
  "schema-sync",
  "schema-runtime",
  "effect-json",
  "effect-ndjson",
  "effect-ws",
  "thread-native",
  "thread-status",
  "thread-progress",
  "thread-submit",
]);

export type Variant = typeof Variant.Type;
export const Payload = Schema.Struct({ version: Schema.Literal(1), text: Schema.String });
export type Payload = typeof Payload.Type;

export const payload = (bytes: number): Payload => ({
  version: 1,
  text: "x".repeat(bytes - JSON.stringify({ version: 1, text: "" }).length),
});

export const replies = { 200: payload(200), 20_000: payload(20_000) };

export const Meta = Schema.Struct({
  round: Schema.String,
  object: Schema.Natural,
  variant: Variant,
  size: Size,
  phase: Schema.Literals(["warmup", "measure", "prepare"]),
  index: Schema.Int,
});

export type Meta = typeof Meta.Type;
export const Call = Schema.Struct({ meta: Meta, build: Schema.String, payload: Payload });
export type Call = typeof Call.Type;

export class Fault extends Schema.TaggedError<Fault>()("RpcOverheadFault", {
  message: Schema.String,
}) {}

export const Rpcs = RpcGroup.make(
  Rpc.make("noop", { payload: Call, success: Payload, error: Fault }),
);

export const Batch = Schema.Struct({
  round: Schema.String,
  object: Schema.Natural,
  variant: Variant,
  size: Size,
  build: Schema.String,
  warmup: Schema.Natural.check(Schema.isLessThanOrEqualTo(100)),
  calls: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 })),
});

export type Batch = typeof Batch.Type;
export const Identity = Schema.Struct({ build: Schema.String, instance: Schema.String });

/** Private orchestration values; results retain only metrics, never these identities. */
export const ThreadCpuControl = Schema.Struct({
  batch: Batch,
  identity: Identity,
  token: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(64)),
  receipt: Receipt,
});

export type ThreadCpuControl = typeof ThreadCpuControl.Type;

export const ThreadCpuSampleResult = Schema.Struct({
  latencyMs: Schema.Finite,
  receipt: Schema.NullOr(Receipt),
  clientWarm: Schema.Boolean,
});

export type ThreadCpuSampleResult = typeof ThreadCpuSampleResult.Type;

export const Result = Schema.Struct({
  ok: Schema.Literal(true),
  batch: Batch,
  latencyMs: Schema.Array(Schema.Number),
  setupMs: Schema.Number,
  sameInstance: Schema.Boolean,
  colo: Schema.NullOr(Schema.String),
  placement: Schema.optionalKey(Schema.NullOr(Schema.String)),
  clientSetup: Schema.String,
});

export type Result = typeof Result.Type;

export const mark = (role: "driver" | "object", meta: Meta, calls = 1, warmup = 0) => {
  console.log({ kind: "rpc-overhead", role, ...meta, calls, warmup });
};

const Cursor = Schema.Natural.check(Schema.isLessThanOrEqualTo(64));

export const PushOptions = Schema.Struct({
  build: Schema.String,
  round: Schema.String,
  object: Schema.Natural,
  framesPerBurst: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 2, maximum: 8 })),
  ),
});

export type PushOptions = typeof PushOptions.Type;

export const BurstMetrics = Schema.Struct({
  from: Cursor,
  sequences: Schema.Array(Cursor),
  publishRttMs: Schema.Number,
  publishToReceiveMs: Schema.Array(Schema.Number),
});

export const IdleProbe = Schema.Struct({
  idleMs: Schema.Literal(45_000),
  wakeHelloMs: Schema.Number,
  wakeAndFirstReplayMs: Schema.NullOr(Schema.Number),
  upgradeCount: Schema.Natural,
  constructorChanged: Schema.Boolean,
  sameSocket: Schema.Boolean,
  replayedSequence: Schema.NullOr(Cursor),
});

export const NativeCancellation = Schema.Struct({
  readerUnlocked: Schema.Boolean,
  succeeded: Schema.Boolean,
  error: Schema.NullOr(Schema.String),
  elapsedMs: Schema.Number,
});

export const NativeMetrics = Schema.Struct({
  setupMs: Schema.Number,
  bursts: Schema.Array(BurstMetrics),
  precommittedBurst: BurstMetrics,
  sameInstance: Schema.Boolean,
  freshStreamSetupAndFirstFrameMs: Schema.Number,
  savedCursor: Cursor,
  replayedSequence: Cursor,
  cancellations: Schema.Array(NativeCancellation),
  release: Schema.NullOr(
    Schema.Struct({
      observersAfterCancel: Schema.Natural,
      diagnosticFrames: Schema.Natural,
      observersAfterWrite: Schema.Natural,
      resetRequired: Schema.Boolean,
      releaseAfterWriteMs: Schema.NullOr(Schema.Number),
      observersAfterRelease: Schema.Natural,
    }),
  ),
});

export const WebSocketMetrics = Schema.Struct({
  setupMs: Schema.Number,
  bursts: Schema.Array(BurstMetrics),
  precommittedBurst: BurstMetrics,
  probes: Schema.Array(IdleProbe),
  recreationObserved: Schema.Boolean,
  upgradeCount: Schema.Natural,
  savedCursor: Cursor,
});

export const ReconnectMetrics = Schema.Struct({
  setupMs: Schema.Number,
  setupAndFirstFrameMs: Schema.Number,
  replayedSequence: Cursor,
  newConnection: Schema.Boolean,
  upgradeCount: Schema.Natural,
  totalUpgradeCount: Schema.Natural,
});

export const PushResult = Schema.Struct({
  ok: Schema.Boolean,
  round: Schema.String,
  object: Schema.Natural,
  framesPerBurst: Schema.Number,
  idleMs: Schema.Literal(45_000),
  native: Schema.NullOr(NativeMetrics),
  websocket: Schema.NullOr(WebSocketMetrics),
  reconnect: Schema.NullOr(ReconnectMetrics),
  failures: Schema.Array(Schema.Struct({ stage: Schema.String, cause: Schema.String })),
});

export type PushResult = typeof PushResult.Type;
