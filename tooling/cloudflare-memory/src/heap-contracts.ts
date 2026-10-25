import { Schema } from "effect";

export class HeapProbeError extends Schema.TaggedError<HeapProbeError>()("HeapProbeError", {
  operation: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export const HeapUsage = Schema.Struct({
  usedSize: Schema.Finite,
  totalSize: Schema.Finite,
  embedderHeapUsedSize: Schema.optionalKey(Schema.Finite),
  backingStorageSize: Schema.optionalKey(Schema.Finite),
});

export const ObjectStatus = Schema.Struct({
  active: Schema.Boolean,
  modelCalls: Schema.Natural,
  toolCalls: Schema.Natural,
});

export const NodeMemory = Schema.Struct({
  rss: Schema.Finite,
  heapTotal: Schema.Finite,
  heapUsed: Schema.Finite,
  external: Schema.Finite,
  arrayBuffers: Schema.Finite,
});

export const NodeSample = Schema.Struct({
  node: Schema.String,
  v8: Schema.String,
  platform: Schema.String,
  arch: Schema.String,
  baseline: NodeMemory,
  evaluated: NodeMemory,
  retained: NodeMemory,
  evaluatedHeapDelta: Schema.Finite,
  retainedHeapDelta: Schema.Finite,
  exports: Schema.Array(Schema.String),
});

export const WorkerdSample = Schema.Struct({
  startup: HeapUsage,
  initialized: HeapUsage,
  active: HeapUsage,
  settled: HeapUsage,
  objects: Schema.Array(ObjectStatus),
});
