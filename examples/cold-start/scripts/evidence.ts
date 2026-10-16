import * as Schema from "effect/Schema";

export const arms = [
  "yielded-single",
  "pi-single",
  "yielded-split",
  "pi-split",
  "yielded-empty",
  "pi-empty",
] as const;

export const Arm = Schema.Literals(arms);
export const Bundle = Schema.Literals(["yielded", "pi", "front", "driver", "provider"]);

export const Identity = Schema.Struct({
  moduleId: Schema.NonEmptyString,
  calls: Schema.Int,
  constructions: Schema.Int,
  alarms: Schema.Int,
  version: Schema.NonEmptyString,
});

export const Frame = Schema.Union([
  Schema.Struct({ type: Schema.Literals(["worker", "object"]), value: Identity }),
  Schema.Struct({ type: Schema.Literal("text"), value: Schema.Struct({ text: Schema.String }) }),
  Schema.Struct({
    type: Schema.Literal("done"),
    value: Schema.Struct({ outcome: Schema.Literal("completed"), calls: Schema.Int }),
  }),
  Schema.Struct({ type: Schema.Literal("error"), value: Schema.String }),
]);

export const Health = Schema.Struct({
  instance: Schema.NonEmptyString,
  colo: Schema.NullOr(Schema.String),
});

export const Capture = Schema.Struct({
  ...Health.fields,
  frames: Schema.Array(Schema.Struct({ at: Schema.Finite, frame: Frame })),
  elapsed: Schema.optionalKey(Schema.Finite),
  error: Schema.optionalKey(Schema.String),
});

export const State = Schema.Struct({
  prefix: Schema.NonEmptyString,
  names: Schema.Array(Schema.NonEmptyString),
});

export const Upload = Schema.Struct({
  version: Schema.NonEmptyString,
  sha256: Schema.NonEmptyString,
  bundleSha256: Schema.NonEmptyString,
  bundle: Bundle,
  startupMs: Schema.NullOr(Schema.Finite),
  uploadedAt: Schema.Finite,
  host: Schema.NullOr(Schema.String),
});

export const Uploads = Schema.Record(Schema.String, Upload);

export const Cell = Schema.Struct({
  block: Schema.Int,
  arm: Arm,
  host: Schema.NonEmptyString,
  front: Schema.NonEmptyString,
});

export const Plan = Schema.Struct({
  cells: Schema.Array(Cell),
  driver: Health,
  blocks: Schema.Int,
  state: Schema.Literal("brand-new Thread"),
  locationHint: Schema.Literal("wnam"),
  propagationSeconds: Schema.Int,
  commit: Schema.NonEmptyString,
});

export const Cleanup = Schema.Struct({
  workers: Schema.Array(Schema.String),
  namespaces: Schema.Array(Schema.String),
  versions: Schema.Array(Schema.String),
  deleteHistory: Schema.Array(Schema.Unknown),
  verified: Schema.Boolean,
});
