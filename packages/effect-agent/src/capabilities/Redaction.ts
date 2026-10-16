import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { utf8ByteLength } from "../core/internal/utf8.ts";
import { RunEvent } from "../core/RunEvent.ts";

const MAX_REDACTED_PREVIEW_BYTES = 8 * 1024;
const MAX_REDACTION_NODES = 4_096;
const MAX_REDACTION_DEPTH = 16;

/** Branded evidence that a preview passed through the configured structural Redactor. */
export const RedactedPreview = Schema.String.pipe(
  Schema.refine((value): value is string => utf8ByteLength(value) <= MAX_REDACTED_PREVIEW_BYTES, {
    expected: `redacted preview of at most ${MAX_REDACTED_PREVIEW_BYTES} UTF-8 bytes`,
  }),
  Schema.brand("@effect-agent/capabilities/RedactedPreview"),
);

export type RedactedPreview = typeof RedactedPreview.Type;

/** Structural redaction failed closed rather than returning unreviewed caller text. */
export class RedactionError extends Schema.TaggedError<RedactionError>()("RedactionError", {
  reason: Schema.Literals(["input-too-deep", "input-too-large", "encoding-failed"]),
  message: Schema.String,
}) {}

/** Redaction owns the boundary from decoded Tool values to bounded audit/model previews. */
export class Redactor extends Context.Service<
  Redactor,
  {
    readonly redact: (decodedValue: unknown) => Effect.Effect<RedactedPreview, RedactionError>;
  }
>()("@effect-agent/capabilities/Redactor") {}

interface RedactionCounter {
  nodes: number;
}

const encodingFailure = () =>
  RedactionError.make({
    reason: "encoding-failed",
    message: "Could not encode the structurally redacted preview",
  });

const redactValue = Effect.fnUntraced(function* (
  value: unknown,
  depth: number,
  counter: RedactionCounter,
): Effect.fn.Return<Schema.Json, RedactionError> {
  if (depth > MAX_REDACTION_DEPTH) {
    return yield* RedactionError.make({
      reason: "input-too-deep",
      message: `Decoded value exceeds redaction depth ${MAX_REDACTION_DEPTH}`,
    });
  }
  counter.nodes += 1;
  if (counter.nodes > MAX_REDACTION_NODES) {
    return yield* RedactionError.make({
      reason: "input-too-large",
      message: `Decoded value exceeds redaction node count ${MAX_REDACTION_NODES}`,
    });
  }

  const collection = yield* Effect.try({
    try: () => {
      if (Array.isArray(value)) {
        const redacted: Array<Schema.Json> = [];

        redacted.length = value.length;

        return { _tag: "Array" as const, values: value, redacted };
      }

      return value !== null && typeof value === "object"
        ? { _tag: "Object" as const, entries: Object.entries(value) }
        : undefined;
    },
    catch: encodingFailure,
  });

  if (collection?._tag === "Array") {
    const { redacted } = collection;

    for (let index = 0; index < redacted.length; index += 1) {
      const item = yield* Effect.try({
        try: () => (index in collection.values ? { value: collection.values[index] } : undefined),
        catch: encodingFailure,
      });

      if (item !== undefined) redacted[index] = yield* redactValue(item.value, depth + 1, counter);
    }

    return redacted;
  }
  if (collection?._tag === "Object") {
    const redacted: Record<string, Schema.Json> = {};

    for (const [key, item] of collection.entries) {
      redacted[key] = yield* redactValue(item, depth + 1, counter);
    }

    return redacted;
  }
  // The default policy is deliberately type-aware and fail-closed: it preserves
  // object/array shape for reviewer context but never copies a decoded scalar,
  // regardless of its field name. Applications that need visible values provide
  // a Tool-specific Redactor after validating those values against that Tool's
  // input Schema.
  if (value === null) return "[REDACTED:null]";
  if (typeof value === "string") return "[REDACTED:string]";
  if (typeof value === "number") return "[REDACTED:number]";
  if (typeof value === "boolean") return "[REDACTED:boolean]";

  return "[REDACTED:unsupported]";
});

const truncateUtf8 = (value: string, maxBytes: number): string => {
  if (utf8ByteLength(value) <= maxBytes) {
    return value;
  }
  const suffix = "…";
  const suffixBytes = utf8ByteLength(suffix);
  let output = "";
  let bytes = 0;

  for (const character of value) {
    const characterBytes = utf8ByteLength(character);

    if (bytes + characterBytes + suffixBytes > maxBytes) {
      break;
    }
    output += character;
    bytes += characterBytes;
  }

  return `${output}${suffix}`;
};

const encodeRunEvent = Schema.encodeEffect(RunEvent);

/**
 * One composed step from live Run Events to structurally redacted transcript lines
 * (P7 WP7 friction fix; SEC-008/testing.md §12 "structurally redacted" live transcripts):
 * Schema-encode each event through the canonical `RunEvent` union, then pass the encoded
 * value through the configured `Redactor` — so the safe path is the short path and no live
 * suite hand-assembles (or accidentally skips) the encode→redact pair. Engine-constructed
 * events always encode, so an encode failure is a defect, never a silent omission.
 */
export const redactedTranscript = (
  events: Iterable<RunEvent>,
): Effect.Effect<ReadonlyArray<RedactedPreview>, RedactionError, Redactor> =>
  Effect.gen(function* () {
    const redactor = yield* Redactor;
    const lines: Array<RedactedPreview> = [];

    for (const event of events) {
      const encoded = yield* encodeRunEvent(event).pipe(Effect.orDie);

      lines.push(yield* redactor.redact(encoded));
    }

    return lines;
  });

/**
 * Default structural implementation. It exposes only collection shape and
 * scalar types, so an ordinary field such as `note` cannot bypass redaction.
 * Tool-specific Layers may expose explicitly reviewed fields instead.
 */
export const StructuralRedactorLive = Layer.succeed(Redactor)({
  redact: Effect.fnUntraced(function* (decodedValue) {
    const structurallyRedacted = yield* redactValue(decodedValue, 0, { nodes: 0 });

    const preview = yield* Effect.try({
      try: () => truncateUtf8(JSON.stringify(structurallyRedacted), MAX_REDACTED_PREVIEW_BYTES),
      catch: encodingFailure,
    });

    return yield* Schema.decodeEffect(RedactedPreview)(preview).pipe(
      Effect.mapError(() =>
        RedactionError.make({
          reason: "encoding-failed",
          message: "Redacted preview did not satisfy its bounded schema",
        }),
      ),
    );
  }),
});
