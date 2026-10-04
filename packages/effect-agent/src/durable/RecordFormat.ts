import { Effect, Schema, SchemaGetter } from "effect";

import { canonicalJson } from "./Digest.ts";
import { CanonicalBatch, CURRENT_RECORD_FORMAT, PersistedJson, RecordEnvelope } from "./Records.ts";

export class RecordFormatError extends Schema.TaggedError<RecordFormatError>()(
  "RecordFormatError",
  {
    format: Schema.String,
    message: Schema.String,
  },
) {}

const wire = new WeakMap<RecordEnvelope, PersistedJson>();
const decodeRecord = Schema.decodeUnknownEffect(RecordEnvelope);
const encodeRecord = Schema.encodeEffect(RecordEnvelope);
const copyJson = Schema.decodeEffect(Schema.fromJsonString(PersistedJson));

/** Decode at the archive boundary, retaining unknown additive fields for a lossless backup. */
export const decodeExportRecord = Effect.fnUntraced(function* (
  format: string,
  input: PersistedJson,
) {
  if (format !== CURRENT_RECORD_FORMAT)
    return yield* RecordFormatError.make({ format, message: "Unsupported record format" });

  const original = yield* copyJson(canonicalJson(input)).pipe(
    Effect.mapError(() => RecordFormatError.make({ format, message: "Invalid record JSON" })),
  );

  const record = yield* decodeRecord(original).pipe(
    Effect.mapError(() => RecordFormatError.make({ format, message: "Invalid canonical record" })),
  );

  wire.set(record, original);

  return record;
});

/** A same-format archive preserves validated wire fields that this reader does not interpret. */
export const ExportRecord = PersistedJson.pipe(
  Schema.decodeTo(Schema.toType(RecordEnvelope), {
    decode: SchemaGetter.transformEffect((value) =>
      decodeRecord(value).pipe(
        Effect.tap((record) =>
          Effect.sync(() => {
            wire.set(record, value);
          }),
        ),
        Effect.mapError((error) => error.issue),
      ),
    ),
    encode: SchemaGetter.transformEffect((record) => {
      const original = wire.get(record);

      return original === undefined
        ? encodeRecord(record).pipe(Effect.mapError((error) => error.issue))
        : Effect.succeed(original);
    }),
  }),
);

/** Batch identity plus lossless record wire, used when auditing an existing digest chain. */
export const ExportBatch = Schema.Struct({
  ...CanonicalBatch.fields,
  records: Schema.NonEmptyArray(ExportRecord).check(Schema.isMaxLength(256)),
}).pipe(
  Schema.decodeTo(Schema.toType(CanonicalBatch), {
    decode: SchemaGetter.transform((fields) => CanonicalBatch.make(fields)),
    encode: SchemaGetter.transform((batch) => batch),
  }),
);
