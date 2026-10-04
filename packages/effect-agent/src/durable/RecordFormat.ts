import { Effect, Schema, SchemaGetter } from "effect";

import { canonicalJson } from "./Digest.ts";
import { PreviousRecord, PREVIOUS_RECORD_VERSION } from "./internal/PreviousRecord.ts";
import { CanonicalBatch, CURRENT_RECORD_FORMAT, PersistedJson, RecordEnvelope } from "./Records.ts";

/** One release boundary, never a chain. Initially it is the current format's frozen baseline. */
export const PREVIOUS_RECORD_FORMAT = `effect-agent/thread@${PREVIOUS_RECORD_VERSION}`;

/**
 * The only semantic upgrade. At a format bump, edit this pure function and retain the frozen
 * predecessor decoder for one release. Additive changes leave this identity function alone.
 */
export const upgradeRecord = (record: PreviousRecord): typeof RecordEnvelope.Encoded => record;

export class RecordFormatError extends Schema.TaggedError<RecordFormatError>()(
  "RecordFormatError",
  {
    format: Schema.String,
    message: Schema.String,
  },
) {}

const wire = new WeakMap<RecordEnvelope, PersistedJson>();
const decodeRecord = Schema.decodeUnknownEffect(RecordEnvelope);
const decodePrevious = Schema.decodeUnknownEffect(PreviousRecord);
const encodeRecord = Schema.encodeEffect(RecordEnvelope);
const copyJson = Schema.decodeEffect(Schema.fromJsonString(PersistedJson));

/** Decode at the archive boundary, retaining unknown additive fields for a lossless backup. */
export const decodeExportRecord = Effect.fnUntraced(function* (
  format: string,
  input: PersistedJson,
) {
  const original = yield* copyJson(canonicalJson(input)).pipe(
    Effect.mapError(() => RecordFormatError.make({ format, message: "Invalid record JSON" })),
  );

  const value =
    format === CURRENT_RECORD_FORMAT
      ? original
      : format === PREVIOUS_RECORD_FORMAT
        ? upgradeRecord(
            yield* decodePrevious(original).pipe(
              Effect.mapError(() =>
                RecordFormatError.make({ format, message: "Invalid predecessor record" }),
              ),
            ),
          )
        : yield* RecordFormatError.make({ format, message: "Unsupported record format" });

  const record = yield* decodeRecord(value).pipe(
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

/** Current wire after the single semantic upgrade; same-format records keep their original fields. */
export const encodeImportedRecord = Effect.fnUntraced(function* (
  format: string,
  record: RecordEnvelope,
) {
  const original = wire.get(record);

  if (format === CURRENT_RECORD_FORMAT && original !== undefined) return original;

  return yield* encodeRecord(record);
});
