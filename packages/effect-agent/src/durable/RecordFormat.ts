import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as SchemaIssue from "effect/SchemaIssue";

import {
  CanonicalBatch,
  CanonicalRecordEnvelope,
  CURRENT_RECORD_FORMAT,
  RecordEnvelope,
  RecordJson,
} from "./Records.ts";

export class RecordFormatError extends Schema.TaggedError<RecordFormatError>()(
  "RecordFormatError",
  {
    format: Schema.String,
    message: Schema.String,
  },
) {}

/** An archive owns its wire JSON explicitly; the typed fields are only a read view. */
export class ExportedRecord extends RecordEnvelope.extend<ExportedRecord>(
  "@effect-agent/thread/ExportedRecord",
)({ wire: RecordJson }) {}

const decodeRecord = Schema.decodeUnknownEffect(RecordEnvelope);

const parseRecordObject = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);

const decodeExportedRecord = Schema.decodeUnknownEffect(ExportedRecord);

/**
 * Decode stored text directly into its final record, retaining additive wire.
 * Parsing owns the object; the final schema checks the wire bounds and supported fields once.
 * Object-input codecs still validate caller-owned wire when constructing the read view.
 */
export const decodeExportRecordJson = (
  encoded: string,
): Effect.Effect<ExportedRecord, Schema.SchemaError> =>
  parseRecordObject(encoded).pipe(
    Effect.flatMap((wire) => decodeExportedRecord({ ...wire, wire })),
  );

// Both codecs validate RecordJson before this getter; decode the supported view once.
const decodeView = SchemaGetter.transformEffect((value: RecordJson) =>
  decodeRecord(value).pipe(
    Effect.map((record) =>
      ExportedRecord.make({ ...record, wire: value }, { disableChecks: true }),
    ),
    Effect.mapError((error) => error.issue),
  ),
);

/** Decode at the archive boundary, retaining unknown additive fields for a lossless backup. */
export const decodeExportRecord = Effect.fnUntraced(function* (format: string, input: RecordJson) {
  if (format !== CURRENT_RECORD_FORMAT)
    return yield* RecordFormatError.make({ format, message: "Unsupported record format" });

  return yield* Schema.decodeEffect(ExportRecord)(input).pipe(
    Effect.mapError(() => RecordFormatError.make({ format, message: "Invalid canonical record" })),
  );
});

/**
 * Preserve additive fields without process-local side state. Copy with ExportedRecord.make
 * or this codec. Normalizing through RecordEnvelope discards the wire and cannot be encoded
 * as an archive record. Editing the typed view also fails rather than silently changing a log.
 */
export const ExportRecord = RecordJson.pipe(
  Schema.decodeTo(Schema.toType(ExportedRecord), {
    decode: decodeView,
    encode: SchemaGetter.transformEffect((record) =>
      decodeRecord(record.wire).pipe(
        Effect.mapError((error) => error.issue),
        // Derive the memoized equivalence only when an archive is encoded.
        Effect.flatMap((view) =>
          Schema.toEquivalence(RecordEnvelope)(view, record)
            ? Effect.succeed(record.wire)
            : Effect.fail(
                new SchemaIssue.InvalidValue({
                  message:
                    "Archive record view differs from its wire JSON; export the source again",
                }),
              ),
        ),
      ),
    ),
  }),
);

/** Canonical read transport preserves exact evidence while exposing the supported typed view. */
export const ReadRecord = RecordJson.pipe(
  Schema.decodeTo(Schema.toType(RecordEnvelope), {
    decode: decodeView,
    encode: SchemaGetter.transformEffect((record) =>
      (Schema.is(ExportedRecord)(record) &&
      Object.prototype.isPrototypeOf.call(ExportedRecord.prototype, record)
        ? Schema.encodeEffect(ExportRecord)(record)
        : Schema.encodeEffect(RecordEnvelope)(record)
      ).pipe(Effect.mapError((error) => error.issue)),
    ),
  }),
);

export const ReadEnvelope = Schema.Struct({
  ...CanonicalRecordEnvelope.fields,
  record: ReadRecord,
}).pipe(
  Schema.decodeTo(Schema.toType(CanonicalRecordEnvelope), {
    decode: SchemaGetter.transform((fields) =>
      CanonicalRecordEnvelope.make(fields, { disableChecks: true }),
    ),
    encode: SchemaGetter.transform((envelope) => envelope),
  }),
);

/** Batch identity plus lossless record wire, used when checking a complete immutable log. */
export const ExportBatch = Schema.Struct({
  ...CanonicalBatch.fields,
  records: Schema.NonEmptyArray(ExportRecord).check(Schema.isMaxLength(256)),
});
