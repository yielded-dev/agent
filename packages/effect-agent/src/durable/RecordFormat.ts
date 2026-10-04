import { Effect, Schema, SchemaGetter, SchemaIssue } from "effect";

import { CanonicalBatch, CURRENT_RECORD_FORMAT, PersistedJson, RecordEnvelope } from "./Records.ts";

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
)({ wire: PersistedJson }) {}

const decodeRecord = Schema.decodeUnknownEffect(RecordEnvelope);
const sameRecord = Schema.toEquivalence(RecordEnvelope);

/** Decode at the archive boundary, retaining unknown additive fields for a lossless backup. */
export const decodeExportRecord = Effect.fnUntraced(function* (
  format: string,
  input: PersistedJson,
) {
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
export const ExportRecord = PersistedJson.pipe(
  Schema.decodeTo(Schema.toType(ExportedRecord), {
    decode: SchemaGetter.transformEffect((value) =>
      decodeRecord(value).pipe(
        Effect.map((record) => ExportedRecord.make({ ...record, wire: value })),
        Effect.mapError((error) => error.issue),
      ),
    ),
    encode: SchemaGetter.transformEffect((record) =>
      decodeRecord(record.wire).pipe(
        Effect.mapError((error) => error.issue),
        Effect.flatMap((view) =>
          sameRecord(view, record)
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

/** Batch identity plus lossless record wire, used when auditing an existing digest chain. */
export const ExportBatch = Schema.Struct({
  ...CanonicalBatch.fields,
  records: Schema.NonEmptyArray(ExportRecord).check(Schema.isMaxLength(256)),
});
