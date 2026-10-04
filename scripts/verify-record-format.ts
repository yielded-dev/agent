import { NodeCrypto, NodeRuntime } from "@effect/platform-node";
import { Console, Effect, Schema } from "effect";

import { digestJson } from "../packages/effect-agent/src/durable/Digest.ts";
import {
  PREVIOUS_RECORD_SCHEMA_DIGEST,
  PREVIOUS_RECORD_VERSION,
  RecordEnvelope as PreviousEnvelope,
} from "../packages/effect-agent/src/durable/internal/PreviousRecord.ts";
import {
  CURRENT_RECORD_FORMAT,
  CURRENT_RECORD_VERSION,
  KnownRecordPayload,
  RecordEnvelope,
} from "../packages/effect-agent/src/durable/Records.ts";

class RecordCompatibilityError extends Schema.TaggedError<RecordCompatibilityError>()(
  "RecordCompatibilityError",
  {
    message: Schema.String,
  },
) {}

const metadata = new Set(["title", "description", "examples", "$comment"]);
const isObject = Schema.is(Schema.Record(Schema.String, Schema.Unknown));

/** Only optional properties and new top-level record variants are additive wire changes. */
const changes = (before: unknown, after: unknown, path: string): ReadonlyArray<string> => {
  if (Object.is(before, after)) return [];
  if (Array.isArray(before) && Array.isArray(after)) {
    if (path === "record.properties.payload.anyOf") {
      // Union members use either a reference or an inline literal _tag. Order is not a contract.
      const identity = (value: unknown): unknown => {
        if (!isObject(value)) return value;
        if (typeof value.$ref === "string") return value.$ref;
        const properties = value.properties;

        return isObject(properties) ? JSON.stringify(properties._tag) : value;
      };

      return before.flatMap((value) =>
        changes(
          value,
          after.find((candidate) => identity(candidate) === identity(value)),
          `${path}.${String(identity(value))}`,
        ),
      );
    }

    return before.length === after.length
      ? before.flatMap((value, index) => changes(value, after[index], `${path}[${index}]`))
      : [path];
  }
  if (isObject(before) && isObject(after)) {
    const issues = Object.keys(before)
      .filter((key) => !metadata.has(key))
      .flatMap((key) => changes(before[key], after[key], `${path}.${key}`));

    for (const key of Object.keys(after)) {
      if (!(key in before) && !metadata.has(key) && !path.endsWith(".properties"))
        issues.push(`${path}.${key}`);
    }

    return issues;
  }

  return [path];
};

/** JSON Schema cannot prove behavioral meaning; semantic review must still bump the format. */
export const verifyRecordFormat = Effect.fnUntraced(function* () {
  const previous = Schema.toJsonSchemaDocument(Schema.Struct(PreviousEnvelope.fields));
  const previousJson = yield* Schema.decodeUnknownEffect(Schema.Json)(previous);

  if ((yield* digestJson(previousJson)) !== PREVIOUS_RECORD_SCHEMA_DIGEST) {
    return yield* RecordCompatibilityError.make({
      message:
        "The frozen previous record union or a transitive wire schema changed. Preserve the predecessor decoder, or replace its snapshot and digest together when retiring that release.",
    });
  }
  if (
    CURRENT_RECORD_FORMAT !== `effect-agent/thread@${CURRENT_RECORD_VERSION}` ||
    CURRENT_RECORD_VERSION < PREVIOUS_RECORD_VERSION ||
    CURRENT_RECORD_VERSION > PREVIOUS_RECORD_VERSION + 1
  ) {
    return yield* RecordCompatibilityError.make({
      message: "Record formats must name the current version and retain at most one predecessor.",
    });
  }
  if (Number(CURRENT_RECORD_VERSION) !== Number(PREVIOUS_RECORD_VERSION)) return;

  const current = Schema.toJsonSchemaDocument(
    Schema.Struct({ ...RecordEnvelope.fields, payload: KnownRecordPayload }),
  );

  const incompatible = [
    ...changes(previous.schema, current.schema, "record"),
    ...Object.entries(previous.definitions).flatMap(([name, schema]) =>
      changes(schema, current.definitions[name], name),
    ),
  ];

  if (incompatible.length > 0) {
    return yield* RecordCompatibilityError.make({
      message: `Existing record wire meaning changed at ${incompatible.slice(0, 8).join(", ")}. Bump CURRENT_RECORD_VERSION and CURRENT_RECORD_FORMAT and implement the single pure upgradeRecord; never rewrite stored payloads in place.`,
    });
  }
});

if (import.meta.main)
  NodeRuntime.runMain(
    verifyRecordFormat().pipe(
      Effect.tap(() => Console.log("Record format compatibility verified")),
      Effect.tapError((error) => Console.error(error.message)),
      Effect.provide(NodeCrypto.layer),
    ),
    { disableErrorReporting: true },
  );
