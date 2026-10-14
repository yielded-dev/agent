import { Array, Predicate, Result, Schema } from "effect";

interface JsonLimits {
  readonly depth: number;
  readonly nodes: number;
  readonly collectionLength: number;
  readonly textUnits: number;
}

export class CanonicalJsonLimitError extends Schema.TaggedError<CanonicalJsonLimitError>()(
  "CanonicalJsonLimitError",
  { message: Schema.String },
) {}

// Only this codec's private synchronous aborts become typed bounds failures.
const boundsFailures = new WeakMap<object, string>();

const exceeded = (bound: "text" | "traversal" | "collection"): never => {
  const message = `Canonical JSON exceeds its ${bound} bound`;
  // oxlint-disable-next-line effect/no-native-error -- Private synchronous codec abort; the Result boundary translates only errors owned here.
  const error = new RangeError(message);

  boundsFailures.set(error, message);
  // oxlint-disable-next-line effect/no-untyped-throw -- Abort synchronous JSON traversal at the first exceeded bound; the Result boundary catches this owned error.
  throw error;
};

/** Values have already crossed their Schema boundary; visit each serialized occurrence. */
const serialize = (value: Schema.Json, limits?: JsonLimits): string => {
  let nodes = 0;
  let textUnits = 0;

  const charge = (length: number) => {
    if (limits !== undefined && (textUnits += length) > limits.textUnits) return exceeded("text");
  };

  const visit = (value: Schema.Json, depth: number): string => {
    if (limits !== undefined && (depth > limits.depth || ++nodes > limits.nodes))
      return exceeded("traversal");
    if (
      value === null ||
      typeof value === "boolean" ||
      typeof value === "number" ||
      typeof value === "string"
    ) {
      const encoded = JSON.stringify(value);

      charge(encoded.length);

      return encoded;
    }
    if (Array.isArray<Schema.Json>(value)) {
      if (limits !== undefined && value.length > limits.collectionLength)
        return exceeded("collection");
      charge(2 + Math.max(0, value.length - 1));

      let json = "[";

      for (let index = 0; index < value.length; index++) {
        if (index > 0) json += ",";
        json += visit(value[index], depth + 1);
      }

      return `${json}]`;
    }

    const keys = Object.keys(value).sort();

    if (limits !== undefined && keys.length > limits.collectionLength)
      return exceeded("collection");
    charge(2 + Math.max(0, keys.length - 1));
    let json = "{";

    for (let index = 0; index < keys.length; index++) {
      const key = keys[index];
      const encodedKey = JSON.stringify(key);

      charge(encodedKey.length + 1);
      if (index > 0) json += ",";
      json += `${encodedKey}:${visit(value[key], depth + 1)}`;
    }

    return `${json}}`;
  };

  return visit(value, 0);
};

export const canonicalJson = (value: Schema.Json): string => serialize(value);

export const boundedCanonicalJson = (
  value: Schema.Json,
  limits: JsonLimits,
): Result.Result<string, CanonicalJsonLimitError> => {
  try {
    return Result.succeed(serialize(value, limits));
  } catch (cause) {
    const message = Predicate.isObjectKeyword(cause) ? boundsFailures.get(cause) : undefined;

    if (message !== undefined) return Result.fail(CanonicalJsonLimitError.make({ message }));
    // oxlint-disable-next-line effect/no-untyped-throw -- Foreign JSON/getter errors retain their identity for the caller's typed capture boundary.
    throw cause;
  }
};
