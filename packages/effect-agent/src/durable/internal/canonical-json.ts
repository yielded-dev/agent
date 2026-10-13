import { Array, type Schema } from "effect";

interface JsonLimits {
  readonly depth: number;
  readonly nodes: number;
  readonly collectionLength: number;
  readonly textUnits: number;
}

/** Values have already crossed their Schema boundary; visit each serialized occurrence. */
export const canonicalJson = (value: Schema.Json, limits?: JsonLimits): string => {
  let nodes = 0;
  let textUnits = 0;

  const charge = (length: number) => {
    if (limits !== undefined && (textUnits += length) > limits.textUnits)
      throw new RangeError("Canonical JSON exceeds its text bound");
  };

  const visit = (value: Schema.Json, depth: number): string => {
    if (limits !== undefined && (depth > limits.depth || ++nodes > limits.nodes))
      throw new RangeError("Canonical JSON exceeds its traversal bound");
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
        throw new RangeError("Canonical JSON exceeds its collection bound");
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
      throw new RangeError("Canonical JSON exceeds its collection bound");
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
