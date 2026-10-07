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

      return `[${globalThis.Array.from(value, (entry) => visit(entry, depth + 1)).join(",")}]`;
    }

    const entries = Object.entries(value).sort(([left], [right]) =>
      left < right ? -1 : left > right ? 1 : 0,
    );

    if (limits !== undefined && entries.length > limits.collectionLength)
      throw new RangeError("Canonical JSON exceeds its collection bound");
    charge(2 + Math.max(0, entries.length - 1));

    return `{${entries
      .map(([key, entry]) => {
        const encodedKey = JSON.stringify(key);

        charge(encodedKey.length + 1);

        return `${encodedKey}:${visit(entry, depth + 1)}`;
      })
      .join(",")}}`;
  };

  return visit(value, 0);
};
