import { Array, Result, type Schema } from "effect";

interface JsonLimits {
  readonly depth: number;
  readonly nodes: number;
  readonly collectionLength: number;
  readonly textUnits: number;
}

export interface CanonicalJsonBoundsError {
  readonly _tag: "CanonicalJsonBoundsError";
  readonly message: string;
}

const exceeded = (bound: string): CanonicalJsonBoundsError => ({
  _tag: "CanonicalJsonBoundsError",
  message: `Canonical JSON exceeds its ${bound} bound`,
});

/** Values have already crossed their Schema boundary; visit each serialized occurrence. */
export const canonicalJsonResult = (
  value: Schema.Json,
  limits?: JsonLimits,
): Result.Result<string, CanonicalJsonBoundsError> => {
  let nodes = 0;
  let textUnits = 0;

  const charge = (length: number): boolean =>
    limits === undefined || (textUnits += length) <= limits.textUnits;

  const visit = (value: Schema.Json, depth: number): string | CanonicalJsonBoundsError => {
    if (limits !== undefined && (depth > limits.depth || ++nodes > limits.nodes))
      return exceeded("traversal");
    if (
      value === null ||
      typeof value === "boolean" ||
      typeof value === "number" ||
      typeof value === "string"
    ) {
      const encoded = JSON.stringify(value);

      return charge(encoded.length) ? encoded : exceeded("text");
    }
    if (Array.isArray<Schema.Json>(value)) {
      if (limits !== undefined && value.length > limits.collectionLength)
        return exceeded("collection");
      if (!charge(2 + Math.max(0, value.length - 1))) return exceeded("text");
      const entries: globalThis.Array<string> = [];

      for (let index = 0; index < value.length; index++) {
        const encoded = visit(value[index], depth + 1);

        if (typeof encoded !== "string") return encoded;
        entries.push(encoded);
      }

      return `[${entries.join(",")}]`;
    }

    const entries = Object.entries(value).sort(([left], [right]) =>
      left < right ? -1 : left > right ? 1 : 0,
    );

    if (limits !== undefined && entries.length > limits.collectionLength)
      return exceeded("collection");
    if (!charge(2 + Math.max(0, entries.length - 1))) return exceeded("text");
    const encodedEntries: globalThis.Array<string> = [];

    for (const [key, entry] of entries) {
      const encodedKey = JSON.stringify(key);

      if (!charge(encodedKey.length + 1)) return exceeded("text");
      const encoded = visit(entry, depth + 1);

      if (typeof encoded !== "string") return encoded;
      encodedEntries.push(`${encodedKey}:${encoded}`);
    }

    return `{${encodedEntries.join(",")}}`;
  };

  const encoded = visit(value, 0);

  return typeof encoded === "string" ? Result.succeed(encoded) : Result.fail(encoded);
};

export const canonicalJson = (value: Schema.Json): string =>
  Result.getOrThrow(canonicalJsonResult(value));
