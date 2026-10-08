import { Schema } from "effect";

import { utf8ByteLength } from "./utf8.ts";

export const MAX_PERSISTED_JSON_DEPTH = 64;
export const MAX_PERSISTED_JSON_COLLECTION_LENGTH = 4_096;
export const MAX_PERSISTED_JSON_NODES = 65_536;
export const MAX_PERSISTED_JSON_BYTES = 1024 * 1024;

/**
 * Iteratively preflights an unknown value before Schema's recursive JSON validation. This is the
 * one narrow `Schema.declare` exception in the persistence model: Effect v4's `Unknown.decodeTo`
 * preserves `unknown` as the encoded type, which would leak through every nested record codec.
 * Schema.Json still owns the accepted value shape after this resource preflight succeeds.
 */
const isJson = Schema.is(Schema.Json);
// No Unicode flag: match surrogate code units so astral characters take the UTF-8 path too.
const nonAscii = /[\u0080-\uFFFF]/;

export const boundedJson =
  (limits: { readonly depth: number; readonly nodes: number; readonly bytes: number }) =>
  (input: unknown): input is Schema.Json => {
    const pending: Array<
      | { readonly _tag: "visit"; readonly value: unknown; readonly depth: number }
      | { readonly _tag: "leave"; readonly value: object }
    > = [{ _tag: "visit", value: input, depth: 0 }];

    // Only ancestors indicate a cycle. Shared acyclic values serialize once per occurrence,
    // so revisit them and charge every occurrence against the same resource limits.
    const ancestors = new WeakSet<object>();
    let nodes = 0;
    let textUnits = 0;

    try {
      while (pending.length > 0) {
        const current = pending.pop();

        if (current === undefined) return false;
        if (current._tag === "leave") {
          ancestors.delete(current.value);
          continue;
        }
        if (current.depth > limits.depth || ++nodes > limits.nodes) {
          return false;
        }

        const value = current.value;

        if (value === null || typeof value === "boolean") continue;
        if (typeof value === "number") {
          if (!Number.isFinite(value)) return false;
          continue;
        }
        if (typeof value === "string") {
          textUnits += value.length;
          if (textUnits > limits.bytes) return false;
          continue;
        }
        if (typeof value !== "object" || ancestors.has(value)) return false;
        ancestors.add(value);
        pending.push({ _tag: "leave", value });

        const entries = Array.isArray(value)
          ? Array.from(value, (entry, index) => [index, entry] as const)
          : Object.entries(value);

        if (entries.length > MAX_PERSISTED_JSON_COLLECTION_LENGTH) return false;
        for (const [key, entry] of entries) {
          textUnits += typeof key === "string" ? key.length : 0;
          if (textUnits > limits.bytes) return false;
          pending.push({ _tag: "visit", value: entry, depth: current.depth + 1 });
        }
      }

      if (!isJson(input)) return false;
      const encoded = JSON.stringify(input);

      // Escaping is already reflected in the serialized text. UTF-8 uses one to three bytes per
      // UTF-16 code unit; ASCII uses exactly one. Only ambiguous Unicode needs the exact count.
      return (
        encoded !== undefined &&
        encoded.length <= limits.bytes &&
        (encoded.length <= limits.bytes / 3 ||
          !nonAscii.test(encoded) ||
          utf8ByteLength(encoded) <= limits.bytes)
      );
    } catch {
      return false;
    }
  };

export function isPersistedJson(
  input: unknown,
  maxBytes = MAX_PERSISTED_JSON_BYTES,
): input is Schema.Json {
  return boundedJson({
    depth: MAX_PERSISTED_JSON_DEPTH,
    nodes: MAX_PERSISTED_JSON_NODES,
    bytes: maxBytes,
  })(input);
}

/** Canonical JSON admitted to persisted records and checkpoints under explicit resource limits. */
export const PersistedJson = Schema.declare(
  boundedJson({
    depth: MAX_PERSISTED_JSON_DEPTH,
    nodes: MAX_PERSISTED_JSON_NODES,
    bytes: MAX_PERSISTED_JSON_BYTES,
  }),
  {
    identifier: "@effect-agent/thread/PersistedJson",
    description: "JSON bounded by canonical persistence depth, collection, node, and byte limits",
  },
);

export type PersistedJson = typeof PersistedJson.Type;
