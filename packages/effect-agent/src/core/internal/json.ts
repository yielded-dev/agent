import type { Schema } from "effect";

/** Own a validated JSON value before exposing a separate copy to application callbacks. */
export const copyJson = (value: Schema.Json): Schema.Json => {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) {
    const copied: Array<Schema.Json> = [];

    for (let index = 0; index < value.length; index++) copied.push(copyJson(value[index]));

    return copied;
  }

  return Object.fromEntries(
    Object.entries<Schema.Json>(value).map(([key, entry]) => [key, copyJson(entry)]),
  );
};
