import { utf8ByteLength } from "../Digest.ts";

/**
 * Count owned JSON from a Schema encoder or JSON parser, before retaining its native Prompt.
 * Keep the conservative object/property/string charges without probing arbitrary object brands.
 * This is not the guard for untrusted provider values, accessors, or other JavaScript objects.
 */
export const promptJsonFootprint = (json: unknown, allowance: number): number | undefined => {
  let bytes = 0;
  const add = (size: number) => (bytes += size) <= allowance;

  const visit = (value: unknown, depth: number): boolean => {
    if (depth > 128) return false;
    if (value === null) return add(4);
    switch (typeof value) {
      case "string":
        return add(utf8ByteLength(value) + 2);
      case "number":
        return add(16);
      case "boolean":
        return add(4);
      case "object": {
        if (!add(32)) return false;
        if (Array.isArray(value)) {
          if (!add(value.length)) return false;
          for (let index = 0; index < value.length; index++) {
            if (!add(8 + String(index).length) || !visit(value[index], depth + 1)) return false;
          }

          return true;
        }
        for (const key of Object.keys(value)) {
          if (!add(8 + utf8ByteLength(key)) || !visit(Reflect.get(value, key), depth + 1))
            return false;
        }

        return true;
      }
      default:
        return false;
    }
  };

  return visit(json, 0) ? bytes : undefined;
};
