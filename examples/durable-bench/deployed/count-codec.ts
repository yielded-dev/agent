import { Schema } from "effect";

export const CodecSite = Schema.Struct({
  source: Schema.String,
  line: Schema.Natural,
  column: Schema.Natural,
  scope: Schema.Literals(["product", "bench", "dependency"]),
  api: Schema.String,
  schema: Schema.String,
});

export type CodecSite = typeof CodecSite.Type;

export const CodecCount = Schema.Struct({
  ...CodecSite.fields,
  calls: Schema.Natural,
  inputArrayElements: Schema.Natural,
  inputStringBytes: Schema.Natural,
});

export type CodecCount = typeof CodecCount.Type;

type MutableCount = { -readonly [Key in keyof CodecCount]: CodecCount[Key] };
let active: Map<string, MutableCount> | undefined;
const encoder = new TextEncoder();

export const beginCodecs = (): void => {
  if (active) throw new Error("A codec counting window is already active");
  active = new Map();
};

export const endCodecs = (): ReadonlyArray<CodecCount> => {
  const rows = [...(active?.values() ?? [])];

  active = undefined;

  return rows.sort(
    (left, right) =>
      right.calls - left.calls || left.source.localeCompare(right.source) || left.line - right.line,
  );
};

export const countCodec = <Args extends ReadonlyArray<unknown>, Result>(
  site: CodecSite,
  codec: (...args: Args) => Result,
): ((...args: Args) => Result) => {
  const key = JSON.stringify(site);

  return (...args) => {
    if (active) {
      let row = active.get(key);

      if (!row) {
        row = { ...site, calls: 0, inputArrayElements: 0, inputStringBytes: 0 };
        active.set(key, row);
      }
      row.calls++;
      if (Array.isArray(args[0])) row.inputArrayElements += args[0].length;
      if (typeof args[0] === "string") row.inputStringBytes += encoder.encode(args[0]).byteLength;
    }

    return codec(...args);
  };
};
