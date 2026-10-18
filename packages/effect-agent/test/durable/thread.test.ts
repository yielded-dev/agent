import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { describe, expect, it, layer } from "@effect/vitest";
import { digestJson } from "@yielded/agent/digest";
import { MAX_PERSISTED_JSON_BYTES, PersistedJson } from "@yielded/agent/records";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

describe("thread canonical contracts", () => {
  layer(NodeCrypto.layer)((it) => {
    it.effect("digests object keys by UTF-16 code units, independent of insertion order", () =>
      Effect.gen(function* () {
        const ordered = yield* digestJson({
          alpha: 1,
          beta: { gamma: [true, null], delta: "d" },
        });

        const reordered = yield* digestJson({
          beta: { delta: "d", gamma: [true, null] },
          alpha: 1,
        });

        expect(reordered).toBe(ordered);

        // Precomposed U+00E0 and decomposed U+0061 U+0300 are canonically equivalent
        // but distinct code-unit sequences. Locale-aware collation treats them as equal,
        // so a stable sort would leak insertion order; code-unit ordering must not.
        const precomposedFirst = yield* digestJson({ "\u00e0": 1, "a\u0300": 2 });
        const decomposedFirst = yield* digestJson({ "a\u0300": 2, "\u00e0": 1 });

        expect(decomposedFirst).toBe(precomposedFirst);

        // Locale-aware collation orders "\u00e4" before "z" in en but after "z" in sv.
        // Code-unit ordering is the same on every host, whatever the process locale.
        const umlautFirst = yield* digestJson({ "\u00e4": 1, z: 2 });
        const umlautLast = yield* digestJson({ z: 2, "\u00e4": 1 });

        expect(umlautLast).toBe(umlautFirst);
        expect(yield* digestJson({ z: "\ud800", value: "😀é" })).toBe(
          "a2ae949a2f22f7fc31138231e3db19f57f017e0d8a09f7b2b049398f9346f57c",
        );
        expect(
          yield* digestJson([
            [{ z: "last", a: [false, null, 2.5] }],
            [],
            { nested: [[1], { b: 2, a: 3 }] },
          ]),
        ).toBe("8f7312be37bd42bc167b496ac42e8e8289a845c133097cf7ab5c8e5f7e38f5ab");
      }),
    );
  });

  it("accepts shared acyclic JSON while rejecting object and array ancestor cycles", () => {
    const shared = { amenities: ["hot tub", "kitchen"] };
    const dag = { first: shared, nested: { second: shared }, list: [shared, shared.amenities] };

    expect(Schema.decodeSync(PersistedJson)(dag)).toEqual(dag);
    const cycle: unknown[] = [];
    const parent = { shared, cycle };

    cycle.push(parent);
    expect(Schema.decodeUnknownExit(PersistedJson)(parent)._tag).toBe("Failure");
    expect(Schema.decodeUnknownExit(PersistedJson)(cycle)._tag).toBe("Failure");
  });

  it("charges every shared occurrence to traversal and byte limits", () => {
    // Fifteen binary levels expand to 65,535 nodes despite containing only fifteen arrays.
    let sharedTree: Schema.Json = null;

    for (let level = 0; level < 15; level++) sharedTree = [sharedTree, sharedTree];
    expect(Schema.decodeExit(PersistedJson)([sharedTree])._tag).toBe("Success");
    expect(Schema.decodeExit(PersistedJson)([sharedTree, null])._tag).toBe("Failure");

    const largeShared = { text: "x".repeat(MAX_PERSISTED_JSON_BYTES / 2) };

    expect(Schema.decodeExit(PersistedJson)(largeShared)._tag).toBe("Success");
    expect(Schema.decodeExit(PersistedJson)([largeShared, largeShared])._tag).toBe("Failure");
  });
});
