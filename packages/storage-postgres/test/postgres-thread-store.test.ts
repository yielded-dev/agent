import { describe, it } from "@effect/vitest";
import { threadStoreConformanceCases } from "@yielded/agent/testing/thread-store-conformance";
import type { ThreadStore } from "@yielded/agent/thread-store";
import * as Effect from "effect/Effect";
import * as String from "effect/String";

import { storage as makeStorage, withTemporaryDatabase } from "./harness.ts";

const withStorage = <A, E>(url: string, effect: Effect.Effect<A, E, ThreadStore>) =>
  Effect.provide(
    effect,
    makeStorage(
      url,
      { schema: "select" },
      {
        startupParameters: { search_path: "pg_catalog" },
        transformQueryNames: String.snakeToCamel,
        transformResultNames: String.snakeToCamel,
      },
    ).threadStore,
  );

describe("PostgresThreadStore", () => {
  describe("shared ThreadStore conformance", () => {
    for (const conformanceCase of threadStoreConformanceCases) {
      it.effect(conformanceCase.name, () =>
        withTemporaryDatabase((url) => withStorage(url, conformanceCase.run)),
      );
    }
  });
});
