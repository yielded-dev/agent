import { defineConfig } from "vite-plus";

// This package ships two public entries: the storage adapters at "." and the
// deterministic test helpers at "./testing". `vp pack` builds only the default
// entry without this config, which would leave the "./testing" subpath broken
// in the published artifact.
export default defineConfig({
  pack: {
    deps: {
      // tsdown <0.23 compatibility: resolve external dependency subpaths.
      // Remove to preserve subpath imports as written (the new default).
      // https://tsdown.dev/options/dependencies#deps-resolvedepsubpath
      resolveDepSubpath: true,
    },
    entry: [
      "src/index.ts",
      "src/MemoryScheduleStore.ts",
      "src/MemorySemanticIndex.ts",
      "src/MemorySubmissionLedger.ts",
      "src/MemorySubscriptionStore.ts",
      "src/MemoryMessageDeliveryStore.ts",
      "src/MemoryThreadStore.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: {
    // Vitest v4 compatibility: preserve mock call history.
    // Remove after tests no longer rely on calls from setup or earlier tests.
    // https://viteplus.dev/guide/vitest-v5#remove-unneeded-compatibility-settings
    // https://vitest.dev/guide/migration/#clearmocks-is-enabled-by-default
    clearMocks: false,
    cache: false,
    silent: "passed-only",
  },
});
