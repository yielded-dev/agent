import { defineConfig } from "vite-plus";

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
      "src/SqlThreadImport.ts",
      "src/SqlStorage.ts",
      "src/SqlRunStorage.ts",
      "src/SqlStorageFailpoint.ts",
      "src/SqlStorageProgress.ts",
      "src/SqlJournal.ts",
      "src/SqlThreadArchiveRange.ts",
      "src/SqlThreadStore.ts",
      "src/SqlThreadWork.ts",
      "src/SqlSubmissionLedger.ts",
      "src/SqlAdmissionFacts.ts",
      "src/SqlActivityStore.ts",
      "src/SqlScheduleStore.ts",
      "src/SqliteLayoutInspection.ts",
      "src/SqlThreadNativeReads.ts",
      "src/SqlMessageDeliveryStore.ts",
      "src/SqlLifecyclePublication.ts",
      "src/SqlSubscriptionStore.ts",
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
