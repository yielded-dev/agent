import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      test: {
        // Postgres certification requires a live database and its configured URL.
        cache: false,
        command: "vp test --passWithNoTests",
      },
    },
  },
  pack: {
    deps: {
      // tsdown <0.23 compatibility: resolve external dependency subpaths.
      // Remove to preserve subpath imports as written (the new default).
      // https://tsdown.dev/options/dependencies#deps-resolvedepsubpath
      resolveDepSubpath: true,
    },
    entry: [
      "src/index.ts",
      "src/Certification.ts",
      "src/Chaos.ts",
      "src/CodeExecutorConformance.ts",
      "src/CodeExecutorSubstitute.ts",
      "src/DocsResearcher.ts",
      "src/ScriptedModel.ts",
      "src/TravelPlanner.ts",
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
