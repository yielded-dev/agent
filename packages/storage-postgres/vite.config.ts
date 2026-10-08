import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      test: {
        // Live database state is not a cache input. Disabling task caching also
        // passes the configured EFFECT_AGENT_TEST_POSTGRES_URL to the test process.
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
    entry: ["src/index.ts", "src/PostgresStorage.ts", "src/PostgresStorageError.ts"],
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
    // Every case creates and drops its own database, so each one is dominated by server round
    // trips rather than by the assertion under test. The default budget is too tight when
    // suites run in parallel against one server.
    testTimeout: 60_000,
  },
});
