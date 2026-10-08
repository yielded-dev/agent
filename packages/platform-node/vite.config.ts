import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      test: {
        command: "vp test --passWithNoTests",
        cache: {
          // Fresh runners lack Vite's temporary directories. Ignore those and
          // dependency directory listings, while retaining dependency file hashes.
          input: [
            { auto: true },
            { pattern: "bun.lock", base: "workspace" },
            { pattern: "!**/node_modules", base: "workspace" },
            { pattern: "!**/node_modules/.vite*", base: "workspace" },
            { pattern: "!**/node_modules/.vite*/**", base: "workspace" },
          ],
          output: [],
        },
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
      "src/NodeDurableAgentRuntime.ts",
      "src/NodeDurableHost.ts",
      "src/NodeScheduling.ts",
      "src/NodeSubscriptions.ts",
      "src/NodeWakeScheduler.ts",
      "src/NodeWorkflow.ts",
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
