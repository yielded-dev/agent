import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig, type UserConfig } from "vite-plus";

// Ignore generated Vite files and node_modules directory listings, while
// retaining dependency file hashes. The lockfile covers dependency additions.
const run: NonNullable<UserConfig["run"]> = {
  tasks: {
    test: {
      command: "vitest run",
      cache: {
        env: ["BROWSER_TEST_EXECUTABLE", "VITEST_MAX_WORKERS"],
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
};

// Two lanes, one runner (WP0 probe contract, D-P6-7 Fallback A):
//
// - `workerd` — tests execute inside workerd against real SQLite-backed Thread Durable
//   Objects via `cloudflareTest`. Durable Object storage is shared across tests within
//   a run, so every suite mints a unique Thread name per case.
// - `restart` — Node-side Miniflare programmatic runtimes for restart-persistence evidence
//   (dispose/reopen over one persist directory); these spawn real runtimes and HTTP
//   listeners and cannot run inside workerd.
export default defineConfig({
  run,
  // A package-level Vite config suppresses `vp pack`'s zero-config library
  // defaults, so the published artifact's declarations and sourcemap are
  // pinned explicitly here.
  pack: {
    deps: {
      // tsdown <0.23 compatibility: resolve external dependency subpaths.
      // Remove to preserve subpath imports as written (the new default).
      // https://tsdown.dev/options/dependencies#deps-resolvedepsubpath
      resolveDepSubpath: true,
    },
    entry: [
      "src/index.ts",
      "src/Alarm.ts",
      "src/BrowserRestCapture.ts",
      "src/BrowserRestCrawl.ts",
      "src/CloudflareBindings.ts",
      "src/CloudflareAiGateway.ts",
      "src/CloudflareBrowser.ts",
      "src/CloudflareCodeMode.ts",
      "src/CloudflareConfig.ts",
      "src/CloudflareMemory.ts",
      "src/CloudflareScheduling.ts",
      "src/CloudflareSubscriptions.ts",
      "src/CloudflareThreadClient.ts",
      "src/InteractiveBrowser.ts",
      "src/BrowserSession.ts",
      "src/BrowserUse.ts",
      "src/BrowserCredentials.ts",
      "src/ThreadObject.ts",
      "src/WakeScheduler.ts",
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
    // Vitest v4 compatibility: keep separate Vite servers for inline projects.
    // Remove when plugins and config hooks can run once for shared projects.
    // https://viteplus.dev/guide/vitest-v5#remove-unneeded-compatibility-settings
    // https://vitest.dev/guide/migration/#inline-projects-share-the-vite-server-by-default
    sharedViteServer: false,
    // Vite Task owns result caching; Vitest's results.json is read and
    // rewritten by every run, which makes the entire task uncacheable.
    cache: false,
    silent: "passed-only",
    projects: [
      {
        // Vitest v4 compatibility: keep this inline project independent of the root config.
        // Remove to inherit root options, including plugins and setup files.
        // https://viteplus.dev/guide/vitest-v5#remove-unneeded-compatibility-settings
        // https://vitest.dev/guide/migration/#inline-projects-inherit-the-root-config-by-default
        extends: false,
        plugins: [
          cloudflareTest({
            main: "./test/worker.ts",
            miniflare: {
              compatibilityDate: "2025-05-01",
              compatibilityFlags: ["nodejs_compat"],
              durableObjects: {
                THREADS: { className: "TestThreadObject", useSQLite: true },
                PUBLICATIONS: { className: "PublicationThreadObject", useSQLite: true },
                PROJECTIONS: { className: "ProjectionThreadObject", useSQLite: true },
                MEMORIES: { className: "TestMemoryObject", useSQLite: true },
                SCHEDULES: { className: "TestScheduleOwnerObject", useSQLite: true },
                SUBSCRIPTIONS: { className: "TestSubscriptionPartitionObject", useSQLite: true },
                SUBAGENTS: { className: "SubagentThreadObject", useSQLite: true },
                TELEMETRY: {
                  className: "TelemetryThreadObject",
                  useSQLite: true,
                },
                CONTEXT_COMPACTOR: {
                  className: "ContextCompactorThreadObject",
                  useSQLite: true,
                },
              },
            },
          }),
        ],
        test: {
          // Vitest v4 compatibility: preserve mock call history.
          // Remove after tests no longer rely on calls from setup or earlier tests.
          // https://viteplus.dev/guide/vitest-v5#remove-unneeded-compatibility-settings
          // https://vitest.dev/guide/migration/#clearmocks-is-enabled-by-default
          clearMocks: false,
          name: "workerd",
          include: ["test/**/*.test.ts"],
          exclude: [
            "test/restart/**",
            "test/code-mode/**",
            "test/interactive-browser-actions.test.ts",
            "test/interactive-browser-native.test.ts",
            "test/browser-credentials-native.test.ts",
          ],
        },
      },
      {
        // Vitest v4 compatibility: keep this inline project independent of the root config.
        // Remove to inherit root options, including plugins and setup files.
        // https://viteplus.dev/guide/vitest-v5#remove-unneeded-compatibility-settings
        // https://vitest.dev/guide/migration/#inline-projects-inherit-the-root-config-by-default
        extends: false,
        test: {
          // Vitest v4 compatibility: preserve mock call history.
          // Remove after tests no longer rely on calls from setup or earlier tests.
          // https://viteplus.dev/guide/vitest-v5#remove-unneeded-compatibility-settings
          // https://vitest.dev/guide/migration/#clearmocks-is-enabled-by-default
          clearMocks: false,
          name: "browser-actions",
          include: [
            "test/interactive-browser-actions.test.ts",
            "test/interactive-browser-native.test.ts",
            "test/browser-credentials-native.test.ts",
          ],
        },
      },
      {
        // Vitest v4 compatibility: keep this inline project independent of the root config.
        // Remove to inherit root options, including plugins and setup files.
        // https://viteplus.dev/guide/vitest-v5#remove-unneeded-compatibility-settings
        // https://vitest.dev/guide/migration/#inline-projects-inherit-the-root-config-by-default
        extends: false,
        // The Code Mode Dynamic Worker executor lane runs the real adapter
        // inside a bundled worker under programmatic Miniflare (like the
        // restart lane) so Worker Loader and cross-event RPC ownership use a
        // real workerd process rather than a Node substitute.
        test: {
          // Vitest v4 compatibility: preserve mock call history.
          // Remove after tests no longer rely on calls from setup or earlier tests.
          // https://viteplus.dev/guide/vitest-v5#remove-unneeded-compatibility-settings
          // https://vitest.dev/guide/migration/#clearmocks-is-enabled-by-default
          clearMocks: false,
          name: "code-mode",
          include: ["test/code-mode/**/*.test.ts"],
        },
      },
      {
        // Vitest v4 compatibility: keep this inline project independent of the root config.
        // Remove to inherit root options, including plugins and setup files.
        // https://viteplus.dev/guide/vitest-v5#remove-unneeded-compatibility-settings
        // https://vitest.dev/guide/migration/#inline-projects-inherit-the-root-config-by-default
        extends: false,
        test: {
          // Vitest v4 compatibility: preserve mock call history.
          // Remove after tests no longer rely on calls from setup or earlier tests.
          // https://viteplus.dev/guide/vitest-v5#remove-unneeded-compatibility-settings
          // https://vitest.dev/guide/migration/#clearmocks-is-enabled-by-default
          clearMocks: false,
          name: "restart",
          include: ["test/restart/**/*.test.ts"],
        },
      },
    ],
  },
});
