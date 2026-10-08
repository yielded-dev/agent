import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig, type UserConfig } from "vite-plus";

// Ignore generated Vite files and node_modules directory listings, while
// retaining dependency file hashes. The lockfile covers dependency additions.
const run: NonNullable<UserConfig["run"]> = {
  tasks: {
    test: {
      command: "vitest run",
      cache: {
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

// Run this package's tests inside workerd with real SQLite-backed Durable Object namespaces.
// `cloudflareTest` installs the Workers runner. Durable Object storage is shared
// across tests within a run, so every suite mints a unique Durable Object name per case.
export default defineConfig({
  run,
  // A package-level Vite config suppresses `vp pack`'s zero-config library
  // defaults, so the published artifact's declarations and sourcemap are
  // pinned explicitly here.
  pack: {
    entry: [
      "src/index.ts",
      "src/DoMessageDeliveryStore.ts",
      "src/DoMemoryStore.ts",
      "src/DoScheduleStore.ts",
      "src/DoStorageConfig.ts",
      "src/DoStorageError.ts",
      "src/DoStorageFailpoint.ts",
      "src/DoStorageVersion.ts",
      "src/DoSubmissionLedger.ts",
      "src/DoSubscriptionStore.ts",
      "src/DoThreadStore.ts",
      "src/MemoryProtocol.ts",
      "src/PortProtocol.ts",
      "src/PortRouting.ts",
      "src/DoStorageFailpointTesting.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: {
    // Leave result caching to Vite Task, without a mutable results.json input.
    cache: false,
    silent: "passed-only",
    projects: [
      {
        plugins: [
          cloudflareTest({
            main: "./test/worker.ts",
            miniflare: {
              compatibilityDate: "2025-05-01",
              compatibilityFlags: ["nodejs_compat"],
              durableObjects: {
                THREADS: { className: "ThreadStorageObject", useSQLite: true },
                SCHEDULES: { className: "ScheduleStorageObject", useSQLite: true },
              },
            },
          }),
        ],
      },
    ],
  },
});
