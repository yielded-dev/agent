import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      // Embed the actual checkout identity on every build, including clean/dirty state.
      build: { command: "bun src/build-cloudflare.ts", cache: false },
      // Like the neighboring workerd examples, drive Miniflare through a Vite task.
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
  },
  test: {
    // Vitest v4 compatibility: preserve mock call history.
    // Remove after tests no longer rely on calls from setup or earlier tests.
    // https://viteplus.dev/guide/vitest-v5#remove-unneeded-compatibility-settings
    // https://vitest.dev/guide/migration/#clearmocks-is-enabled-by-default
    clearMocks: false,
    cache: false,
    silent: "passed-only",
    maxWorkers: 1,
  },
});
