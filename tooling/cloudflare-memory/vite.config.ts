import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      build: {
        command: "wrangler deploy --dry-run",
        cache: {
          // Wrangler reads its own temporary bundle during validation.
          input: [
            { auto: true },
            "*",
            // Excluding the workspace root (".") would drop every file input.
            { pattern: "!tooling/cloudflare-memory", base: "workspace" },
            "!.wrangler",
            "!.wrangler/**",
            { pattern: "bun.lock", base: "workspace" },
            { pattern: "!**/node_modules", base: "workspace" },
            { pattern: "!**/node_modules/.vite*", base: "workspace" },
            { pattern: "!**/node_modules/.vite*/**", base: "workspace" },
          ],
          output: [],
        },
      },
      test: {
        command: "vp test --passWithNoTests",
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
    // Preserve existing mock call history when upgrading from Vitest 4.
    clearMocks: false,
    cache: false,
    silent: "passed-only",
  },
});
