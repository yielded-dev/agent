import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      build: {
        command: "wrangler deploy --dry-run",
        // Wrangler reads its own temporary bundle during validation.
        input: [
          { auto: true },
          "*",
          { pattern: "!.", base: "workspace" },
          { pattern: "!tooling/browser-run-worker-proof", base: "workspace" },
          "!.wrangler",
          "!.wrangler/**",
          { pattern: "bun.lock", base: "workspace" },
          { pattern: "!**/node_modules", base: "workspace" },
          { pattern: "!**/node_modules/.vite*", base: "workspace" },
          { pattern: "!**/node_modules/.vite*/**", base: "workspace" },
        ],
        output: [],
      },
      test: {
        command: "vp test --passWithNoTests",
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
  test: { include: ["test/**/*.test.ts"], cache: false, silent: "passed-only", maxWorkers: 1 },
});
