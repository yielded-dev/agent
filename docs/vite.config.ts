import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      build: {
        command: "NODE_OPTIONS=--max-old-space-size=8192 astro build",
        cache: {
          // Fresh checkouts have no Astro/Vite output. Track root files without
          // fingerprinting the directory listing that contains generated paths.
          input: [
            { auto: true },
            "*",
            { pattern: "!docs", base: "workspace" },
            "!.astro",
            "!.astro/**",
            "!dist",
            "!dist/**",
            { pattern: "bun.lock", base: "workspace" },
            { pattern: "!**/node_modules", base: "workspace" },
            { pattern: "!**/node_modules/.astro", base: "workspace" },
            { pattern: "!**/node_modules/.astro/**", base: "workspace" },
            { pattern: "!**/node_modules/.vite*", base: "workspace" },
            { pattern: "!**/node_modules/.vite*/**", base: "workspace" },
          ],
          // Runner identity changes every job but does not affect the site.
          untrackedEnv: ["ACTIONS_ORCHESTRATION_ID"],
          output: ["dist/**", ".astro/**"],
        },
      },
    },
  },
});
