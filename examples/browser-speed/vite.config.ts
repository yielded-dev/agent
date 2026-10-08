import react from "@vitejs/plugin-react";
import { defineConfig } from "vite-plus";

// Served at agent.yielded.dev/browser-use. Files keep that prefix inside dist so Workers
// static assets match request paths directly.
export default defineConfig({
  base: "/browser-use/",
  build: { outDir: "dist/browser-use", emptyOutDir: true },
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 5191,
    strictPort: true,
    proxy: {
      "/browser-use/api": {
        target: "http://127.0.0.1:8791",
        changeOrigin: true,
        configure: (proxy) => {
          proxy.on("proxyReq", (outgoing, incoming) => {
            if (incoming.headers.origin === "http://127.0.0.1:5191")
              outgoing.setHeader("origin", "http://127.0.0.1:8791");
          });
        },
      },
    },
  },
  test: {
    include: ["test/**/*.test.ts"],
    cache: false,
    silent: "passed-only",
    maxWorkers: 1,
  },
  run: {
    tasks: {
      check: {
        command: "tsc --noEmit -p tsconfig.json",
        cache: {
          input: [{ auto: true }, "src/**", "test/**", "tsconfig.json"],
          output: [],
        },
      },
      test: {
        command: "vp test",
        cache: {
          env: ["BROWSER_TEST_EXECUTABLE"],
          input: [
            { auto: true },
            "src/**",
            "test/**",
            { pattern: "bun.lock", base: "workspace" },
            { pattern: "!**/node_modules", base: "workspace" },
            { pattern: "!**/node_modules/.vite*", base: "workspace" },
            { pattern: "!**/node_modules/.vite*/**", base: "workspace" },
          ],
          output: [],
        },
      },
      build: {
        command: "vp build && wrangler deploy --dry-run",
        cache: {
          env: ["VITE_BROWSER_BENCHMARK_REPORT_URL"],
          input: [
            { auto: true },
            "*",
            // Excluding the workspace root (".") would drop every file input.
            { pattern: "!examples/browser-speed", base: "workspace" },
            "src/**",
            "!.wrangler",
            "!.wrangler/**",
            "!dist",
            "!dist/**",
            { pattern: "bun.lock", base: "workspace" },
            { pattern: "!**/node_modules", base: "workspace" },
            { pattern: "!**/node_modules/.vite*", base: "workspace" },
            { pattern: "!**/node_modules/.vite*/**", base: "workspace" },
          ],
          untrackedEnv: ["WRANGLER_LOG_PATH"],
          output: ["dist/**"],
        },
      },
      journey: {
        command: "bun src/journey-main.ts",
        cache: false,
      },
      worker: {
        command: "wrangler dev --port 8791",
        cache: false,
      },
      deploy: {
        command: "vp build && wrangler deploy",
        cache: false,
      },
      "deploy:public": {
        command: "vp build && wrangler deploy --env public",
        cache: false,
      },
    },
  },
});
