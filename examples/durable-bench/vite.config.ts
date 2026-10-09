import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      deployed: { command: "vp exec bun deployed/main.ts", cache: false },
      seed: { command: "bun bench/seed.ts", cache: false },
      vendor: { command: "bun install --cwd third-party", cache: false },
    },
  },
});
