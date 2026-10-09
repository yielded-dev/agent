import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      "rpc-overhead": {
        command: "direnv exec . vp exec bun deployed/rpc-overhead/main.ts",
        cache: false,
      },
      "rpc-overhead:check": {
        command: "tsc --noEmit -p deployed/rpc-overhead/tsconfig.json",
        cache: false,
      },
      deployed: { command: "direnv exec . vp exec bun deployed/main.ts", cache: false },
      seed: { command: "bun bench/seed.ts", cache: false },
      vendor: { command: "bun install --cwd third-party", cache: false },
    },
  },
});
