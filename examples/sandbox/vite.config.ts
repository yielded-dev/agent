import react from "@vitejs/plugin-react";
import { defineConfig } from "vite-plus";

export default defineConfig({
  base: "/sandbox/",
  build: { outDir: "dist/sandbox", emptyOutDir: true },
  plugins: [react()],
  run: {
    tasks: {
      check: { command: "tsc --noEmit -p tsconfig.json", cache: false },
      build: { command: "vp build && wrangler deploy --dry-run", cache: false },
      "deploy:production": { command: "vp build && wrangler deploy", cache: false },
      worker: { command: "wrangler dev --port 8793", cache: false },
    },
  },
});
