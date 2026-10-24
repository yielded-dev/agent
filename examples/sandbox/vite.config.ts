import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite-plus";

export default defineConfig({
  base: "/sandbox/",
  build: { outDir: "dist/sandbox", emptyOutDir: true },
  plugins: [tailwindcss(), react()],
  run: {
    tasks: {
      "db:generate": { command: "drizzle-kit generate", cache: false },
      check: { command: "effect-tsrs diagnostics --strict --project tsconfig.json", cache: false },
      build: { command: "vp build && wrangler deploy --dry-run", cache: false },
      "deploy:production": { command: "vp build && wrangler deploy", cache: false },
      worker: { command: "wrangler dev --port 8793", cache: false },
    },
  },
});
