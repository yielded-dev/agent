import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      vendor: { command: "vp install --frozen-lockfile -- --cwd third-party", cache: false },
      bundle: { command: "node --experimental-strip-types scripts/build.ts", cache: false },
      measure: { command: "node --experimental-strip-types scripts/measure-main.ts", cache: false },
      profile: { command: "node --experimental-strip-types scripts/profile-main.ts", cache: false },
      analyze: { command: "node --experimental-strip-types scripts/analyze-main.ts", cache: false },
      local: {
        command: "node --experimental-strip-types --experimental-vm-modules scripts/local-main.ts",
        cache: false,
      },
    },
  },
});
