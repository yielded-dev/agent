import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      vendor: { command: "vp install --frozen-lockfile -- --cwd third-party", cache: false },
      bundle: { command: "node --experimental-strip-types scripts/build.ts", cache: false },
      measure: { command: "python3 scripts/measure.py", cache: false },
      profile: { command: "python3 scripts/profile.py", cache: false },
      analyze: { command: "python3 scripts/analyze.py", cache: false },
      local: { command: "node --experimental-vm-modules scripts/local.mjs", cache: false },
    },
  },
});
