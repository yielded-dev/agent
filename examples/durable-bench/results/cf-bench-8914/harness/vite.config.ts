import { defineConfig } from "vite-plus";

import repository from "../../../../../vite.config.ts";

// Standalone task package: retain the repository's rules while allowing the
// explicitly selected harness files under the otherwise excluded results/**.
export default defineConfig({
  run: { cache: { scripts: false } },
  fmt: { ...repository.fmt, ignorePatterns: [] },
  lint: {
    ...repository.lint,
    ignorePatterns: [],
    jsPlugins: [
      { name: "vite-plus", specifier: "vite-plus/oxlint-plugin" },
      {
        name: "exports",
        specifier: new URL("../../../../../oxlint/plugin-exports.ts", import.meta.url).pathname,
      },
    ],
  },
});
