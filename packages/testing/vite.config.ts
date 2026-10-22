import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      test: {
        // Postgres coordinator conformance requires a live database and its configured URL.
        cache: false,
        command: "vp test --passWithNoTests",
      },
    },
  },
  pack: {
    entry: [
      "src/index.ts",
      "src/DurableCoordinatorConformance.ts",
      "src/CodeExecutorConformance.ts",
      "src/CodeExecutorSubstitute.ts",
      "src/DocsResearcher.ts",
      "src/ScriptedModel.ts",
      "src/TravelPlanner.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: {
    cache: false,
    silent: "passed-only",
  },
});
