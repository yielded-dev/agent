import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/MemoryScheduleStore.ts",
      "src/MemorySemanticIndex.ts",
      "src/MemorySubmissionLedger.ts",
      "src/MemorySubscriptionStore.ts",
      "src/MemoryMessageDeliveryStore.ts",
      "src/MemoryThreadStore.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: {
    cache: false,
    silent: "passed-only",
  },
});
