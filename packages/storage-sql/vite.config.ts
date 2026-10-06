import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/SqlThreadImport.ts",
      "src/SqlStorage.ts",
      "src/SqlRunStorage.ts",
      "src/SqlStorageFailpoint.ts",
      "src/SqlStorageProgress.ts",
      "src/SqlJournal.ts",
      "src/SqlThreadStore.ts",
      "src/SqlThreadWork.ts",
      "src/SqlSubmissionLedger.ts",
      "src/SqlAdmissionFacts.ts",
      "src/SqlActivityStore.ts",
      "src/SqlScheduleStore.ts",
      "src/SqliteLayoutInspection.ts",
      "src/SqlThreadNativeReads.ts",
      "src/SqlMessageDeliveryStore.ts",
      "src/SqlLifecyclePublication.ts",
      "src/SqlSubscriptionStore.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: { cache: false, silent: "passed-only" },
});
