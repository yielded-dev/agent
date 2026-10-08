import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      "cf-latency:build": {
        command: "node results/cf-latency/build.mjs",
        cache: false,
      },
      "cf-latency": {
        command: "direnv exec ../.. node results/cf-latency/run.mjs",
        cache: false,
      },
      "cf-latency:analyze": {
        command: "node results/cf-latency/analyze.mjs",
        cache: false,
      },
    },
  },
});
