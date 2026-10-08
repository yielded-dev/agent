// Limit existing correctness-test concurrency; preserve every test and timeout.
process.env.VITEST_MAX_WORKERS = "1";
