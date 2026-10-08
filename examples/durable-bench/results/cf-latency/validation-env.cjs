// Limit the process-heavy testing package only; preserve Cloudflare pool settings.
// Every existing test and timeout is unchanged. Vite Task preserves NODE_OPTIONS.
if (process.cwd().endsWith("/packages/testing")) process.env.VITEST_MAX_WORKERS = "1";
