// Bound file workers in Node test packages while the root task runs four packages.
// Preserve Cloudflare pool settings, every test, and every existing timeout.
// Apply in package children: setting this in the root would leak into Cloudflare.
const cwd = process.cwd();

if (
  /\/(packages|tooling|examples)\/[^/]+$/.test(cwd) &&
  !/\/packages\/(platform-cloudflare|storage-cloudflare)$/.test(cwd)
) {
  process.env.VITEST_MAX_WORKERS = "1";
}
