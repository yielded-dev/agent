import { createHash } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { basename, dirname, join, relative, resolve } from "node:path";
import { gunzipSync } from "node:zlib";

// Task-local evidence reduction, not a committed product test suite.
const [captureArg, repeatArg, plainArg, outputArg] = process.argv.slice(2);
if (!outputArg) throw new Error("summarize <capture-dir> <repeat-dir> <plain-dir> <output-prefix>");
const load = (dir, name) => JSON.parse(existsSync(join(dir, name))
  ? readFileSync(join(dir, name), "utf8")
  : gunzipSync(readFileSync(join(dir, name + ".gz"))).toString());
const hash = (value) => createHash("sha256").update(value).digest("hex");
const capture = load(captureArg, "report.json");
const repeat = load(repeatArg, "report.json");
const plain = load(plainArg, "report.json");
const selectors = load(captureArg, "sites.json");
const rows = capture.measured.stats.kom433;
const assert = (ok, message) => { if (!ok) throw new Error(message); };
for (const run of [capture, repeat, plain]) {
  assert(run.failure === null, "A run failed");
  assert(run.sourceRevision === "8c05714de84d68961b14e5ab7a3b7d809599563f", "Source revision changed");
  assert(run.seed.fingerprint === "b017b487524e44a4", "50-turn fingerprint mismatch");
  assert(run.measured.fingerprint === "b73859cee894aca6", "Measured fingerprint mismatch");
}
assert(rows.length === 10, "Expected ten counted turns");
assert(capture.harnessSha256 === repeat.harnessSha256 && capture.probeSha256 === repeat.probeSha256, "Repeat harness mismatch");
assert(JSON.stringify(rows) === JSON.stringify(repeat.measured.stats.kom433), "Repeat raw counters differ");
for (const phase of ["seed", "measured"]) assert(JSON.stringify(capture[phase].stats.tables) === JSON.stringify(plain[phase].stats.tables), "Plain/count table shape differs");
const metrics = ["evaluations", "allocations", "inlineSuccesses", "iteratorSuccesses"];
const sum = (table, key) => Object.values(table).reduce((n, row) => n + row[key], 0);
for (const row of rows) {
  assert(row.unclosedSites.length === 0, `${row.input.id}: open scopes`);
  for (const table of ["exclusiveStages", "exclusiveSites", "exclusiveModules"]) for (const key of metrics) assert(sum(row[table], key) === row.total[key], `${row.input.id}: ${table} ${key} does not close`);
  for (const table of ["originSites", "originModules"]) for (const key of metrics.filter((k) => k !== "inlineSuccesses")) assert(sum(row[table], key) === row.total[key], `${row.input.id}: ${table} ${key} does not close`);
  assert(Object.values(row.operations).reduce((a,b) => a+b, 0) === row.total.evaluations, "Opcode counts do not close");
  assert(Object.values(row.allocationKinds).reduce((a,b) => a+b, 0) === row.total.allocations, "Constructor counts do not close");
  for (const key of metrics) assert(sum(row.outsideStageSites, key) === row.exclusiveStages["outside-eight-stages"][key], "Outside-stage breakdown does not close");
  assert(row.exclusiveSites.unattributed.evaluations / row.total.evaluations < 0.05, "Unassigned evaluations exceed 5%");
  assert(row.exclusiveSites.unattributed.allocations / row.total.allocations < 0.05, "Unassigned allocations exceed 5%");
  for (const [site, value] of Object.entries(row.originSites)) if (site.startsWith("unassigned:") || site.startsWith("runtime-constructor:")) assert(value.evaluations / row.total.evaluations < 0.05, "Origin bucket without a caller exceeds 5%");
}
const first = rows[0];
const rank = (table, key = "evaluations") => Object.entries(table).map(([site, counts]) => ({site, ...counts})).sort((a,b) => b[key] - a[key] || a.site.localeCompare(b.site));
const metadata = new Map();
for (const selector of selectors) {
  try { metadata.set(JSON.parse(selector.siteExpression), selector); } catch { /* Dynamic selectors below. */ }
}
const findMetadata = (site) => metadata.get(site) ?? selectors.find((s) => site.startsWith("DurableAgentRuntime.commitTurn.") && s.owner?.endsWith("commitTurn")) ?? (site.startsWith("Digest.") ? selectors.find((s) => s.owner?.endsWith("digestText")) : undefined);
const sourceLink = (site) => {
  const info = findMetadata(site);
  if (!info) return `\`${site}\``;
  const source = info.file.startsWith("effect/") ? "node_modules/" + info.file : info.file;
  const label = `${info.file.split("/").at(-1)}:${info.sourcePosition?.line ?? info.owner} · ${info.owner}`;
  return `[${label}](${relative(dirname(resolve(outputArg)), join(capture.root, source))})`;
};
const percent = (n, denominator) => (100 * n / denominator).toFixed(2) + "%";
const table = (values, link = true) => [
  "| Site / module | Evaluations | % evals | Allocations | % allocations |",
  "|---|---:|---:|---:|---:|",
  ...values.map((r) => `| ${link ? sourceLink(r.site) : `\`${r.site}\``} | ${r.evaluations} | ${percent(r.evaluations, first.total.evaluations)} | ${r.allocations} | ${percent(r.allocations, first.total.allocations)} |`),
].join("\n");
const coverage = rows.map((row, index) => ({
  input: row.input, historicalTurns: 50 + index, ...row.total,
  dynamicUnassigned: row.exclusiveSites.unattributed,
  largestUnassignedOrigin: rank(Object.fromEntries(Object.entries(row.originSites).filter(([s]) => s.startsWith("unassigned:"))))[0],
  largestNoCallerOrigin: rank(Object.fromEntries(Object.entries(row.originSites).filter(([s]) => s.startsWith("runtime-constructor:"))))[0],
  unclosedSites: row.unclosedSites,
}));
const summary = {
  sourceRevision: capture.sourceRevision, sourceTree: capture.sourceTree,
  bundleSha256: capture.bundleSha256, buildInputsSha256: capture.buildInputsSha256,
  harnessSha256: capture.harnessSha256, probeSha256: capture.probeSha256,
  rawReportSha256: hash(JSON.stringify(capture)), repeatedCounterRowsSha256: hash(JSON.stringify(rows)),
  verification: { repeatAllCounterRowsIdentical: true, plainFingerprintsAndTableCountsMatch: true, allExclusiveCountersClose: true, allOriginCountersClose: true, allScopesClose: true, everyUnassignedBucketBelowFivePercent: true },
  coverage,
  firstTurn: Object.fromEntries(["exclusiveSites", "inclusiveSites", "originSites", "exclusiveModules", "inclusiveModules", "originModules", "outsideStageSites"].map((key) => [key, rank(first[key])])),
  stages: {exclusive:first.exclusiveStages, inclusive:first.inclusiveStages},
  operations:first.operations, allocationKinds:first.allocationKinds,
  stageSelectors: selectors.filter((s) => s.stageExpression && s.stageExpression !== "null"),
};
writeFileSync(outputArg + ".json", JSON.stringify(summary, null, 2) + "\n");
const stageRows = capture.stageNames.map((stage) => `| ${stage} | ${first.exclusiveStages[stage].evaluations} | ${first.inclusiveStages[stage].evaluations} | ${first.exclusiveStages[stage].allocations} | ${first.inclusiveStages[stage].allocations} | ${first.exclusiveStages[stage].calls} | ${first.inclusiveStages[stage].calls} |`);
const focus = Object.fromEntries(Object.entries(first.originSites).filter(([s]) => /Semaphore|[Tt]elemetry|[Tt]racer|[Ff]ailpoint|internal\/effect.js:(?:makeFn|.*Span)/.test(s)));
const engineFocus = Object.fromEntries(Object.entries(first.inclusiveSites).filter(([s]) => /LanguageModel.js|ownModelResponse|captureModelResponse|executePreparedToolCall|executeToolBatch|makeTurn/.test(s)));
const summaryFile = basename(outputArg) + (existsSync(outputArg + ".json.gz") ? ".json.gz" : ".json");
const text = `# Deterministic attribution: effect-eval-cost

At exactly 50 historical turns, the first reopened turn performs **${first.total.evaluations} run-loop evaluations**, **${first.total.allocations} selected primitive allocations**, **${first.total.inlineSuccesses} inline-success continuations**, and **${first.total.iteratorSuccesses} successful Exits consumed directly by generators**. The dynamic unassigned remainder is **30 evaluations (${percent(30, first.total.evaluations)}) / 111 allocations (${percent(111, first.total.allocations)})**. The largest unknown protocol origin is Service dispatch: **665 evaluations (${percent(665, first.total.evaluations)})**. The largest known-constructor bucket without a selected caller is Success: **1,259 evaluations (${percent(1259, first.total.evaluations)})**. Every individual unassigned bucket is below 5% for all ten turns.

Two independent final captures have identical complete counter rows, including all site/module tables and scope-abandonment records. Both retain the historical fingerprint **b017b487524e44a4** and measured fingerprint **b73859cee894aca6**. The uninstrumented control has both fingerprints and identical canonical table row counts. This is deterministic work attribution, with no CPU, latency, Inspector, profiling, or clock sampling.

## Identity and scope

- Source: \`${capture.sourceRevision}\`; tree \`${capture.sourceTree}\`.
- Counted bundle SHA-256: \`${capture.bundleSha256}\`.
- Build-input inventory digest: \`${capture.buildInputsSha256}\`.
- Harness SHA-256: \`${capture.harnessSha256}\`; probe: \`${capture.probeSha256}\`.
- Effect ${capture.effectVersion} with the repository patch; esbuild ${capture.esbuildVersion}; Node ${capture.node}; ${capture.environment.platform}/${capture.environment.arch}.
- The preserved harness path is retained: workspace TypeScript source plus installed Effect JavaScript, bundled for workerd. These are source attribution counts, not a claim that a separately built uninstrumented hosted bundle has identical optimization behavior. Every bundle input is hashed in \`capture/build-inputs.json\`.
- Seed h0–h49 uses the repeating tool pattern 1,1,0. The worker closes/reopens, runs recovery outside the window, then m0–m9 each issue eight sequential tool calls and nine model requests. Only m0 has exactly 50 preceding turns; later rows include prior measured turns. No provider network calls occur.
- Wake/recovery warms storage, but m0 still constructs some model/decoder caches after reopening. The original fixture is preserved; it is not a separate steady-cache run at a fixed 50-turn history.
- The fingerprint hashes normalized last-provider-request messages; it excludes system messages and the final answer emitted afterward. It is not a canonical archive hash.

## Growing-history rows

| Turn | History | Evaluations | Allocations | Inline success | Generator success |
|---|---:|---:|---:|---:|---:|
${coverage.map(r=>`| ${r.input.id} | ${r.historicalTurns} | ${r.evaluations} | ${r.allocations} | ${r.inlineSuccesses} | ${r.iteratorSuccesses} |`).join("\n")}

## Eight-stage counters at 50 history turns

| Stage | Exclusive evals | Inclusive evals | Exclusive allocations | Inclusive allocations | Entries | Outermost entries |
|---|---:|---:|---:|---:|---:|---:|
${stageRows.join("\n")}
| Outside eight named stages | ${first.exclusiveStages["outside-eight-stages"].evaluations} | — | ${first.exclusiveStages["outside-eight-stages"].allocations} | — | — | — |
| Whole turn | ${first.total.evaluations} | — | ${first.total.allocations} | — | — | — |

The 15,149-evaluation stage remainder is classified by the site tables below; it is not a single unknown bucket. Stage entries count all selected boundaries, while outermost entries deduplicate nested identical stage names (e.g. append facade plus journal: 23 entries, 12 outermost entries). Inclusive stages overlap and must not be summed. Exact boundaries and scope semantics are in [README.md](README.md).

## Construction-origin modules, ranked by evaluations

${table(rank(first.originModules).slice(0,20),false)}

## Construction-origin sites, ranked by evaluations

${table(rank(first.originSites).slice(0,25))}

## Construction-origin modules, ranked by allocations

${table(rank(first.originModules,"allocations").slice(0,20),false)}

## Construction-origin sites, ranked by allocations

${table(rank(first.originSites,"allocations").slice(0,20))}

## Dynamic exclusive modules, ranked by evaluations

${table(rank(first.exclusiveModules).slice(0,20),false)}

## Dynamic exclusive sites, ranked by evaluations

${table(rank(first.exclusiveSites).slice(0,25))}

## Outside the eight stages, by dynamic exclusive site

${table(rank(first.outsideStageSites).slice(0,25))}

## Engine tool/model loops and response handling, inclusive dynamic sites

${table(rank(engineFocus).slice(0,19))}

These rows overlap: for example executeToolBatch includes executePreparedToolCall. At m0 there are eight executeToolBatch entries, eight executePreparedToolCall entries, nine makeTurn / LanguageModel.streamText entries, and twenty ownModelResponsePart entries (sixteen tool-call/finish parts, then four final-text parts). General response capture runs seventeen times; the three primitive text parts use the direct path. Raw call counters and selectors preserve the exact boundaries. Decoder factories can return effects that run after the factory scope closes, so construction-origin schema tables supply the complementary view.

## Semaphore, tracing and failpoint construction origins

${table(rank(focus).slice(0,30))}

These origin counts describe primitives constructed by each helper and subsequent dispatches of those objects. A helper returning a shared singleton can have calls but no allocations. A tracing helper's primitive count excludes ordinary JavaScript span-object allocation.

## Zero-dispatch successes and interpretation

The patched Effect interpreter can bypass runLoop for successful Exits, and ContImpl continuations can call succeedWith directly. Schema.decodeEffect may parse synchronously, allocate success values, and never dispatch those values. Effect.void is a reusable Success singleton; yielding it need not allocate or dispatch. Therefore evaluation count is neither allocation count nor the count of semantic Effect operations.

The ${first.total.inlineSuccesses} succeedWith entries and ${first.total.iteratorSuccesses} direct successful-Exit consumptions are separate observations, not synthetic run-loop evaluations. succeedWith periodically returns an Exit to the loop, so not every entry bypasses dispatch. The iterator counter covers the ordinary/eager generator interpreter paths, not every eager combinator or every decoder fast path. Allocations are nine selected constructor families (primitive/Exit plus Async, Iterator, Cont, OnFailure, OnSuccessAndFailure, Match, OnExit), not bytes or all JavaScript heap allocation. Don't estimate total CPU from evaluations alone, or apply one unit cost to both dispatching and zero-dispatch primitives.

The dynamic readPrompt body owns 7,615 evaluations, while its construction-origin count is much smaller: schema parser continuations execute beneath the prompt-read scope after their factory functions returned. Expanded SchemaAST selectors expose the construction source rather than leaving that work lumped into the parent. Dynamic and origin views are alternative partitions, never additive.

## Evidence

- [${summaryFile}](${summaryFile}): all first-turn ranked tables, exact stage selector map, per-turn closure/coverage checks and identities.
- \`capture/report.json.gz\`: complete final raw rows; \`repeat/report.json.gz\`: independent identical raw rows; \`plain-control/report.json.gz\`: uninstrumented fixture control.
- \`capture/sites.json.gz\`: every selector, original module hash, source-mapped location and transformed-function range; \`capture/build-inputs.json\`: complete source hashes; \`capture/inputs.json\`: instrumented module hashes.
- [attempts.json](attempts.json): concise intermediate/failed capture metadata. Earlier stage tables are superseded.

No product file or toolchain manifest changed. The parent owns the combined repository gate and final report; no suite was added or run by this attribution harness.
`;
writeFileSync(outputArg + ".md", text);
console.log(JSON.stringify({ verified: summary.verification, first: coverage[0], outputs:[outputArg + ".json", outputArg + ".md"] }));
