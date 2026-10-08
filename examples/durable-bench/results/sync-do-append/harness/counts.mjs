import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync, existsSync, realpathSync, readdirSync, copyFileSync } from "node:fs";
import { builtinModules, createRequire } from "node:module";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { arch, cpus, platform, release, totalmem } from "node:os";



const args = process.argv.slice(2);
if (args[0] === "--") args.shift();
const [rootArg, outputArg, mode = "counted", historyArg = "50"] = args;
const historicalTurns = Number(historyArg);
if (![50, 250].includes(historicalTurns)) throw new Error("History must be 50 or 250 turns");
if (!rootArg || !outputArg || !["counted", "plain"].includes(mode)) throw new Error("counts <checkout> <new-output> [counted|plain]");
const root = realpathSync(rootArg);
process.chdir(join(root, "examples/durable-bench"));
const { build, transform, version: esbuildVersion } = await import("esbuild");
const stock = await import(join(root, "examples/durable-bench/bench/targets.ts"));
const { Miniflare, convertV4MiniflareOptions } = await import(createRequire(join(root, "examples/durable-bench/package.json")).resolve("miniflare"));
const output = resolve(outputArg);
if (existsSync(output)) throw new Error(`Refusing existing output ${output}`);
mkdirSync(output, { recursive: true });
const hash = (value) => createHash("sha256").update(value).digest("hex");
const acornDir = readdirSync(join(root, "node_modules/.bun")).find((name) => name.startsWith("acorn@"));
if (!acornDir) throw new Error("Baseline installation has no Acorn parser");
const { parse } = await import(join(root, "node_modules/.bun", acornDir, "node_modules/acorn/dist/acorn.mjs"));
const sites = [];
const metrics = ["admission", "ownership-acquisition", "context-assembly", "model-response-commit", "tool-settlement-commit", "continuation-preparation", "durable-object-append", "settlement"];
const selectors = new Map();
const add = (file, owner, stage = null, options = {}) => {
  const list = selectors.get(file) ?? [];
  list.push({ owner, stage, ...options });
  selectors.set(file, list);
};
const durable = "packages/effect-agent/src/durable/";
add(durable + "DurableAgentRuntime.ts", "submit", "admission");
add(durable + "RunStorage.ts", "claim", "ownership-acquisition");
add("packages/storage-cloudflare/src/DoSubmissionLedger.ts", "claim", "ownership-acquisition");
add(durable + "internal/initial-context.ts", "initialContext", "context-assembly");
add(durable + "RunJournal.ts", "projectRunJournalStream", "context-assembly");
add(durable + "DurableAgentRuntime.ts", "commitTurn", null, {
  property: true,
  stageExpression: '(commit._tag === "Response" || (commit._tag === "Settled" && commit.results.length === 0 && commit.response !== undefined)) ? "model-response-commit" : "tool-settlement-commit"',
  siteExpression: '"DurableAgentRuntime.commitTurn." + commit._tag',
});
add(durable + "DurableAgentRuntime.ts", "terminalize", "settlement");
add(durable + "RunContinuation.ts", "prepare", "continuation-preparation");
for (const name of ["advanceFacts", "commitCaptured", "publishCaptured"]) add(durable + "RunContinuation.ts", name);
for (const name of ["modelResponseRecord", "toolSettledRecords", "runCompletionRecord", "turnCanonicalBatch", "turnResponseBatch", "turnResultsBatch"]) add(durable + "RunJournal.ts", name);
add("packages/storage-cloudflare/src/DoThreadStore.ts", "append", "durable-object-append");
add("packages/storage-cloudflare/src/internal/do-journal.ts", "appendPrepared", "durable-object-append");
for (const name of ["getThread", "prepareAppend", "append"]) add("packages/storage-cloudflare/src/internal/do-journal.ts", name);
add("packages/storage-cloudflare/src/internal/sync-append.ts", "prepareSyncReferences");
for (const name of ["append", "appendRange"]) add("packages/storage-cloudflare/src/internal/sync-append.ts", name, null, { plain: true });
for (const name of ["validateProgress", "workChanges"]) add("packages/storage-cloudflare/src/internal/sync-append-model.ts", name, null, { plain: true });
add("packages/storage-cloudflare/src/internal/canonical-append.ts", "prepareCanonicalAppend");
add("packages/storage-cloudflare/src/DoSubmissionLedger.ts", "publish");
for (const name of ["requireSubmission", "requireOwnership", "readCanonicalSettlement"]) add("packages/storage-cloudflare/src/DoSubmissionLedger.ts", name);
for (const name of ["append", "descriptor"]) add("packages/storage-sql/src/SqlThreadArchiveRange.ts", name);
for (const name of ["apply", "fold", "header", "present", "getEntry", "saveHeader", "indexCanonicalPointers"]) add("packages/storage-sql/src/SqlThreadWork.ts", name);
for (const name of ["apply", "boundary", "publish"]) add("packages/storage-sql/src/internal/settlement-intervals.ts", name);
add("packages/storage-sql/src/SqlThreadNativeReads.ts", "makeProgressAppendValidation", null, { generatorIndex: 1 });
add(durable + "RunContinuation.ts", "validateProgressAppend");
add(durable + "ThreadWork.ts", "workIndexChanges");
add("packages/platform-cloudflare/src/Alarm.ts", "recordProgress");
for (const name of ["write", "flush"]) add("packages/platform-cloudflare/src/internal/due-queue.ts", name, null, { plain: true });
add(durable + "Digest.ts", "digestText", null, { siteExpression: 'value.startsWith(\'{"batch":\') ? "Digest.batchDigest" : "Digest.otherDigest"' });
add(durable + "internal/record-encoding.ts", "captureRecord", null, { plain: true });
add(durable + "internal/canonical-json.ts", "canonicalJson", null, { plain: true });
for (const name of ["sumRunTotals", "summarizeModelUsage"]) add("packages/effect-agent/src/core/Usage.ts", name);
const walk = (node, visit) => {
  if (!node || typeof node !== "object") return;
  if (typeof node.type === "string") visit(node);
  for (const value of Object.values(node)) {
    if (Array.isArray(value)) for (const child of value) walk(child, visit);
    else if (value && typeof value === "object" && typeof value.type === "string") walk(value, visit);
  }
};
const replaceOne = (text, before, after, file) => {
  if (text.split(before).length !== 2) throw new Error(`Expected one ${JSON.stringify(before)} in ${file}`);
  return text.replace(before, after);
};
const saveTransform = (file, original, transformed) => {
  for (const [kind, value] of [["before", original], ["after", transformed]]) {
    const dest = join(output, "transforms", kind, file.replace(/\.ts$/, ".js"));
    mkdirSync(dirname(dest), { recursive: true });
    writeFileSync(dest, value);
  }
};
const instrument = {
  name: "kom433-counts",
  setup(builder) {
    builder.onLoad({ filter: /\.(?:ts|js)$/ }, async ({ path }) => {
      const relative = path.startsWith(root + "/") ? path.slice(root.length + 1) : path;
      if (path.endsWith("/effect/dist/internal/core.js") || path.endsWith("/effect/dist/internal/effect.js")) {
        const original = readFileSync(path, "utf8");
        let text = original;
        const core = path.endsWith("/core.js");
        const constructors = core ? ["PrimitiveImpl", "ExitPrimitive"] : ["AsyncImpl", "IteratorImpl", "ContImpl", "OnFailureImpl", "OnSuccessAndFailureImpl", "MatchImpl", "OnExitImpl"];
        for (const name of constructors) {
          const expression = new RegExp(`(?:const|let) ${name} = function \\([^)]*\\) \\{`);
          const matches = [...text.matchAll(new RegExp(expression.source, "g"))];
          if (matches.length !== 1) throw new Error(`Expected one constructor ${name}: ${matches.length}`);
          const first = matches[0][0];
          const label = core ? `"${name}:" + options.op` : JSON.stringify(name);
          text = text.replace(first, first + `\n    globalThis.__kom433.allocation(${label});`);
          sites.push({ file: relative, sourceSha256: hash(original), kind: "primitive-allocation", site: name });
        }
        if (!core) {
          text = replaceOne(text, "        current = cache.tracerContext ? cache.tracerContext(current, this) : current[evaluate](this);", "        globalThis.__kom433.evaluation(current[\"~effect/Effect/identifier\"], this);\n        current = cache.tracerContext ? cache.tracerContext(current, this) : current[evaluate](this);", relative);
          text = replaceOne(text, "  succeedWith(value) {", "  succeedWith(value) {\n    globalThis.__kom433.inline(this);", relative);
          text = replaceOne(text, "    this._stack = [];", "    this._stack = [];\n    globalThis.__kom433.inherit(this, getCurrentFiber());", relative);
          text = replaceOne(text, "    const span = makeSpanUnsafe(fiber, name, options);", '    const span = makeSpanUnsafe(fiber, name, options);\n    if (name === "sql.execute") globalThis.__kom433.sqlSpan(span);', relative);
          text = replaceOne(text, '  if (span.status._tag === "Ended") return;', '  globalThis.__kom433.closeSqlSpan(span);\n  if (span.status._tag === "Ended") return;', relative);
          sites.push({ file: relative, sourceSha256: hash(original), kind: "evaluation", site: "FiberImpl.runLoop", inlineSite: "FiberImpl.succeedWith", allocationInheritance: "FiberImpl.constructor" });
        }
        saveTransform(core ? "effect/internal/core.js" : "effect/internal/effect.js", original, text);
        return { contents: text, loader: "js" };
      }
      if (relative === "examples/durable-bench/src/yielded.ts") {
        const original = readFileSync(path, "utf8");
        let text = replaceOne(original, "  turn(input: Turn): Promise<void> {\n    return this.run(execute(input));\n  }", "  async turn(input: Turn): Promise<void> {\n    globalThis.__kom433.begin(input);\n    try { await this.run(execute(input)); } finally { globalThis.__kom433.end(); }\n  }", relative);
        text = replaceOne(text, "return { bytes: sql.databaseSize, tables: tables(sql) };", "return { bytes: sql.databaseSize, tables: tables(sql), kom433: globalThis.__kom433.rows() };", relative);
        saveTransform(relative, original, text);
        return { contents: text, loader: "ts" };
      }
      if (path.endsWith("/effect/dist/sql/Statement.js")) {
        const original = readFileSync(path, "utf8");
        let text = replaceOne(original, '    const span = internalEffect.makeSpanUnsafe(fiber, "sql.execute", {', '    const __sqlToken = globalThis.__kom433.enter("effect/sql/Statement.evaluate", null);\n    const span = internalEffect.makeSpanUnsafe(fiber, "sql.execute", {', relative);
        text = replaceOne(text, 'exit => internalEffect.endSpan(span, exit, clock, timingEnabled)', 'exit => { globalThis.__kom433.leave(__sqlToken); return internalEffect.endSpan(span, exit, clock, timingEnabled); }', relative);
        saveTransform("effect/sql/Statement.js", original, text);
        return { contents: text, loader: "js" };
      }
      const selected = selectors.get(relative) ?? [];
      const countSql = /packages\/(?:platform-cloudflare|storage-cloudflare)\/src\//.test(relative) || path.endsWith("/sql-sqlite-do/dist/SqliteClient.js");
      if (selected.length === 0 && !countSql) return undefined;
      const original = readFileSync(path, "utf8");
      const transformed = (await transform(original, { loader: "ts", format: "esm", target: "es2024" })).code;
      const ast = parse(transformed, { ecmaVersion: "latest", sourceType: "module", locations: true });
      const edits = [];
      if (countSql) walk(ast, (node) => {
        if (node.type !== "CallExpression" || node.callee.type !== "MemberExpression" || node.callee.computed || node.callee.property.name !== "exec") return;
        edits.push([node.start, "globalThis.__kom433.exec("]);
        edits.push([node.callee.object.end, ", ", node.arguments[0]?.start ?? node.end - 1]);
      });
      for (const selection of selected) {
        const owners = [];
        walk(ast, (node) => {
          if (selection.property ? node.type === "Property" && (node.key.name ?? node.key.value) === selection.owner : node.type === "VariableDeclarator" && node.id.name === selection.owner) owners.push(node);
        });
        if (owners.length !== 1) throw new Error(`Expected one owner ${relative}:${selection.owner}, got ${owners.length}`);
        const functions = [];
        walk(selection.property ? owners[0].value : owners[0].init, (node) => {
          if (["FunctionExpression", "ArrowFunctionExpression"].includes(node.type) && node.body.type === "BlockStatement" && (selection.plain || node.generator)) functions.push(node);
        });
        const fn = functions[selection.generatorIndex ?? 0];
        if (!fn) throw new Error(`No function body for ${relative}:${selection.owner}`);
        const site = selection.siteExpression ?? JSON.stringify(relative.split("/").at(-1).replace(/\.ts$/, "") + "." + selection.owner);
        const stage = selection.stageExpression ?? JSON.stringify(selection.stage);
        const token = `__kom433_${sites.length}`;
        edits.push([fn.body.start + 1, `\nconst ${token} = globalThis.__kom433.enter(${site}, ${stage}); try {\n`]);
        edits.push([fn.body.end - 1, `\n} finally { globalThis.__kom433.leave(${token}); }\n`]);
        sites.push({ file: relative, sourceSha256: hash(original), kind: selection.plain ? "plain-function" : "generator-body", owner: selection.owner, siteExpression: site, stageExpression: stage, transformedFunction: { start: fn.loc.start, end: fn.loc.end } });
      }
      let text = transformed;
      for (const [offset, insertion, end = offset] of edits.sort((a, b) => b[0] - a[0])) text = text.slice(0, offset) + insertion + text.slice(end);
      saveTransform(relative, transformed, text);
      return { contents: text, loader: "js" };
    });
  },
};
const outfile = join(output, "yielded.mjs");
const report = { mode, fixture: `durable-bench:${historicalTurns}-history-plus-10-eight-tool-turns`, root, node: process.version, esbuildVersion, environment: { arch: arch(), platform: platform(), release: release(), cpu: cpus()[0].model, cpuCount: cpus().length, memoryBytes: totalmem() }, stageNames: metrics, seed: null, measured: null, failure: null };
let mf;
const persist = () => writeFileSync(join(output, "report.json"), JSON.stringify(report, null, 2) + "\n");
const start = async () => {
  mf = new Miniflare(convertV4MiniflareOptions({
    modulesRoot: "/",
    modules: [{ type: "ESModule", path: outfile }],
    compatibilityDate: "2026-08-18",
    compatibilityFlags: ["nodejs_compat"],
    durableObjects: { THREADS: { className: "YieldedDO", useSQLite: true } },
    resourcePersistencePath: join(output, "persistence"),
  }));
  await mf.ready;
};
const call = (path, body) => stock.call(mf, path, body);
try {
  await build({ entryPoints: [join(root, "examples/durable-bench/src/yielded.ts")], outfile, bundle: true, format: "esm", platform: "neutral", target: "es2024", conditions: ["workerd", "worker", "browser", "import"], mainFields: ["module", "main"], external: ["cloudflare:*", "node:*", ...builtinModules], logLevel: "error", ...(mode === "counted" ? { plugins: [instrument], banner: { js: readFileSync(fileURLToPath(new URL("./count-probe.js", import.meta.url)), "utf8") } } : {}) });
  writeFileSync(join(output, "sites.json"), JSON.stringify(sites, null, 2) + "\n");
  report.bundleSha256 = hash(readFileSync(outfile));
  report.controller = { bun: process.versions.bun ?? null, nodeCompatibility: process.version };
  persist();
  await start();
  await call("/setup");
  const history = Array.from({ length: historicalTurns }, (_, n) => ({ id: `h${n}`, text: `turn h${n} tools=${[1, 1, 0][n % 3]}` }));
  const seedFingerprint = await call("/seed", history);
  const reference = historicalTurns === 50 ? "b017b487524e44a4" : "dcea9f30b0917245";
  if (seedFingerprint !== reference) throw new Error(`Fingerprint ${seedFingerprint} != ${reference}`);
  const seedStats = await call("/stats");
  delete seedStats.kom433;
  report.seed = { turns: historicalTurns, fingerprint: seedFingerprint, reference, stats: seedStats };
  persist();
  console.log(JSON.stringify({ phase: "seed-complete", fingerprint: seedFingerprint }));
  await mf.dispose();
  mf = undefined;
  await start();
  await call("/wake");
  const turns = Array.from({ length: 10 }, (_, n) => ({ id: `m${n}`, text: `turn m${n} tools=8` }));
  const measuredFingerprint = await call("/seed", turns);
  report.measured = { turns: 10, historicalTurns, toolCallsPerTurn: 8, fingerprint: measuredFingerprint, stats: await call("/stats") };
  console.log(JSON.stringify({ phase: "measured-complete", fingerprint: measuredFingerprint }));
} catch (error) {
  report.failure = error instanceof Error ? error.stack : String(error);
  process.exitCode = 1;
  console.error(report.failure);
} finally {
  try {
    if (mf) await mf.dispose();
  } catch (error) {
    report.disposeFailure = error instanceof Error ? error.stack : String(error);
    process.exitCode = 1;
  }
  persist();
}
