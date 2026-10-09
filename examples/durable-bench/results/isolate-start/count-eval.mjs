// Count function entries during global evaluation of copies; never measure time.
// Usage from the repository root: vp node count-eval.mjs --out /private/tmp/isolate-counts [--instrument-only] [--by-origin] bundle.mjs ...
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { basename, dirname, join, resolve, sep } from "node:path";

const args = process.argv.slice(2);
const outputIndex = args.indexOf("--out");
if (outputIndex < 0 || !args[outputIndex + 1]) throw new Error("Specify --out outside the repository");
const outputDir = resolve(args.splice(outputIndex, 2)[1]);
const repository = resolve(".");
if (outputDir === repository || outputDir.startsWith(repository + sep)) throw new Error("Keep instrumented copies outside the repository");
mkdirSync(outputDir, { recursive: true, mode: 0o700 });
const benchRequire = createRequire(resolve("examples/durable-bench/package.json"));
const { parse } = createRequire(benchRequire.resolve("miniflare"))("acorn");
const instrumentOnly = process.argv.includes("--instrument-only");
const byOrigin = process.argv.includes("--by-origin");
const bundles = args.filter(arg => !["--instrument-only", "--by-origin"].includes(arg));
if (!bundles.length) throw new Error("Usage: vp node count-eval.mjs --out <temporary-directory> [--instrument-only] bundle.mjs ...");
const hash = text => createHash("sha256").update(text).digest("hex");
const compare = (a, b) => a < b ? -1 : a > b ? 1 : 0;
const ordered = object => Object.fromEntries(Object.entries(object).sort(([a], [b]) => compare(a, b)));
const counter = "__yieldedIsolateEvalCounts_v1";
const initCounter = "__yieldedIsolateModuleInits_v1";
const functionTypes = new Set(["FunctionDeclaration", "FunctionExpression", "ArrowFunctionExpression"]);
const children = node => Object.entries(node).filter(([key]) => !["start", "end", "loc"].includes(key))
  .flatMap(([, value]) => Array.isArray(value) ? value : [value]).filter(value => value?.type);
const family = file => file.match(/(?:^|\/)node_modules\/effect\/(?:src|dist)\/(Schema|SchemaAST|internal\/schema\/toEquivalence)\.[cm]?[jt]s$/)?.[1];

function instrument(file) {
  const source = readFileSync(file, "utf8"), comments = [];
  if (source.includes(counter)) throw new Error("Input already contains counter identifier");
  const ast = parse(source, { ecmaVersion: "latest", sourceType: "module", locations: true, onComment: comments });
  const markers = comments.filter(c => !c.block && /(?:^|\/)(?:node_modules|packages|tooling|examples)\//.test(c.value.trim()) && !/\s/.test(c.value.trim()));
  const aliases = new Map(), sites = [], edits = [], ordinals = new Map(), initializers = [];
  function findExports(node) {
    if (node.type === "CallExpression" && node.callee.name === "__export") {
      for (const property of node.arguments[1].properties) {
        const symbol = property.value?.body;
        if (symbol?.type === "Identifier") aliases.set(symbol.name, property.key.name ?? property.key.value);
      }
      return;
    }
    children(node).forEach(findExports);
  }
  let current = -1, module;
  for (const statement of ast.body) {
    while (current + 1 < markers.length && markers[current + 1].end < statement.start) current++;
    module = family(markers[current]?.value.trim() ?? "");
    // Canonical public names keep esbuild's numeric suffixes out of useful labels.
    if (module) findExports(statement);
    // Generated lazy module wrappers are counted separately from Effect functions.
    for (const declaration of statement.declarations ?? []) {
      const call = declaration.init;
      if (call?.type !== "CallExpression" || call.callee.name !== "__esm" || call.arguments[0]?.type !== "ObjectExpression") continue;
      for (const property of call.arguments[0].properties) {
        const fn = property.value;
        if (!functionTypes.has(fn?.type) || fn.body.type !== "BlockStatement") throw new Error("Unexpected __esm wrapper");
        const id = initializers.length;
        initializers.push({ id, module: property.key.value ?? property.key.name, binding: declaration.id.name, bundleLine: fn.loc.start.line });
        edits.push({ start: fn.body.start + 1, end: fn.body.start + 1, text: `;${initCounter}[${id}]++;`, order: edits.length });
      }
    }
  }
  const name = node => node?.type === "Identifier" ? aliases.get(node.name) ?? node.name : node?.value ?? node?.type ?? "anonymous";
  function visit(node, path) {
    if (!node) return;
    if (node.type === "VariableDeclarator") { visit(node.init, [...path, name(node.id)]); return; }
    if (node.type === "AssignmentExpression" && node.left.type === "Identifier") { visit(node.right, [...path, name(node.left)]); return; }
    if (node.type === "CallExpression" && node.callee.name === "__export") return;
    if (node.type === "CallExpression" && node.callee.name === "__esm") {
      for (const property of node.arguments[0].properties) property.value.body.body.forEach(statement => visit(statement, []));
      return;
    }
    if (node.type === "ClassDeclaration" || node.type === "ClassExpression") {
      const scope = node.id && path.at(-1) !== name(node.id) ? [...path, name(node.id)] : path;
      visit(node.superClass, [...scope, "heritage"]);
      node.body.body.forEach(member => visit(member, scope)); return;
    }
    if (["Property", "MethodDefinition", "PropertyDefinition"].includes(node.type)) {
      if (node.computed) visit(node.key, [...path, "computed-key"]);
      visit(node.value, [...path, `${node.kind === "get" || node.kind === "set" ? node.kind + ":" : ""}${name(node.key)}`]); return;
    }
    if (functionTypes.has(node.type)) {
      const scope = node.id && path.at(-1) !== name(node.id) ? [...path, name(node.id)] : path;
      const label = scope.join("/") || "anonymous", base = `${module}:${label}`;
      const ordinal = (ordinals.get(base) ?? 0) + 1; ordinals.set(base, ordinal);
      const site = { id: sites.length, key: ordinal === 1 ? base : `${base}#${ordinal}`, module, function: label,
        bundleLine: node.loc.start.line, kind: node.type, generator: Boolean(node.generator), async: Boolean(node.async) };
      sites.push(site);
      const increment = `${counter}[${site.id}]++`;
      if (node.body.type === "BlockStatement") {
        let at = node.body.start + 1;
        for (const statement of node.body.body) {
          if (statement.type !== "ExpressionStatement" || typeof statement.expression.value !== "string") break;
          at = statement.end;
        }
        edits.push({ start: at, end: at, text: `;${increment};`, order: edits.length });
      } else {
        edits.push({ start: node.body.start, end: node.body.start, text: `(${increment}, (`, order: edits.length });
        edits.push({ start: node.body.end, end: node.body.end, text: "))", order: edits.length });
      }
      node.params.forEach(parameter => visit(parameter, [...scope, "parameter"]));
      visit(node.body, scope); return;
    }
    if (node.type === "CallExpression" || node.type === "NewExpression") {
      visit(node.callee, [...path, "callee"]);
      node.arguments.forEach((arg, index) => visit(arg, [...path, `arg${index}`])); return;
    }
    if (node.type === "ReturnStatement") { visit(node.argument, [...path, "return"]); return; }
    children(node).forEach(child => visit(child, path));
  }
  current = -1;
  let previousOrigin;
  for (const statement of ast.body) {
    while (current + 1 < markers.length && markers[current + 1].end < statement.start) current++;
    const origin = markers[current]?.value.trim() ?? "<esbuild helpers>";
    module = family(origin);
    if (byOrigin && previousOrigin !== origin) {
      edits.push({ start: statement.start, end: statement.start, text: `__isolateFlush_v1(${JSON.stringify(origin)});\n`, order: edits.length });
      previousOrigin = origin;
    }
    if (statement.type === "ExportNamedDeclaration" && statement.specifiers.some(s => (s.exported.name ?? s.exported.value) === "default")) {
      const remaining = statement.specifiers.filter(s => (s.exported.name ?? s.exported.value) !== "default");
      if (statement.source) throw new Error("External default re-export is unsupported");
      edits.push({ start: statement.start, end: statement.end, text: `export { ${remaining.map(s => source.slice(s.start, s.end)).join(", ")} };`, order: edits.length });
    } else if (statement.type === "ExportDefaultDeclaration") {
      const declaration = statement.declaration;
      const text = source.slice(declaration.start, declaration.end);
      const replacement = declaration.id && ["FunctionDeclaration", "ClassDeclaration"].includes(declaration.type)
        ? text : `const __isolateOriginalDefault_v1 = (${text});`;
      edits.push({ start: statement.start, end: statement.end, text: replacement, order: edits.length });
    }
    if (!module || statement.expression?.callee?.name === "__export") continue;
    visit(statement, []);
  }
  if (!["Schema", "SchemaAST", "internal/schema/toEquivalence"].every(m => sites.some(s => s.module === m))) {
    throw new Error("All three requested Effect source sections must be present");
  }
  let rewritten = source;
  for (const edit of edits.sort((a, b) => b.start - a.start || a.order - b.order)) {
    rewritten = rewritten.slice(0, edit.start) + edit.text + rewritten.slice(edit.end);
  }
  const originPrefix = byOrigin ? `
let __isolateOrigin_v1 = "<esbuild helpers>";
const __isolateOrigins_v1 = {}, __isolatePrevious_v1 = ${counter}.slice();
function __isolateFlush_v1(next) {
  const row = __isolateOrigins_v1[__isolateOrigin_v1] ??= {};
  for (let i = 0; i < ${counter}.length; i++) {
    const delta = ${counter}[i] - __isolatePrevious_v1[i];
    if (delta) row[i] = (row[i] ?? 0) + delta;
    __isolatePrevious_v1[i] = ${counter}[i];
  }
  __isolateOrigin_v1 = next;
}\n` : "";
  const snapshot = `${byOrigin ? '__isolateFlush_v1("<end>");\n' : ""}const __isolateSnapshot_v1 = { counts: ${counter}.slice(), initializers: ${initCounter}.slice()${byOrigin ? ", origins: __isolateOrigins_v1" : ""} };`;
  rewritten = `const ${counter} = new Array(${sites.length}).fill(0), ${initCounter} = new Array(${initializers.length}).fill(0);\n` + originPrefix + rewritten +
    `\n${snapshot}\nexport default { fetch() { return Response.json(__isolateSnapshot_v1); } };\n`;
  // Parsing the copy catches invalid insertions without executing it in Node.
  parse(rewritten, { ecmaVersion: "latest", sourceType: "module" });
  const stem = basename(file).replace(/\.[^.]+$/, "") + "-" + hash(source).slice(0, 8) + (byOrigin ? "-origins" : ""), copy = join(outputDir, `${stem}.instrumented.mjs`);
  const manifest = { input: basename(file), inputSha256: hash(source), instrumentedSha256: hash(rewritten), sites, initializers };
  writeFileSync(copy, rewritten);
  writeFileSync(join(outputDir, `${stem}-instrumentation.json`), JSON.stringify(manifest, null, 2) + "\n");
  return { stem, copy, manifest };
}

const jobs = bundles.map(instrument);
if (instrumentOnly) {
  process.stdout.write(JSON.stringify(jobs.map(job => ({ copy: job.copy, functions: job.manifest.sites.length })), null, 2) + "\n");
  process.exit(0);
}
const runtimeDir = join(outputDir, "runtime-tmp"); mkdirSync(runtimeDir, { recursive: true });
process.env.TMPDIR = runtimeDir;
const miniflarePath = dirname(benchRequire.resolve("miniflare/package.json"));
const require = createRequire(import.meta.url);
const { Miniflare, NoOpLog, convertV4MiniflareOptions } = require(join(miniflarePath, "dist/src/index.js"));
const configuration = { compatibilityDate: "2026-08-01", compatibilityFlags: ["nodejs_compat"] };
const reports = [];
for (const job of jobs) {
  const runtime = new Miniflare(convertV4MiniflareOptions({ rootPath: outputDir, scriptPath: job.copy, modules: true, host: "127.0.0.1", port: 0,
    ...configuration, log: new NoOpLog(), cachePersist: false, durableObjectsPersist: false }));
  let counts, origins, initializers;
  try {
    const response = await runtime.dispatchFetch("http://isolate-count.local/");
    if (!response.ok) throw new Error(`Counter fetch failed: ${response.status} ${await response.text()}`);
    const snapshot = await response.json();
    counts = snapshot.counts;
    initializers = snapshot.initializers;
    origins = byOrigin ? snapshot.origins : undefined;
  } finally { await runtime.dispose(); }
  if (!Array.isArray(counts) || counts.length !== job.manifest.sites.length || counts.some(n => !Number.isSafeInteger(n) || n < 0)) throw new Error("Invalid counter snapshot");
  if (!Array.isArray(initializers) || initializers.length !== job.manifest.initializers.length || initializers.some(n => !Number.isSafeInteger(n) || n < 0)) throw new Error("Invalid initializer snapshot");
  const byModule = {}, entries = job.manifest.sites.map((site, index) => ({ ...site, count: counts[index] }));
  for (const entry of entries) byModule[entry.module] = (byModule[entry.module] ?? 0) + entry.count;
  const report = { version: 1, input: job.manifest.input, inputSha256: job.manifest.inputSha256, toolSha256: hash(readFileSync(new URL(import.meta.url))),
    runtime: { miniflare: require(join(miniflarePath, "package.json")).version, ...configuration },
    semantics: "Instrumented function-entry counts through the end of module evaluation only. Includes nested callbacks, methods, getters, and constructors in the three selected Effect modules; excludes generated namespace export thunks and module wrapper functions. Generated __esm initializer entries are recorded separately in moduleInitializers. No CPU/time measurement. Original global initializers retained; fetch and later runtime work excluded by an end-of-module snapshot. Counts from instrumented copies, not deployed production.",
    totalEntries: counts.reduce((sum, value) => sum + value, 0), byModule: ordered(byModule), functions: entries.sort((a, b) => compare(a.key, b.key)),
    moduleInitializers: job.manifest.initializers.map((site, i) => ({ ...site, count: initializers[i] })).sort((a, b) => compare(a.module, b.module)),
    largest: entries.filter(e => e.count).sort((a, b) => b.count - a.count || compare(a.key, b.key)).slice(0, 40) };
  if (byOrigin) {
    report.originSemantics = "Counters accumulated while each emitted top-level source section is active, including synchronous transitive calls. This identifies construction-triggering sections, not function-definition ownership or CPU. No interprocedural call stacks. For wrapped/lazy modules the active caller section owns their work.";
    report.byOrigin = Object.entries(origins).map(([origin, row]) => {
      const functions = Object.entries(row).map(([id, count]) => ({ key: job.manifest.sites[Number(id)].key, count }));
      return { origin, totalEntries: functions.reduce((sum, f) => sum + f.count, 0), functions: functions.sort((a, b) => b.count - a.count || compare(a.key, b.key)) };
    }).filter(row => row.totalEntries).sort((a, b) => b.totalEntries - a.totalEntries || compare(a.origin, b.origin));
    if (report.byOrigin.reduce((sum, row) => sum + row.totalEntries, 0) !== report.totalEntries) throw new Error("Origin counters do not reconcile");
  }
  writeFileSync(join(outputDir, `${job.stem}-eval-counts.json`), JSON.stringify(report, null, 2) + "\n");
  reports.push(report);
  process.stdout.write(JSON.stringify({ input: report.input, totalEntries: report.totalEntries, byModule: report.byModule }) + "\n");
}
if (reports.length === 2) {
  const before = new Map(reports[0].functions.map(e => [e.key, e.count])), after = new Map(reports[1].functions.map(e => [e.key, e.count]));
  const changes = [...new Set([...before.keys(), ...after.keys()])].sort(compare).map(key => ({ key, before: before.get(key) ?? 0, after: after.get(key) ?? 0, delta: (after.get(key) ?? 0) - (before.get(key) ?? 0) })).filter(row => row.delta);
  const comparison = { before: reports[0].input, after: reports[1].input, beforeSha256: reports[0].inputSha256, afterSha256: reports[1].inputSha256,
    totalDelta: reports[1].totalEntries - reports[0].totalEntries, changes };
  writeFileSync(join(outputDir, byOrigin ? "eval-origin-comparison.json" : "eval-comparison.json"), JSON.stringify(comparison, null, 2) + "\n");
  process.stdout.write(JSON.stringify({ totalDelta: comparison.totalDelta, changedFunctions: changes.length }) + "\n");
}
