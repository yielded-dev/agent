// Static esbuild startup inventory; never imports or executes the analyzed bundle.
// Usage: vp node count.mjs bundle.mjs metafile.json > counts.json
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { basename, resolve } from "node:path";
import { createRequire } from "node:module";

const benchRequire = createRequire(resolve("examples/durable-bench/package.json"));
const { parse } = createRequire(benchRequire.resolve("miniflare"))("acorn");

const [bundle, metafile] = process.argv.slice(2);
if (!bundle || !metafile || process.argv.length !== 4) {
  throw new Error("Usage: vp node count.mjs bundle.mjs metafile.json > counts.json");
}
const source = readFileSync(bundle, "utf8");
const metaText = readFileSync(metafile, "utf8");
const meta = JSON.parse(metaText);
const outputs = Object.entries(meta.outputs).filter(([name]) => basename(name) === basename(bundle));
if (outputs.length !== 1 || outputs[0][1].bytes !== Buffer.byteLength(source)) {
  throw new Error("Bundle name/byte length does not match exactly one metafile output");
}
const retained = Object.entries(outputs[0][1].inputs).filter(([, value]) => value.bytesInOutput > 0);
const inputs = new Set([...Object.keys(meta.inputs), ...retained.map(([name]) => name)]);
const comments = [];
const ast = parse(source, { ecmaVersion: "latest", sourceType: "module", locations: true, onComment: comments });
const sections = comments.filter(c => !c.block && inputs.has(c.value.trim())).map(c => ({ start: c.end, name: c.value.trim() }));
if (!sections.length) throw new Error("No esbuild source-section comments match the metafile");
const compare = (a, b) => a < b ? -1 : a > b ? 1 : 0;
const sorted = object => Object.fromEntries(Object.entries(object).sort(([a], [b]) => compare(a, b)));
const hash = text => createHash("sha256").update(text).digest("hex");
const snippet = node => source.slice(node.start, node.end).replace(/\s+/g, " ").slice(0, 220);
const familyOf = file => file.match(/(?:^|\/)node_modules\/effect\/(?:src|dist)\/(SchemaAST|Schema|Layer|Context)\.[cm]?[jt]s$/)?.[1] ?? "Other";
const packageOf = file => file.includes("node_modules/")
  ? file.slice(file.lastIndexOf("node_modules/") + 13).match(/^(?:@[^/]+\/)?[^/]+/)[0]
  : file.match(/(?:^|\/)((?:packages|examples|tooling)\/[^/]+)/)?.[1] ?? "<entry/helpers>";
let section = -1;
const owner = new Map();
for (const node of ast.body) {
  while (section + 1 < sections.length && sections[section + 1].start < node.start) section++;
  owner.set(node, sections[section]?.name ?? "<esbuild helpers>");
}
const declarations = new Map(), namespaces = new Map(), publicNames = new Map();
const unwrap = node => node.type.startsWith("Export") && node.declaration ? node.declaration : node;
function bindings(node, names) {
  if (!node) return;
  if (node.type === "Identifier") names.add(node.name);
  else if (node.type === "RestElement") bindings(node.argument, names);
  else if (node.type === "AssignmentPattern") bindings(node.left, names);
  else if (node.type === "ArrayPattern") node.elements.forEach(n => bindings(n, names));
  else if (node.type === "ObjectPattern") node.properties.forEach(n => bindings(n.type === "RestElement" ? n.argument : n.value, names));
}
for (const statement of ast.body) {
  const node = unwrap(statement), file = owner.get(statement), names = new Set();
  if (node.type === "VariableDeclaration") node.declarations.forEach(d => bindings(d.id, names));
  else if (node.id) bindings(node.id, names);
  for (const name of names) declarations.set(name, { file, family: familyOf(file) });
  const call = node.type === "ExpressionStatement" && node.expression;
  if (call?.type !== "CallExpression" || call.callee.name !== "__export" || call.arguments[1]?.type !== "ObjectExpression") continue;
  const family = familyOf(file), exports = new Map();
  for (const property of call.arguments[1].properties) {
    const symbol = property.value?.body;
    if (symbol?.type !== "Identifier") continue;
    const name = property.key.name ?? property.key.value;
    exports.set(name, symbol.name);
    if (family !== "Other") {
      const candidate = `${family}.${name}`, previous = publicNames.get(symbol.name);
      if (!previous || compare(candidate, previous) < 0) publicNames.set(symbol.name, candidate);
    }
  }
  namespaces.set(call.arguments[0].name, { file, family, exports });
}
function callee(node, shadow) {
  if (node.type === "Identifier") {
    const definition = shadow.has(node.name) ? undefined : declarations.get(node.name);
    const family = definition?.family ?? "Other";
    return { family, name: family === "Other" ? node.name : publicNames.get(node.name) ?? `${family}.${node.name}`, form: "direct", stage: 0 };
  }
  if (node.type === "CallExpression") {
    const base = callee(node.callee, shadow);
    return { ...base, name: `${base.name}()`, form: "curried", stage: base.stage + 1 };
  }
  if (node.type === "MemberExpression") {
    const key = node.computed ? node.property.value ?? "[computed]" : node.property.name;
    const space = node.object.type === "Identifier" && !shadow.has(node.object.name) && namespaces.get(node.object.name);
    if (space) return { family: space.family, name: `${space.family === "Other" ? node.object.name : space.family}.${key}`, form: "direct", stage: 0 };
    const base = callee(node.object, shadow);
    return { ...base, name: `${base.name}.${key}`, form: "member", stage: 0 };
  }
  if (node.type === "ChainExpression") return callee(node.expression, shadow);
  return { family: "Other", name: `[${node.type}]`, form: "direct", stage: 0 };
}
const functions = new Set(["FunctionDeclaration", "FunctionExpression", "ArrowFunctionExpression"]);
function children(node) {
  return Object.entries(node).filter(([key]) => !["start", "end", "loc"].includes(key))
    .flatMap(([, value]) => Array.isArray(value) ? value : [value]).filter(value => value?.type);
}
function localVars(node, names) {
  if (functions.has(node.type) || node.type.startsWith("Class")) return;
  if (node.type === "VariableDeclaration" && node.kind === "var") node.declarations.forEach(d => bindings(d.id, names));
  children(node).forEach(child => localVars(child, names));
}
function blockScope(body, outer) {
  const names = new Set(outer);
  for (const node of body) {
    if (node.type === "VariableDeclaration") node.declarations.forEach(d => bindings(d.id, names));
    else if (node.type === "FunctionDeclaration" || node.type === "ClassDeclaration") bindings(node.id, names);
  }
  return names;
}
function immediate(node) {
  if (node.type === "ChainExpression") return immediate(node.expression);
  if (node.type === "SequenceExpression") return immediate(node.expressions.at(-1));
  if (node.type === "MemberExpression" && ["call", "apply"].includes(node.computed ? node.property.value : node.property.name)) return immediate(node.object);
  return functions.has(node.type) && !node.generator ? node : undefined;
}
const events = [], classes = [], roots = [];
function walk(node, root, shadow = new Set(), phase = "initializer") {
  if (!node || functions.has(node.type)) return;
  if (node.type === "ClassExpression" || node.type === "ClassDeclaration") {
    classes.push({ module: root.module, root: root.id });
    walk(node.superClass, root, shadow, "heritage");
    for (const field of node.body.body) {
      if (field.computed) walk(field.key, root, shadow, "computed key");
      if (field.type === "StaticBlock") walk(field, root, shadow, "static block");
      else if (field.static && field.type === "PropertyDefinition") walk(field.value, root, shadow, "static field");
    }
    return;
  }
  if (node.type === "BlockStatement" || node.type === "StaticBlock") {
    const scope = blockScope(node.body, shadow);
    if (node.type === "StaticBlock") localVars(node, scope);
    node.body.forEach(child => walk(child, root, scope, phase));
    return;
  }
  if (node.type === "CatchClause") {
    const scope = new Set(shadow); bindings(node.param, scope);
    walk(node.body, root, scope, phase); return;
  }
  if (["ForStatement", "ForInStatement", "ForOfStatement"].includes(node.type)) {
    const binding = node.init ?? node.left;
    shadow = binding?.type === "VariableDeclaration" ? blockScope([binding], shadow) : shadow;
  }
  if (node.type === "CallExpression" || node.type === "NewExpression") {
    const invoked = node.type === "CallExpression" && immediate(node.callee);
    const target = callee(node.callee, shadow);
    const event = { module: root.module, root: root.id, bundleLine: node.loc.start.line, column: node.loc.start.column + 1,
      kind: node.type === "CallExpression" ? "call" : "new", ...target, phase, immediate: Boolean(invoked), expression: snippet(node) };
    events.push(event); root.events.push(event);
    walk(node.callee, root, shadow, phase); node.arguments.forEach(arg => walk(arg, root, shadow, phase));
    if (invoked) {
      const scope = new Set(shadow); bindings(invoked.id, scope); invoked.params.forEach(param => bindings(param, scope));
      localVars(invoked.body, scope);
      invoked.params.forEach(param => walk(param, root, scope, "IIFE parameter"));
      walk(invoked.body, root, scope, "IIFE body");
    }
    return;
  }
  children(node).forEach(child => walk(child, root, shadow, phase));
}
for (const statement of ast.body) {
  const node = unwrap(statement), file = owner.get(statement);
  const initializers = node.type === "VariableDeclaration"
    ? node.declarations.map(d => [snippet(d.id), d.init, d.id]).filter(([, init]) => init)
    : [[node.id?.name ?? node.type, node]];
  for (const [binding, init, pattern] of initializers) {
    const root = { id: roots.length, module: file, binding, bundleLine: init.loc.start.line, expression: snippet(init), events: [] };
    roots.push(root); walk(init, root); walk(pattern, root);
  }
}
function summary(items) {
  const categories = {}, composition = {}, phases = {}, forms = {};
  const increment = (object, key) => object[key] = (object[key] ?? 0) + 1;
  for (const event of items) {
    increment(categories, event.family); increment(composition, `${event.kind}:${event.name}`);
    increment(phases, event.phase); increment(forms, event.form);
  }
  return { sites: items.length, calls: items.filter(e => e.kind === "call").length, news: items.filter(e => e.kind === "new").length,
    immediateInvocations: items.filter(e => e.immediate).length, categories: sorted(categories), forms: sorted(forms), phases: sorted(phases), composition: sorted(composition) };
}
const modules = new Map(retained.map(([file, value]) => [file, { bytes: value.bytesInOutput, events: [], classes: 0 }]));
for (const event of events) {
  if (!modules.has(event.module)) modules.set(event.module, { bytes: 0, events: [], classes: 0 });
  modules.get(event.module).events.push(event);
}
for (const item of classes) {
  if (!modules.has(item.module)) modules.set(item.module, { bytes: 0, events: [], classes: 0 });
  modules.get(item.module).classes++;
}
const packages = new Map();
for (const [file, value] of modules) {
  const name = packageOf(file), group = packages.get(name) ?? { bytes: 0, modules: 0, classes: 0, events: [] };
  group.bytes += value.bytes; group.modules++; group.classes += value.classes; group.events.push(...value.events); packages.set(name, group);
}
const rankedRoots = roots.filter(root => root.events.length).sort((a, b) => b.events.length - a.events.length || compare(a.module, b.module) || a.bundleLine - b.bundleLine);
const describeRoot = root => ({ module: root.module, binding: root.binding, bundleLine: root.bundleLine, expression: root.expression, ...summary(root.events) });
const isAgent = event => /(?:^|\/)packages\/effect-agent\/src\//.test(event.module);
const report = {
  version: 1,
  toolSha256: hash(readFileSync(new URL(import.meta.url))),
  inputs: { bundle: basename(bundle), bytes: Buffer.byteLength(source), bundleSha256: hash(source), metafileSha256: hash(metaText), retainedModules: retained.length },
  semantics: [
    "Static call/new sites are a proxy, not CPU, allocations, or expanded dynamic invocation counts.",
    "Includes initializers, class heritage/computed keys/static initialization, and literal immediate IIFEs (also .call/.apply).",
    "Excludes lazy function/getter/method and instance-initializer bodies; does not follow named calls, callbacks, imports, or getters.",
    "Counts potentially executed sites in both branches and each loop body once; no path selection or loop/callback expansion.",
    "Each CallExpression counts once: f(a)(b) contributes two sites, attributed to f and f(); member calls stay separate.",
    "Schema/SchemaAST/Layer/Context provenance comes from esbuild section declarations and __export namespace aliases; other/local/dynamic callees remain Other.",
    "Bundle lines are emitted-code locations, not original source lines. Largest sites rank syntactic subtree counts, not measured cost."
  ],
  totals: { classDefinitions: classes.length, ...summary(events) },
  effectAgent: { classDefinitions: classes.filter(isAgent).length, ...summary(events.filter(isAgent)) },
  families: Object.fromEntries(["Schema", "SchemaAST", "Layer", "Context"].map(family => [family, summary(events.filter(e => e.family === family))])),
  packages: [...packages].sort(([a], [b]) => compare(a, b)).map(([name, value]) => ({ package: name, bytes: value.bytes, modules: value.modules, classDefinitions: value.classes, ...summary(value.events) })),
  modules: [...modules].sort(([a], [b]) => compare(a, b)).map(([file, value]) => ({ module: file, package: packageOf(file), bytes: value.bytes, classDefinitions: value.classes, ...summary(value.events) })),
  largestStartupSites: rankedRoots.slice(0, 50).map(describeRoot),
  largestEffectAgentSites: rankedRoots.filter(isAgent).slice(0, 30).map(describeRoot),
  effectAgentDerivations: events.filter(e => isAgent(e) && e.family === "Schema" && e.form === "direct" && /^Schema\.(?:toEquivalence|toCodec\w+|toEncoded|toType|encode\w*|decode\w*|is)$/.test(e.name))
    .map(({ root, ...event }) => ({ ...event, binding: roots[root].binding }))
};
process.stdout.write(JSON.stringify(report, null, 2) + "\n");
