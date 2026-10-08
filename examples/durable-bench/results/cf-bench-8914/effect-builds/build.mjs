// Task-local package preparation. Run through vp node; no benchmarks or timings.
import { createHash } from "node:crypto";
import * as fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { execFileSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";

const task = fs.realpathSync(process.argv[2]);
if (!task.startsWith("/private/tmp/cf-bench-8914-effect-")) throw new Error("Expected private task directory");
if ((fs.statSync(task).mode & 0o777) !== 0o700) throw new Error("Task directory must be mode 700");
const evidence = path.dirname(fileURLToPath(import.meta.url));
const requireTool = createRequire(path.join(task, "tooling/package.json"));
const ts = requireTool("typescript");
const babel = await import(pathToFileURL(requireTool.resolve("@babel/core")));
const pure = requireTool.resolve("babel-plugin-annotate-pure-calls");
const sha = (value) => createHash("sha256").update(value).digest("hex");
const git = (cwd, ...args) => execFileSync("git", ["-C", cwd, ...args], { encoding: "utf8" }).trim();
const head = "01c6222ccf74390848595633ef23410cbfa6983b";
const main = "13ace20e6c0501a0d01eb2b028672081c9780e42";
const base = "757821fe99b7179f907d6d1a34a4e86de4173112";
if (git(path.join(task, "upstream"), "merge-base", "--all", head, main) !== base) throw new Error("Merge-base mismatch");
if (git(path.join(task, "upstream"), "rev-parse", "--is-shallow-repository") !== "false") throw new Error("Shallow clone");
const patch = fs.readFileSync(path.join(evidence, "effect@4.0.0.patch"), "utf8");
const sourcePatch = patch.slice(0, patch.indexOf("diff --git a/dist/"));
if (!sourcePatch.includes("streamDecoderFor")) throw new Error("Missing source patch");
const patchFile = path.join(task, "LanguageModel.source.patch");
fs.writeFileSync(patchFile, sourcePatch);
const files = (dir) => fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
  const full = path.join(dir, entry.name);
  return entry.isDirectory() ? files(full) : [full];
}).sort();
const record = {
  head, fetchedMain: main, mergeBase: base,
  mergeBaseMethod: "git merge-base --all <head> <fetched-main>; full non-shallow commit history; exactly one result",
  node: process.version, platform: process.platform, arch: process.arch,
  tools: { typescript: ts.version, babel: babel.version, purePlugin: requireTool("babel-plugin-annotate-pure-calls/package.json").version },
  patchSha256: sha(patch), appliedSourcePatchSha256: sha(sourcePatch),
  buildMethod: "TypeScript compiler API emit with upstream config, noCheck=true, stripInternal=true; Babel annotate-pure-calls; publishConfig.exports promoted",
  limitation: "TypeScript 6.0.3 API emits JS/declarations instead of upstream TypeScript 7.0.2 CLI; no semantic typecheck or upstream tests. Primary owns installed consumer validation and deployment.",
  packages: {},
};
for (const [label, expected] of [["base", base], ["head", head]]) {
  const checkout = path.join(task, label);
  if (git(checkout, "rev-parse", "HEAD") !== expected) throw new Error("Checkout mismatch");
  if (git(checkout, "status", "--porcelain", "--untracked-files=no") !== "") throw new Error("Checkout must be pristine");
  const packageDir = path.join(checkout, "packages/effect");
  const sourceLanguageModel = path.join(packageDir, "src/ai/LanguageModel.ts");
  const languageModelBefore = sha(fs.readFileSync(sourceLanguageModel));
  git(checkout, "apply", "--check", "--directory=packages/effect", patchFile);
  git(checkout, "apply", "--directory=packages/effect", patchFile);
  const appliedDiff = git(checkout, "diff", "--", "packages/effect/src/ai/LanguageModel.ts") + "\n";
  fs.writeFileSync(path.join(task, `${label}.applied.patch`), appliedDiff);
  const out = path.join(task, "packages", label, "effect");
  if (fs.existsSync(out)) throw new Error("Refusing existing output");
  fs.mkdirSync(out, { recursive: true });
  fs.cpSync(path.join(packageDir, "src"), path.join(out, "src"), { recursive: true });
  for (const name of ["README.md", "LICENSE"]) fs.copyFileSync(path.join(packageDir, name), path.join(out, name));
  for (const name of ["AGENTS.md", "CLAUDE.md"]) fs.copyFileSync(path.join(checkout, "LLMS.md"), path.join(out, name));
  fs.cpSync(path.join(checkout, "ai-docs"), path.join(out, "ai-docs"), { recursive: true });
  const manifest = JSON.parse(fs.readFileSync(path.join(packageDir, "package.json"), "utf8"));
  manifest.exports = manifest.publishConfig.exports;
  delete manifest.publishConfig.exports;
  fs.writeFileSync(path.join(out, "package.json"), JSON.stringify(manifest, null, 2) + "\n");
  const configFile = ts.readConfigFile(path.join(packageDir, "tsconfig.json"), ts.sys.readFile);
  if (configFile.error) throw new Error(ts.flattenDiagnosticMessageText(configFile.error.messageText, "\n"));
  const config = ts.parseJsonConfigFileContent(configFile.config, ts.sys, packageDir, {
    outDir: path.join(out, "dist"), rootDir: path.join(out, "src"),
    noCheck: true, incremental: false, composite: false, declaration: true,
    stripInternal: true, typeRoots: [path.join(task, "tooling/node_modules/@types")],
  });
  if (config.errors.length) throw new Error(ts.formatDiagnosticsWithColorAndContext(config.errors, { getCurrentDirectory: () => packageDir, getCanonicalFileName: (f) => f, getNewLine: () => "\n" }));
  const sourceFiles = files(path.join(out, "src")).filter((f) => f.endsWith(".ts"));
  const program = ts.createProgram(sourceFiles, config.options);
  const emitted = program.emit();
  if (emitted.emitSkipped || emitted.diagnostics.length) throw new Error(ts.formatDiagnosticsWithColorAndContext(emitted.diagnostics, { getCurrentDirectory: () => out, getCanonicalFileName: (f) => f, getNewLine: () => "\n" }));
  const jsFiles = files(path.join(out, "dist")).filter((f) => f.endsWith(".js"));
  console.log(`${label}: emitted ${jsFiles.length} JavaScript modules; applying upstream pure annotations`);
  for (const file of jsFiles) {
    const result = await babel.transformFileAsync(file, {
      babelrc: false, configFile: false, plugins: [pure], sourceMaps: true,
      sourceFileName: path.basename(file),
    });
    fs.writeFileSync(file, result.code + "\n//# sourceMappingURL=" + path.basename(file) + ".map\n");
    fs.writeFileSync(file + ".map", JSON.stringify(result.map) + "\n");
  }
  const hashes = files(out).map((f) => `${sha(fs.readFileSync(f))}  ${path.relative(out, f)}`).join("\n") + "\n";
  fs.writeFileSync(path.join(task, `${label}.sha256`), hashes);
  record.packages[label] = {
    sha: expected, gitTree: git(checkout, "rev-parse", "HEAD^{tree}"), version: manifest.version,
    root: out, files: files(out).length, javascriptModules: jsFiles.length,
    languageModelBeforeSha256: languageModelBefore, languageModelAfterSha256: sha(fs.readFileSync(sourceLanguageModel)),
    appliedDiffSha256: sha(appliedDiff), fileManifestSha256: sha(hashes),
    coreJsSha256: sha(fs.readFileSync(path.join(out, "dist/internal/core.js"))),
    effectJsSha256: sha(fs.readFileSync(path.join(out, "dist/internal/effect.js"))),
    languageModelJsSha256: sha(fs.readFileSync(path.join(out, "dist/ai/LanguageModel.js"))),
  };
  console.log(`${label}: prepared ${out}`);
}
fs.writeFileSync(path.join(task, "build.json"), JSON.stringify(record, null, 2) + "\n");
