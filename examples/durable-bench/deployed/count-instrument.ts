import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import type { Plugin } from "esbuild";
import ts from "typescript-twoslash";

import type { CodecSite } from "./count-codec.ts";

const runtime = fileURLToPath(new URL("./count-runtime.ts", import.meta.url));
const codecs = fileURLToPath(new URL("./count-codec.ts", import.meta.url));

const methods = new Set(
  ["decode", "encode"].flatMap((direction) =>
    ["Effect", "Exit", "Option", "Result", "Promise", "Sync"].flatMap((adapter) => [
      `${direction}${adapter}`,
      `${direction}Unknown${adapter}`,
    ]),
  ),
);

const sourceName = (path: string): { source: string; scope: CodecSite["scope"] } => {
  const dependencies = path.lastIndexOf("/node_modules/");

  if (dependencies >= 0) return { source: path.slice(dependencies + 1), scope: "dependency" };
  const packages = path.indexOf("/packages/");

  if (packages >= 0) return { source: path.slice(packages + 1), scope: "product" };
  const bench = path.indexOf("/examples/durable-bench/");

  if (bench >= 0) return { source: path.slice(bench + 1), scope: "bench" };

  return { source: path, scope: "dependency" };
};

const replaceOnce = (text: string, from: string, to: string, name: string): string => {
  if (text.split(from).length !== 2)
    throw new Error(`Counting hook ${name} no longer matches once`);

  return text.replace(from, to);
};

/** Build-time diagnostics only. Normal bundles contain none of these hooks. */
export const countInstrumentation = (): Plugin => ({
  name: "durable-bench-counts",
  setup(bundler) {
    bundler.onLoad({ filter: /\.[cm]?[jt]s$/ }, async ({ path }) => {
      if (/\/count-[^/]+\.[cm]?[jt]s$/.test(path)) return;
      // Count public codec call sites once; avoid their delegating implementation.
      if (/\/effect\/(src|dist)\/Schema(?:Parser)?\.[jt]s$/.test(path)) return;
      let text = await readFile(path, "utf8");
      const source = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true);
      const namespaces = new Map<string, string>();
      const functions = new Map<string, string>();

      for (const statement of source.statements) {
        if (!ts.isImportDeclaration(statement) || !ts.isStringLiteral(statement.moduleSpecifier))
          continue;
        const module = statement.moduleSpecifier.text;
        const bindings = statement.importClause?.namedBindings;
        const schemaModule = /(?:^|\/)(Schema|SchemaParser)(?:\.[cm]?[jt]s)?$/.exec(module)?.[1];

        const effectIndex =
          module === "effect" || /\/effect\/(src|dist)\/index\.[cm]?js$/.test(module);

        if (!bindings || statement.importClause?.isTypeOnly) continue;
        if (ts.isNamespaceImport(bindings) && schemaModule)
          namespaces.set(bindings.name.text, schemaModule);
        if (!ts.isNamedImports(bindings)) continue;
        for (const binding of bindings.elements) {
          if (binding.isTypeOnly) continue;
          const original = binding.propertyName?.text ?? binding.name.text;

          if (effectIndex && (original === "Schema" || original === "SchemaParser"))
            namespaces.set(binding.name.text, original);
          if (schemaModule && methods.has(original))
            functions.set(binding.name.text, `${schemaModule}.${original}`);
        }
      }
      const edits: Array<{ position: number; value: string }> = [];

      const visit = (node: ts.Node): void => {
        if (ts.isCallExpression(node)) {
          let api: string | undefined;
          const callee = node.expression;

          if (ts.isPropertyAccessExpression(callee) && ts.isIdentifier(callee.expression)) {
            const namespace = namespaces.get(callee.expression.text);

            if (namespace && methods.has(callee.name.text))
              api = `${namespace}.${callee.name.text}`;
          } else if (ts.isIdentifier(callee)) api = functions.get(callee.text);
          if (api) {
            const position = node.getStart(source);
            const location = source.getLineAndCharacterOfPosition(position);

            const site: CodecSite = {
              ...sourceName(path),
              line: location.line + 1,
              column: location.character + 1,
              api,
              schema: node.arguments[0]?.getText(source).replace(/\s+/g, " ").slice(0, 180) ?? "",
            };

            edits.push({ position, value: `__durableBenchCountCodec(${JSON.stringify(site)}, ` });
            edits.push({ position: node.end, value: ")" });
          }
        }
        ts.forEachChild(node, visit);
      };

      visit(source);
      for (const edit of edits.sort((left, right) => right.position - left.position))
        text = text.slice(0, edit.position) + edit.value + text.slice(edit.position);
      const imports: Array<string> = [];

      if (edits.length)
        imports.push(
          `import { countCodec as __durableBenchCountCodec } from ${JSON.stringify(codecs)};`,
        );
      if (path.endsWith("/deployed/worker/observe.ts")) {
        imports.push(
          `import { attachCount as __durableBenchCountAttach } from ${JSON.stringify(runtime)};`,
        );
        text = replaceOnce(
          text,
          "  meters.set(state.storage, meter);",
          "  __durableBenchCountAttach(state.storage, meter);\n  meters.set(state.storage, meter);",
          "observation attach",
        );
      }
      if (path.endsWith("/deployed/worker/host.ts")) {
        imports.push(
          `import { stopCount as __durableBenchCountStop } from ${JSON.stringify(runtime)};`,
        );
        text = replaceOnce(
          text,
          "        return Response.json({\n          ok: true,\n          query: this.meter.query,",
          "        const __durableBenchCounts = __durableBenchCountStop();\n\n        return Response.json({\n          ok: true,\n          query: this.meter.query,",
          "metrics freeze",
        );
        text = replaceOnce(
          text,
          "calls: this.meter.calls,",
          "calls: this.meter.calls,\n          counts: __durableBenchCounts,",
          "metrics report",
        );
      }
      if (path.endsWith("/deployed/worker/protocol.ts")) {
        imports.push(
          `import { CountReport as __durableBenchCountReport } from ${JSON.stringify(runtime)};`,
        );
        text = replaceOnce(
          text,
          "export const Metrics = Schema.Struct({",
          "export const Metrics = Schema.Struct({\n  counts: Schema.optionalKey(__durableBenchCountReport),",
          "metrics schema",
        );
      }
      if (
        path.endsWith("/deployed/worker/yielded.ts") ||
        path.endsWith("/third-party/src/deployed/pi.ts")
      ) {
        imports.push(
          `import { installCountStorage as __durableBenchCountStorage } from ${JSON.stringify(runtime)};`,
        );
        text = replaceOnce(
          text,
          "super(ctx, env);",
          "__durableBenchCountStorage(ctx.storage);\n    super(ctx, env);",
          "pre-constructor SQL",
        );
      }
      if (!imports.length) return;

      return {
        contents: imports.join("\n") + "\n" + text,
        loader: path.endsWith(".ts") ? "ts" : "js",
      };
    });
  },
});
