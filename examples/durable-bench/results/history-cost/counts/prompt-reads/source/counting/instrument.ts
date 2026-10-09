import { readFileSync } from "node:fs";

import type { Plugin } from "esbuild";

// Assert anchors so a new upstream source cannot silently produce incomplete counts.
const replace = (text: string, from: string, to: string) => {
  if (!text.includes(from)) throw new Error(`Missing instrumentation anchor: ${from}`);
  return text.replace(from, to);
};

export const instrument = (
  variant: string,
  candidateRecordsPath: string,
  promptReadsSqlPath: string,
): Plugin => ({
  name: "history-counts",
  setup(build) {
    build.onLoad({ filter: /\.(ts|js|mjs)$/ }, ({ path }) => {
      if (
        !/\/src\/yielded\.ts$|\/third-party\/src\/pi\.ts$|\/SqlThreadNativeReads\.ts$|\/RunJournal\.ts$|\/journal-metadata\.ts$|\/DoThreadStore\.ts$|\/effect\/dist\/ai\/Prompt\.js$|\/effect\/dist\/internal\/effect\.js$|\/effect\/dist\/internal\/schema\/compilerRegistry\.js$|\/sql-sqlite-do\/dist\/SqliteClient\.js$|\/Records\.ts$|\/effect\/dist\/SchemaParser\.js$|\/due-queue\.ts$|\/pi-durable\/dist\/harness\/context\.js$/.test(
          path,
        )
      )
        return;
      let s = readFileSync(path, "utf8");
      if (path.endsWith("/SqlThreadNativeReads.ts") && variant === "prompt-reads") {
        s = readFileSync(promptReadsSqlPath, "utf8");
      }
      const hc = "globalThis.__hc";
      if (path.endsWith("/src/yielded.ts")) {
        s =
          'import { exportCanonical, importCanonical, transcript as storedTranscript } from "../counting/storage.ts";\n' +
          s;
        s = replace(s, "seen = transcript(prompt);", `${hc}.model(); seen = transcript(prompt);`);
        s = replace(
          s,
          "  private run<A>",
          `  async exportSeed() {
    const archive = await Effect.runPromise(exportCanonical(this.ctx.storage));
    return Response.json({ archive, fingerprint: await fingerprint(storedTranscript(this.ctx.storage.sql, "yielded")), tables: tables(this.ctx.storage.sql) });
  }
  async importSeed(request) {
    const archive = await request.json();
    await Effect.runPromise(importCanonical(this.ctx.storage, archive, "main"));
    return { fingerprint: await fingerprint(storedTranscript(this.ctx.storage.sql, "yielded")), tables: tables(this.ctx.storage.sql) };
  }
  alarm() {}
  async measure(input) {
    ${hc}.start();
    try { await this.turn(input); }
    catch (error) { ${hc}.stop(); throw error; }
    const result = ${hc}.stop();
    return { ...result, fingerprint: await fingerprint(seen), seedFingerprint: await fingerprint(seen.slice(0, seen.findIndex((message) => message.role === "user" && message.text === input.text) - 1)), messages: seen.length };
  }
  private run<A>`,
        );
        s = replace(
          s,
          "export const inlineWorker = serve<Env>",
          "export const inlineWorker = serve<Env>",
        );
        s += `\nexport const countWorker = { async fetch(request, env) {
          const stub = env.THREADS.getByName("main");
          const path = new URL(request.url).pathname;
          const result = path === "/export" ? await stub.exportSeed() : path === "/import" ? await stub.importSeed(request) : await stub.measure(await request.json());
          return result instanceof Response ? result : Response.json(result);
        }};\n`;
      }
      if (path.endsWith("/third-party/src/pi.ts")) {
        s = replace(
          s,
          "  async wake()",
          `  async measure(input) {
    ${hc}.piSeedMax(this.ctx.storage.sql.exec("SELECT MAX(id) n FROM entries").one().n);
    ${hc}.start();
    try { await this.turn(input); } catch(error) { ${hc}.stop(); throw error; }
    const result = ${hc}.stop();
    return { ...result, fingerprint: await fingerprint(seen), seedFingerprint: await fingerprint(seen.slice(0, seen.findIndex((message) => message.role === "user" && message.text === input.text) - 1)), messages: seen.length };
  }
  async wake()`,
        );
        s = replace(
          s,
          "  seen = transcript.messages",
          `  ${hc}.model();\n  seen = transcript.messages`,
        );
        s = s.replaceAll(
          "sql.exec(query, ...bind(params))",
          `${hc}.sql(sql.exec(query, ...bind(params)), query)`,
        );
        s += `\nexport const countWorker = { async fetch(request, env) { return Response.json(await env.PI.getByName("main").measure(await request.json())); }};\n`;
      }
      if (path.endsWith("/SqlThreadNativeReads.ts")) {
        s = replace(
          s,
          "function* (input: ThreadPromptRead) {",
          `function* (input: ThreadPromptRead) { ${hc}.add("readPromptCalls"); ${hc}.enter("readPrompt"); try {`,
        );
        s = replace(s, 'failure("readPrompt", cause)', 'failure("readPrompt", cause)');
        const start = s.indexOf("const readPrompt =");
        const end = s.indexOf("  const readIdentity:", start);
        let part = s.slice(start, end);
        part = replace(
          part,
          "    },\n    Effect.mapError",
          `    } finally { ${hc}.leave(); } },\n    Effect.mapError`,
        );
        part = replace(
          part,
          "for (const row of plan) {",
          `for (const row of plan) { ${hc}.add("planRows"); ${hc}.add("plannedBytes", row.record_json_bytes);`,
        );
        part = replace(
          part,
          "            const rows = yield* decodePromptRows(\n              yield* sql`",
          "            const wireRows = yield* sql`",
        );
        part = replace(
          part,
          "              ORDER BY sequence`.pipe(execute),\n            );",
          '              ORDER BY sequence`.pipe(execute);\n            globalThis.__hc.enter("promptDecode");\n            globalThis.__hc.add("promptDecodeBatches");\n            const rows = yield* decodePromptRows(wireRows);\n            globalThis.__hc.leave();',
        );
        part = replace(
          part,
          "          return records;",
          `          for (const record of records) ${hc}.record("readPrompt", record);\n          return records;`,
        );
        s = s.slice(0, start) + part + s.slice(end);
      }
      if (path.endsWith("/RunJournal.ts")) {
        s = replace(
          s,
          "  if (savedContext !== undefined && savedContext.runId",
          `  ${hc}.add("journalProjections");\n  if (savedContext !== undefined && savedContext.runId`,
        );
        s = s.replaceAll(
          "Effect.gen(function* () {\n      const payload = envelope.record.payload;",
          `Effect.gen(function* () {\n      ${hc}.record("journalFold", envelope);\n      const payload = envelope.record.payload;`,
        );
      }
      if (path.endsWith("/journal-metadata.ts"))
        s = replace(
          s,
          "add: (envelope: JournalRecordEnvelope): void => {",
          `add: (envelope: JournalRecordEnvelope): void => { ${hc}.record("journalMetadata", envelope);`,
        );
      if (path.endsWith("/DoThreadStore.ts"))
        s = replace(
          s,
          "  const record = yield* decodeCanonicalRecord(row.record_json)",
          `  ${hc}.add("fullEnvelopeDecodes");\n  const record = yield* decodeCanonicalRecord(row.record_json)`,
        );
      if (path.endsWith("/effect/dist/ai/Prompt.js")) {
        s = replace(
          s,
          "Schema.Array(Schema.toEncoded(Message))",
          `Schema.Array(${hc}.label(Schema.toEncoded(Message), "encodedMessagePasses"))`,
        );
        s = s.replaceAll(
          "Schema.Array(Message)",
          `Schema.Array(${hc}.label(Message, "nativeMessagePasses"))`,
        );
      }
      if (path.endsWith("/effect/dist/SchemaParser.js"))
        s = replace(
          s,
          "    const result = (parser ??= compiler(ast))(input, options ?? SchemaAST.defaultParseOptions);",
          `${hc}.add("schemaRootDecoderCalls");\n    const result = (parser ??= compiler(ast))(input, options ?? SchemaAST.defaultParseOptions);`,
        );
      if (path.endsWith("/effect/dist/internal/effect.js"))
        s = replace(
          s,
          "this.currentOpCount++;",
          `${hc}.add("effectEvaluations"); this.currentOpCount++;`,
        );
      if (path.endsWith("/effect/dist/internal/schema/compilerRegistry.js")) {
        s = replace(
          s,
          "Interpreter.compile(this.ast, decodeChild)",
          `${hc}.schema(this.ast, Interpreter.compile(this.ast, decodeChild))`,
        );
        s = replace(
          s,
          "Interpreter.compile(this.ast, makeChild, makeField)",
          `${hc}.schema(this.ast, Interpreter.compile(this.ast, makeChild, makeField))`,
        );
      }
      if (path.endsWith("/sql-sqlite-do/dist/SqliteClient.js"))
        s = s.replaceAll(
          "sqlStorage.exec(sql, ...params)",
          `${hc}.sql(sqlStorage.exec(sql, ...params), sql)`,
        );
      if (path.endsWith("/due-queue.ts"))
        s = replace(
          s,
          "const sql = storage.sql;",
          `const sql = { exec: (...args) => ${hc}.sql(storage.sql.exec(...args), args[0]) };`,
        );
      if (path.endsWith("/pi-durable/dist/harness/context.js")) {
        s = replace(
          s,
          "export async function deriveContext(storage, conversationId, bounds, context) {",
          `export async function deriveContext(storage, conversationId, bounds, context) { ${hc}.add("piContextDerivations");`,
        );
        s = replace(
          s,
          "    const entries = selectActive(head, range);",
          `    ${hc}.add("piMetadataEntries", range.length);\n    const entries = selectActive(head, range);`,
        );
        s = replace(
          s,
          "    const contributions = entries.map((entry) => {",
          `    const contributions = entries.map((entry) => { ${hc}.add("piProjectionEntries");`,
        );
        s = replace(
          s,
          "async function scanRange(storage, conversationId, bounds, context) {",
          `async function scanRange(storage, conversationId, bounds, context) { ${hc}.add("piRangeScans");`,
        );
      }
      if (path.endsWith("/Records.ts") && variant === "candidate") {
        s = readFileSync(candidateRecordsPath, "utf8");
      }
      return { contents: s, loader: path.endsWith(".ts") ? "ts" : "js" };
    });
  },
});
