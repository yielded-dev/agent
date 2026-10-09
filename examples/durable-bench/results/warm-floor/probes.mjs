import { readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { join, relative, resolve } from "node:path";

export const baselineRevision = "b246f8aaa3a92d5f82934b1fc7a82356d1ad6664";
const baseline = (root, path) => execFileSync("vp", ["exec", "git", "show", `${baselineRevision}:${relative(root, path)}`], { cwd: root, encoding: "utf8" });
const between = (source, start, end) => {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from + start.length);
  if (from < 0 || to < 0) throw new Error(`Missing variant boundary: ${start} / ${end}`);
  return source.slice(from, to);
};

// Reuse prod-turn's build-only observation. No probe changes a durable fact.
export const probes = (here) => ({
  name: "warm-floor-observation",
  setup(build) {
    build.onLoad({ filter: /packages\/(effect-agent|storage-cloudflare|platform-cloudflare)\/src\/.*\.ts$/ }, (args) => {
      let source = readFileSync(args.path, "utf8");
      let changed = false;
      const replace = (before, after) => {
        if (!source.includes(before)) throw new Error(`Missing probe anchor in ${args.path}: ${before}`);
        source = source.replaceAll(before, after);
        changed = true;
      };
      const span = (name, label = name) => {
        const pattern = new RegExp(`(const ${name}(?:[^=]*?) = )Effect\\.fnUntraced\\(`);
        if (!pattern.test(source)) throw new Error(`Missing span anchor ${name} in ${args.path}`);
        source = source.replace(pattern, `$1Effect.fn("warm-floor.${label}")(`);
        changed = true;
      };
      // Exact baseline function bodies and the candidate share all other source and
      // one native Object. Only observer-owned per-turn labels choose these paths.
      if (args.path.endsWith("/DoSubmissionLedger.ts") && source.includes("const finalizePublication =")) {
        const original = baseline(resolve(here, "../../../.."), args.path);
        for (const [name, next, type] of [
          ["publish", "validateFinalization", 'SettlementPublisher["Service"]["publish"]'],
          ["finalizeSettlement", "inspectWorker", 'SubmissionLedger["Service"]["finalizeSettlement"]'],
        ]) {
          const candidate = between(source, `  const ${name}:`, `  const ${next} =`);
          const control = between(original, `  const ${name}:`, `  const ${next} =`);
          replace(candidate, `${control.replace(`const ${name}:`, `const baseline_${name}:`)}${candidate.replace(`const ${name}:`, `const candidate_${name}:`)}  const ${name}: ${type} = (input) => candidateEnabled("settlement") ? candidate_${name}(input) : baseline_${name}(input);\n\n`);
        }
      }
      if (args.path.endsWith("/platform-cloudflare/src/internal/layers.ts") && source.includes("const hostLanes = yield* selectHostLanes")) {
        const original = baseline(resolve(here, "../../../.."), args.path);
        const start = "                  Context.add(SettlementPublisher, {\n";
        const end = "                  Context.add(SubmissionLedger, {\n";
        const candidate = between(source, start, end);
        const control = between(original, start, end);
        const body = (block) => block.slice(block.indexOf("                      Effect.gen"), block.lastIndexOf(",\n                  }),"));
        replace(candidate, `${start}                    publish: (input) => candidateEnabled("settlement")\n                      ? (${body(candidate).trim()})\n                      : (${body(control).trim()}),\n                  }),\n`);
      }
      if (args.path.endsWith("/platform-cloudflare/src/Alarm.ts") && source.includes("Math.max(deadline, now)")) {
        replace("Math.max(deadline, now)", 'Math.max(deadline, candidateEnabled("prearm") ? now : now + minimumAlarmDelay)');
      }
      if (args.path.endsWith("/storage-cloudflare/src/internal/owned-state.ts")) {
        const original = baseline(resolve(here, "../../../.."), args.path);
        const start = "    const apply =\n";
        const end = "    return { by, byFields, matching, seed, write: apply(false), remove: apply(true) };";
        const candidate = between(source, start, end);
        const control = between(original, start, end);
        replace(candidate, `${control.replace("const apply =", "const baseline_apply =")}${candidate.replace("const apply =", "const candidate_apply =")}    const apply = (remove: boolean) => {
      const control = baseline_apply(remove);
      const candidate = candidate_apply(remove);
      return <E, R>(effect: Effect.Effect<ReadonlyArray<unknown>, E, R>) => candidateEnabled("views") ? candidate(effect) : control(effect);
    };\n\n`);
        replace("const bytes = new TextEncoder().encode(JSON.stringify(rows)).byteLength;", 'const bytes = new TextEncoder().encode(JSON.stringify(rows)).byteLength;\n      probe("owned.rows.retain", { table, rows: rows.length, bytes, views: current.size });');
        replace("const rows = yield* decode(yield* effect);", 'const rows = yield* decode(yield* effect);\n          probe("owned.rows.apply", { table, rows: rows.length, views: current.size });');
        replace("const next = prior.filter((row) => !changed.has(key(row)));", 'probe("owned.rows.copy", { table, rows: prior.length });\n            const next = prior.filter((row) => !changed.has(key(row)));');
      }
      if (args.path.endsWith("/durable/DurableAgentRuntime.ts")) {
        span("readFinalizedSubmission");
        span("terminalize");
        span("settleJoinedSubmissions");
        replace('const awaitHint = yield* wake.subscribe(receipt.threadId, "settlement");', 'probe("waiter.subscribe");\n          const awaitHint = yield* wake.subscribe(receipt.threadId, "settlement");');
        replace('yield* Effect.raceFirst(awaitHint, Effect.sleep(config.settlementPollInterval));', 'yield* Effect.raceFirst(awaitHint.pipe(Effect.tap(() => Effect.sync(() => probe("waiter.hint")))), Effect.sleep(config.settlementPollInterval).pipe(Effect.tap(() => Effect.sync(() => probe("waiter.fallback")))));');
      }
      if (args.path.endsWith("/durable/RunContinuation.ts")) span("prepare", "continuation.prepare");
      if (args.path.endsWith("/engine/internal/agent-runtime.ts")) {
        for (const name of ["executeToolBatch", "decodeFinalOutput"]) span(name);
        replace('const accepted = checkpointExecution(context, durability).pipe(', 'probe("turn.commit.start", { tag: commit._tag, turn: commit.turn });\n    const accepted = checkpointExecution(context, durability).pipe(');
        replace('Effect.flatMap((receipt) => {\n        if (receipt === "deferred")', 'Effect.flatMap((receipt) => {\n        probe("turn.commit.end", { tag: commit._tag, turn: commit.turn, receipt });\n        if (receipt === "deferred")');
        replace('const compactedOutgoing = (): Prompt.Prompt => {', 'const compactedOutgoing = (): Prompt.Prompt => {\n        probe("prompt.materialize", { turn, messages: prepared.length });');
        replace('const toolChoice = modelToolChoice();', 'probe("prompt.ready", { turn, messages: providerPrompt.content.length });\n                const toolChoice = modelToolChoice();');
      }
      if (args.path.endsWith("/internal/canonical-append.ts")) {
        span("prepareCanonicalAppend");
        replace('const request = yield* PreparedAppend.capture(input);', 'const request = yield* PreparedAppend.capture(input);\n  probe("append.records", { tags: request.batch.records.map((r) => r.payload._tag) });');
      }
      if (args.path.endsWith("/DoSubmissionLedger.ts")) {
        replace('const existing = submissions.get(row);', 'probe("submission.snapshot.lookup");\n    const existing = submissions.get(row);');
        replace('const admission = yield* decodeAdmissionFact(row).pipe(Effect.mapError(decodeFailure));', 'probe("submission.snapshot.decode");\n    const admission = yield* decodeAdmissionFact(row).pipe(Effect.mapError(decodeFailure));');
      }
      if (args.path.endsWith("/platform-cloudflare/src/WakeScheduler.ts")) {
        replace('placement.ownsThread(threadId) ? notifyLocal(threadId, kind) : notifyRemote(threadId)', '(probe("wake.notify", { kind: kind ?? "all" }), placement.ownsThread(threadId) ? notifyLocal(threadId, kind) : notifyRemote(threadId))');
      }
      if (args.path.endsWith("/platform-cloudflare/src/Alarm.ts")) {
        span("beginMutation");
        span("beginPass");
      }
      if (!changed) return;
      return { contents: `import { probe, candidateEnabled } from ${JSON.stringify(join(here,"network/observe.ts"))};\n${source}`, loader: "ts" };
    });
  },
});
