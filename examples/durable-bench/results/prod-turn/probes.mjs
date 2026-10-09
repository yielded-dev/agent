import { readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { join, resolve } from "node:path";

const baseline = "d52ee32a51c7bf93a226272578f13cebcc66f2e1";
const block = (source, name, next) => {
  const start = source.indexOf(`  const ${name}:`);
  const end = source.indexOf(`  const ${next}`, start + 1);
  if (start < 0 || end < 0) throw new Error(`Missing comparison function ${name}`);
  return source.slice(start, end);
};

// Build-only observation and diagnostic waiter control. The production source
// keeps subscribe-before-read and its 500 ms fallback in every published fix.
export const probes = (here) => ({
  name: "prod-turn-observation",
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
        source = source.replace(pattern, `$1Effect.fn("prod-turn.${label}")(`);
        changed = true;
      };
      if (args.path.endsWith("/durable/DurableAgentRuntime.ts")) {
        span("readFinalizedSubmission");
        span("terminalize");
        span("settleJoinedSubmissions");
        replace('const awaitHint = yield* wake.subscribe(receipt.threadId, "settlement");', 'probe("waiter.subscribe");\n          const awaitHint = yield* wake.subscribe(receipt.threadId, "settlement");');
        replace('yield* Effect.raceFirst(awaitHint, Effect.sleep(config.settlementPollInterval));', 'yield* (candidateEnabled("hints") ? awaitHint.pipe(Effect.tap(() => Effect.sync(() => probe("waiter.hint")))) : Effect.raceFirst(awaitHint.pipe(Effect.tap(() => Effect.sync(() => probe("waiter.hint")))), Effect.sleep(config.settlementPollInterval).pipe(Effect.tap(() => Effect.sync(() => probe("waiter.fallback"))))));');
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
        const original = execFileSync("vp", ["exec", "git", "show", `${baseline}:packages/storage-cloudflare/src/DoSubmissionLedger.ts`], {
          cwd: resolve(here, "../../../.."), encoding: "utf8",
        });
        // Retain the exact baseline bodies beside the candidate. Only this
        // disposable bundle contains both paths; no flag enters a production PR.
        for (const [name, next, type] of [
          ["publish", "validateFinalization", 'SettlementPublisher["Service"]["publish"]'],
          ["finalizeSettlement", "inspectWorker", 'SubmissionLedger["Service"]["finalizeSettlement"]'],
        ]) {
          const current = block(source, name, next);
          const old = block(original, name, next);
          const suffix = name[0].toUpperCase() + name.slice(1);
          replace(current,
            old.replace(`const ${name}:`, `const baseline${suffix}:`) +
            current.replace(`const ${name}:`, `const candidate${suffix}:`) +
            `  const ${name}: ${type} = (request) => candidateEnabled("settlement") ? candidate${suffix}(request) : baseline${suffix}(request);\n\n`,
          );
        }
        replace('const existing = submissions.get(row);', 'probe("submission.snapshot.lookup");\n    const existing = submissions.get(row);');
        replace('const admission = yield* decodeAdmissionFact(row).pipe(Effect.mapError(decodeFailure));', 'probe("submission.snapshot.decode");\n    const admission = yield* decodeAdmissionFact(row).pipe(Effect.mapError(decodeFailure));');
      }
      if (args.path.endsWith("/platform-cloudflare/src/internal/layers.ts")) {
        replace(
          'const hostLanes = yield* selectHostLanes({\n                          _tag: "Settlement",',
          'const hostLanes = candidateEnabled("settlement") ? yield* selectHostLanes({\n                          _tag: "Settlement",',
        );
        replace(
          'settlementId: settlement.settlementId,\n                          }),\n                        });',
          'settlementId: settlement.settlementId,\n                          }),\n                        }) : [];',
        );
      }
      if (args.path.endsWith("/platform-cloudflare/src/WakeScheduler.ts")) {
        replace('placement.ownsThread(threadId) ? notifyLocal(threadId, kind) : notifyRemote(threadId)', '(probe("wake.notify", { kind: kind ?? "all" }), placement.ownsThread(threadId) ? notifyLocal(threadId, kind) : notifyRemote(threadId))');
      }
      if (args.path.endsWith("/platform-cloudflare/src/Alarm.ts")) {
        // The production limit belongs to the composed runtime. Evaluate only
        // the disposable A/B control at use time, so a warm Object can alternate
        // paths without rebuilding its cached ManagedRuntime.
        replace(
          'const nativeDispatchConcurrency = () => (framework.singleThreadRuntime === runtime ? 1 : 2);',
          'const nativeDispatchConcurrency = () => (candidateEnabled("dispatch") && framework.singleThreadRuntime === runtime ? 1 : 2);',
        );
        replace(
          'if (active) return;\n\n                  const now = yield* Clock.currentTimeMillis;',
          'if (candidateEnabled("maintenance") && active) return;\n\n                  const now = yield* Clock.currentTimeMillis;',
        );
        replace(
          'const checkpointed = yield* runTransaction("checkpoint maintenance lane", () =>\n                    dueQueue.transaction(async () => {',
          'const checkpointed = yield* runTransaction("checkpoint maintenance lane", () =>\n                    dueQueue.transaction(async () => {\n                      if (active) return;',
        );
        replace(
          'const { state } = yield* runTransaction("read native maintenance deadline", () =>\n                readMaintenanceState(ctx.storage),',
          'const { state } = yield* runTransaction("read native maintenance deadline", () =>\n                candidateEnabled("maintenance") ? readMaintenanceState(ctx.storage) : dueQueue.transaction((transaction) => readMaintenanceState(transaction)),',
        );
        replace(
          'if (\n          recovery.started &&\n          native.active.size > 0 &&',
          'if (\n          candidateEnabled("maintenance") &&\n          recovery.started &&\n          native.active.size > 0 &&',
        );
      }
      if (!changed) return;
      return { contents: `import { probe, candidateEnabled } from ${JSON.stringify(join(here,"network/observe.ts"))};\n${source}`, loader: "ts" };
    });
  },
});
