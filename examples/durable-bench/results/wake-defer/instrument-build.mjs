import { join } from "node:path";

// Content-free observation of the current baseline. Adapted from the main agent's
// /private/tmp/wake-defer-mechanism.mjs; strict matches make source drift fail closed.
export function instrumentSource(path, source, here, mode) {
  const changes = [];
  const replace = (label, from, to, all = false) => {
    const count = source.split(from).length - 1;
    if (!count || (!all && count !== 1)) throw new Error(`Instrumentation seam ${label}: expected ${all ? "one or more" : "one"}, found ${count}`);
    source = all ? source.replaceAll(from, to) : source.replace(from, to);
    changes.push({ label, occurrences: count });
  };
  const record = (event, detail = "{}") => `__wd?.record(${JSON.stringify(event)}, ${detail});`;
  const header = `import { mechanismMeter as __wakeDeferMeter, observeWakeLayer as __observeWakeLayer } from ${JSON.stringify(join(here, "network/instrumentation.ts"))};\n`;
  if (path.endsWith("packages/platform-cloudflare/src/internal/layers.ts")) {
    replace("wake-layer", "const wakes = cloudflareWakeSchedulerLayer.pipe(Layer.provide(base));", "const wakes = __observeWakeLayer(cloudflareWakeSchedulerLayer).pipe(Layer.provide(base));");
  } else if (path.endsWith("packages/platform-cloudflare/src/Alarm.ts")) {
    replace("alarm-meter", "const runningPasses = yield* Ref.make(0);", "const __wd = yield* __wakeDeferMeter;\n        const runningPasses = yield* Ref.make(0);");
    replace("scheduleNow", "Effect.flatMap((passes) => (passes > 0 ? Effect.void : armNow)),", `Effect.flatMap((passes) => { ${record("scheduleNow", "{ deferred: passes > 0, runningPasses: passes }")} return passes > 0 ? Effect.void : armNow; }),`);
    replace("armNow", "if (Number.isFinite(next))\n                  await ensureTransactionAlarmBy(transaction, Math.max(now, next));", `${record("armNow", "{ due: Number.isFinite(next), nextDelay: Number.isFinite(next) ? next - now : null }")}\n                if (Number.isFinite(next))\n                  await ensureTransactionAlarmBy(transaction, Math.max(now, next));`);
    replace("maintenance-meter", "const dueQueue = DueQueue.make(ctx.storage);\n      const storage", "const __wd = yield* __wakeDeferMeter;\n      const dueQueue = DueQueue.make(ctx.storage);\n      const storage");
    replace("beginPass", "        return result;\n      });\n\n      const backoffDelay", `        ${record("beginPass", '{ tag: result._tag, generation: String(result.generation ?? ""), nonterminal: result.nonterminal }')}\n        return result;\n      });\n\n      const backoffDelay`);
    replace("recovery", "          // Persist each bounded pass before a claim;", `          ${record("recovery", "{ reports: result.reports.map(r => ({ decision: r.decision._tag, disposition: r.disposition })), work: (result.workReports ?? []).map(r => ({ disposition: r.disposition })), blocked: result.blocked.length }")}\n          // Persist each bounded pass before a claim;`);
    replace("maintenanceReport", "        return report;\n      });\n\n      return ThreadMaintenance.of", `        ${record("maintenanceReport", "{ ...report }")}\n        return report;\n      });\n\n      return ThreadMaintenance.of`);
    replace("scan", "const current = yield* Stream.runCollect(ledger.scanNonterminal);", `const current = yield* Stream.runCollect(ledger.scanNonterminal);\n        ${record("maintenanceScan", "{ states: current.map(r => r.state), count: current.length }")}`);
    replace("remaining-scan", "const remaining = yield* Stream.runCollect(ledger.scanNonterminal);", `const remaining = yield* Stream.runCollect(ledger.scanNonterminal);\n        ${record("checkpointScan", "{ states: remaining.map(r => r.state), count: remaining.length }")}`);
    replace("checkpoint", "dueQueue.checkpointNative(dueAt);", `dueQueue.checkpointNative(dueAt);\n              ${record("generationCheckpoint", "{ dirty: String(next.dirty), processed: String(next.processed), observedGeneration: String(observation.generation), scannedGeneration: String(scannedGeneration), startedGeneration: String(started.generation), mutationOverlap, autonomous, progressed, canBackoff, nonterminal: remaining.length, dueAt }")}`);
  } else if (path.endsWith("packages/effect-agent/src/durable/DurableAgentRuntime.ts")) {
    replace("runtime-meter", "const failpoint = yield* DurableRuntimeFailpoint;", "const failpoint = yield* DurableRuntimeFailpoint;\n  const __wd = yield* __wakeDeferMeter;");
    replace("appendBatch", 'ctx.append(batch).pipe(Effect.tap(() => wake.notify(ctx.threadId, "progress")));', `ctx.append(batch).pipe(Effect.tap(() => Effect.sync(() => { ${record("notifySite", '{ site: "appendBatch", tags: batch.records.map(r => r.payload._tag) }')} }).pipe(Effect.andThen(wake.notify(ctx.threadId, "progress")))));`);
    replace("canonicalSettlement", 'yield* wake.notify(ctx.threadId, "progress");', `${record("notifySite", '{ site: "canonicalSettlement" }')}\n    yield* wake.notify(ctx.threadId, "progress");`, true);
    replace("settlement", "yield* wake.notify(submission.threadId);", `${record("notifySite", '{ site: "settlementOrRecovery" }')}\n    yield* wake.notify(submission.threadId);`, true);
    replace("claim-attempt", "const claimed = yield* runStorage.claim(", `${record("claimAttempt", "{ threadId }")}\n          const claimed = yield* runStorage.claim(`);
    replace("claim", "          if (Option.isNone(claimed)) return Option.none();", `          ${record("claim", "{ claimed: Option.isSome(claimed), ...(Option.isSome(claimed) ? { attemptId: claimed.value.claim.attemptId, submissionId: claimed.value.claim.submissionId, producerEpoch: claimed.value.claim.producerEpoch } : {}) }")}\n          if (Option.isNone(claimed)) return Option.none();`);
    replace("recovery-decision", "const decision = classifyRecovery(snapshot, evidence);\n\n    // Untouched ready input", `const decision = classifyRecovery(snapshot, evidence);\n    ${record("recoveryDecision", "{ decision: decision._tag, state: snapshot.submission.state, submissionId: submission.submissionId }")}\n\n    // Untouched ready input`);
    if (mode === "ab" && !source.includes("wake.withProcessing")) throw new Error("A/B build requires the product withProcessing call at processThreadHead");
  } else return { source, changes };
  return { source: header + source, changes };
}
