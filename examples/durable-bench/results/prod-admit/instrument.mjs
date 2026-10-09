import { readFileSync } from "node:fs";
import { join } from "node:path";

const FAST_ARM_NOW = "        const fastArmNow = Clock.currentTimeMillis.pipe(\n          Effect.flatMap((now) =>\n            storageOperation(\"wake maintenance lanes\", async () => {\n              const next = DueQueue.next(DueQueue.make(ctx.storage).read());\n\n              if (!Number.isFinite(next)) return;\n              const existing = await ctx.storage.getAlarm();\n\n              // A pending due alarm already owns this droppable promptness hint.\n              if (existing !== null && existing <= Math.max(now, next)) return;\n\n              await ctx.storage.transaction(async (transaction) => {\n                const next = DueQueue.next(DueQueue.make(ctx.storage).read());\n\n                if (Number.isFinite(next))\n                  await ensureTransactionAlarmBy(transaction, Math.max(now, next));\n              });\n            }),\n          ),\n        );\n\n";

// Build-only observations: product files remain byte-identical for the initial map.
export const instrumentAdmission = (root, here, effectiveSources = new Map()) => ({
  name: "prod-admit-await-map",
  setup(build) {
    build.onLoad({ filter: /packages\/(?:platform-cloudflare|effect-agent)\/src\/.*\.ts$/ }, ({ path }) => {
      let source = readFileSync(path, "utf8");
      const relative = path.slice(root.length + 1);
      const replace = (from, to) => {
        if (!source.includes(from)) throw new Error(`Missing observation anchor in ${relative}: ${from}`);
        source = source.replace(from, to);
      };
      if (relative === "packages/platform-cloudflare/src/ThreadObject.ts") {
        replace("  const mutations = yield* ThreadMutationGate;\n  const runtime = yield* DurableAgentRuntime;", '  const mutations = yield* ThreadMutationGate;\n  const runtime = yield* DurableAgentRuntime;\n  const admissionAlarm = yield* DurableAlarmService;\n  const admissionObservation = yield* AdmissionContext;\n  yield* Effect.sync(() => markAdmission("candidate:selected", admissionObservation?.variant ?? "baseline-a", admissionObservation));');
        replace("yield* gateAdmissionLimits(threadId, request);", 'yield* traceAdmission("limits", gateAdmissionLimits(threadId, request));');
        replace("decodeSubmitRequest(encoded).pipe(", 'traceAdmission("decode-request", decodeSubmitRequest(encoded)).pipe(');
        replace(".pipe(Effect.tap(() => publishCommitted)),", '.pipe((body) => (admissionObservation?.variant === "prearm-only" || admissionObservation?.variant === "prearm-only-probe") ? admissionAlarm.withWakesDeferred(body) : body, Effect.tap(() => traceAdmission("publish-committed", publishCommitted))),');
        const start = source.indexOf("const submitEndpoint =");
        const end = source.indexOf("const submissionStatusEndpoint =", start);
        const endpoint = source.slice(start, end).replace("const submitEndpoint =", "const rawSubmitEndpoint =").replace("Effect.flatMap(encodeResponse)", 'Effect.flatMap((response) => traceAdmission("encode-response", encodeResponse(response)))');
        source = source.slice(0, start) + endpoint + 'const submitEndpoint = (encoded: unknown) => observeAdmission(encoded, rawSubmitEndpoint(encoded));\n\n' + source.slice(end);
      } else if (relative === "packages/platform-cloudflare/src/Alarm.ts") {
        replace('run(operation, Effect.tryPromise({ try: execute, catch: alarmFailure(operation) }))', 'traceAdmission(`alarm/${operation}`, run(operation, Effect.tryPromise({ try: execute, catch: alarmFailure(operation) })))');
        const armStart = source.indexOf("        const armNow =");
        const armEnd = source.indexOf("        const scheduleNow =", armStart);
        if (armStart < 0 || armEnd < 0) throw new Error("Alarm wake boundary missing");
        const baselineArm = source.slice(armStart, armEnd).replace("const armNow =", "const baselineArmNow =");
        const fastArm = FAST_ARM_NOW;
        source = source.slice(0, armStart) + baselineArm + fastArm + '        const armNow = Effect.flatMap(AdmissionContext, (observation) => (observation?.variant === "prearm-fast" || observation?.variant === "prearm-fast-probe") ? fastArmNow : baselineArmNow);\n\n' + source.slice(armEnd);
        const begin = source.indexOf("      const beginMutation =");
        const end = source.indexOf("      const releaseLanes =", begin);
        const body = source.slice(begin, end);
        if (!body.includes("Math.max(deadline, now + minimumAlarmDelay)")) throw new Error("Prearm deadline anchor missing");
        source = source.slice(0, begin) + body.replace('        const now = yield* Clock.currentTimeMillis;', '        const admissionObservation = yield* AdmissionContext;\n        const now = yield* Clock.currentTimeMillis;').replace("Math.max(deadline, now + minimumAlarmDelay)", 'Math.max(deadline, now + ((admissionObservation?.variant === "prearm-now" || admissionObservation?.variant === "prearm-fast" || admissionObservation?.variant === "prearm-fast-probe") ? 0 : minimumAlarmDelay))') + source.slice(end);
      } else if (relative === "packages/effect-agent/src/durable/DurableAgentRuntime.ts") {
        const start = source.indexOf("  const submit = Effect.fnUntraced(");
        const end = source.indexOf("  const authorizeSettlement =", start);
        if (start < 0 || end < 0) throw new Error("Submit observation boundary missing");
        let submit = source.slice(start, end);
        const replacements = [
          ["withCrypto(digestJson(inputPayload))", 'traceAdmission("input-digest", withCrypto(digestJson(inputPayload)))'],
          ["ledger.admit(request)", 'traceAdmission("ledger-admit", ledger.admit(request))'],
          ["materializeAtLeast(options.threadId, ZERO_EPOCH)", 'traceAdmission("materialize", materializeAtLeast(options.threadId, ZERO_EPOCH))'],
          ["ensureThreadCreated(options.threadId, agent.definition.id, options.definitions)", 'traceAdmission("thread-created", ensureThreadCreated(options.threadId, agent.definition.id, options.definitions))'],
          ["ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }))", 'traceAdmission("mark-ready", ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId })))'],
          ["wake.notify(options.threadId)", 'traceAdmission("wake", wake.notify(options.threadId))'],
        ];
        for (const [from, to] of replacements) {
          if (!submit.includes(from)) throw new Error(`Submit anchor missing: ${from}`);
          submit = submit.replace(from, to);
        }
        source = source.slice(0, start) + submit + source.slice(end);
      } else if (relative === "packages/platform-cloudflare/src/CloudflareThreadClient.ts") {
        replace('const raw = yield* callThreadObject(', 'markAdmission("client/rpc:start");\n          const raw = yield* callThreadObject(');
        replace('return yield* decodeHostResponse(raw).pipe(', 'markAdmission("client/rpc:end");\n          return yield* traceAdmission("client/decode-response", decodeHostResponse(raw)).pipe(');
        replace('const response = yield* call(options.threadId, "submit", encoded);', 'markAdmission("client/encode:end");\n            const response = yield* call(options.threadId, "submit", encoded);');
        replace('return succeeded.receipt;', 'markAdmission("client/receipt");\n            return succeeded.receipt;');
      } else return;
      const contents = `import { AdmissionContext, observeAdmission, traceAdmission, markAdmission } from ${JSON.stringify(join(here, "network/admission.ts"))};\n${source}`;
      effectiveSources.set(relative, contents);
      return { contents, loader: "ts" };
    });
  },
});
