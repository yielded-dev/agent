import { readFileSync } from "node:fs";
import { join } from "node:path";

// Build-only observations: product files remain byte-identical for the initial map.
export const instrumentAdmission = (root, here) => ({
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
        replace("yield* gateAdmissionLimits(threadId, request);", 'yield* traceAdmission("limits", gateAdmissionLimits(threadId, request));');
        replace("decodeSubmitRequest(encoded).pipe(", 'traceAdmission("decode-request", decodeSubmitRequest(encoded)).pipe(');
        replace(".pipe(Effect.tap(() => publishCommitted)),", '.pipe(Effect.tap(() => traceAdmission("publish-committed", publishCommitted))),');
        const start = source.indexOf("const submitEndpoint =");
        const end = source.indexOf("const submissionStatusEndpoint =", start);
        const endpoint = source.slice(start, end).replace("Effect.flatMap(encodeResponse)", 'Effect.flatMap((response) => traceAdmission("encode-response", encodeResponse(response)))');
        source = source.slice(0, start) + endpoint + source.slice(end);
      } else if (relative === "packages/platform-cloudflare/src/Alarm.ts") {
        replace('run(operation, Effect.tryPromise({ try: execute, catch: alarmFailure(operation) }))', 'traceAdmission(`alarm/${operation}`, run(operation, Effect.tryPromise({ try: execute, catch: alarmFailure(operation) })))');
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
      return { contents: `import { traceAdmission, markAdmission } from ${JSON.stringify(join(here, "network/admission.ts"))};\n${source}`, loader: "ts" };
    });
  },
});

