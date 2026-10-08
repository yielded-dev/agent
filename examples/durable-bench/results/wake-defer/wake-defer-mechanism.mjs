import { createRequire, builtinModules } from "node:module";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { join } from "node:path";

const root = process.cwd();
const require = createRequire(join(root, "examples/durable-bench/package.json"));
const { build } = require("esbuild");
const { Miniflare, convertV4MiniflareOptions } = require("miniflare");
const dir = await mkdtemp("/private/tmp/wake-defer-counts-");
const outfile = join(dir, "probe.mjs");
const record = (event, data = "{}") => `globalThis.__wakeProbe?.events.push({event:${JSON.stringify(event)},phase:globalThis.__wakeProbe.phase,alarms:globalThis.__wakeProbe.activeAlarms,...${data}})`;
const replace = (source, from, to) => {
  if (!source.includes(from)) throw new Error(`Missing instrumentation seam: ${from}`);
  return source.replace(from, to);
};

await build({
  entryPoints: [join(root, "examples/durable-bench/src/yielded.ts")],
  outfile, bundle: true, format: "esm", platform: "neutral", target: "es2024",
  conditions: ["workerd", "worker", "browser", "import"], mainFields: ["module", "main"],
  external: ["cloudflare:*", "node:*", ...builtinModules], logLevel: "error",
  plugins: [{name:"wake-mechanism-counts", setup(b) {
    b.onLoad({filter:/\/(yielded|Alarm|WakeScheduler|DurableAgentRuntime)\.ts$/}, async ({path}) => {
      let source = await readFile(path, "utf8");
      if (path.endsWith("examples/durable-bench/src/yielded.ts")) {
        source = replace(source, 'streamText: (request) => Stream.fromIterable(respond(request.prompt)),', `streamText: (request) => Stream.fromEffect(Effect.sync(() => { ${record("model:start")}; }).pipe(Effect.andThen(Effect.sleep("25 millis")))) .pipe(Stream.flatMap(() => { ${record("model:end")}; return Stream.fromIterable(respond(request.prompt)); })),`);
        source = replace(source, 'const receipt = yield* agentRuntime.submitRegistered', 'if(globalThis.__wakeProbe) globalThis.__wakeProbe.phase = "submit";\n    const receipt = yield* agentRuntime.submitRegistered');
        source = replace(source, 'const settled = yield* agentRuntime.processThreadResolved(threadId);', 'if(globalThis.__wakeProbe) globalThis.__wakeProbe.phase = "process";\n    const settled = yield* agentRuntime.processThreadResolved(threadId);\n    if(globalThis.__wakeProbe) globalThis.__wakeProbe.phase = "returned";');
        source = replace(source, '  async stats() {', `  constructor(ctx, env) { super(ctx, env); }
  async diagnose(input) {
    globalThis.__wakeProbe = {events:[], phase:"entry", activeAlarms:0};
    await this.turn(input);
    const result = {events: globalThis.__wakeProbe.events.slice()};
    await new Promise(resolve => setTimeout(resolve, 250));
    result.after = globalThis.__wakeProbe.events.slice(result.events.length);
    globalThis.__wakeProbe = undefined;
    return result;
  }
  override async alarm(...args) {
    const probe = globalThis.__wakeProbe;
    if (probe) {probe.activeAlarms++; ${record("alarm:start")};}
    try {await super.alarm(...args);} finally {
      if (probe) {${record("alarm:end")}; probe.activeAlarms--;}
    }
  }
  async stats() {`);
        source = replace(source, 'export default serve<{ THREADS: DurableObjectNamespace<YieldedDO> }>((env) =>\n  env.THREADS.getByName("main"),\n);', `export default {async fetch(request, env) {
  const stub = env.THREADS.getByName("wake-defer-local");
  if (new URL(request.url).pathname === "/diagnose") return Response.json(await stub.diagnose(await request.json()));
  return serve(() => stub).fetch(request, env);
}};`);
      } else if (path.endsWith("packages/platform-cloudflare/src/WakeScheduler.ts")) {
        source = replace(source, 'Effect.andThen(PubSub.publish(hints, threadId)),', `Effect.andThen(Effect.sync(() => {${record("notifyLocal",'{kind:kind ?? "settlement"}')};})),\n        Effect.andThen(PubSub.publish(hints, threadId)),`);
      } else if (path.endsWith("packages/platform-cloudflare/src/Alarm.ts")) {
        source = replace(source, 'Effect.flatMap((passes) => (passes > 0 ? Effect.void : armNow)),', `Effect.flatMap((passes) => {${record("scheduleNow",'{deferred:passes > 0}')};return passes > 0 ? Effect.void : armNow;}),`);
        source = replace(source, 'if (Number.isFinite(next))\n                  await ensureTransactionAlarmBy(transaction, Math.max(now, next));', `${record("armNow",'{due:Number.isFinite(next), nextDelay:Number.isFinite(next)?next-now:null}')};\n                if (Number.isFinite(next))\n                  await ensureTransactionAlarmBy(transaction, Math.max(now, next));`);
        source = replace(source, '        return result;\n      });\n\n      const backoffDelay', `        ${record("beginPass",'{tag:result._tag, generation:String(result.generation ?? ""),nonterminal:result.nonterminal}')};\n        return result;\n      });\n\n      const backoffDelay`);
        source = replace(source, '          // Persist each bounded pass before a claim;', `          ${record("recovery",'{reports:result.reports.map(r=>({decision:r.decision._tag,disposition:r.disposition})),work:(result.workReports??[]).map(r=>({disposition:r.disposition})),blocked:result.blocked.length}')};\n          // Persist each bounded pass before a claim;`);
        source = replace(source, '        return report;\n      });\n\n      return ThreadMaintenance.of', `        ${record("passReport",'{...report}')};\n        return report;\n      });\n\n      return ThreadMaintenance.of`);
        source = replace(source, '        const current = yield* Stream.runCollect(ledger.scanNonterminal);', `        const current = yield* Stream.runCollect(ledger.scanNonterminal);\n        ${record("maintenanceScan",'{states:current.map(r=>r.state)}')};`);
      } else if (path.endsWith("packages/effect-agent/src/durable/DurableAgentRuntime.ts")) {
        source = replace(source, 'ctx.append(batch).pipe(Effect.tap(() => wake.notify(ctx.threadId, "progress")));', `ctx.append(batch).pipe(Effect.tap(() => Effect.sync(() => {${record("notifySite",'{site:"appendBatch",tags:batch.records.map(r=>r.payload._tag)}')};}).pipe(Effect.andThen(wake.notify(ctx.threadId, "progress")))));`);
        source = source.replaceAll('yield* wake.notify(ctx.threadId, "progress");', `yield* Effect.sync(() => {${record("notifySite",'{site:"canonicalSettlement"}')};});\n    yield* wake.notify(ctx.threadId, "progress");`);
        source = source.replaceAll('yield* wake.notify(submission.threadId);', `yield* Effect.sync(() => {${record("notifySite",'{site:"settlementOrRecovery"}')};});\n    yield* wake.notify(submission.threadId);`);
        source = replace(source, '          if (Option.isNone(claimed)) return Option.none();', `          ${record("claim",'{claimed:Option.isSome(claimed)}')};\n          if (Option.isNone(claimed)) return Option.none();`);
      } else return;
      return {contents: source, loader:"ts"};
    });
  }}]
});
console.log(JSON.stringify({bundle:outfile}));
const mf = new Miniflare(convertV4MiniflareOptions({
  modules:[{type:"ESModule",path:outfile}], modulesRoot:dir, compatibilityDate:"2026-08-18",
  compatibilityFlags:["nodejs_compat"], durableObjects:{THREADS:{className:"YieldedDO",useSQLite:true}},
  resourcePersistencePath:join(dir,"state"),
}));
const call = async (path, body) => {
  const response = await mf.dispatchFetch(`http://probe${path}`,{method:"POST",body:JSON.stringify(body)});
  if(!response.ok) throw new Error(`${response.status}: ${await response.text()}`);
  return response.json();
};
try {
  await mf.ready;
  await call("/seed",[{id:"h0",text:"turn h0 tools=1"},{id:"h1",text:"turn h1 tools=1"}]);
  await new Promise(resolve => setTimeout(resolve,250));
  const turns=[];
  for(let i=0;i<3;i++) turns.push(await call("/diagnose",{id:`m${i}`,text:`turn m${i} tools=8`}));
  const result={
    kind:"deterministic-counts-only; local timings are not evidence",
    repositoryCommit:execFileSync("vp",["exec","git","rev-parse","HEAD"],{encoding:"utf8"}).trim(),
    repositoryStatus:execFileSync("vp",["exec","git","status","--short"],{encoding:"utf8"}).trim(),
    bundleSha256:createHash("sha256").update(await readFile(outfile)).digest("hex"),
    turns,
  };
  const output=process.argv[2] ?? join(dir,"counts.json");
  await writeFile(output,JSON.stringify(result,null,2)+"\n");
  console.log(JSON.stringify({output,turns:turns.map(({events,after})=>({counts:Object.fromEntries([...new Set(events.map(x=>x.event))].map(event=>[event,events.filter(x=>x.event===event).length])),claims:events.filter(x=>x.event==="claim").map(x=>x.claimed),schedule:Object.fromEntries(["submit","process","returned"].map(phase=>[phase,{calls:events.filter(x=>x.event==="scheduleNow"&&x.phase===phase).length,effective:events.filter(x=>x.event==="scheduleNow"&&x.phase===phase&&!x.deferred).length}])),passKinds:[...new Set(events.filter(x=>x.event==="passReport").map(x=>JSON.stringify({phase:x.phase,settled:x.settled,nonterminal:x.nonterminal,alarm:x.alarm})))],afterCounts:Object.fromEntries([...new Set(after.map(x=>x.event))].map(event=>[event,after.filter(x=>x.event===event).length]))}))},null,2));
} finally {await mf.dispose();}
