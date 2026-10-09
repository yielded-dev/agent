import { rmSync, writeFileSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { Effect, Schema } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { call, stage, start } from "../../bench/targets.ts";
import { turn } from "../../src/plan.ts";
const Failure = Schema.TaggedError()("ProdTurnCountError", { message: Schema.String });
NodeRuntime.runMain(Effect.tryPromise({
  try: async () => {
    process.chdir(fileURLToPath(new URL("../../", import.meta.url)));
    const rows=[];
    for (const history of [50,250]) {
      const directory=stage("yielded",history);
      const mf=await start("yielded",directory);
      try {
        const before=await call(mf,"/stats");
        await call(mf,"/turn",turn("count",8));
        const after=await call(mf,"/stats");
        rows.push({history, seedFingerprint: JSON.parse(readFileSync(`fixtures/yielded-${history}.json`,"utf8")).fingerprint, tableDeltas:Object.fromEntries(Object.entries(after.tables).map(([name,n])=>[name,n-before.tables[name]])), outcome:"completed"});
      } finally { await mf.dispose(); rmSync(directory,{recursive:true}); }
    }
    const artifact={source:"#821 production target", timings:"excluded: local counts only", rows};
    writeFileSync("results/prod-turn/local-counts.json",JSON.stringify(artifact,null,2)+"\n");
    console.log(JSON.stringify(artifact));
  }, catch: cause => new Failure({message:String(cause)})
}));
