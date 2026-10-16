import { IdGenerator } from "@yielded/agent/id-generator";
import { RunId, ThreadId, TurnId } from "@yielded/agent/identifiers";
import * as EffectAiIdGenerator from "effect/ai/IdGenerator";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

/** Supply the same identity authority to current code and releases predating default IDs. */
export const BenchmarkIdsLive = Layer.succeed(IdGenerator, {
  nextThreadId: EffectAiIdGenerator.defaultIdGenerator
    .generateId()
    .pipe(Effect.map((id) => Schema.decodeSync(ThreadId)(`thread-${id}`))),
  nextRunId: EffectAiIdGenerator.defaultIdGenerator
    .generateId()
    .pipe(Effect.map((id) => Schema.decodeSync(RunId)(`run-${id}`))),
  nextTurnId: EffectAiIdGenerator.defaultIdGenerator
    .generateId()
    .pipe(Effect.map((id) => Schema.decodeSync(TurnId)(`turn-${id}`))),
});
