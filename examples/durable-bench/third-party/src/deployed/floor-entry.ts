import { EffectAiFloorDO } from "../../../deployed/worker/effect-ai-floor.ts";
import type { Env } from "../../../deployed/worker/protocol.ts";
import { router } from "./router.ts";
export { EffectAiFloorDO };
export default router<Env & { FLOOR: DurableObjectNamespace<EffectAiFloorDO> }>("effect-ai-floor", {
  stub: (env, query) => env.FLOOR.getByName(query.storageObject ?? query.object, { locationHint: "wnam" }),
});
