import { PiDO } from "./pi.ts";
import type { Env } from "../../../deployed/worker/protocol.ts";
import { router } from "./router.ts";
export { PiDO };
export default router<Env & { PI: DurableObjectNamespace<PiDO> }>("pi", {
  stub: (env, query) => env.PI.getByName(query.storageObject ?? query.object, { locationHint: "wnam" }),
});
