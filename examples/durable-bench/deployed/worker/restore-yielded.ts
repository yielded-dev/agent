import { router } from "../../third-party/src/deployed/router.ts";
import type { Env } from "./protocol.ts";
import { RestoreDO } from "./restore-worker.ts";

export class YieldedDO extends RestoreDO {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env, "yielded");
  }
}

export default router<Env & { YIELDED: DurableObjectNamespace<YieldedDO> }>("yielded", {
  stub: (env, query) =>
    env.YIELDED.getByName(query.storageObject ?? query.object, { locationHint: "wnam" }),
});
