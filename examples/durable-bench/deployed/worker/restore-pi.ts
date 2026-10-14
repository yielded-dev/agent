import { router } from "../../third-party/src/deployed/router.ts";
import type { Env } from "./protocol.ts";
import { RestoreDO } from "./restore-worker.ts";

export class PiDO extends RestoreDO {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env, "pi");
  }
}

export default router<Env & { PI: DurableObjectNamespace<PiDO> }>("pi", {
  stub: (env, query) =>
    env.PI.getByName(query.storageObject ?? query.object, { locationHint: "wnam" }),
});
