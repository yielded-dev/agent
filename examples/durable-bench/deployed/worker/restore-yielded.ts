import { layer } from "@yielded/agent-storage-cloudflare/do-thread-store";
import { Effect } from "effect";

import { router } from "../../third-party/src/deployed/router.ts";
import type { Env } from "./protocol.ts";
import { RestoreDO } from "./restore-worker.ts";
import { FixtureError } from "./storage.ts";

export class YieldedDO extends RestoreDO {
  constructor(ctx: DurableObjectState, env: Env) {
    super(
      ctx,
      env,
      "yielded",
      Effect.void.pipe(
        Effect.provide(layer({ storage: ctx.storage, verifyOnOpen: true })),
        Effect.mapError((cause) =>
          FixtureError.make({ message: "Native fixture open refused", cause }),
        ),
      ),
    );
  }
}

export default router<Env & { YIELDED: DurableObjectNamespace<YieldedDO> }>("yielded", {
  stub: (env, query) =>
    env.YIELDED.getByName(query.storageObject ?? query.object, { locationHint: "wnam" }),
});
