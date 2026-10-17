import { exportThread, layer } from "@yielded/agent-storage-cloudflare/do-thread-store";
import { Effect, Schema } from "effect";

import { router } from "../../third-party/src/deployed/router.ts";
import { errorText, Failure, readQuery, type Env } from "./protocol.ts";
import { RestoreDO } from "./restore-worker.ts";
import { restore } from "./restore.ts";
import { FixtureError, FixtureExport, RestoreStorage } from "./storage.ts";
import { installUsage } from "./usage.ts";

export class YieldedDO extends RestoreDO {
  constructor(ctx: DurableObjectState, env: Env) {
    if (env.WRITES === "true") installUsage(ctx.storage);
    super(ctx, env, "yielded");
  }
  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (!url.pathname.startsWith("/restore/") || url.pathname === "/restore/clear")
      return this.host.fetch(request, {});
    try {
      const query = readQuery(url);

      if (query.target !== "yielded") return new Response("Target mismatch", { status: 400 });
      this.host.meter.assertBuild(query);

      // Bind capabilities at the native Object boundary; restore acquires them through R.
      const restored = await Effect.runPromise(
        restore(request, query).pipe(
          Effect.provideService(RestoreStorage, this.ctx.storage),
          Effect.provideService(FixtureExport, {
            open: Effect.void.pipe(
              Effect.provide(layer({ storage: this.ctx.storage, verifyOnOpen: true })),
              Effect.mapError((cause) =>
                FixtureError.make({ message: "Native fixture open refused", cause }),
              ),
            ),
            page: (input) => exportThread({ storage: this.ctx.storage }, input),
          }),
          Effect.scoped,
        ),
      );

      return Response.json({
        ...restored,
        identity: this.host.meter.identity(),
        ...this.host.storageUsage(),
      });
    } catch (cause) {
      return Response.json(
        Schema.encodeSync(Failure)({
          ok: false,
          error: errorText(cause),
          sample: url.searchParams.get("sample"),
        }),
        { status: 500 },
      );
    }
  }
}

export default router<Env & { YIELDED: DurableObjectNamespace<YieldedDO> }>("yielded", {
  stub: (env, query) =>
    env.YIELDED.getByName(query.storageObject ?? query.object, { locationHint: "wnam" }),
});
