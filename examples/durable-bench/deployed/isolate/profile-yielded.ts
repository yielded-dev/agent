import { DurableObject } from "cloudflare:workers";

import { YieldedDO as ProductionThreadObject } from "../worker/yielded.ts";
import { isolateObservation } from "./observation.ts";
import type { Env } from "./protocol.ts";
import { coldBisectBeforeInit, timeline } from "./timeline.ts";

export { default } from "./yielded.ts";

/** Profiling-only shell: make the new isolate discoverable before its first ThreadObject.
 * No storage access, layout, recovery, or ThreadObject acquisition occurs in this shell.
 * Its elapsed time is deliberately excluded from all fresh-start latency evidence.
 */
export class YieldedDO extends DurableObject<Env> {
  private inner?: ProductionThreadObject;
  private instance() {
    return (this.inner ??= new ProductionThreadObject(this.ctx, this.env));
  }
  async benchSubmit(...args: Parameters<ProductionThreadObject["benchSubmit"]>) {
    const inner = this.instance();

    await timeline(this.ctx.storage)?.opened;

    return inner.benchSubmit(...args);
  }
  awaitSettlementEncoded(encoded: unknown, ...trace: [] | [unknown]) {
    return this.instance().awaitSettlementEncoded(encoded, ...trace);
  }
  override alarm(...args: Parameters<ProductionThreadObject["alarm"]>) {
    return this.instance().alarm(...args);
  }
  override async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;

    if (path === "/profile-id")
      return Response.json({
        actorId: this.ctx.id.toString(),
        version: this.env.VERSION?.id,
        isolate: isolateObservation(this.env),
        initialized: this.inner !== undefined,
      });
    if (path === "/sentinel") return Response.json({ ok: true, result: coldBisectBeforeInit(7) });

    return this.instance().fetch(request);
  }
}
