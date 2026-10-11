import { DurableObject } from "cloudflare:workers";

import { Host } from "./host.ts";
import { type Env, type Target } from "./protocol.ts";

export { default, ActorDO, ThreadDO } from "../../third-party/src/deployed/index.ts";

/** No production constructor or alarm may mutate an unfinished SQLite restore. */
class RestoreDO extends DurableObject<Env> {
  private readonly host: Host;
  constructor(ctx: DurableObjectState, env: Env, target: Target) {
    super(ctx, env);
    this.host = new Host(ctx, env, target);
  }
  override async fetch(request: Request): Promise<Response> {
    return this.host.fetch(request, {});
  }
}

export class YieldedDO extends RestoreDO {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env, "yielded");
  }
  override fetch(request: Request): Promise<Response> {
    return super.fetch(request);
  }
}

export class PiDO extends RestoreDO {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env, "pi");
  }
  override fetch(request: Request): Promise<Response> {
    return super.fetch(request);
  }
}
