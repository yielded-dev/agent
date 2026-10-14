import { DurableObject } from "cloudflare:workers";

import { Host } from "./host.ts";
import { type Env, type Target } from "./protocol.ts";

/** No production constructor or alarm may mutate an unfinished SQLite restore. */
export class RestoreDO extends DurableObject<Env> {
  protected readonly host: Host;
  constructor(ctx: DurableObjectState, env: Env, target: Target) {
    super(ctx, env);
    this.host = new Host(ctx, env, target);
  }
  override async fetch(request: Request): Promise<Response> {
    return this.host.fetch(request, {});
  }
}
