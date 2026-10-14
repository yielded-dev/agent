import { DurableObject } from "cloudflare:workers";
import type { Effect } from "effect";

import { Host } from "./host.ts";
import { type Env, type Target } from "./protocol.ts";
import type { FixtureError } from "./storage.ts";

/** No production constructor or alarm may mutate an unfinished SQLite restore. */
export class RestoreDO extends DurableObject<Env> {
  private readonly host: Host;
  constructor(
    ctx: DurableObjectState,
    env: Env,
    target: Target,
    private readonly openFixture?: Effect.Effect<void, FixtureError>,
  ) {
    super(ctx, env);
    this.host = new Host(ctx, env, target);
  }
  override async fetch(request: Request): Promise<Response> {
    return this.host.fetch(
      request,
      this.openFixture === undefined ? {} : { openFixture: this.openFixture },
    );
  }
}
