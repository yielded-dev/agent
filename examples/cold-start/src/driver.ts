import { DurableObject } from "cloudflare:workers";
import * as Schema from "effect/Schema";

import { lines } from "./protocol.ts";

const Identity = Schema.Struct({
  moduleId: Schema.NonEmptyString,
  calls: Schema.Int,
  constructions: Schema.Int,
  alarms: Schema.Int,
  version: Schema.NonEmptyString,
});

const Frame = Schema.Union([
  Schema.Struct({ type: Schema.Literals(["worker", "object"]), value: Identity }),
  Schema.Struct({ type: Schema.Literal("text"), value: Schema.Struct({ text: Schema.String }) }),
  Schema.Struct({
    type: Schema.Literal("done"),
    value: Schema.Struct({
      outcome: Schema.Literal("completed"),
      calls: Schema.Int,
    }),
  }),
  Schema.Struct({ type: Schema.Literal("error"), value: Schema.String }),
]);

const Capture = Schema.Struct({ name: Schema.NonEmptyString, sample: Schema.NonEmptyString });

class DriverError extends Schema.TaggedError<DriverError>()("DriverError", {
  message: Schema.String,
}) {}
interface Env {
  TOKEN: string;
  PREFIX: string;
  SUBDOMAIN: string;
  DRIVER: DurableObjectNamespace<Driver>;
}

/** One stable location per deployment; laptop/network setup is outside all timers. */
export class Driver extends DurableObject<Env> {
  private readonly instance = crypto.randomUUID();

  override async fetch(request: Request): Promise<Response> {
    const colo = request.cf?.colo ?? null;

    if (new URL(request.url).pathname === "/health")
      return Response.json({ instance: this.instance, colo });
    const query = Schema.decodeUnknownSync(Capture)(await request.json());

    if (!query.name.startsWith(this.env.PREFIX + "-"))
      // oxlint-disable-next-line effect/no-untyped-throw -- Native Worker callback rejects a typed boundary error.
      throw DriverError.make({ message: "Target is outside this run" });
    // A retained claim prevents an ambiguous laptop disconnect from replaying a cold turn.
    const key = `${query.name}/${query.sample}`;

    if (await this.ctx.storage.get(key))
      return new Response("Capture already attempted", { status: 409 });
    await this.ctx.storage.put(key, true);
    const frames: { at: number; frame: typeof Frame.Type }[] = [];
    const start = Date.now();

    try {
      const response = await fetch(
        `https://${query.name}.${this.env.SUBDOMAIN}.workers.dev/capture?sample=${encodeURIComponent(query.sample)}`,
        {
          headers: { authorization: `Bearer ${this.env.TOKEN}` },
          signal: AbortSignal.timeout(60_000),
        },
      );

      if (!response.ok || !response.body)
        // oxlint-disable-next-line effect/no-untyped-throw -- Native Worker callback rejects a typed boundary error.
        throw DriverError.make({ message: `Target HTTP ${response.status}` });
      const reader = response.body.getReader();

      const parse = lines((line) => {
        if (line)
          frames.push({
            at: Date.now() - start,
            frame: Schema.decodeSync(Schema.fromJsonString(Frame))(line),
          });
      });

      try {
        for (;;) {
          const next = await reader.read();

          if (next.done) break;
          parse.write(next.value);
        }
        parse.finish();
      } finally {
        reader.releaseLock();
      }
      const result = { instance: this.instance, colo, frames, elapsed: Date.now() - start };

      await this.ctx.storage.put(key, result);

      return Response.json(result);
    } catch (cause) {
      const result = { instance: this.instance, colo, frames, error: String(cause) };

      await this.ctx.storage.put(key, result);

      return Response.json(result, { status: 502 });
    }
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.headers.get("authorization") !== `Bearer ${env.TOKEN}`)
      return new Response(null, { status: 401 });

    return env.DRIVER.getByName("driver", { locationHint: "wnam" }).fetch(request);
  },
};
