import { DurableObject } from "cloudflare:workers";
import { Schema } from "effect";
import { cases, runCase } from "./cases.ts";

declare const BUILD_ID: string;
declare const FIXTURE_SHA: string;

const Input = Schema.Struct({
  id: Schema.NonEmptyString,
  case: Schema.Literals(cases),
  mode: Schema.Literals(["effect", "plain"]),
  iterations: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 8_000_000 })),
});
const parse = Schema.decodeUnknownSync(Input);
type Env = { BENCH: DurableObjectNamespace<Calibration>; TOKEN: string; VERSION: { id: string } };
let incarnation: string | undefined;
let finalized = 0;
const registry = new FinalizationRegistry(() => { finalized++; });

export class Calibration extends DurableObject<Env> {
  readonly instance = crypto.randomUUID();
  identity() {
    return { build: BUILD_ID, fixture: FIXTURE_SHA, incarnation: incarnation ??= crypto.randomUUID(), instance: this.instance,
      objectId: this.ctx.id.toString(), version: this.env.VERSION.id, finalized };
  }
  primeGc() {
    for (let i = 0; i < 4096; i++) registry.register({ i }, i);
    return this.identity();
  }
  run(raw: unknown) {
    const input = parse(raw);
    console.log({ evalCostSample: input.id });
    const started = Date.now();
    const checksum = runCase(input.case, input.mode, input.iterations, {
      exec: (query, value) => this.ctx.storage.sql.exec<{ n: number }>(query, value),
    });
    return { ...input, checksum, doWallMs: Date.now() - started, ...this.identity() };
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.headers.get("authorization") !== `Bearer ${env.TOKEN}`) return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);
    const name = url.searchParams.get("object") ?? "micro";
    const stub = env.BENCH.get(env.BENCH.idFromName(name), { locationHint: "wnam" });
    if (url.pathname === "/identity") return Response.json(await stub.identity());
    if (url.pathname === "/prime-gc") return Response.json(await stub.primeGc());
    if (url.pathname === "/run" && request.method === "POST") return Response.json(await stub.run(await request.json()));
    return new Response("not found", { status: 404 });
  },
};
