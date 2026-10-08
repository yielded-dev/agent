import { Schema } from "effect";
import { YieldedDO } from "../../../src/yielded.ts";

declare const BUILD_ID: string;
declare const FIXTURE_SHA: string;

const Turn = Schema.Struct({ id: Schema.NonEmptyString, text: Schema.String });
const Run = Schema.Struct({ sampleId: Schema.NonEmptyString, turn: Turn });
const parseRun = Schema.decodeUnknownSync(Run);
const parseSeed = Schema.decodeUnknownSync(Schema.Array(Turn));
let incarnation: string | undefined;

// The original benchmark owns every engine, model, tool and storage operation.
// This wrapper adds only identity receipts, RPC boundaries and observability.
export class MeasuredYieldedDO extends YieldedDO {
  readonly instance = crypto.randomUUID();
  identity() {
    return { build: BUILD_ID, fixture: FIXTURE_SHA, incarnation: incarnation ??= crypto.randomUUID(), instance: this.instance,
      objectId: this.ctx.id.toString(), version: this.env.VERSION.id, generation: this.env.PHASE };
  }
  async seedPhase(raw: unknown) {
    const fingerprint = await super.seed(parseSeed(raw));
    return { fingerprint, stats: await super.stats(), ...this.identity() };
  }
  async wakePhase() {
    const start = Date.now();
    await super.wake();
    return { doWallMs: Date.now() - start, ...this.identity() };
  }
  async measured(raw: unknown) {
    const input = parseRun(raw);
    console.log({ evalCostSample: input.sampleId });
    const start = Date.now();
    await super.turn(input.turn);
    return { ...input, doWallMs: Date.now() - start, ...this.identity() };
  }
  async fingerprint() {
    return { fingerprint: await super.seed([]), stats: await super.stats(), ...this.identity() };
  }
}

type Env = { THREADS: DurableObjectNamespace<MeasuredYieldedDO>; TOKEN: string; PHASE: string };
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.headers.get("authorization") !== `Bearer ${env.TOKEN}`) return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);
    const name = url.searchParams.get("object");
    if (!name) return new Response("missing object", { status: 400 });
    const stub = env.THREADS.get(env.THREADS.idFromName(name), { locationHint: "wnam" });
    if (url.pathname === "/identity") return Response.json(await stub.identity());
    if (url.pathname === "/seed" && request.method === "POST" && env.PHASE === "seed") return Response.json(await stub.seedPhase(await request.json()));
    if (env.PHASE !== "measure") return new Response("wrong phase", { status: 412 });
    if (url.pathname === "/wake") return Response.json(await stub.wakePhase());
    if (url.pathname === "/fingerprint") return Response.json(await stub.fingerprint());
    if (url.pathname === "/run" && request.method === "POST") return Response.json(await stub.measured(await request.json()));
    return new Response("not found", { status: 404 });
  },
};
