import { BrowserCrypto } from "@effect/platform-browser";
import { CloudflareThreadClient } from "@yielded/agent-platform-cloudflare/cloudflare-thread-client";
import { digestDefinitions } from "@yielded/agent/digest";
import { ThreadId } from "@yielded/agent/identifiers";
import { IdempotencyKey, Principal, Receipt } from "@yielded/agent/receipt";
import { CanonicalSequence } from "@yielded/agent/records";
import { Effect, Schema } from "effect";
import { Prompt } from "effect/ai";

import { turn, MEASURED_TOOLS } from "../../../src/plan.ts";
import { NetworkPiDO } from "./pi.ts";
import { decodeIdentity, errorText, readQuery, type Env } from "./protocol.ts";
import { agent, definitions, NetworkYieldedDO } from "./yielded.ts";

export { NetworkYieldedDO, NetworkPiDO };

type Bindings = Env & {
  YIELDED: DurableObjectNamespace<NetworkYieldedDO>;
  PI: DurableObjectNamespace<NetworkPiDO>;
};
type TextObservation = {
  call: number;
  firstTextMs: number;
  completeMs?: number;
  finalizedMs?: number;
  text: string;
  source: string;
};

const objectResult = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown));
const decodePrompt = Schema.decodeUnknownSync(Prompt.Prompt);
const clientLayer = (env: Bindings) => [
  CloudflareThreadClient.layerFromBinding({ namespace: env.YIELDED }),
  BrowserCrypto.layer,
];

async function probe(env: Env, sample: string, via?: (request: Request) => Promise<Response>) {
  const beforeMs = Date.now();
  const url = new URL(via ? "/clock" : "/echo", env.PROVIDER_URL);
  url.searchParams.set("sample", sample);
  const request = new Request(url, { headers: { authorization: `Bearer ${env.TOKEN}` } });
  const response = await (via ? via(request) : fetch(request));
  const provider = objectResult(await response.json());
  const afterMs = Date.now();
  if (!response.ok) throw new Error(`Clock probe failed: ${response.status}`);
  return { beforeMs, afterMs, provider, viaObject: !!via };
}

const yieldedTurn = (url: URL, env: Bindings) => Effect.gen(function* () {
  const query = readQuery(url);
  const client = yield* CloudflareThreadClient;
  const digests = yield* digestDefinitions(definitions);
  const input = turn(query.sample, MEASURED_TOOLS);
  const threadId = ThreadId.make(query.object);
  let after = CanonicalSequence.make(Number(url.searchParams.get("afterSequence")));
  const observations: TextObservation[] = [];
  const submitStartedMs = Date.now();
  const receipt = yield* client.submit(agent, input.text, {
    threadId,
    principal: Principal.make("bench"),
    idempotencyKey: IdempotencyKey.make(input.id),
    definitions: digests,
  });
  const receiptReceivedMs = Date.now();

  const observe = Effect.gen(function* () {
    while (true) {
      const page = yield* client.readPage(threadId, { afterSequence: after, limit: 64 });
      const receivedMs = Date.now();
      for (const envelope of page) {
        after = envelope.sequence;
        const record = envelope.record.payload;
        if (record._tag === "ModelResponseRecorded") {
          const prompt = decodePrompt(record.messages);
          const text = prompt.content.flatMap(message => message.role === "assistant"
            ? message.content.flatMap(part => part.type === "text" ? [part.text] : []) : []).join("");
          if (text) observations.push({ call: observations.length, firstTextMs: receivedMs, completeMs: receivedMs, finalizedMs: receivedMs, text, source: "canonical" });
        }
        if (record._tag === "SubmissionSettled" && record.submissionId === receipt.submissionId) return;
      }
      if (page.length === 0) yield* client.awaitProgress(threadId, after);
    }
  });
  const results = yield* Effect.all({
    observation: observe,
    settlement: Effect.gen(function* () {
      const value = yield* client.awaitSettlement(receipt);
      return { outcome: value.outcome, settlementObservedMs: Date.now() };
    }),
  }, { concurrency: "unbounded" });
  if (results.settlement.outcome !== "completed") return yield* Effect.die(`Settlement: ${results.settlement.outcome}`);
  return {
    submitStartedMs,
    receiptReceivedMs,
    ...results.settlement,
    turnMs: results.settlement.settlementObservedMs - submitStartedMs,
    firstVisibleMs: observations[0]!.firstTextMs - submitStartedMs,
    observations,
    afterSequence: after,
    receipt: Schema.encodeSync(Receipt)(receipt),
  };
}).pipe(Effect.scoped, Effect.timeout("90 seconds"), Effect.provide(clientLayer(env)));

// Decode only the native observation fields a text UI consumes.
const PiBlock = Schema.Struct({ type: Schema.String, text: Schema.optionalKey(Schema.String) });
const PiMessage = Schema.Struct({ role: Schema.String, content: Schema.Union([Schema.String, Schema.Array(PiBlock)]) });
const PiChange = Schema.Struct({ type: Schema.String, contentIndex: Schema.optionalKey(Schema.Number),
  delta: Schema.optionalKey(Schema.String), block: Schema.optionalKey(PiBlock), message: Schema.optionalKey(PiMessage) });
const PiEvent = Schema.Struct({
  type: Schema.String, message: Schema.optionalKey(PiMessage), changes: Schema.optionalKey(Schema.Array(PiChange)),
  entry: Schema.optionalKey(Schema.Struct({ model: Schema.optionalKey(Schema.Array(PiMessage)) })),
  generation: Schema.optionalKey(Schema.Struct({ message: Schema.optionalKey(PiMessage) })),
});
const decodePiFrame = Schema.decodeUnknownSync(Schema.Union([
  Schema.Array(PiEvent), PiEvent,
  Schema.Struct({kind:Schema.Literal("settled"),ok:Schema.Boolean,status:Schema.Number,error:Schema.optionalKey(Schema.String)}),
]));

async function piTurn(request: Request, stub: DurableObjectStub<NetworkPiDO>) {
  const observations: TextObservation[] = [];
  const submitStartedMs = Date.now();
  const response = await stub.fetch(request);
  if (!response.ok || !response.body) throw new Error("Pi stream failed: " + response.status);
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let pending = "";
  let settlementObservedMs: number | undefined;
  let current: TextObservation | undefined;
  let blocks = new Map<number, string>();
  const textOf = (message: typeof PiMessage.Type) => typeof message.content === "string" ? message.content
    : message.content.flatMap(block => block.type === "text" ? [block.text ?? ""] : []).join("");
  const setText = (text: string, at: number) => {
    if (!text) return;
    if (!current) {
      current = {call:observations.length,firstTextMs:at,text:"",source:"watchEvents"};
      observations.push(current);
    }
    if(current.text !== text) current.completeMs=at;
    current.text = text;
  };
  const resetMessage = (message: typeof PiMessage.Type, at: number) => {
    blocks = new Map(typeof message.content === "string" ? [[0,message.content]]
      : message.content.flatMap((block,index) => block.type === "text" ? [[index,block.text ?? ""] as const] : []));
    setText(textOf(message),at);
  };
  try {
    while (true) {
      const item = await reader.read();
      const receivedMs = Date.now();
      if (item.done) break;
      pending += decoder.decode(item.value, { stream: true });
      const lines = pending.split("\n");
      pending = lines.pop()!;
      for (const line of lines) {
        if (!line) continue;
        const frame = decodePiFrame(JSON.parse(line));
        if (!Array.isArray(frame) && "kind" in frame) {
          if (!frame.ok || frame.status !== 200) throw new Error("Pi turn failed: " + JSON.stringify(frame));
          settlementObservedMs = receivedMs;
          continue;
        }
        const events = Array.isArray(frame) ? frame : [frame];
        for (const event of events) {
          if (event.type === "snapshot" && event.generation?.message) resetMessage(event.generation.message,receivedMs);
          if (event.type === "message_start" && event.message?.role === "assistant") {
            current=undefined; resetMessage(event.message,receivedMs);
          }
          if (event.type === "message_update") for(const change of event.changes ?? []) {
            if(change.type === "message" && change.message) resetMessage(change.message,receivedMs);
            else if(change.contentIndex !== undefined) {
              if(change.type === "text_delta") blocks.set(change.contentIndex,(blocks.get(change.contentIndex) ?? "")+(change.delta ?? ""));
              else if(change.block?.type === "text") blocks.set(change.contentIndex,change.block.text ?? "");
              setText([...blocks].sort(([a],[b])=>a-b).map(([,text])=>text).join(""),receivedMs);
            }
          }
          if(event.type === "message_end") {
            const assistant=event.entry?.model?.find(message=>message.role === "assistant");
            if(assistant) {
              setText(textOf(assistant),receivedMs);
              if(current) current.finalizedMs=receivedMs;
              current=undefined; blocks.clear();
            }
          }
        }
      }
    }
  } finally { await reader.cancel(); reader.releaseLock(); }
  if (settlementObservedMs === undefined || pending) throw new Error("Pi stream ended without settlement");
  return { submitStartedMs, settlementObservedMs, turnMs: settlementObservedMs - submitStartedMs,
    firstVisibleMs: observations[0]!.firstTextMs - submitStartedMs, observations, outcome: "completed" };
}

export default {
  async fetch(request: Request, env: Bindings): Promise<Response> {
    if (!env.TOKEN || request.headers.get("authorization") !== `Bearer ${env.TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);
    try {
      if (url.pathname === "/driver") return Response.json({ ok: true, version: env.VERSION.id, generation: env.PHASE });
      const query = readQuery(url);
      if (!query.object.startsWith("first-text-")) throw new Error("Object prefix mismatch");
      const stub = query.target === "pi"
        ? env.PI.getByName(query.object, { locationHint: "wnam" })
        : env.YIELDED.getByName(query.object, { locationHint: "wnam" });
      if (url.pathname === "/cursor") {
        const afterSequence = await Effect.runPromise(Effect.gen(function* () {
          const client = yield* CloudflareThreadClient;
          const all = yield* client.readAll(ThreadId.make(query.object));
          return all.at(-1)?.sequence ?? 0;
        }).pipe(Effect.provide(clientLayer(env))));
        return Response.json({ ok: true, afterSequence });
      }
      if (url.pathname === "/cold") {
        const identityUrl = new URL(url); identityUrl.pathname = "/identity";
        const response = await stub.fetch(new Request(identityUrl, { headers: request.headers }));
        if (!response.ok) throw new Error(`Identity failed: ${response.status}`);
        const before = decodeIdentity(await response.json());
        let expectedAbort = false;
        try {
          const result = await stub.fetch(request);
          expectedAbort = result.status === 500 && (await result.text()).includes("first-text explicit cold");
        } catch (cause) { expectedAbort = errorText(cause).includes("first-text explicit cold"); }
        return Response.json({ ok: expectedAbort, before, expectedAbort }, { status: expectedAbort ? 200 : 502 });
      }
      if (url.pathname !== "/run") return stub.fetch(request);
      if (env.PHASE !== "measure") throw new Error("Measurement generation required");
      const clockBefore = await probe(env, `${query.sample}-before`);
      const timing = query.target === "yielded"
        ? await Effect.runPromise(yieldedTurn(url, env))
        : await piTurn(request, env.PI.getByName(query.object, { locationHint: "wnam" }));
      const metricsUrl = new URL(url); metricsUrl.pathname = "/metrics";
      const response = await stub.fetch(new Request(metricsUrl, { headers: request.headers }));
      const metrics = objectResult(await response.json());
      if (!response.ok || metrics.ok !== true) throw new Error(`Metrics failed: ${JSON.stringify(metrics)}`);
      const clocks = [clockBefore, await probe(env, `${query.sample}-after`),
        await probe(env, `${query.sample}-routed`, request => stub.fetch(request)),
        await probe(env, `${query.sample}-routed-repeat`, request => stub.fetch(request))];
      return Response.json({ ok: true, ...query, ...timing, metrics, clocks, driverVersion: env.VERSION.id });
    } catch (cause) {
      const error = errorText(cause);
      console.error({ firstText: "driver-failure", sample: url.searchParams.get("sample"), error });
      return Response.json({ ok: false, error }, { status: 500 });
    }
  },
};
