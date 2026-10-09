import { AsyncLocalStorage } from "node:async_hooks";
import type { Message } from "../../../src/plan.ts";
import { decodeProviderReceipt, errorText, type Env, type ProviderReceipt, type Query } from "./protocol.ts";

export const invocation = new AsyncLocalStorage<{ kind: string; id: string }>();
interface Call {
  call: number;
  startMs: number;
  fetchStartedMs?: number;
  headersMs?: number;
  firstByteMs?: number;
  endMs?: number;
  status?: number;
  responseBytes: number;
  providerRequest?: string | null;
  providerReceipt?: ProviderReceipt;
  sseDone?: boolean;
  error?: string;
  invocation?: { kind: string; id: string };
}

/** Receipts only: no SQL proxies, tracing, clock probes, or extra I/O in execution. */
export class Observation {
  constructor(readonly env: Env) {}
  calls: Call[] = [];
  measurement?: Record<string, unknown>;
  seen: readonly Message[] = [];
  query?: Query;
  active = false;
  readonly incarnation = crypto.randomUUID();
  readonly constructedMs = Date.now();
  private firstRequest = true;
  private alarmStarts = 0;
  private activeAlarms = new Set<string>();
  entry() {
    const result = { firstHarnessRequest: this.firstRequest, priorAlarmStarts: this.alarmStarts };
    this.firstRequest = false;
    return result;
  }
  reset(query: Query) { this.query = query; this.calls = []; }
  identity(state: DurableObjectState, env: Env) {
    return { objectId: state.id.toString(), incarnation: this.incarnation, constructedMs: this.constructedMs, version: env.VERSION.id, generation: env.PHASE };
  }
  begin(query: Query, state: DurableObjectState) {
    this.reset(query);
    this.active = true;
    this.measurement = { ...query, ...this.identity(state, this.env), entry: this.entry() };
  }
  alarmEvent(id: string, edge: "start" | "end", _state: DurableObjectState) {
    if(edge === "start") { this.alarmStarts++; this.activeAlarms.add(id); }
    else this.activeAlarms.delete(id);
  }
  fetch =
    (env: Env): typeof globalThis.fetch =>
    async (input, init) => {
      if (!this.active || !this.query || env.PHASE !== "measure")
        throw new Error("Provider dispatch outside a measured request");
      const query = this.query;
      const original = new Request(input, init);
      const url = new URL(original.url);
      const base = new URL(env.PROVIDER_URL);

      if (
        base.protocol !== "https:" ||
        url.origin !== base.origin ||
        url.pathname !== `${base.pathname.replace(/\/$/, "")}/chat/completions`
      )
        throw new Error(`Unexpected provider destination ${url.origin}${url.pathname}`);
      for (const [key, value] of Object.entries(query)) url.searchParams.set(key, String(value));

      const call: Call = {
        call: this.calls.length,
        invocation: invocation.getStore(),
        startMs: Date.now(),
        responseBytes: 0,
      };
      this.calls.push(call);
      url.searchParams.set("call", String(call.call));
      const headers = new Headers(original.headers);

      headers.set("x-first-text-sample", query.sample);
      headers.set("authorization", `Bearer ${env.TOKEN}`);
      const request = new Request(url, new Request(original, { headers }));

      try {
        call.fetchStartedMs = Date.now();
        const response = await globalThis.fetch(request);

        call.headersMs = Date.now();
        call.status = response.status;
        call.providerRequest = response.headers.get("x-first-text-request");
        if (!response.body) throw new Error("Provider response has no body");
        const reader = response.body.getReader();
        let sseDone = false;
        let finished = false;
        let cancelled = false;
        let pendingLine = "";
        const decoder = new TextDecoder();

        const finish = (cause?: unknown) => {
          if (finished) return;
          finished = true;
          call.endMs = Date.now();
          call.sseDone = sseDone;
          if (cause !== undefined) call.error = errorText(cause);
        };

        const stream = new ReadableStream<Uint8Array>({
          async pull(controller) {
            try {
              const item = await reader.read();

              if (cancelled) return;

              if (item.done) {
                finish(sseDone ? undefined : "EOF before [DONE]");
                reader.releaseLock();
                controller.close();

                return;
              }
              call.firstByteMs ??= Date.now();
              call.responseBytes += item.value.byteLength;
              pendingLine += decoder.decode(item.value, { stream: true });
              const lines = pendingLine.split("\n");

              pendingLine = lines.pop() ?? "";
              for (const line of lines) {
                if (line === "data: [DONE]") sseDone = true;
                if (line.startsWith(": first-text-receipt ")) {
                  if (call.providerReceipt) throw new Error("Duplicate provider stream receipt");
                  call.providerReceipt = decodeProviderReceipt(
                    JSON.parse(line.slice(": first-text-receipt ".length)),
                  );
                  if (
                    call.providerReceipt.requestId !== call.providerRequest ||
                    call.providerReceipt.call !== call.call
                  )
                    throw new Error("Provider receipt identity mismatch");
                }
              }
              controller.enqueue(item.value);
            } catch (cause) {
              if (cancelled) return;
              // Effect disposes its HTTP scope after [DONE], which can reject
              // this wrapper's already pending read. The SSE completed first.
              finish(sseDone ? undefined : cause);
              reader.releaseLock();
              if (sseDone) controller.close();
              else controller.error(cause);
            }
          },
          async cancel(reason) {
            cancelled = true;
            finish(sseDone ? undefined : (reason ?? "consumer cancelled before [DONE]"));
            try {
              await reader.cancel(reason);
            } finally {
              reader.releaseLock();
            }
          },
        });

        return new Response(stream, {
          status: response.status,
          statusText: response.statusText,
          headers: response.headers,
        });
      } catch (cause) {
        call.endMs = Date.now();
        call.error = errorText(cause);
        throw cause;
      }
    };
  finish() { return { calls: this.calls, activeAlarmCount: this.activeAlarms.size }; }
}
const observations = new WeakMap<DurableObjectStorage, Observation>();
export const observation = (storage: DurableObjectStorage): Observation => {
  const meter = observations.get(storage);
  if (!meter) throw new Error("Missing benchmark observation");
  return meter;
};
export const instrument = (state: DurableObjectState, env: Env): DurableObjectState => {
  observations.set(state.storage, new Observation(env));
  return state;
};
