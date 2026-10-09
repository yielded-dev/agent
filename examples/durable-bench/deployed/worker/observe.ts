import { Schema } from "effect";

import { fingerprint } from "../../src/plan.ts";
import {
  chatTranscript,
  decodeChat,
  errorText,
  ProviderReceipt,
  separateTextAndTools,
  type Env,
  type Identity,
  type ProviderCall,
  type Query,
} from "./protocol.ts";

const meters = new WeakMap<DurableObjectStorage, Observation>();

export const observation = (storage: DurableObjectStorage): Observation => {
  const meter = meters.get(storage);

  if (!meter) throw new Error("Missing benchmark observation");

  return meter;
};

export const attach = (state: DurableObjectState, env: Env): Observation => {
  const meter = new Observation(env);

  meters.set(state.storage, meter);

  return meter;
};

export class Observation {
  readonly incarnation = crypto.randomUUID();
  readonly constructedMs = Date.now();
  private entries = 0;
  private alarms = 0;
  private entryMs = 0;
  query?: Query;
  entry?: Identity;
  calls: Array<{ -readonly [K in keyof ProviderCall]: ProviderCall[K] }> = [];
  constructor(readonly env: Env) {}
  identity(): Identity {
    return {
      build: this.env.BUILD,
      incarnation: this.incarnation,
      constructedMs: this.constructedMs,
      firstEntry: this.entries === 0,
      priorAlarms: this.alarms,
    };
  }
  assertBuild(query: Query) {
    if (query.expectedBuild !== this.env.BUILD)
      throw new Error("Benchmark Object build mismatch; input was not admitted");
  }
  begin(query: Query) {
    this.assertBuild(query);
    this.entryMs = Date.now();
    this.entry = this.identity();
    this.entries++;
    this.query = query;
    this.calls = [];
  }
  alarm() {
    this.alarms++;
  }
  marker(kind: string) {
    if (this.env.CPU !== true && this.env.CPU !== "true") return;
    console.log({
      target: this.query?.target,
      object: this.query?.object,
      sample: this.query?.sample,
      kind,
    });
  }
  readonly fetch: typeof globalThis.fetch = async (input, init) => {
    const query = this.query;

    if (!query) throw new Error("Provider request without a benchmark sample");
    this.assertBuild(query);
    const objectBuild = this.env.BUILD;
    const original = new Request(input, init);
    const url = new URL(original.url);
    const base = new URL(this.env.PROVIDER_URL);

    if (
      url.origin !== base.origin ||
      url.pathname !== `${base.pathname.replace(/\/$/, "")}/chat/completions`
    )
      throw new Error("Unexpected provider destination");
    const decoded = decodeChat(await original.clone().json());
    const chat = query.textStreaming ? separateTextAndTools(decoded) : decoded;
    const transcript = chatTranscript(chat);

    const digest = await fingerprint(transcript);
    const startMs = Date.now();

    const call: (typeof this.calls)[number] = {
      call: this.calls.length,
      fingerprint: digest,
      startMs,
      // The Object and driver clocks cannot be subtracted to obtain submit latency.
      sinceEntryMs: startMs - this.entryMs,
    };

    this.calls.push(call);
    for (const [key, value] of Object.entries(query)) url.searchParams.set(key, String(value));
    url.searchParams.set("objectBuild", objectBuild);
    url.searchParams.set("call", String(call.call));
    const headers = new Headers(original.headers);

    headers.set("authorization", `Bearer ${this.env.BENCH_TOKEN}`);
    if (query.textStreaming) headers.delete("content-length");
    try {
      const response = await globalThis.fetch(
        new Request(
          url,
          new Request(original, {
            headers,
            ...(query.textStreaming ? { body: JSON.stringify(chat) } : {}),
          }),
        ),
      );

      call.status = response.status;
      if (!response.body) throw new Error("Provider response has no body");
      const requestId = response.headers.get("x-rebench-request");
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let pending = "";
      let done = false;
      let cancelled = false;

      const finish = (cause?: unknown) => {
        call.endMs ??= Date.now();
        if (cause !== undefined && !done) call.error = errorText(cause);
      };

      return new Response(
        new ReadableStream<Uint8Array>({
          async pull(controller) {
            try {
              const item = await reader.read();

              if (cancelled) return;
              if (item.done) {
                finish(done ? undefined : "EOF before [DONE]");
                reader.releaseLock();
                controller.close();

                return;
              }
              pending += decoder.decode(item.value, { stream: true });
              const lines = pending.split("\n");

              pending = lines.pop() ?? "";
              for (const raw of lines) {
                const line = raw.trimEnd();

                if (line === "data: [DONE]") done = true;
                if (line.startsWith(": rebench-receipt ")) {
                  if (call.receipt) throw new Error("Duplicate provider receipt");
                  call.receipt = Schema.decodeUnknownSync(ProviderReceipt)(
                    JSON.parse(line.slice(": rebench-receipt ".length)),
                  );
                  if (
                    call.receipt.call !== call.call ||
                    call.receipt.requestId !== requestId ||
                    call.receipt.fingerprint !== call.fingerprint ||
                    call.receipt.sample !== query.sample ||
                    call.receipt.object !== query.object ||
                    call.receipt.expectedBuild !== query.expectedBuild ||
                    call.receipt.objectBuild !== objectBuild
                  )
                    throw new Error("Provider receipt mismatch");
                }
              }
              controller.enqueue(item.value);
            } catch (cause) {
              if (cancelled) return;
              finish(cause);
              reader.releaseLock();
              if (done) controller.close();
              else controller.error(cause);
            }
          },
          async cancel(reason) {
            cancelled = true;
            finish(reason ?? "Provider stream cancelled");
            try {
              await reader.cancel(reason);
            } finally {
              reader.releaseLock();
            }
          },
        }),
        { status: response.status, statusText: response.statusText, headers: response.headers },
      );
    } catch (cause) {
      call.endMs = Date.now();
      call.error = errorText(cause);
      throw cause;
    }
  };
}
