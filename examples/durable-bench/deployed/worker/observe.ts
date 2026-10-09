import { fingerprint } from "../../src/plan.ts";
import { storageProbe } from "../isolate/cold-storage.ts";
import { chatTranscript, errorText, parseReceipt } from "../isolate/native-protocol.ts";
import { observeConstructor, isolateObservation } from "../isolate/observation.ts";
import type { Env, Identity, ProviderCall, IsolateState, Query } from "../isolate/protocol.ts";
import { coldBisectAfterInit, timeline } from "../isolate/timeline.ts";

const meters = new WeakMap<DurableObjectStorage, Observation>();

export const observation = (storage: DurableObjectStorage): Observation => {
  const meter = meters.get(storage);

  if (!meter) throw new Error("Missing benchmark observation");

  return meter;
};

export const attach = (state: DurableObjectState, env: Env): Observation => {
  const meter = new Observation(env, state.storage);

  meters.set(state.storage, meter);

  return meter;
};

export class Observation {
  readonly incarnation = crypto.randomUUID();
  readonly constructedMs = Date.now();
  private entries = 0;
  private alarms = 0;
  query?: Query;
  entry?: Identity;
  calls: Array<{ -readonly [K in keyof ProviderCall]: ProviderCall[K] }> = [];
  constructor(
    readonly env: Env,
    readonly storage: DurableObjectStorage,
  ) {
    observeConstructor(env);
  }
  identity(): Identity {
    return {
      incarnation: this.incarnation,
      constructedMs: this.constructedMs,
      firstEntry: this.entries === 0,
      priorAlarms: this.alarms,
      isolate: isolateObservation(this.env),
    };
  }
  begin(query: Query, workerIsolate?: IsolateState) {
    // Isolated routes carry the expected routing build; reject before admission.
    if (workerIsolate !== undefined && workerIsolate.build !== this.env.BUILD)
      throw new Error("Object BUILD does not match the expected input build");
    timeline(this.storage)?.begin();
    storageProbe(this.storage)?.begin();
    this.entry = {
      ...this.identity(),
      ...(workerIsolate === undefined ? {} : { workerIsolate }),
    };
    this.entries++;
    this.query = query;
    this.calls = [];
  }
  alarm() {
    this.alarms++;
  }
  marker(kind: string, query = this.query) {
    if (this.env.CPU !== true && this.env.CPU !== "true") return;
    console.log({
      target: query?.target,
      object: query?.object,
      sample: query?.sample,
      kind,
    });
  }
  readonly fetch: typeof globalThis.fetch = async (input, init) => {
    const query = this.query;
    const objectBuild = this.env.BUILD;

    if (!query) throw new Error("Provider request without a benchmark sample");
    if (this.calls.length === 0) {
      storageProbe(this.storage)?.point("first-provider");
      timeline(this.storage)?.point("provider.prepare");
    }
    const original = new Request(input, init);
    const url = new URL(original.url);
    const base = new URL(this.env.PROVIDER_URL);

    if (
      url.origin !== base.origin ||
      url.pathname !== `${base.pathname.replace(/\/$/, "")}/chat/completions`
    )
      throw new Error("Unexpected provider destination");
    const transcript = chatTranscript(await original.clone().json());

    const call: (typeof this.calls)[number] = {
      call: this.calls.length,
      fingerprint: await fingerprint(transcript),
      startMs: Date.now(),
    };

    this.calls.push(call);
    for (const [key, value] of Object.entries(query)) url.searchParams.set(key, String(value));
    url.searchParams.set("call", String(call.call));
    const headers = new Headers(original.headers);

    headers.set("authorization", `Bearer ${this.env.BENCH_TOKEN}`);
    headers.set("x-cold-bisect-object-build", objectBuild);
    try {
      if (call.call === 0 && query.sample.startsWith("profile-"))
        timeline(this.storage)?.point("profile.after:" + coldBisectAfterInit(7));
      if (call.call === 0) timeline(this.storage)?.point("provider.dispatch");
      const response = await globalThis.fetch(new Request(url, new Request(original, { headers })));

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
                  call.receipt = parseReceipt(JSON.parse(line.slice(": rebench-receipt ".length)));
                  if (
                    call.receipt.objectBuild !== objectBuild ||
                    call.receipt.call !== call.call ||
                    call.receipt.requestId !== requestId ||
                    call.receipt.fingerprint !== call.fingerprint ||
                    call.receipt.sample !== query.sample ||
                    call.receipt.object !== query.object
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
