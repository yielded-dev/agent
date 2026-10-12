import { Schema } from "effect";

import {
  errorText,
  ProviderReceipt,
  type Env,
  type Identity,
  type ProviderCall,
  type Query,
} from "./protocol.ts";

const Token = Schema.Struct({
  choices: Schema.Array(
    Schema.Struct({
      delta: Schema.Struct({
        content: Schema.optionalKey(Schema.NullOr(Schema.String)),
        tool_calls: Schema.optionalKey(
          Schema.Array(
            Schema.Struct({
              function: Schema.optionalKey(
                Schema.Struct({
                  name: Schema.optionalKey(Schema.String),
                  arguments: Schema.optionalKey(Schema.String),
                }),
              ),
            }),
          ),
        ),
      }),
    }),
  ),
});

const meters = new WeakMap<DurableObjectStorage, Observation>();
// Lazily draw randomness in a request context, then retain one token per Worker isolate.
let isolate: string | undefined;

export const observation = (storage: DurableObjectStorage): Observation => {
  const meter = meters.get(storage);

  if (!meter) throw new Error("Missing benchmark observation");

  return meter;
};

export const attach = (
  state: DurableObjectState,
  env: Env,
  constructedMs = Date.now(),
): Observation => {
  const meter = new Observation(env, constructedMs);

  meters.set(state.storage, meter);

  return meter;
};

export class Observation {
  readonly incarnation = crypto.randomUUID();
  private entries = 0;
  private alarms = 0;
  private entryMs = 0;
  private admittedInput?: string;
  private readonly forwarded = new Map<string, number>();
  private settlementMs?: number;
  private validations: Array<() => void> = [];
  /** Called after the driver timer stops, or after an untimed history turn settles. */
  validateCalls() {
    for (const validate of this.validations.splice(0)) validate();
  }
  admitted(input: string) {
    this.admittedInput = input;
  }
  forwardText(input: string) {
    if (!this.forwarded.has(input)) this.forwarded.set(input, Date.now());
  }
  settled() {
    this.settlementMs ??= Date.now();
  }
  phases() {
    const firstTextForwardedMs =
      this.admittedInput === undefined ? undefined : this.forwarded.get(this.admittedInput);

    return {
      entryMs: this.entryMs,
      ...(firstTextForwardedMs === undefined ? {} : { firstTextForwardedMs }),
      ...(this.settlementMs === undefined ? {} : { settlementMs: this.settlementMs }),
    };
  }
  query?: Query;
  entry?: Identity;
  calls: Array<{ -readonly [K in keyof ProviderCall]: ProviderCall[K] }> = [];
  constructor(
    readonly env: Env,
    readonly constructedMs = Date.now(),
  ) {}
  identity(): Identity {
    return {
      build: this.env.BUILD,
      isolate: (isolate ??= crypto.randomUUID()),
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
    for (const sample of this.cpuInvocations?.keys() ?? []) {
      if (sample !== query.sample) this.cpuInvocations?.delete(sample);
    }
    this.assertBuild(query);
    this.entryMs = Date.now();
    this.entry = this.identity();
    this.entries++;
    this.query = query;
    this.calls = [];
    this.validations = [];
    this.admittedInput = query.sample;
    this.forwarded.clear();
    this.settlementMs = undefined;
  }
  alarm() {
    this.alarms++;
  }
  cpuInvocations: Map<string, string[]> | undefined;
  get cpuEnabled() {
    return this.env.CPU === true || this.env.CPU === "true";
  }
  marker(kind: string, query = this.query, dispatchSource?: string, completes?: string) {
    if (!this.cpuEnabled) return;
    if (!query) return;
    const invocation = crypto.randomUUID();

    this.cpuInvocations ??= new Map();
    this.cpuInvocations.set(query.sample, [
      ...(this.cpuInvocations.get(query.sample) ?? []),
      invocation,
    ]);
    console.log({
      target: query.target,
      object: query.object,
      sample: query.sample,
      kind,
      invocation,
      ...(dispatchSource === undefined ? {} : { dispatchSource }),
      ...(completes === undefined ? {} : { completes }),
    });

    return invocation;
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
    const startMs = Date.now();

    const call: (typeof this.calls)[number] = {
      call: this.calls.length,
      fingerprint: "",
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
    try {
      const response = await globalThis.fetch(new Request(url, new Request(original, { headers })));

      call.status = response.status;
      if (!response.body) throw new Error("Provider response has no body");
      const requestId = response.headers.get("x-rebench-request");
      const reader = response.body.getReader();
      const chunks: Array<{ bytes: Uint8Array; receivedMs: number }> = [];
      let responseBytes = 0;
      let cancelled = false;
      let terminalError: unknown;

      const finish = (cause?: unknown) => {
        call.endMs ??= Date.now();
        terminalError ??= cause;
      };

      // Keep only small response chunks on the measured path. Decode and verify after settlement.
      this.validations.push(() => {
        const decoder = new TextDecoder();
        let pending = "";
        let done = false;

        for (const chunk of chunks) {
          pending += decoder.decode(chunk.bytes, { stream: true });
          const lines = pending.split("\n");

          pending = lines.pop() ?? "";
          for (const raw of lines) {
            const line = raw.trimEnd();

            if (
              call.firstTokenMs === undefined &&
              line.startsWith("data: ") &&
              line !== "data: [DONE]"
            ) {
              const token = Schema.decodeUnknownSync(Token)(JSON.parse(line.slice(6)));

              if (
                token.choices.some(
                  ({ delta }) =>
                    delta.content ||
                    delta.tool_calls?.some(
                      (tool) => tool.function?.name || tool.function?.arguments,
                    ),
                )
              )
                call.firstTokenMs = chunk.receivedMs;
            }
            if (line === "data: [DONE]") done = true;
            if (line.startsWith(": rebench-receipt ")) {
              if (call.receipt) throw new Error("Duplicate provider receipt");
              call.receipt = Schema.decodeUnknownSync(ProviderReceipt)(
                JSON.parse(line.slice(": rebench-receipt ".length)),
              );
              if (
                call.receipt.call !== call.call ||
                call.receipt.requestId !== requestId ||
                call.receipt.sample !== query.sample ||
                call.receipt.object !== query.object ||
                call.receipt.target !== query.target ||
                call.receipt.expectedBuild !== query.expectedBuild ||
                call.receipt.objectBuild !== objectBuild
              )
                throw new Error("Provider receipt mismatch");
              call.fingerprint = call.receipt.fingerprint;
            }
          }
        }
        chunks.length = 0;
        if (!done) call.error = errorText(terminalError ?? "EOF before [DONE]");
        if (!call.receipt) call.error ??= "Missing provider receipt";
      });

      return new Response(
        new ReadableStream<Uint8Array>({
          async pull(controller) {
            try {
              const item = await reader.read();

              if (cancelled) return;
              if (item.done) {
                finish();
                reader.releaseLock();
                controller.close();

                return;
              }
              responseBytes += item.value.byteLength;
              if (responseBytes > 4 * 1024 * 1024)
                throw new Error("Benchmark provider response exceeded 4 MiB");
              chunks.push({ bytes: item.value, receivedMs: Date.now() });
              controller.enqueue(item.value);
            } catch (cause) {
              if (cancelled) return;
              finish(cause);
              reader.releaseLock();
              controller.error(cause);
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
