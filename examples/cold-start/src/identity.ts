import { DurableObject } from "cloudflare:workers";

import { encode, type Env } from "./protocol.ts";

// Evaluated independently in the front Worker and the Object isolate.
const moduleId = Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
let constructions = 0;
let calls = 0;
let alarms = 0;

export const constructed = () => {
  constructions++;
};

export const alarmEntered = () => {
  alarms++;
};

export const identity = (version: string) => ({
  moduleId,
  calls: ++calls,
  constructions,
  alarms,
  version,
});

/** Enqueue the Object marker before starting its native public-API turn. */
export const probe = (version: string, run?: () => Promise<ReadableStream<Uint8Array>>) => {
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;

  return new ReadableStream<Uint8Array>({
    async start(controller) {
      controller.enqueue(encode("object", identity(version)));
      try {
        if (run) {
          reader = (await run()).getReader();
          for (;;) {
            const next = await reader.read();

            if (next.done) break;
            controller.enqueue(next.value);
          }
        }
      } catch (cause) {
        controller.enqueue(encode("error", String(cause)));
      } finally {
        reader?.releaseLock();
        controller.close();
      }
    },
    async cancel() {
      await reader?.cancel();
    },
  });
};

/** Retains the same exported bundle, but never constructs the framework host. */
export class EmptyObject extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    constructed();
  }
  probe() {
    return probe(this.env.VERSION.id);
  }
}
