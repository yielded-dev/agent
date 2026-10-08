import { DurableObjectContext } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import { WakeScheduler } from "@yielded/agent/wake-scheduler";
import type { ThreadId } from "@yielded/agent/identifiers";
import { Effect, Layer, Option } from "effect";
import { observation } from "./observe.ts";

// Injected only by this experiment's esbuild plugin. No product flag or global Object state.
export const mechanismMeter = Effect.map(
  Effect.serviceOption(DurableObjectContext),
  (context) => Option.isSome(context) ? observation(context.value.ctx.storage) : undefined,
);

type Processing = <A, E, R>(threadId: ThreadId, body: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;

export const observeWakeLayer = <E, R>(layer: Layer.Layer<WakeScheduler, E, R>) =>
  Layer.effect(WakeScheduler)(Effect.gen(function* () {
    const original: WakeScheduler["Service"] & { readonly withProcessing?: Processing } = yield* WakeScheduler;
    const meter = yield* mechanismMeter;
    const processing = original.withProcessing;
    const withProcessing: Processing = (threadId, body) => Effect.suspend(() => {
      const variant = meter?.query?.variant ?? "baseline";
      meter?.record("processing", { edge: "start", variant, threadId });
      const selected = variant === "baseline" ? body : processing === undefined
        ? Effect.die("Candidate requires WakeScheduler.withProcessing; rebuild after integration")
        : processing(threadId, body);
      return selected.pipe(Effect.onExit((exit) => Effect.sync(() =>
        meter?.record("processing", { edge: "end", variant, outcome: exit._tag }),
      )));
    });
    return {
      ...original,
      ...(processing === undefined ? {} : { withProcessing }),
      notify: (threadId, kind) => Effect.suspend(() => {
        meter?.record("notify", { threadId, kind: kind ?? "settlement" });
        return original.notify(threadId, kind);
      }),
    };
  })).pipe(Layer.provide(layer));
