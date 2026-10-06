import { type ThreadId } from "@yielded/agent/identifiers";
import type { ThreadStore } from "@yielded/agent/thread-store";
import { ThreadWorkDiscovery } from "@yielded/agent/thread-work";
import { makeWakeSubscriptionHub, WakeScheduler } from "@yielded/agent/wake-scheduler";
import type { Duration } from "effect";
import { Clock, Context, Effect, Layer, Option, PubSub, Stream } from "effect";

/**
 * Bounded in-process wake buffer. Wake hints are droppable by contract (native work inventory
 * keeps liveness), so a full buffer slides out the oldest hint instead of growing without bound.
 */
const WAKE_BUFFER_CAPACITY = 1_024;

/** Cadence authority for the Node wake scheduler's native inventory fallback. */
export class NodeWakeSchedulerConfig extends Context.Service<
  NodeWakeSchedulerConfig,
  {
    /** Interval between scans of Threads with outstanding work, including terminal obligations. */
    readonly scanInterval: Duration.Duration;
  }
>()("@effect-agent/platform-node/NodeWakeSchedulerConfig") {
  static layer(options: {
    readonly scanInterval: Duration.Duration;
  }): Layer.Layer<NodeWakeSchedulerConfig> {
    return Layer.succeed(NodeWakeSchedulerConfig)({ scanInterval: options.scanInterval });
  }
}

const makeWakeScheduler = Effect.gen(function* () {
  const work = yield* ThreadWorkDiscovery.pipe(Effect.provide(ThreadWorkDiscovery.layer));
  const config = yield* NodeWakeSchedulerConfig;
  const clock = yield* Clock.Clock;
  const hints = yield* PubSub.sliding<ThreadId>(WAKE_BUFFER_CAPACITY);
  const progress = yield* makeWakeSubscriptionHub;
  const settlements = yield* makeWakeSubscriptionHub;

  yield* Effect.addFinalizer(() => PubSub.shutdown(hints));

  const scanOnce = Stream.paginate(undefined, (afterThreadId: ThreadId | undefined) =>
    work
      .threads({ limit: 32, ...(afterThreadId === undefined ? {} : { afterThreadId }) })
      .pipe(
        Effect.map(
          (page): readonly [ReadonlyArray<ThreadId>, Option.Option<ThreadId | undefined>] => [
            page.threadIds,
            page.afterThreadId === undefined ? Option.none() : Option.some(page.afterThreadId),
          ],
        ),
      ),
  ).pipe(
    Stream.catch((error) =>
      Stream.fromEffect(
        Effect.logWarning("NodeWakeScheduler native work inventory failed", error),
      ).pipe(Stream.drain),
    ),
  );

  // Each subscriber owns its cursor. A slow subscriber retains one page and resumes it;
  // faster subscribers keep scanning without an unbounded shared snapshot or dropped pages.
  const fallbackScans = Stream.fromEffectRepeat(clock.sleep(config.scanInterval)).pipe(
    Stream.flatMap(() => scanOnce),
  );

  return WakeScheduler.of({
    notify: (threadId, kind) =>
      (kind === "progress"
        ? progress.notify(threadId)
        : settlements.notify(threadId).pipe(Effect.andThen(progress.notify(threadId)))
      ).pipe(Effect.andThen(PubSub.publish(hints, threadId)), Effect.asVoid),
    subscribe: (threadId, kind) =>
      (kind === "settlement" ? settlements : progress).subscribe(threadId),
    wakes: Stream.merge(Stream.fromPubSub(hints), fallbackScans),
  });
});

/**
 * In-process Node `WakeScheduler`: `notify` publishes to a bounded sliding PubSub for prompt
 * same-process worker wakeups. Thread waiters use separate progress and settlement registrations;
 * broad hints wake both. Each active `wakes` subscription pages the native work inventory
 * periodically so a dropped, coalesced, or never-sent notification cannot strand accepted work
 * or a terminal side obligation. Delivery may duplicate; consumers already
 * treat wakes as pure liveness hints.
 */
export const nodeWakeSchedulerLayer: Layer.Layer<
  WakeScheduler,
  never,
  ThreadStore | NodeWakeSchedulerConfig
> = Layer.effect(WakeScheduler)(makeWakeScheduler);
