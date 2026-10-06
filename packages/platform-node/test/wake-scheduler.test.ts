import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { layer as threadStoreLayer } from "@yielded/agent-storage-sqlite/sqlite-thread-store";
import type { SubmissionLedger } from "@yielded/agent/submission-ledger";
import { SubmissionSnapshot } from "@yielded/agent/submission-ledger";
import { ThreadStore, ThreadStoreError } from "@yielded/agent/thread-store";
import { WakeScheduler } from "@yielded/agent/wake-scheduler";
import {
  Deferred,
  Duration,
  Effect,
  Fiber,
  FileSystem,
  Layer,
  Queue,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";

import { NodeWakeSchedulerConfig, nodeWakeSchedulerLayer } from "../src/NodeWakeScheduler.ts";

const snapshot = (index: number) =>
  Schema.decodeSync(SubmissionSnapshot)({
    submissionId: `submission-${index}`,
    threadId: `thread-${index}`,
    queueSequence: index + 1,
    principal: "scheduler-test",
    idempotencyKey: `key-${index}`,
    agentId: "scheduler-agent",
    agentDigests: { agent: "a".repeat(64), model: "a".repeat(64), tools: "a".repeat(64) },
    deploymentId: "scheduler-deployment",
    inputPayload: null,
    inputDigest: "a".repeat(64),
    receiptId: `receipt-${index}`,
    state: "ready",
    createdAt: "2026-09-01T00:00:00.000Z",
  });

// Retain the SQLite port; control only paginated native owner discovery for lifecycle evidence.
const schedulerLayer = (scan: SubmissionLedger["Service"]["scanNonterminal"]) =>
  nodeWakeSchedulerLayer.pipe(
    Layer.provide(
      Layer.effect(
        ThreadStore,
        Effect.gen(function* () {
          const store = yield* ThreadStore;
          const work = store.work;

          if (work === undefined)
            return yield* ThreadStoreError.make({
              operation: "wake fixture",
              message: "Native work discovery is required",
            });
          const rows = (yield* Stream.runCollect(scan)).map((row) => row.threadId).sort();

          return ThreadStore.of({
            ...store,
            work: {
              ...work,
              threads: (request) => {
                const start =
                  request.afterThreadId === undefined
                    ? 0
                    : rows.findIndex((id) => id === request.afterThreadId) + 1;

                const threadIds = rows.slice(start, start + request.limit);

                return Effect.succeed({
                  threadIds,
                  ...(start + request.limit < rows.length
                    ? { afterThreadId: threadIds.at(-1) }
                    : {}),
                });
              },
            },
          });
        }),
      ).pipe(
        Layer.provide(
          Layer.unwrap(
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem;
              const directory = yield* fs.makeTempDirectoryScoped({ prefix: "wake-scheduler-" });

              return threadStoreLayer({ filename: `${directory}/ledger.sqlite` });
            }),
          ).pipe(Layer.provide(NodeFileSystem.layer)),
        ),
      ),
    ),
    Layer.provide(NodeWakeSchedulerConfig.layer({ scanInterval: Duration.seconds(1) })),
  );

const withScheduler = <A, E, R>(
  scan: SubmissionLedger["Service"]["scanNonterminal"],
  body: Effect.Effect<A, E, R | WakeScheduler>,
) => body.pipe(Effect.provide(schedulerLayer(scan)), Effect.scoped);

it.effect("retains every lane in a large scan without blocking faster subscribers", () => {
  const rows = Array.from({ length: 1_050 }, (_, index) => snapshot(index));

  const scan = Stream.suspend(() => {
    return Stream.fromIterable(rows);
  });

  return withScheduler(
    scan,
    Effect.gen(function* () {
      const wake = yield* WakeScheduler;
      const parked = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const slowFirstScan = yield* Deferred.make<void>();
      const fastScans = yield* Queue.bounded<void>(4);
      let first = true;
      let slowCount = 0;
      let fastCount = 0;

      const slow = yield* wake.wakes.pipe(
        Stream.tap(() =>
          Effect.gen(function* () {
            if (first) {
              first = false;
              yield* Deferred.succeed(parked, undefined);
              yield* Deferred.await(release);
            }
            slowCount++;
            if (slowCount === rows.length) yield* Deferred.succeed(slowFirstScan, undefined);
          }),
        ),
        Stream.take(rows.length * 2),
        Stream.runCollect,
        Effect.forkChild,
      );

      const fast = yield* wake.wakes.pipe(
        Stream.tap(() =>
          Effect.gen(function* () {
            fastCount++;
            if (fastCount % rows.length === 0) yield* Queue.offer(fastScans, undefined);
          }),
        ),
        Stream.take(rows.length * 4),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* TestClock.adjust("1 second");
      yield* Deferred.await(parked);
      yield* Queue.take(fastScans);
      for (let i = 0; i < 3; i++) {
        yield* TestClock.adjust("1 second");
        yield* Queue.take(fastScans);
      }
      const expected = rows.map((row) => row.threadId).sort();

      expect(yield* Fiber.join(fast)).toEqual(Array.from({ length: 4 }, () => expected).flat());
      yield* Deferred.succeed(release, undefined);
      yield* Deferred.await(slowFirstScan);
      yield* TestClock.adjust("1 second");
      expect(yield* Fiber.join(slow)).toEqual([...expected, ...expected]);
    }),
  );
});
