import { NodeFileSystem } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { NodeDurableHost } from "@yielded/agent-platform-node/node-durable-host";
import * as Agent from "@yielded/agent/agent";
import { DurableAgentRuntime } from "@yielded/agent/durable-agent-runtime";
import { Effect, Layer, Stream } from "effect";
import { LanguageModel, Model, type Prompt } from "effect/ai";

import {
  CHECKPOINT_HANDOFF,
  CHECKPOINT_INSTRUCTIONS,
  CRASH_QUESTION,
  checkpointContextLayer,
  checkpointDefinition,
  checkpointParts,
  decodeThreadId,
  makeCheckpointToolLayer,
  supplierCounts,
} from "./fixtures.ts";
import {
  CHILD_LEASE_MS,
  assertConvergence,
  expectKilled,
  lookupByKey,
  readLog,
  runWorkerToExit,
  waitAfterChildExit,
  withCrashSite,
  withHost,
  withRuntime,
} from "./harness.ts";

const crashPoints = [
  {
    name: "before canonical compaction",
    killAt: "compaction:before-canonical-append",
    present: false,
  },
  {
    name: "after canonical compaction",
    killAt: "compaction:after-canonical-append",
    present: true,
  },
] as const;

// Each child exits without finalizers after committing the native compaction. Reopening the
// same SQLite file must use exact canonical progress, input, and context across either boundary.
layer(NodeFileSystem.layer, { excludeTestServices: true })(
  "Run continuation process loss",
  (it) => {
    it.effect.each(crashPoints)(
      "recovers after $name",
      (point) =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "checkpoint-crash";
            const key = "checkpoint-crash";
            const threadId = decodeThreadId(thread);

            const child = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-checkpoint",
              thread,
              key,
              supplierDir: site.supplier,
              leaseMillis: CHILD_LEASE_MS,
              ...point,
            });

            expectKilled(child);
            expect(supplierCounts(site.supplier)).toEqual({
              "checkpoint-read:checkpoint-read-1": 1,
              "checkpoint-read:checkpoint-read-2": 1,
            });

            const before = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const records = yield* readLog(thread);
                const snapshot = yield* lookupByKey(thread, key);

                const compactions = records.filter(
                  ({ record }) => record.payload._tag === "CompactionCreated",
                );

                expect(compactions).toHaveLength(point.present ? 1 : 0);
                if (point.present)
                  expect(JSON.stringify(compactions)).toContain(CHECKPOINT_HANDOFF);

                const continuation = records.findLast(
                  ({ record }) => record.payload._tag === "RunContinuation",
                );

                expect(continuation?.record.payload).toMatchObject({
                  version: 1,
                  accounting: { committedTurns: 2, modelCalls: 2 },
                });
                expect(
                  records.filter(
                    ({ record }) =>
                      record.payload._tag === "RunStarted" && record.payload.context !== undefined,
                  ),
                ).toHaveLength(1);
                expect(
                  records.filter(({ record }) => record.payload._tag === "ModelResponseRecorded"),
                ).toHaveLength(2);
                expect(
                  records.filter(({ record }) => record.payload._tag === "ToolCallSettled"),
                ).toHaveLength(2);

                return { records, submissionId: snapshot.submissionId };
              }),
            );

            yield* waitAfterChildExit;
            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const runtime = yield* DurableAgentRuntime;

                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === before.submissionId,
                );

                expect(report?.decision._tag).toBe("ResumeFromTurnBoundary");
                expect(report?.disposition).toBe("deferred");
                const requests: Array<Prompt.Prompt> = [];

                const model = Model.make(
                  "scripted",
                  "crash-harness",
                  Layer.effect(
                    LanguageModel.LanguageModel,
                    LanguageModel.make({
                      generateText: () => Effect.succeed([]),
                      streamText: (request) => {
                        requests.push(request.prompt);

                        return Stream.fromIterable(checkpointParts(requests.length + 2));
                      },
                    }),
                  ),
                );

                const settlements = yield* runtime
                  .processThread(Agent.withModel(checkpointDefinition, model), threadId)
                  .pipe(Effect.provide(makeCheckpointToolLayer(site.supplier)));

                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(settlements[0]?.usageSummary?.modelCalls).toBe(4);
                expect(settlements[0]?.usageSummary?.inputTokens.total).toBe(400);
                expect(settlements[0]?.usageSummary?.outputTokens.total).toBe(40);
                expect(requests).toHaveLength(2);
                for (const request of requests) {
                  const prompt = JSON.stringify(request);

                  expect(prompt).toContain(CRASH_QUESTION);
                  expect(prompt).toContain(CHECKPOINT_INSTRUCTIONS);
                  expect(prompt).toContain(CHECKPOINT_HANDOFF);
                  expect(prompt).not.toContain('"id":"checkpoint-call-1"');
                  expect(prompt).not.toContain('"id":"checkpoint-call-2"');
                }

                const records = yield* readLog(thread);

                expect(records.slice(0, before.records.length)).toEqual(before.records);
                expect(
                  records.filter(({ record }) => record.payload._tag === "RunStarted"),
                ).toHaveLength(1);
                expect(
                  records.filter(({ record }) => record.payload._tag === "CompactionCreated"),
                ).toHaveLength(1);
                expect(
                  records.filter(({ record }) => record.payload._tag === "ModelResponseRecorded"),
                ).toHaveLength(4);
                expect(
                  records.filter(({ record }) => record.payload._tag === "ToolCallSettled"),
                ).toHaveLength(3);
                yield* assertConvergence(thread, [before.submissionId], {
                  site,
                  counts: {
                    "checkpoint-read:checkpoint-read-1": 1,
                    "checkpoint-read:checkpoint-read-2": 1,
                    "checkpoint-read:checkpoint-read-3": 1,
                  },
                });
              }),
              { runContext: checkpointContextLayer },
            );
          }),
        ),
      30_000,
    );
  },
);
