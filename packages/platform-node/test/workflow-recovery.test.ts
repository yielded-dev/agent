import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import { expect, it } from "@effect/vitest";
import { WorkflowAgentHost } from "@yielded/agent-workflow/workflow-agent-host";
import * as Agent from "@yielded/agent/agent";
import { digestDefinitions } from "@yielded/agent/digest";
import { DurableAgentRuntime } from "@yielded/agent/durable-agent-runtime";
import { StorageOwner } from "@yielded/agent/run-storage";
import * as Subagent from "@yielded/agent/subagent";
import { SubagentReservationsMemoryLive } from "@yielded/agent/subagent-reservations";
import type * as Response from "effect/ai/Response";
import * as Toolkit from "effect/ai/Toolkit";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import {
  definitionsFor,
  finalParts,
  hostLayer,
  makeModel,
  makePlanner,
  pendingIntents,
  planner,
  readLog,
  submitOptions,
  temporaryDirectory,
  until,
  usage,
} from "./workflow-fixtures.ts";

const platform = Layer.mergeAll(NodeCrypto.layer, NodeFileSystem.layer);

it.live(
  "releases the only execution permit while an attached child runs and recovers both after restart",
  () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory;
      const started = yield* Deferred.make<void>();
      const finalized = yield* Ref.make(0);

      const childModel = yield* makeModel((call) =>
        call === 0
          ? Stream.fromEffect(
              Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
            ).pipe(Stream.ensuring(Ref.update(finalized, (n) => n + 1)))
          : Stream.fromIterable(finalParts("child")),
      );

      const childDefinitions = definitionsFor(planner.id);

      const delegation = Subagent.make("delegate_workflow", {
        target: planner,
        failureMode: "return",
      });

      const parentModel = yield* makeModel((call) =>
        Stream.fromIterable<Response.StreamPartEncoded>(
          call === 0
            ? [
                {
                  type: "tool-call",
                  id: "delegate-1",
                  name: delegation.name,
                  params: { question: "child" },
                  providerExecuted: false,
                },
                { type: "finish", reason: "tool-calls", usage },
              ]
            : finalParts("parent"),
        ),
      );

      const parent = Agent.withModel(
        Agent.make("workflow-parent", {
          input: planner.input,
          output: planner.output,
          instructions: "Delegate once then answer.",
          toolkit: Toolkit.make(delegation.tool),
          policy: planner.policy,
        }),
        parentModel.model,
      );

      const definitions = definitionsFor(parent.definition.id);
      const digests = yield* digestDefinitions(definitions);

      const handlers = Subagent.layer(delegation, childModel.model).pipe(
        Layer.provide([SubagentReservationsMemoryLive]),
      );

      const stack = hostLayer(directory, [
        { agent: parent, definitions },
        { agent: planner, model: childModel.model, definitions: childDefinitions },
      ]).pipe(Layer.provide(handlers));

      const first = yield* Effect.gen(function* () {
        const host = yield* WorkflowAgentHost;

        yield* host.submit(parent, { question: "parent" }, submitOptions(digests));

        return yield* Effect.never;
      }).pipe(Effect.provide(stack), Effect.forkChild);

      yield* Deferred.await(started);
      yield* Fiber.interrupt(first);
      expect(yield* Ref.get(finalized)).toBe(1);

      yield* Effect.gen(function* () {
        const host = yield* WorkflowAgentHost;
        const receipt = yield* host.submit(parent, { question: "parent" }, submitOptions(digests));

        expect((yield* host.awaitSettlement(receipt)).outcome).toBe("completed");
        yield* until(pendingIntents, (rows) => rows.length === 0);
        const records = yield* readLog(receipt.threadId);

        expect(
          records.filter((row) => row.record.payload._tag === "SubagentRequested"),
        ).toHaveLength(1);
        expect(records.filter((row) => row.record.payload._tag === "ToolCallSettled")).toHaveLength(
          1,
        );
        expect(
          records.filter((row) => row.record.payload._tag === "SubmissionSettled"),
        ).toHaveLength(1);
        expect(yield* Ref.get(childModel.calls)).toBe(2);
      }).pipe(Effect.provide(stack));
    }).pipe(Effect.scoped, Effect.provide(platform)),
  // Both SQLite hosts restart before the cleanup wait, which allows up to ten seconds.
  30_000,
);

it.live("a replacement Run rejects a late Workflow model result through the same owner", () =>
  Effect.gen(function* () {
    const directory = yield* temporaryDirectory;
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();

    const fixture = yield* makePlanner((call) =>
      call === 0
        ? Stream.fromEffect(
            Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ).pipe(Stream.flatMap(() => Stream.fromIterable(finalParts("stale"))))
        : Stream.fromIterable(finalParts("fresh")),
    );

    yield* Effect.gen(function* () {
      const host = yield* WorkflowAgentHost;
      const runtime = yield* DurableAgentRuntime;
      const owner = yield* StorageOwner;

      const receipt = yield* host.submit(
        fixture.agent,
        { question: "fence" },
        submitOptions(fixture.digests),
      );

      yield* Deferred.await(started);
      // End this local generation while its external response is still outstanding.
      // Both drivers share the same physical owner; there is no distributed takeover.
      expect(owner.hasOwner(receipt.threadId, receipt.submissionId)).toBe(true);
      yield* owner.withMutation(
        Effect.sync(() => owner.closeSubmission(receipt.threadId, receipt.submissionId)),
      );
      const settlements = yield* runtime.processThreadResolved(receipt.threadId);

      expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      yield* Deferred.succeed(release, undefined);
      expect((yield* host.awaitSettlement(receipt)).outcome).toBe("completed");
      yield* until(pendingIntents, (rows) => rows.length === 0);
      yield* host.repair;
      const log = yield* readLog(receipt.threadId);

      expect(log.filter((row) => row.record.payload._tag === "SubmissionSettled")).toHaveLength(1);
      const responses = log.filter((row) => row.record.payload._tag === "ModelResponseRecorded");

      expect(responses).toHaveLength(1);
      expect(JSON.stringify(responses)).toContain("fresh");
      expect(JSON.stringify(responses)).not.toContain("stale");
      expect(yield* Ref.get(fixture.calls)).toBe(2);
    }).pipe(Effect.provide(hostLayer(directory, [fixture])));
  }).pipe(Effect.scoped, Effect.provide(platform)),
);

it.live("a one-row repair page reaches Unicode thread names in SQLite order", () =>
  Effect.gen(function* () {
    const directory = yield* temporaryDirectory;
    const fixture = yield* makePlanner(() => Stream.never);
    const ids = ["\uE000", "😀"];

    yield* Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;

      for (const id of ids)
        yield* runtime.submit(fixture.agent, { question: id }, submitOptions(fixture.digests, id));
      const rows = yield* until(pendingIntents, (intents) => intents.length === 2);

      expect(rows.map((row) => row.receipt.threadId).sort()).toEqual([...ids].sort());
    }).pipe(Effect.provide(hostLayer(directory, [fixture], {}, 1)));
  }).pipe(Effect.scoped, Effect.provide(platform)),
);
