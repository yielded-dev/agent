import { DurableAlarmService } from "@yielded/agent-platform-cloudflare/alarm";
import { CloudflareThreadClient } from "@yielded/agent-platform-cloudflare/cloudflare-thread-client";
import * as ThreadObject from "@yielded/agent-platform-cloudflare/thread-object";
import { type Receipt } from "@yielded/agent/durable-agent-runtime";
import { type DurableRuntimeFailpointLocation } from "@yielded/agent/durable-failpoint";
import {
  AbortCommand,
  IdempotencyKey,
  Principal,
  SubmissionLedger,
  SubmissionLookupByKey,
} from "@yielded/agent/submission-ledger";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Deferred, Effect, Option } from "effect";
import { DurableObject } from "effect-cf";
import { describe, expect, it } from "vite-plus/test";

import {
  alarmAttemptHolds,
  armRuntimeEviction,
  armStorageEviction,
  armedEvictionsRemaining,
  decodeThreadId,
  lostBookReplies,
  maintenanceFinishObservers,
  supplierCountsFor,
  submitOptions,
} from "./fixtures.ts";
import {
  anyInState,
  laneRows,
  readCanonical,
  runClient,
  scheduledAlarm,
  stubFor,
} from "./harness.ts";
import {
  armTransportFault,
  childModelInvocations,
  coordinatorDefinition,
  delegateCallIdFor,
  gateChildModel,
  healTransportFault,
  releaseChildModel,
  uncertainChildRefs,
} from "./subagent-fixtures.ts";
import {
  SUBAGENTS,
  allLanesSettled,
  assertDelegationConverged,
  childThreadOf,
  completedDelegation,
  drainDelegationUntil,
  parentOutcomeOf,
  payloadsOf,
  reservationStatuses,
  settlementMarkers,
  waitFor,
} from "./subagent-harness.ts";

/** Native child admission, parent wake, abort/join, and unreachable-Object recovery. */

let laneCounter = 0;

const lane = (location: string): string =>
  `cf-s2-${location.replaceAll(":", "-")}-${laneCounter++}`;

const submitCoordinator = (thread: string, key: string): Promise<Receipt> =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.submit(
        {
          definition: coordinatorDefinition,
        },
        { mission: "cross-Object delegation", ref: thread },
        submitOptions(thread, key),
      );
    }),
    SUBAGENTS,
  );

/**
 * Fire-and-forget delivery of a possibly-HANGING lane's alarm: a gated researcher blocks its
 * Object's maintenance pass mid-model-stream, so the delivery promise must never be awaited
 * (the pool also auto-fires due alarms; this only removes the dependence on that cadence).
 */
const kickWithoutAwaiting = (thread: string): void => {
  void runDurableObjectAlarm(stubFor(thread, SUBAGENTS)).catch(() => {});
};

const abortParent = (thread: string, receipt: Receipt) =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.abort(
        decodeThreadId(thread),
        AbortCommand.make({
          submissionId: receipt.submissionId,
          author: "operator",
          reason: "cross-Object abort propagation",
        }),
      );
    }),
    SUBAGENTS,
  );

/**
 * One coordinator eviction row: arm the location on the PARENT lane, submit, and let the two
 * Objects' persisted alarms alone converge parent and child to one established, joined,
 * settled delegation. The armed location must actually have fired.
 */
const coordinatorEvictionRow = async (
  location: DurableRuntimeFailpointLocation,
): Promise<{ readonly ref: string; readonly receipt: Receipt }> => {
  const ref = lane(location);
  const key = `${ref}-key`;

  armRuntimeEviction(ref, location);
  const receipt = await submitCoordinator(ref, key);
  const child = childThreadOf(receipt, ref);

  await drainDelegationUntil([ref, child], allLanesSettled(ref, child));
  expect(armedEvictionsRemaining(ref), `${location} must actually have fired`).toBe(0);
  await assertDelegationConverged(completedDelegation(receipt, ref));

  return { ref, receipt };
};

describe("Cross-Object child admission, wake, and abort ownership", () => {
  it("eviction at subagent:after-admit: the routed admission committed in the child's Object survives and resolveAdmission reattaches to it", async () => {
    await coordinatorEvictionRow("subagent:after-admit");
  }, 40_000);

  it("a child never runs a Turn before its lineage record is canonical", async ({ signal }) => {
    // The child admission is durable at after-admit, before lineage or readiness. Arm the
    // transport fault at that exact hit, then evict the real parent Object before it proceeds.
    const location: DurableRuntimeFailpointLocation = "subagent:after-admit";
    const ref = lane("child-awaits-lineage");
    const faultSuffix = `:${delegateCallIdFor(ref)}`;

    armRuntimeEviction(ref, location, location);
    alarmAttemptHolds.set(ref, {
      location,
      entered: Effect.sync(() => armTransportFault(faultSuffix)),
      release: Effect.void,
      finished: Effect.void,
    });
    try {
      const receipt = await submitCoordinator(ref, `${ref}-key`);
      const child = childThreadOf(receipt, ref);

      await drainDelegationUntil([ref], async () => armedEvictionsRemaining(ref) < 2);
      expect(armedEvictionsRemaining(ref), "the first native eviction must have fired").toBe(1);

      // Exercise the parent's routed admission reader. The native RPC fault must yield
      // Indeterminate, not absence; a stable counter alone does not establish that boundary.
      const requested = payloadsOf(await readCanonical(ref, SUBAGENTS), "SubagentRequested")[0]
        ?.record.payload;

      if (requested?._tag !== "SubagentRequested") throw new Error("Expected SubagentRequested");

      const resolution = await runInDurableObject(stubFor(ref, SUBAGENTS), (instance) =>
        instance[DurableObject.RunSymbol](
          SubmissionLedger.use((ledger) =>
            ledger.resolveAdmission(
              SubmissionLookupByKey.make({
                threadId: requested.childThreadId,
                principal: Principal.make(requested.childPrincipal),
                idempotencyKey: IdempotencyKey.make(requested.childIdempotencyKey),
              }),
            ),
          ),
        ),
      );

      expect(resolution).toMatchObject({
        _tag: "Indeterminate",
        reason: expect.stringContaining("injected transport fault"),
      });

      // A consumed native alarm slot can be empty while maintenance is still running.
      // Wait for its named checkpoint to retire, then join the event's cleanup.
      const quiesced = Deferred.makeUnsafe<void>();

      try {
        await runInDurableObject(stubFor(child, SUBAGENTS), (instance) =>
          instance[DurableObject.RunSymbol](
            Effect.gen(function* () {
              const alarm = yield* DurableAlarmService;

              const observe = alarm.scheduled.pipe(
                Effect.flatMap((scheduled) =>
                  Option.isNone(scheduled)
                    ? Deferred.succeed(quiesced, undefined).pipe(Effect.asVoid)
                    : Effect.void,
                ),
                Effect.orDie,
              );

              maintenanceFinishObservers.set(child, observe);
              yield* observe;
            }),
          ),
        );
        await Effect.runPromise(Deferred.await(quiesced), { signal });
        await runInDurableObject(stubFor(child, SUBAGENTS), (instance) =>
          Promise.resolve(instance.alarm()),
        );
      } finally {
        maintenanceFinishObservers.delete(child);
      }
      const rows = await laneRows(child, SUBAGENTS);

      expect(rows).toHaveLength(1);
      expect(rows[0]?.state, "child state before parent establishment").toBe("admitted");
      expect(childModelInvocations(ref)).toBe(0);
      expect(
        await scheduledAlarm(child, SUBAGENTS),
        "AwaitParentEstablishment is a stable external wait and must quiesce (#93)",
      ).toBeNull();

      // The WP1 admin surface names the deferral: the child Object explains its own lane.
      const explainedRaw: unknown = await Promise.resolve(
        stubFor(child, SUBAGENTS).explainEncoded({}),
      );

      const explained = await Effect.runPromise(ThreadObject.decodeAdminResponse(explainedRaw));

      expect(explained._tag).toBe("ExplainedRecovery");
      if (explained._tag === "ExplainedRecovery") {
        expect(explained.explanations).toHaveLength(1);
        expect(explained.explanations[0]?.decision._tag).toBe("AwaitParentEstablishment");
        expect(explained.explanations[0]?.disposition).toBe("deferred");
      }

      // Heal the transport: the remaining armed eviction still fires, then idempotent
      // establishment completes the child with lineage BEFORE readiness and one invocation.
      healTransportFault(faultSuffix);
      await drainDelegationUntil([ref, child], allLanesSettled(ref, child));
      expect(armedEvictionsRemaining(ref), "every armed eviction must have fired").toBe(0);
      await assertDelegationConverged(completedDelegation(receipt, ref));
      const childRecords = await readCanonical(child, SUBAGENTS);
      const tags = childRecords.map((envelope) => envelope.record.payload._tag);
      const lineageIndex = tags.indexOf("SubagentLineageRecorded");
      const firstTurnIndex = tags.indexOf("ModelResponseRecorded");

      expect(lineageIndex, "the child log must carry its lineage record").toBeGreaterThanOrEqual(0);
      expect(firstTurnIndex, "the child must have run exactly after establishment").toBeGreaterThan(
        lineageIndex,
      );
      expect(childModelInvocations(ref)).toBe(1);
    } finally {
      alarmAttemptHolds.delete(ref);
      healTransportFault(faultSuffix);
    }
  }, 60_000);

  // -------------------------------------------------------------------------
  // Issue #93: parent wake is durable before child finalization can commit.
  // -------------------------------------------------------------------------

  it("issue #93: child eviction after finalize cannot lose its precommitted parent wake", async () => {
    const ref = lane("lost-notification");
    const key = `${ref}-key`;

    // The researcher hangs mid-stream so the eviction can be armed with the child provably
    // active — no race against the child's own alarm-driven progress.
    gateChildModel(ref);
    const receipt = await submitCoordinator(ref, key);
    const child = childThreadOf(receipt, ref);

    await drainDelegationUntil([ref], anyInState(ref, "suspended", SUBAGENTS));
    await waitFor(() => {
      kickWithoutAwaiting(child);

      return childModelInvocations(ref) === 1;
    }, "the hanging researcher invocation");
    await waitFor(
      async () => (await scheduledAlarm(ref, SUBAGENTS)) === null,
      "the parent AwaitChildSettlement alarm to quiesce (#93)",
    );

    // The child's Object dies right AFTER its settlement finalize commits. The protocol must
    // have durably routed `recordChildSettled` and dirtied the parent BEFORE this can happen.
    armStorageEviction(child, "ledger:finalize-settlement:after");
    releaseChildModel(ref);
    await waitFor(async () => {
      const rows = await laneRows(child, SUBAGENTS);

      return rows.length === 1 && rows[0]?.state === "settled";
    }, "the child settlement to commit before the eviction");
    expect(armedEvictionsRemaining(child)).toBe(0);
    expect(await settlementMarkers(ref)).toHaveLength(1);

    // Fire ONLY the parent's persisted alarm: the precommitted marker/generation resumes the
    // join after quiescence; no background polling contract is needed.
    await drainDelegationUntil([ref], allLanesSettled(ref, child));
    await assertDelegationConverged(completedDelegation(receipt, ref));
  }, 40_000);

  // -------------------------------------------------------------------------
  // Abort propagation parent→child across Objects (request-abort-and-join).
  // -------------------------------------------------------------------------

  it.each(["unknown"])(
    "aborting the waiting parent joins its %s child before settlement",
    async (childState) => {
      const ref = lane(`abort-propagation-${childState}`);
      const key = `${ref}-key`;

      gateChildModel(ref);
      {
        uncertainChildRefs.add(ref);
        lostBookReplies.add(ref);
      }
      const receipt = await submitCoordinator(ref, key);
      const child = childThreadOf(receipt, ref);

      await drainDelegationUntil([ref], anyInState(ref, "suspended", SUBAGENTS));
      await waitFor(() => {
        kickWithoutAwaiting(child);

        return childModelInvocations(ref) === 1;
      }, "the hanging researcher invocation");

      {
        armStorageEviction(child, "ledger:mark-unknown:after");
        releaseChildModel(ref);
        await drainDelegationUntil([child], anyInState(child, "unknown", SUBAGENTS));
        expect(supplierCountsFor(ref).book).toBe(1);
      }

      await abortParent(ref, receipt);
      // The parent's alarm passes propagate the abort across the Object boundary; the child's
      // active Attempt observes its durable intent, interrupts the hanging stream, settles
      // aborted, and the join wakes the parent (spec §13.1).
      await drainDelegationUntil([ref, child], allLanesSettled(ref, child));

      // Exactly one canonical child abort command, authored by the propagation (DUR-012).
      const childRecords = await readCanonical(child, SUBAGENTS);
      const abortRecords = payloadsOf(childRecords, "AbortRequested");

      expect(abortRecords).toHaveLength(1);
      const abortPayload = abortRecords[0]?.record.payload;

      if (abortPayload?._tag === "AbortRequested") {
        expect(abortPayload.author).toBe("subagent-parent-abort");
      }
      const childSettled = payloadsOf(childRecords, "SubmissionSettled")[0]?.record.payload;

      if (childSettled?._tag === "SubmissionSettled") {
        expect(childSettled.outcome).toBe("aborted");
      }

      // The join committed with the child's ACTUAL outcome, strictly BEFORE the parent's
      // aborted settlement ("the parent settles only after the joins").
      const parentRecords = await readCanonical(ref, SUBAGENTS);
      const joined = payloadsOf(parentRecords, "SubagentJoined");

      expect(joined).toHaveLength(1);
      const joinedPayload = joined[0]?.record.payload;

      if (joinedPayload?._tag !== "SubagentJoined") throw new Error("Expected SubagentJoined");
      expect(joinedPayload.childOutcome).toBe("aborted");
      const settledEnvelope = payloadsOf(parentRecords, "SubmissionSettled")[0];

      expect(settledEnvelope).toBeDefined();
      if (settledEnvelope !== undefined && joined[0] !== undefined) {
        expect(Number(joined[0].sequence)).toBeLessThan(Number(settledEnvelope.sequence));
      }
      expect(await parentOutcomeOf(ref)).toBe("aborted");
      expect(await reservationStatuses(ref)).toEqual(["released"]);
      // The interrupted researcher was invoked exactly once and never re-executed to abort it.
      expect(childModelInvocations(ref)).toBe(1);
      {
        expect(supplierCountsFor(ref).book).toBe(1);
        expect(payloadsOf(childRecords, "ToolCallUnknown")).toHaveLength(1);
        expect(payloadsOf(childRecords, "ToolCallSettled")).toHaveLength(0);
        expect(armedEvictionsRemaining(child)).toBe(0);
      }
    },
    40_000,
  );

  // -------------------------------------------------------------------------
  // The DO-unreachable establishment row — AdmissionIndeterminate is real
  // in DC, and it NEVER becomes a second admission (SUB-031).
  // -------------------------------------------------------------------------

  it("an unreachable child Object during establishment recovery yields Indeterminate, never a duplicate child, and converges when transport heals", async () => {
    const ref = lane("indeterminate-establishment");
    const key = `${ref}-key`;
    const faultSuffix = `:${delegateCallIdFor(ref)}`;

    // Both levers arm BEFORE the submit: the child Thread's name is suffix-derivable
    // from the ref, so the fault covers the child Object before it can ever be reached.
    armTransportFault(faultSuffix);
    armRuntimeEviction(ref, "subagent:after-reserve");
    const receipt = await submitCoordinator(ref, key);
    const child = childThreadOf(receipt, ref);

    // Let the doomed incarnation die at after-reserve, then let MANY recovery passes retry
    // establishment against the unreachable admission authority.
    await drainDelegationUntil([ref], async () => armedEvictionsRemaining(ref) === 0);
    for (let round = 0; round < 20; round++) {
      try {
        await runDurableObjectAlarm(stubFor(ref, SUBAGENTS));
      } catch {
        // Typed pass failures (the SUB-031 wait/retry LedgerError) reject the delivery; the
        // pre-armed alarm survives for the next round.
      }
      await new Promise((resolve) => setTimeout(resolve, 15));
    }

    // SUB-031 under a genuinely unreachable authority: the parent lane is still owed its
    // settlement, NO child Submission exists in the child's Object, no start link was
    // recorded, and the researcher never ran — an Indeterminate answer admitted nothing.
    const parentLane = await laneRows(ref, SUBAGENTS);

    expect(parentLane).toHaveLength(1);
    expect(parentLane[0]?.state).not.toBe("settled");
    expect(await laneRows(child, SUBAGENTS)).toHaveLength(0);
    const duringFault = await readCanonical(ref, SUBAGENTS);

    expect(payloadsOf(duringFault, "SubagentStarted")).toHaveLength(0);
    expect(childModelInvocations(ref)).toBe(0);
    // Each recovery pass re-arms the alarm inside its own delivery, and workerd auto-retries
    // a rejected delivery on its own timers — an instant probe can land mid-delivery, after
    // the alarm slot was consumed but before the pass re-arms it. The durable SUB-031 claim
    // is that autonomous retry state keeps an alarm armed, so poll for it.
    await waitFor(
      async () => (await scheduledAlarm(ref, SUBAGENTS)) !== null,
      "indeterminate establishment to retain its autonomous retry alarm (#93)",
    );

    // Heal the transport: the next alarm passes resolve the admission (fresh), establish the
    // ONE child, and the delegation completes — no duplicate was ever possible.
    healTransportFault(faultSuffix);
    await drainDelegationUntil([ref, child], allLanesSettled(ref, child));
    const parentRecords = await readCanonical(ref, SUBAGENTS);

    expect(payloadsOf(parentRecords, "SubagentRequested")).toHaveLength(1);
    await assertDelegationConverged(completedDelegation(receipt, ref));
  }, 60_000);
});
