import { NodeFileSystem } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { NodeDurableHost } from "@yielded/agent-platform-node/node-durable-host";
import { DurableAgentRuntime, DurableRuntimeConfig } from "@yielded/agent/durable-agent-runtime";
import {
  DurableRuntimeFailpoint,
  type DurableRuntimeFailpointLocation,
} from "@yielded/agent/durable-failpoint";
import { type SubmissionId } from "@yielded/agent/identifiers";
import { DeploymentId, ProducerId } from "@yielded/agent/records";
import {
  childThreadIdFor,
  childIdempotencyKeyFor,
  runIdForSubmission,
  toolCallSettledRecordId,
} from "@yielded/agent/run-journal";
import { RunToolAuthorization } from "@yielded/agent/run-options";
import { StorageOwner } from "@yielded/agent/run-storage";
import {
  AbortCommand,
  RecoverySnapshotRequest,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
} from "@yielded/agent/submission-ledger";
import { ToolReconciler } from "@yielded/agent/tool-reconciler";
import { Effect, Fiber, Layer, Option } from "effect";

import {
  CHILD_MODEL_OP,
  CRASH_DEPLOYMENT_ID,
  HOST_PRODUCER_ID,
  CRASH_PRINCIPAL,
  CRASH_QUESTION,
  DELEGATE_CALL_ID,
  PROJECTED_SUMMARY,
  RESEARCH_TOPIC,
  childModelInvocations,
  coordinatorSubmitSlice,
  crashSubmitOptions,
  decodeThreadId,
  decodeToolCallId,
  makeCrashSubagentBindings,
} from "./fixtures.ts";
import {
  assertConvergence,
  failureTag,
  expectKilled,
  lookupByKey,
  payloadsOf,
  readLog,
  runWorkerToExit,
  touchFile,
  waitForFile,
  withCrashSite,
  withHost,
  withRuntime,
  type CrashSite,
} from "./harness.ts";

/**
 * S2 durable attached Subagents — process-kill crash matrix.
 * Every row spawns `worker-entry.ts` as a REAL child process over a temp SQLite file, kills it
 * at an armed coordinator/storage failpoint (or SIGKILLs it while blocked mid-Attempt), restarts
 * a host against the same file, and asserts the required durable outcome. The scripted child
 * LanguageModel counts its invocations through the FILE-BACKED supplier store, so "a completed
 * child is never re-executed" (§16.4) is asserted from external truth that survives the kill —
 * never assumed. Real clocks, files, and processes, hence `excludeTestServices`.
 */

const DELEGATE_CALL = decodeToolCallId(DELEGATE_CALL_ID);
const CHILD_MODEL_COUNT_KEY = `${CHILD_MODEL_OP}:${RESEARCH_TOPIC}`;

const submitCoordinator = (thread: string, key: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;

    return yield* runtime.submit(
      coordinatorSubmitSlice,
      { mission: CRASH_QUESTION },
      crashSubmitOptions(thread, key),
    );
  });

/** Drive one Thread lane through the S2 multi-binding worker path (SUB-023). */
const drive = (thread: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;

    return yield* runtime.processThreadResolved(decodeThreadId(thread));
  });

/** Restart-drive bindings: the parent model always answers final (batch resume, P5 precedent). */
const restartBindings = (site: CrashSite) =>
  makeCrashSubagentBindings({ supplierDir: site.supplier });

const reservationStatuses = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;

    const snapshot = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId }),
    );

    return snapshot.childReservations.map((row) => row.status);
  });

const submissionSnapshot = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));

    expect(Option.isSome(snapshot)).toBe(true);
    if (Option.isNone(snapshot)) throw new Error("Expected the Submission to exist");

    return snapshot.value;
  });

const startedPayloadOf = (thread: string) =>
  Effect.gen(function* () {
    const log = yield* readLog(thread);
    const payload = payloadsOf(log, "SubagentStarted")[0]?.record.payload;

    if (payload?._tag !== "SubagentStarted") throw new Error("Expected SubagentStarted");

    return payload;
  });

/** The delegation call settled exactly once with the given payload expectations. */
const assertDelegationSettled = (
  thread: string,
  parentSubmissionId: SubmissionId,
  expected: { readonly isFailure: boolean; readonly result: unknown },
) =>
  Effect.gen(function* () {
    const runId = runIdForSubmission(parentSubmissionId);
    const log = yield* readLog(thread);

    const settled = log.filter(
      (envelope) => envelope.record.recordId === toolCallSettledRecordId(runId, 1, DELEGATE_CALL),
    );

    expect(settled).toHaveLength(1);
    const payload = settled[0]?.record.payload;

    expect(payload?._tag).toBe("ToolCallSettled");
    if (payload?._tag === "ToolCallSettled") {
      expect(payload.isFailure).toBe(expected.isFailure);
      expect(payload.result).toEqual(expected.result);
    }
  });

/**
 * The one-child identity claims common to every converged establishment (SUB-016/SUB-017):
 * exactly one requested/started/joined record, one child Thread with one lineage record,
 * and the recorded Receipt naming the one admitted child.
 */
const assertOneEstablishedChild = (thread: string, childThread: string) =>
  Effect.gen(function* () {
    const log = yield* readLog(thread);

    expect(payloadsOf(log, "SubagentRequested")).toHaveLength(1);
    expect(payloadsOf(log, "SubagentStarted")).toHaveLength(1);
    expect(payloadsOf(log, "SubagentJoined")).toHaveLength(1);
    const childLog = yield* readLog(childThread);

    expect(payloadsOf(childLog, "ThreadCreated")).toHaveLength(1);
    expect(payloadsOf(childLog, "SubagentLineageRecorded")).toHaveLength(1);
    const started = yield* startedPayloadOf(thread);
    const child = yield* submissionSnapshot(started.childSubmissionId);

    expect(child.receiptId).toBe(started.childReceiptId);
    expect(child.state).toBe("settled");

    return started;
  });

layer(NodeFileSystem.layer, { excludeTestServices: true })(
  "S2 durable Subagent crash matrix (real process kills)",
  (it) => {
    it.effect(
      "establishment boundary kills converge on one child Receipt, Thread, and join",
      () =>
        Effect.gen(function* () {
          const rows: ReadonlyArray<{
            readonly location: DurableRuntimeFailpointLocation;
            readonly decision: string;
            readonly disposition: string;
          }> = [
            {
              location: "subagent:after-reserve",
              decision: "ResumePendingToolBatch",
              disposition: "deferred",
            },
            {
              location: "subagent:after-request-append",
              decision: "CompleteChildAdmission",
              disposition: "repaired",
            },
            {
              location: "subagent:after-admit",
              decision: "RepairSubagentStartLink",
              disposition: "repaired",
            },
            {
              location: "subagent:after-start-append",
              decision: "EnsureWaitingForChild",
              disposition: "repaired",
            },
          ];

          for (const row of rows) {
            yield* withCrashSite((site) =>
              Effect.gen(function* () {
                const thread = `thread-${row.location.replaceAll(":", "-")}`;
                const key = `key-${row.location}`;

                const result = yield* runWorkerToExit({
                  db: site.db,
                  scenario: "subagent-run",
                  thread,
                  key,
                  killAt: row.location,
                  supplierDir: site.supplier,
                });

                expectKilled(result);

                const bindings = yield* restartBindings(site);

                yield* withHost(
                  site.db,
                  Effect.gen(function* () {
                    const host = yield* NodeDurableHost;
                    const parent = yield* lookupByKey(thread, key);

                    const report = host.startupRecovery.find(
                      (entry) => entry.submissionId === parent.submissionId,
                    );

                    expect(report?.decision._tag, row.location).toBe(row.decision);
                    expect(report?.disposition, row.location).toBe(row.disposition);

                    const childThread = childThreadIdFor(parent.submissionId, DELEGATE_CALL);

                    // Worker convergence: the parent (re)establishes and suspends without
                    // settling, the child lane settles, the woken parent joins.
                    expect(yield* drive(thread), row.location).toHaveLength(0);
                    const childSettlements = yield* drive(childThread);

                    expect(
                      childSettlements.map((settlement) => settlement.outcome),
                      row.location,
                    ).toEqual(["completed"]);
                    const settlements = yield* drive(thread);

                    expect(
                      settlements.map((settlement) => settlement.outcome),
                      row.location,
                    ).toEqual(["completed"]);

                    const started = yield* assertOneEstablishedChild(thread, childThread);

                    yield* assertDelegationSettled(thread, parent.submissionId, {
                      isFailure: false,
                      result: { summary: PROJECTED_SUMMARY },
                    });
                    expect(yield* reservationStatuses(parent.submissionId), row.location).toEqual([
                      "released",
                    ]);
                    // The one child model invocation across the kill and every replay.
                    expect(childModelInvocations(site.supplier), row.location).toBe(1);
                    yield* assertConvergence(thread, [parent.submissionId], {
                      site,
                      counts: { [CHILD_MODEL_COUNT_KEY]: 1 },
                    });
                    yield* assertConvergence(childThread, [started.childSubmissionId]);
                  }),
                  undefined,
                  bindings,
                );
              }),
            );
          }
        }),
      150_000,
    );

    it.effect(
      "a reservation orphaned before its request releases exactly once under parent abort",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-s2-orphan";
            const key = "s2-orphan-1";

            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "subagent-run",
              thread,
              key,
              killAt: "subagent:after-reserve",
              supplierDir: site.supplier,
            });

            expectKilled(result);

            // Abort the parent from a client-only restart before any recovery pass runs.
            const parentId = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const runtime = yield* DurableAgentRuntime;
                const parent = yield* lookupByKey(thread, key);

                yield* runtime.abort(
                  AbortCommand.make({
                    submissionId: parent.submissionId,
                    author: "operator",
                    reason: "abandon before request",
                  }),
                );

                return parent.submissionId;
              }),
            );

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const runtime = yield* DurableAgentRuntime;
                const ledger = yield* SubmissionLedger;

                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === parentId,
                );

                // Provably childless reservation under abort: released exactly once, never a
                // child admitted merely to abort it (spec §13/§14).
                expect(report?.decision._tag).toBe("ReleaseOrphanChildReservation");
                expect(report?.disposition).toBe("repaired");
                expect(yield* reservationStatuses(parentId)).toEqual(["released"]);

                const second = (yield* runtime.runRecovery()).reports;
                const settleReport = second.find((entry) => entry.submissionId === parentId);

                expect(settleReport?.decision._tag).toBe("SettleAborted");
                expect(settleReport?.disposition).toBe("repaired");
                const parent = yield* submissionSnapshot(parentId);

                expect(parent.state).toBe("settled");
                expect(parent.settledOutcome).toBe("aborted");

                // No child was ever admitted for the orphaned reservation (SUB-031 evidence).
                const resolution = yield* ledger.resolveAdmission(
                  SubmissionLookupByKey.make({
                    threadId: childThreadIdFor(parentId, DELEGATE_CALL),
                    principal: CRASH_PRINCIPAL,
                    idempotencyKey: childIdempotencyKeyFor(
                      runIdForSubmission(parentId),
                      DELEGATE_CALL,
                    ),
                  }),
                );

                expect(resolution._tag).toBe("NotAdmitted");
                const log = yield* readLog(thread);

                expect(payloadsOf(log, "SubagentRequested")).toHaveLength(0);
                // The delegation call is never marked Unknown (plan §4.3 classifier row 6).
                expect(payloadsOf(log, "ToolCallUnknown")).toHaveLength(0);
                expect(childModelInvocations(site.supplier)).toBe(0);
                yield* assertConvergence(thread, [parentId], { site, counts: {} });
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill after the precommitted parent wake and child finalize: recovery replays the wake idempotently",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-s2-wake";
            const key = "s2-wake-1";

            // The FIRST settlement finalization of the scenario is the child's. The parent wake
            // is now durably committed before this boundary, so the kill cannot strand a
            // suspended parent after the child becomes settled (issue #93).
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "subagent-run",
              thread,
              key,
              killAtStorage: "ledger:finalize-settlement:after",
              supplierDir: site.supplier,
            });

            expectKilled(result);

            // Before recovery: the child is settled and its parent wake is already durable.
            const ids = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const parent = yield* lookupByKey(thread, key);

                expect(parent.state).toBe("input-applied");
                const started = yield* startedPayloadOf(thread);
                const child = yield* submissionSnapshot(started.childSubmissionId);

                expect(child.state).toBe("settled");

                return { parent: parent.submissionId, child: started.childSubmissionId };
              }),
            );

            const bindings = yield* restartBindings(site);

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;

                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === ids.parent,
                );

                // The parent wake was already committed before the kill. Recovery still repairs
                // the open canonical delegation through the same idempotent wake operation.
                expect(report?.decision._tag).toBe("ResumeWaitingParent");
                expect(report?.disposition).toBe("repaired");
                expect((yield* submissionSnapshot(ids.parent)).state).toBe("input-applied");

                const settlements = yield* drive(thread);

                expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
                // The settled child was read, verified, and joined — never re-executed.
                expect(childModelInvocations(site.supplier)).toBe(1);
                const childThread = childThreadIdFor(ids.parent, DELEGATE_CALL);

                yield* assertOneEstablishedChild(thread, childThread);
                yield* assertConvergence(thread, [ids.parent], {
                  site,
                  counts: { [CHILD_MODEL_COUNT_KEY]: 1 },
                });
                yield* assertConvergence(childThread, [ids.child]);
              }),
              undefined,
              bindings,
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "join and release failpoint kills replay the canonical accounting and never re-execute the child",
      () =>
        Effect.gen(function* () {
          const rows: ReadonlyArray<{
            readonly location: DurableRuntimeFailpointLocation;
            readonly decision: string | undefined;
          }> = [
            { location: "subagent:after-join-append", decision: "ApplyJoinAccounting" },
            { location: "subagent:after-release-pending", decision: "ApplyJoinAccounting" },
          ];

          for (const row of rows) {
            yield* withCrashSite((site) =>
              Effect.gen(function* () {
                const thread = `thread-${row.location.replaceAll(":", "-")}`;
                const key = `key-${row.location}`;

                const result = yield* runWorkerToExit({
                  db: site.db,
                  scenario: "subagent-run",
                  thread,
                  key,
                  killAt: row.location,
                  supplierDir: site.supplier,
                });

                expectKilled(result);
                // The child ran exactly once BEFORE the kill; everything after is replay.
                expect(childModelInvocations(site.supplier), row.location).toBe(1);

                const bindings = yield* restartBindings(site);

                yield* withHost(
                  site.db,
                  Effect.gen(function* () {
                    const host = yield* NodeDurableHost;
                    const parent = yield* lookupByKey(thread, key);

                    if (row.decision !== undefined) {
                      const report = host.startupRecovery.find(
                        (entry) => entry.submissionId === parent.submissionId,
                      );

                      expect(report?.decision._tag, row.location).toBe(row.decision);
                      expect(report?.disposition, row.location).toBe("repaired");
                    }
                    expect(yield* reservationStatuses(parent.submissionId), row.location).toEqual([
                      "released",
                    ]);

                    const settlements = yield* drive(thread);

                    expect(
                      settlements.map((settlement) => settlement.outcome),
                      row.location,
                    ).toEqual(["completed"]);

                    const childThread = childThreadIdFor(parent.submissionId, DELEGATE_CALL);
                    const started = yield* assertOneEstablishedChild(thread, childThread);

                    yield* assertDelegationSettled(thread, parent.submissionId, {
                      isFailure: false,
                      result: { summary: PROJECTED_SUMMARY },
                    });
                    // §16.4: a completed child is never re-executed merely because the parent
                    // join acknowledgment was lost — the file-backed count stays at ONE.
                    expect(childModelInvocations(site.supplier), row.location).toBe(1);
                    yield* assertConvergence(thread, [parent.submissionId], {
                      site,
                      counts: { [CHILD_MODEL_COUNT_KEY]: 1 },
                    });
                    yield* assertConvergence(childThread, [started.childSubmissionId]);
                  }),
                  undefined,
                  bindings,
                );
              }),
            );
          }
        }),
      90_000,
    );

    it.effect(
      "kill at subagent:after-child-abort-intent: the replayed propagation is a no-op, never a second command",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            // Visit the parent before the `subagent:` lane so startup replays the abort
            // command while the child remains unsettled, exercising its idempotence.
            const thread = "parent-thread-s2-abort-marker";
            const key = "s2-abort-marker-1";

            const bindings = yield* makeCrashSubagentBindings({
              supplierDir: site.supplier,
              parentScript: "delegate-then-final",
            });

            // Arrange in-process: established waiting parent + durable parent abort intent,
            // with NO propagation yet (abort of a suspended lane only records the intent).
            const parentId = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const runtime = yield* DurableAgentRuntime;

                yield* submitCoordinator(thread, key);
                expect(yield* drive(thread)).toHaveLength(0);
                const parent = yield* lookupByKey(thread, key);

                expect(parent.state).toBe("suspended");
                yield* runtime.abort(
                  AbortCommand.make({
                    submissionId: parent.submissionId,
                    author: "operator",
                    reason: "abort the waiting parent",
                  }),
                );

                return parent.submissionId;
              }),
              undefined,
              bindings,
            );

            // A recovery pass in a REAL process dies right after the child abort intent
            // commits — the recorded intent IS the propagation marker (spec §14).
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "subagent-recover",
              thread,
              key,
              killAt: "subagent:after-child-abort-intent",
            });

            expectKilled(result);

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const runtime = yield* DurableAgentRuntime;
                const started = yield* startedPayloadOf(thread);

                // The replayed idempotent command returns the recorded intent unchanged
                // (DUR-012); recovery repairs the marker without a second command.
                const parentReport = host.startupRecovery.find(
                  (entry) => entry.submissionId === parentId,
                );

                expect(parentReport?.decision._tag).toBe("PropagateChildAbort");
                expect(parentReport?.disposition).toBe("repaired");
                const child = yield* submissionSnapshot(started.childSubmissionId);

                expect(child.state).toBe("settled");
                expect(child.settledOutcome).toBe("aborted");

                const second = (yield* runtime.runRecovery()).reports;
                const wakeReport = second.find((entry) => entry.submissionId === parentId);

                expect(wakeReport?.decision._tag).toBe("ResumeWaitingParent");
                const settlements = yield* drive(thread);

                expect(settlements.map((settlement) => settlement.outcome)).toEqual(["aborted"]);

                // One canonical child abort command across the killed pass and every replay.
                const childThread = childThreadIdFor(parentId, DELEGATE_CALL);
                const childLog = yield* readLog(childThread);
                const abortRecords = payloadsOf(childLog, "AbortRequested");

                expect(abortRecords).toHaveLength(1);
                const abortPayload = abortRecords[0]?.record.payload;

                if (abortPayload?._tag === "AbortRequested") {
                  expect(abortPayload.author).toBe("subagent-parent-abort");
                }
                const joined = payloadsOf(yield* readLog(thread), "SubagentJoined");

                expect(joined).toHaveLength(1);
                expect(childModelInvocations(site.supplier)).toBe(0);
                yield* assertConvergence(thread, [parentId], { site, counts: {} });
                yield* assertConvergence(childThread, [started.childSubmissionId]);
              }),
              undefined,
              bindings,
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "a closed parent rejects a late join projection without changing its completed child",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-s2-stale-join";
            const key = "s2-stale-join-1";

            const staleBindings = yield* makeCrashSubagentBindings({
              supplierDir: site.supplier,
              parentScript: "delegate-then-final",
              projectionGate: { markerFile: site.marker, releaseFile: site.release },
            });

            const freshBindings = yield* restartBindings(site);

            yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const owner = yield* StorageOwner;
                const receipt = yield* submitCoordinator(thread, key);

                expect(yield* drive(thread)).toHaveLength(0);
                const childThread = childThreadIdFor(receipt.submissionId, DELEGATE_CALL);

                expect((yield* drive(childThread)).map((result) => result.outcome)).toEqual([
                  "completed",
                ]);
                const childBefore = yield* readLog(childThread);
                const stale = yield* drive(thread).pipe(Effect.forkChild);

                yield* waitForFile(site.marker);
                expect(owner.hasOwner(receipt.threadId, receipt.submissionId)).toBe(true);
                yield* owner.withMutation(
                  Effect.sync(() => owner.closeSubmission(receipt.threadId, receipt.submissionId)),
                );

                const fresh = yield* DurableAgentRuntime.pipe(
                  Effect.provide(
                    DurableAgentRuntime.layerWithBindings(freshBindings).pipe(
                      Layer.provide([
                        RunToolAuthorization.allowAll,
                        DurableRuntimeFailpoint.layer,
                        ToolReconciler.uncertain,
                        DurableRuntimeConfig.layer({
                          deploymentId: DeploymentId.make(CRASH_DEPLOYMENT_ID),
                          producerId: ProducerId.make(HOST_PRODUCER_ID),
                        }),
                      ]),
                    ),
                  ),
                );

                const settlements = yield* fresh.processThreadResolved(receipt.threadId);

                expect(settlements.map((result) => result.outcome)).toEqual(["completed"]);
                yield* touchFile(site.release);
                expect(failureTag(yield* Fiber.await(stale))).toBe("OwnershipLost");
                expect(yield* readLog(childThread)).toEqual(childBefore);
                yield* assertOneEstablishedChild(thread, childThread);
                yield* assertDelegationSettled(thread, receipt.submissionId, {
                  isFailure: false,
                  result: { summary: PROJECTED_SUMMARY },
                });
                expect(yield* reservationStatuses(receipt.submissionId)).toEqual(["released"]);
                expect(childModelInvocations(site.supplier)).toBe(1);
                yield* assertConvergence(thread, [receipt.submissionId], {
                  site,
                  counts: { [CHILD_MODEL_COUNT_KEY]: 1 },
                });
                const started = yield* startedPayloadOf(thread);

                yield* assertConvergence(childThread, [started.childSubmissionId]);
              }),
              undefined,
              staleBindings,
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "replacing a child owner rejects its late result and leaves the waiting parent's history intact",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-s2-child-fence";
            const key = "s2-child-fence-1";

            const staleBindings = yield* makeCrashSubagentBindings({
              supplierDir: site.supplier,
              parentScript: "delegate-then-final",
              childBlock: { markerFile: site.marker, releaseFile: site.release },
            });

            const freshBindings = yield* restartBindings(site);

            yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const owner = yield* StorageOwner;
                const receipt = yield* submitCoordinator(thread, key);

                expect(yield* drive(thread)).toHaveLength(0);
                expect((yield* submissionSnapshot(receipt.submissionId)).state).toBe("suspended");
                const childThread = childThreadIdFor(receipt.submissionId, DELEGATE_CALL);
                const started = yield* startedPayloadOf(thread);
                const parentBefore = yield* readLog(thread);
                const stale = yield* drive(childThread).pipe(Effect.forkChild);

                yield* waitForFile(site.marker);
                expect(owner.hasOwner(childThread, started.childSubmissionId)).toBe(true);
                yield* owner.withMutation(
                  Effect.sync(() => owner.closeSubmission(childThread, started.childSubmissionId)),
                );

                const fresh = yield* DurableAgentRuntime.pipe(
                  Effect.provide(
                    DurableAgentRuntime.layerWithBindings(freshBindings).pipe(
                      Layer.provide([
                        RunToolAuthorization.allowAll,
                        DurableRuntimeFailpoint.layer,
                        ToolReconciler.uncertain,
                        DurableRuntimeConfig.layer({
                          deploymentId: DeploymentId.make(CRASH_DEPLOYMENT_ID),
                          producerId: ProducerId.make(HOST_PRODUCER_ID),
                        }),
                      ]),
                    ),
                  ),
                );

                const settlements = yield* fresh.processThreadResolved(childThread);

                expect(settlements.map((result) => result.outcome)).toEqual(["completed"]);
                expect(childModelInvocations(site.supplier)).toBe(2);
                yield* touchFile(site.release);
                expect(failureTag(yield* Fiber.await(stale))).toBe("OwnershipLost");
                expect(yield* readLog(thread)).toEqual(parentBefore);
                expect((yield* submissionSnapshot(receipt.submissionId)).state).toBe(
                  "input-applied",
                );
                expect(
                  payloadsOf(yield* readLog(childThread), "ModelResponseRecorded"),
                ).toHaveLength(1);
                expect(
                  (yield* fresh.processThreadResolved(receipt.threadId)).map(
                    (result) => result.outcome,
                  ),
                ).toEqual(["completed"]);
                yield* assertDelegationSettled(thread, receipt.submissionId, {
                  isFailure: false,
                  result: { summary: PROJECTED_SUMMARY },
                });
                yield* assertConvergence(thread, [receipt.submissionId], {
                  site,
                  counts: { [CHILD_MODEL_COUNT_KEY]: 2 },
                });
                yield* assertConvergence(childThread, [started.childSubmissionId]);
              }),
              undefined,
              staleBindings,
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "workerConcurrency=1: the suspension frees the single worker, the child runs, and the woken parent joins",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-s2-smallest-pool";
            const key = "s2-smallest-pool-1";

            const bindings = yield* makeCrashSubagentBindings({
              supplierDir: site.supplier,
              parentScript: "delegate-then-final",
            });

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;

                const receipt = yield* host.submit(
                  coordinatorSubmitSlice,
                  { mission: CRASH_QUESTION },
                  crashSubmitOptions(thread, key),
                );

                // The §12 smallest-pool proof: ONE resolved worker loop serves the parent AND
                // the child lane. Completion is only possible because waitingForChild released
                // the single worker permit (SUB-030) — a held permit would deadlock here.
                const settlement = yield* Effect.raceFirst(
                  host.awaitSettlement(receipt),
                  host.runResolvedWorkers.pipe(
                    Effect.andThen(
                      Effect.die(new Error("the resolved worker loop ended unexpectedly")),
                    ),
                  ),
                );

                expect(settlement.outcome).toBe("completed");

                const childThread = childThreadIdFor(receipt.submissionId, DELEGATE_CALL);
                const started = yield* assertOneEstablishedChild(thread, childThread);

                yield* assertDelegationSettled(thread, receipt.submissionId, {
                  isFailure: false,
                  result: { summary: PROJECTED_SUMMARY },
                });
                expect(yield* reservationStatuses(receipt.submissionId)).toEqual(["released"]);
                expect(childModelInvocations(site.supplier)).toBe(1);
                yield* assertConvergence(thread, [receipt.submissionId], {
                  site,
                  counts: { [CHILD_MODEL_COUNT_KEY]: 1 },
                });
                yield* assertConvergence(childThread, [started.childSubmissionId]);
              }),
              {
                workerConcurrency: 1,
                wakeScanInterval: 200,
                settlementPollInterval: 50,
              },
              bindings,
            );
          }),
        ),
      30_000,
    );
  },
);
