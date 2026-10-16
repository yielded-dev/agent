import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { describe, expect, it } from "@effect/vitest";
import {
  PostgresStorageFailpointError,
  type PostgresStorageFailpointLocation,
  PostgresWriteContention,
} from "@yielded/agent-storage-postgres/postgres-storage-error";
import { digestJson } from "@yielded/agent/digest";
import {
  CanonicalBatch,
  CanonicalSequence,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
  RecordEnvelope,
  SubmissionSettled,
  SubmissionSettledRecord,
  type PersistedJson,
  type SettlementOutcome,
} from "@yielded/agent/records";
import { runIdForSubmission } from "@yielded/agent/run-journal";
import type { StorageOwner } from "@yielded/agent/run-storage";
import { SettlementPublication, SettlementPublisher } from "@yielded/agent/settlement-publisher";
import {
  type ParentLinkage,
  AbortCommand,
  AdmissionRequest,
  ApprovalDecisionCommand,
  ApprovalPendingSuspension,
  AttachChildToReservationRequest,
  BeginChildBudgetReleaseRequest,
  ChildBudgetReservationRequest,
  ChildReservationId,
  ChildSettledNotification,
  ClaimJoiningRequest,
  ClaimRequest,
  IdempotencyKey,
  LedgerError,
  MarkInputAppliedRequest,
  MarkJoinedRequest,
  MarkReadyRequest,
  MarkUnknownRequest,
  OwnershipLost,
  Principal,
  ReleaseChildBudgetRequest,
  ResolutionCompletedWithResult,
  ResolutionNeverHappened,
  RevertJoiningRequest,
  SettlementFinalization,
  SubmissionLedger,
  SuspendRequest,
  UnknownResolutionCommand,
  WaitingChild,
  WaitingForChildSuspension,
  submissionInputRecordId,
  submissionSettlementId,
  submissionSettlementBatchId,
  submissionSettlementRecordId,
  type AdmissionResult,
} from "@yielded/agent/submission-ledger";
import type { ThreadStore } from "@yielded/agent/thread-store";
import { AppendRequest, ThreadMaterialization } from "@yielded/agent/thread-store";
import * as Cause from "effect/Cause";
import type * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SqlClientService from "effect/sql/SqlClient";

import { withSubmissionOwner } from "../../../test/fixtures/ledger-read-contracts.ts";
import {
  clientLayer,
  storage as makeStorage,
  singleConnectionStorage,
  whileHoldingTableLock,
  withTemporaryDatabase,
} from "./harness.ts";

const id = <A>(schema: Schema.Codec<A, string>, value: string): A =>
  Schema.decodeSync(schema)(value);

const thread = (value: string) => id(ThreadMaterialization.fields.threadId, value);
const sequence = (value: number) => Schema.decodeSync(CanonicalSequence)(value);
const at = (millis: number) => DateTime.toUtc(DateTime.makeUnsafe(millis));

const toolCall = (value: string) => id(ApprovalDecisionCommand.fields.toolCallId, value);
const isLedgerError = Schema.is(LedgerError);
const isPostgresStorageFailpointError = Schema.is(PostgresStorageFailpointError);

const TEST_PRINCIPAL = id(Principal, "principal-postgres-ledger");
const TEST_PRODUCER = id(ProducerId, "producer-postgres-ledger");
const TEST_AGENT = id(AdmissionRequest.fields.agentId, "agent-postgres-ledger");
const TEST_DEPLOYMENT = id(DeploymentId, "deployment-postgres-ledger");
const TEST_DEFINITION_DIGEST = Schema.decodeSync(Digest)("a".repeat(64));

const S2_FAILPOINT_RESERVATION = Schema.decodeSync(ChildReservationId)(
  "child-reservation:run-s2fp:call-1",
);

const TEST_DIGESTS = DefinitionDigests.make({
  agent: TEST_DEFINITION_DIGEST,
  model: TEST_DEFINITION_DIGEST,
  tools: TEST_DEFINITION_DIGEST,
});

const admission = Effect.fn("PostgresLedgerTest.admission")(function* (
  threadId: string,
  idempotencyKey: string,
  input: PersistedJson,
  parentLinkage?: ParentLinkage,
) {
  const inputDigest = yield* digestJson(input);

  return AdmissionRequest.make({
    threadId: thread(threadId),
    principal: TEST_PRINCIPAL,
    idempotencyKey: id(IdempotencyKey, idempotencyKey),
    agentId: TEST_AGENT,
    agentDigests: TEST_DIGESTS,
    deploymentId: TEST_DEPLOYMENT,
    inputPayload: input,
    inputDigest,
    ...(parentLinkage === undefined ? {} : { parentLinkage }),
  });
});

const settlementPublication = Effect.fn("PostgresLedgerTest.settlementPublication")(function* (
  admitted: AdmissionResult,
  threadId: string,
  outcome: SettlementOutcome,
) {
  const settlementId = submissionSettlementId(admitted.submissionId);

  const payload = yield* Schema.decodeEffect(SubmissionSettledRecord)(
    SubmissionSettled.make({
      submissionId: admitted.submissionId,
      settlementId,
      receiptId: admitted.receiptId,
      outcome,
      runId: runIdForSubmission(admitted.submissionId),
      ...(outcome === "failed"
        ? {
            result: {
              errorTag: "PostgresLedgerTestFailure",
              message: "The Postgres ledger test Submission failed",
            },
          }
        : {}),
    }),
  ).pipe(Effect.orDie);

  const record = RecordEnvelope.make({
    recordId: submissionSettlementRecordId(admitted.submissionId),
    family: "thread",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: TEST_DEPLOYMENT,
    payload,
  });

  return SettlementPublication.make({
    submissionId: admitted.submissionId,
    authority: { _tag: "Owned" },
    append: AppendRequest.make({
      threadId: thread(threadId),
      expectedTailSequence: sequence(0),

      batch: CanonicalBatch.make({
        batchId: submissionSettlementBatchId(admitted.submissionId),
        producerId: TEST_PRODUCER,
        records: [record],
      }),
    }),
  });
});

const withLedger = <A, E>(
  url: string,
  effect: Effect.Effect<A, E, SubmissionLedger | SettlementPublisher | Crypto.Crypto>,
) => Effect.provide(effect, [makeStorage(url).submissionLedger, NodeCrypto.layer]);

const withSql = <A, E>(url: string, effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
  Effect.provide(effect, clientLayer(url));

const singleConnectionLedger = (url: string, lockTimeout: number) => {
  const storage = singleConnectionStorage(url, lockTimeout);

  return Layer.mergeAll(storage.submissionLedger, storage.clientLayer, NodeCrypto.layer);
};

const expectInjectedFailure = <A>(
  exit: Exit.Exit<A, unknown>,
  location: PostgresStorageFailpointLocation,
) => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isFailure(exit)) {
    const error = Cause.squash(exit.cause);

    expect(error).toBeInstanceOf(LedgerError);
    if (isLedgerError(error)) {
      expect(error.cause).toBeInstanceOf(PostgresStorageFailpointError);
      if (isPostgresStorageFailpointError(error.cause)) {
        expect(error.cause.location).toBe(location);
      }
    }
  }
};

const makeFailpointHarness = Effect.fnUntraced(function* (url: string) {
  const active = yield* Ref.make<PostgresStorageFailpointLocation | undefined>(undefined);

  const select = (location: PostgresStorageFailpointLocation | undefined) =>
    Ref.set(active, location);

  const storage = makeStorage(url, {
    failpoint: (location) =>
      Ref.get(active).pipe(
        Effect.flatMap((selected) =>
          selected === location
            ? Effect.fail(PostgresStorageFailpointError.make({ location }))
            : Effect.void,
        ),
      ),
  });

  const failingLedger = <A, E>(
    effect: Effect.Effect<
      A,
      E,
      SubmissionLedger | SettlementPublisher | ThreadStore | StorageOwner | Crypto.Crypto
    >,
  ) =>
    Effect.provide(effect, [
      Layer.merge(storage.threadStore, storage.submissionLedger),
      NodeCrypto.layer,
    ]);

  return { select, failingLedger } as const;
});

describe("PostgresSubmissionLedger faults", () => {
  it.effect("classifies a native table lock timeout as retryable typed contention", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        yield* withLedger(
          url,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.admit(yield* admission("thread-busy", "busy-key-1", { step: 1 }));
          }),
        );

        // Hold the administrative table lock after storage initialization. `lock_timeout = 0` disables the bound in Postgres, so the
        // shortest bounded wait is used instead.
        const contended = yield* Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const request = yield* admission("thread-busy", "busy-key-2", { step: 2 });

          return yield* whileHoldingTableLock(
            url,
            ledger.admit(request).pipe(Effect.exit),
            "effect_agent_submissions",
          );
        }).pipe(Effect.provide(singleConnectionLedger(url, 50)));

        expect(Exit.isFailure(contended)).toBe(true);
        if (Exit.isFailure(contended)) {
          const error = Cause.squash(contended.cause);

          expect(error).toBeInstanceOf(LedgerError);
          if (isLedgerError(error)) {
            expect(error.cause).toBeInstanceOf(PostgresWriteContention);
          }
        }

        // Once the administrative transaction releases the lock, the identical admission commits.
        const recovered = yield* withLedger(
          url,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.admit(yield* admission("thread-busy", "busy-key-2", { step: 2 }));
          }),
        );

        expect(recovered.replayed).toBe(false);
      }),
    ),
  );

  it.effect("recovers ledger identity and input progress after lost acknowledgements", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const { select, failingLedger } = yield* makeFailpointHarness(url);

        const submissionStates = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, state, receipt_id, input_applied_record_id, finalized_at
              FROM effect_agent_submissions
              ORDER BY thread_id, queue_sequence
            `;
          }),
        );

        const canonicalRows = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT record_id, payload_json
              FROM effect_agent_canonical_records
            `;
          }),
        );

        const abortRows = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, reason, requested_at
              FROM effect_agent_abort_intents
            `;
          }),
        );

        const lane = "thread-failpoints";

        const admitOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.admit(yield* admission(lane, "failpoint-key", { work: "fail" }));
          }),
        );

        // Admission commits before the lost acknowledgement; retry preserves its identity.
        yield* select("ledger:admit:after");
        expectInjectedFailure(yield* admitOnce.pipe(Effect.exit), "ledger:admit:after");
        const admittedRows = yield* submissionStates;

        expect(admittedRows).toHaveLength(1);
        expect(admittedRows[0]?.state).toBe("admitted");
        yield* select(undefined);
        const admitted = yield* admitOnce;

        expect(admitted.replayed).toBe(true);
        expect(admitted.submissionId).toBe(admittedRows[0]?.submission_id);
        expect(admitted.receiptId).toBe(admittedRows[0]?.receipt_id);

        // Retrying the committed ready transition is a no-op.
        const markReadyOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
          }),
        );

        yield* select("ledger:mark-ready:after");
        expectInjectedFailure(yield* markReadyOnce.pipe(Effect.exit), "ledger:mark-ready:after");
        expect((yield* submissionStates)[0]?.state).toBe("ready");
        yield* select(undefined);
        yield* markReadyOnce;

        // A lost selection acknowledgement leaves the accepted input available to recovery.
        const claimOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.selectHead(ClaimRequest.make({ threadId: thread(lane) }));
          }),
        );

        yield* select("ledger:claim:after");
        expectInjectedFailure(yield* claimOnce.pipe(Effect.exit), "ledger:claim:after");
        expect((yield* submissionStates)[0]?.state).toBe("ready");
        yield* select(undefined);
        const claim = yield* claimOnce;

        expect(Option.isSome(claim)).toBe(true);
        if (Option.isNone(claim)) return;

        const markInputOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger
              .markInputApplied(
                MarkInputAppliedRequest.make({
                  submissionId: admitted.submissionId,
                  recordId: submissionInputRecordId(admitted.submissionId),
                  sequence: sequence(1),
                }),
              )
              .pipe(withSubmissionOwner(admitted.submissionId));
          }),
        );

        yield* select("ledger:mark-input-applied:after");
        expectInjectedFailure(
          yield* markInputOnce.pipe(Effect.exit),
          "ledger:mark-input-applied:after",
        );
        expect((yield* submissionStates)[0]?.input_applied_record_id).toBe(
          submissionInputRecordId(admitted.submissionId),
        );
        expect((yield* submissionStates)[0]?.state).toBe("input-applied");
        yield* select(undefined);
        yield* markInputOnce;

        // Canonical publication retains the terminal intent for recovery.
        const publication = yield* settlementPublication(admitted, lane, "completed");

        const publishOnce = failingLedger(
          Effect.gen(function* () {
            const publisher = yield* SettlementPublisher;

            return yield* publisher
              .publish(publication)
              .pipe(withSubmissionOwner(publication.submissionId));
          }),
        );

        yield* select("append:after");
        expectInjectedFailure(yield* publishOnce.pipe(Effect.exit), "append:after");
        const publishedRows = yield* canonicalRows;

        expect(publishedRows).toHaveLength(1);
        expect(publishedRows[0]?.record_id).toBe(
          JSON.stringify(submissionSettlementRecordId(admitted.submissionId)),
        );
        expect((yield* submissionStates)[0]?.finalized_at).not.toBeNull();
        expect((yield* submissionStates)[0]?.state).toBe("settled");
        yield* select(undefined);
        // Cofinalization closed the owner; recover the committed receipt through finalization.
        expect(yield* publishOnce.pipe(Effect.flip)).toMatchObject({ _tag: "OwnershipLost" });

        const finalizeOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.finalizeSettlement(
              SettlementFinalization.make({
                submissionId: admitted.submissionId,
                settlementId: submissionSettlementId(admitted.submissionId),
              }),
            );
          }),
        );

        yield* select("ledger:finalize-settlement:after");
        expectInjectedFailure(
          yield* finalizeOnce.pipe(Effect.exit),
          "ledger:finalize-settlement:after",
        );
        expect((yield* submissionStates)[0]?.finalized_at).not.toBeNull();
        expect((yield* submissionStates)[0]?.state).toBe("settled");
        yield* select(undefined);
        const settlement = yield* finalizeOnce;

        expect(settlement.outcome).toBe("completed");

        // Retrying the committed abort intent returns it unchanged.
        const abortLane = "thread-failpoints-abort";

        yield* select(undefined);

        const abortAdmitted = yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            const result = yield* ledger.admit(
              yield* admission(abortLane, "failpoint-abort-key", { work: "abort" }),
            );

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: result.submissionId }));

            return result;
          }),
        );

        const abortOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.requestAbort(
              AbortCommand.make({
                submissionId: abortAdmitted.submissionId,
                author: "failpoint-operator",
                reason: "failpoint abort",
              }),
            );
          }),
        );

        yield* select("ledger:request-abort:after");
        expectInjectedFailure(yield* abortOnce.pipe(Effect.exit), "ledger:request-abort:after");
        const abortIntents = yield* abortRows;

        expect(abortIntents).toHaveLength(1);
        yield* select(undefined);
        const intent = yield* abortOnce;

        expect(intent.reason).toBe("failpoint abort");

        // The committed release makes a retry observe OwnershipLost.
      }),
    ),
  );

  it.effect("recovers join, approval and unknown-operation evidence", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const { select, failingLedger } = yield* makeFailpointHarness(url);

        const submissionMarkers = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT
                submission_id,
                state,
                joined_host_submission_id,
                input_applied_record_id,
                suspended_reason_json,
                unknown_reason,
                unknown_tool_call_ids_json
              FROM effect_agent_submissions
              ORDER BY thread_id, queue_sequence
            `;
          }),
        );

        const approvalRows = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, tool_call_id, decision, decided_at
              FROM effect_agent_approval_decisions
              ORDER BY tool_call_id
            `;
          }),
        );

        const resolutionRows = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, tool_call_id, resolution_json
              FROM effect_agent_unknown_resolutions
              ORDER BY tool_call_id
            `;
          }),
        );

        const markerFor = (rows: ReadonlyArray<Record<string, unknown>>, submissionId: string) =>
          rows.find((row) => row.submission_id === submissionId);

        const lane = "thread-p5-failpoints";

        const { host, queued, queuedSecond } = yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            const host = yield* ledger.admit(
              yield* admission(lane, "p5-host-key", { work: "host" }),
            );

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: host.submissionId }));

            const queued = yield* ledger.admit(
              yield* admission(lane, "p5-queued-key", { queued: 2 }),
            );

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: queued.submissionId }));

            const queuedSecond = yield* ledger.admit(
              yield* admission(lane, "p5-queued-second-key", { queued: 3 }),
            );

            yield* ledger.markReady(
              MarkReadyRequest.make({ submissionId: queuedSecond.submissionId }),
            );

            const claim = yield* ledger.selectHead(ClaimRequest.make({ threadId: thread(lane) }));

            if (Option.isNone(claim)) return yield* Effect.die("missing host claim");

            return { host, hostClaim: claim.value, queued, queuedSecond };
          }),
        );

        // Recovery can identify the durable joining state without canonical input.
        const claimJoiningOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger
              .claimJoining(
                ClaimJoiningRequest.make({
                  threadId: thread(lane),
                  hostSubmissionId: host.submissionId,
                  maxCount: 1,
                }),
              )
              .pipe(withSubmissionOwner(host.submissionId));
          }),
        );

        yield* select("ledger:claim-joining:after");
        expectInjectedFailure(
          yield* claimJoiningOnce.pipe(Effect.exit),
          "ledger:claim-joining:after",
        );
        const joiningMarker = markerFor(yield* submissionMarkers, queued.submissionId);

        expect(joiningMarker?.state).toBe("joining");
        expect(joiningMarker?.joined_host_submission_id).toBe(host.submissionId);
        expect(joiningMarker?.input_applied_record_id).toBeNull();
        yield* select(undefined);
        const secondClaims = yield* claimJoiningOnce;

        expect(secondClaims.map((claim) => claim.submissionId)).toEqual([
          queuedSecond.submissionId,
        ]);

        // The committed join marker makes retry an idempotent no-op.
        const markJoinedOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger
              .markJoined(
                MarkJoinedRequest.make({
                  submissionId: queued.submissionId,
                  recordId: submissionInputRecordId(queued.submissionId),
                  sequence: sequence(2),
                }),
              )
              .pipe(withSubmissionOwner(host.submissionId));
          }),
        );

        yield* select("ledger:mark-joined:after");
        expectInjectedFailure(yield* markJoinedOnce.pipe(Effect.exit), "ledger:mark-joined:after");
        const joinedMarker = markerFor(yield* submissionMarkers, queued.submissionId);

        expect(joinedMarker?.state).toBe("joined");
        expect(joinedMarker?.input_applied_record_id).toBe(
          submissionInputRecordId(queued.submissionId),
        );
        yield* select(undefined);
        yield* markJoinedOnce;

        // The committed revert clears the linkage; retry is a no-op.
        const revertOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.revertJoining(
              RevertJoiningRequest.make({ submissionId: queuedSecond.submissionId }),
            );
          }),
        );

        yield* select("ledger:revert-joining:after");
        expectInjectedFailure(yield* revertOnce.pipe(Effect.exit), "ledger:revert-joining:after");
        const revertedMarker = markerFor(yield* submissionMarkers, queuedSecond.submissionId);

        expect(revertedMarker?.state).toBe("ready");
        expect(revertedMarker?.joined_host_submission_id).toBeNull();
        yield* select(undefined);
        yield* revertOnce;

        // Retrying the committed approval decision preserves the recorded intent.
        const decideOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.recordApprovalDecision(
              ApprovalDecisionCommand.make({
                submissionId: host.submissionId,
                toolCallId: toolCall("call-fp-a"),
                decision: "approved",
                resolver: "failpoint-approver",
                reason: "failpoint decision",
              }),
            );
          }),
        );

        yield* select("ledger:approval-decision:after");
        expectInjectedFailure(
          yield* decideOnce.pipe(Effect.exit),
          "ledger:approval-decision:after",
        );
        const decidedRows = yield* approvalRows;

        expect(decidedRows).toHaveLength(1);
        yield* select(undefined);
        const replayedIntent = yield* decideOnce;

        expect(replayedIntent.decision).toBe("approved");
        expect(yield* approvalRows).toHaveLength(1);

        // The durable suspension ends ownership; retry observes OwnershipLost.
        const suspendOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger
              .suspend(
                SuspendRequest.make({
                  submissionId: host.submissionId,
                  reason: ApprovalPendingSuspension.make({ toolCallIds: [toolCall("call-fp-b")] }),
                }),
              )
              .pipe(withSubmissionOwner(host.submissionId));
          }),
        );

        yield* select("ledger:suspend:after");
        expectInjectedFailure(yield* suspendOnce.pipe(Effect.exit), "ledger:suspend:after");
        const suspendedMarker = markerFor(yield* submissionMarkers, host.submissionId);

        expect(suspendedMarker?.state).toBe("suspended");
        expect(suspendedMarker?.suspended_reason_json).not.toBeNull();
        yield* select(undefined);
        const retriedSuspend = yield* suspendOnce.pipe(Effect.exit);

        expect(Exit.isFailure(retriedSuspend)).toBe(true);
        if (Exit.isFailure(retriedSuspend)) {
          expect(Cause.squash(retriedSuspend.cause)).toBeInstanceOf(OwnershipLost);
        }

        // Wake the lane and reclaim it for the unknown-outcome failpoints.
        yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.recordApprovalDecision(
              ApprovalDecisionCommand.make({
                submissionId: host.submissionId,
                toolCallId: toolCall("call-fp-b"),
                decision: "approved",
                resolver: "failpoint-approver",
                reason: "wake the suspended lane",
              }),
            );
          }),
        );
        expect(markerFor(yield* submissionMarkers, host.submissionId)?.state).toBe("input-applied");

        // The unknown marker survives reopen; retry is a no-op.
        const markUnknownOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.markUnknown(
              MarkUnknownRequest.make({
                submissionId: host.submissionId,
                toolCallIds: [toolCall("call-fp-c"), toolCall("call-fp-d")],
                reason: "failpoint uncertainty",
              }),
            );
          }),
        );

        yield* select("ledger:mark-unknown:after");
        expectInjectedFailure(
          yield* markUnknownOnce.pipe(Effect.exit),
          "ledger:mark-unknown:after",
        );
        const unknownMarker = markerFor(yield* submissionMarkers, host.submissionId);

        expect(unknownMarker?.state).toBe("unknown");
        expect(unknownMarker?.unknown_reason).toBe("failpoint uncertainty");
        expect(unknownMarker?.unknown_tool_call_ids_json).not.toBeNull();
        yield* select(undefined);
        yield* markUnknownOnce;

        // Resolution intent and its wake transition must commit atomically.
        const resolveOnce = (call: string, resolution: "never" | "completed") =>
          failingLedger(
            Effect.gen(function* () {
              const ledger = yield* SubmissionLedger;

              return yield* ledger.recordUnknownResolution(
                UnknownResolutionCommand.make({
                  submissionId: host.submissionId,
                  toolCallId: toolCall(call),
                  author: "failpoint-operator",
                  reason: "failpoint resolution",
                  resolution:
                    resolution === "never"
                      ? ResolutionNeverHappened.make()
                      : ResolutionCompletedWithResult.make({
                          result: { bookingRef: "booking-fp-1" },
                          isFailure: false,
                        }),
                }),
              );
            }),
          );

        yield* select("ledger:unknown-resolution:after");
        expectInjectedFailure(
          yield* resolveOnce("call-fp-c", "never").pipe(Effect.exit),
          "ledger:unknown-resolution:after",
        );
        expect(yield* resolutionRows).toHaveLength(1);
        expect(markerFor(yield* submissionMarkers, host.submissionId)?.state).toBe("unknown");
        yield* select(undefined);
        yield* resolveOnce("call-fp-c", "never");

        yield* select("ledger:unknown-resolution:after");
        expectInjectedFailure(
          yield* resolveOnce("call-fp-d", "completed").pipe(Effect.exit),
          "ledger:unknown-resolution:after",
        );
        // The covering resolution and its wake transition are one atomic durable step.
        expect(yield* resolutionRows).toHaveLength(2);
        const wokenMarker = markerFor(yield* submissionMarkers, host.submissionId);

        expect(wokenMarker?.state).toBe("input-applied");
        expect(wokenMarker?.unknown_reason).toBeNull();
        expect(wokenMarker?.unknown_tool_call_ids_json).toBeNull();
        yield* select(undefined);
        const replayedResolution = yield* resolveOnce("call-fp-d", "completed");

        expect(replayedResolution.resolution._tag).toBe("CompletedWithResult");
        expect(yield* resolutionRows).toHaveLength(2);
      }),
    ),
  );

  it.effect("preserves child accounting and wake identity after lost acknowledgements", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const { select, failingLedger } = yield* makeFailpointHarness(url);

        const reservationRows = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT
                reservation_id,
                status,
                child_submission_id,
                accounting_json,
                release_began_at,
                released_at
              FROM effect_agent_child_reservations
            `;
          }),
        );

        const parentMarkers = (submissionId: string) =>
          withSql(
            url,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;

              return yield* sql<Record<string, unknown>>`
                SELECT state, suspended_reason_json
                FROM effect_agent_submissions
                WHERE submission_id = ${submissionId}
              `;
            }),
          );

        const parentLane = "thread-s2-failpoints";
        const childLane = "thread-s2-failpoints-child";
        const reservationId = S2_FAILPOINT_RESERVATION;
        const delegationCall = toolCall("call-s2-fp");

        const { child, parent } = yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            const parent = yield* ledger.admit(
              yield* admission(parentLane, "s2-fp-parent-key", { work: "parent" }),
            );

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: parent.submissionId }));

            const child = yield* ledger.admit(
              yield* admission(childLane, "s2-fp-child-key", { task: "child" }),
            );

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: child.submissionId }));

            const claim = yield* ledger.selectHead(
              ClaimRequest.make({
                threadId: thread(parentLane),
              }),
            );

            if (Option.isNone(claim)) return yield* Effect.die("missing parent claim");

            return { parent, child, parentClaim: claim.value };
          }),
        );

        // Retrying the committed child reservation preserves its identity.
        const allocation = { turns: 2 };

        const allocationDigest = yield* digestJson(allocation).pipe(
          Effect.provide(NodeCrypto.layer),
        );

        const reserveOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger
              .reserveChildBudget(
                ChildBudgetReservationRequest.make({
                  reservationId,
                  parentSubmissionId: parent.submissionId,
                  parentToolCallId: delegationCall,
                  allocation,
                  allocationDigest,
                }),
              )
              .pipe(withSubmissionOwner(parent.submissionId));
          }),
        );

        yield* select("ledger:child-reservation:after");
        expectInjectedFailure(
          yield* reserveOnce.pipe(Effect.exit),
          "ledger:child-reservation:after",
        );
        const reservedRows = yield* reservationRows;

        expect(reservedRows).toHaveLength(1);
        expect(reservedRows[0]?.status).toBe("reserved");
        expect(reservedRows[0]?.child_submission_id).toBeNull();
        yield* select(undefined);
        const replayedReserve = yield* reserveOnce;

        expect(replayedReserve.replayed).toBe(true);

        // The child attachment survives reopen; retry is a no-op.
        const attachOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger
              .attachChildToReservation(
                AttachChildToReservationRequest.make({
                  reservationId,
                  childSubmissionId: child.submissionId,
                }),
              )
              .pipe(withSubmissionOwner(parent.submissionId));
          }),
        );

        yield* select("ledger:child-attach:after");
        expectInjectedFailure(yield* attachOnce.pipe(Effect.exit), "ledger:child-attach:after");
        expect((yield* reservationRows)[0]?.child_submission_id).toBe(child.submissionId);
        yield* select(undefined);
        const replayedAttach = yield* attachOnce;

        expect(replayedAttach.childSubmissionId).toBe(child.submissionId);

        // The pending release preserves its frozen accounting across retry.
        const accounting = { consumed: { turns: 1 }, released: { turns: 1 } };

        const beginOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.beginChildBudgetRelease(
              BeginChildBudgetReleaseRequest.make({ reservationId, accounting }),
            );
          }),
        );

        yield* select("ledger:child-release-pending:after");
        expectInjectedFailure(
          yield* beginOnce.pipe(Effect.exit),
          "ledger:child-release-pending:after",
        );
        const frozenRows = yield* reservationRows;

        expect(frozenRows[0]?.status).toBe("releasePending");
        expect(frozenRows[0]?.accounting_json).not.toBeNull();
        yield* select(undefined);
        const replayedBegin = yield* beginOnce;

        expect(replayedBegin.status).toBe("releasePending");

        // Retry replays the released row; the unused allocation never returns twice.
        const releaseOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.releaseChildBudget(
              ReleaseChildBudgetRequest.make({ reservationId }),
            );
          }),
        );

        yield* select("ledger:child-release:after");
        expectInjectedFailure(yield* releaseOnce.pipe(Effect.exit), "ledger:child-release:after");
        expect((yield* reservationRows)[0]?.status).toBe("released");
        expect((yield* reservationRows)[0]?.released_at).not.toBeNull();
        yield* select(undefined);
        const replayedRelease = yield* releaseOnce;

        expect(replayedRelease.status).toBe("released");

        // The committed wake transition makes retry answer not-waiting.
        yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            const suspended = yield* ledger
              .suspend(
                SuspendRequest.make({
                  submissionId: parent.submissionId,
                  reason: WaitingForChildSuspension.make({
                    children: [
                      WaitingChild.make({
                        toolCallId: delegationCall,
                        childSubmissionId: child.submissionId,
                      }),
                    ],
                  }),
                }),
              )
              .pipe(withSubmissionOwner(parent.submissionId));

            expect(suspended).toBe("suspended");

            const childClaim = yield* ledger.selectHead(
              ClaimRequest.make({
                threadId: thread(childLane),
              }),
            );

            if (Option.isNone(childClaim)) return yield* Effect.die("missing child claim");

            const publisher = yield* SettlementPublisher;

            yield* publisher
              .publish(yield* settlementPublication(child, childLane, "completed"))
              .pipe(withSubmissionOwner(child.submissionId));
            yield* ledger.finalizeSettlement(
              SettlementFinalization.make({
                submissionId: child.submissionId,
                settlementId: submissionSettlementId(child.submissionId),
              }),
            );
          }),
        );

        const notifyOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.recordChildSettled(
              ChildSettledNotification.make({
                parentSubmissionId: parent.submissionId,
                childSubmissionId: child.submissionId,
              }),
            );
          }),
        );

        yield* select("ledger:child-settled:after");
        expectInjectedFailure(yield* notifyOnce.pipe(Effect.exit), "ledger:child-settled:after");
        const wokenMarkers = yield* parentMarkers(parent.submissionId);

        expect(wokenMarkers[0]?.state).toBe("input-applied");
        expect(wokenMarkers[0]?.suspended_reason_json).toBeNull();
        yield* select(undefined);
        const replayedNotification = yield* notifyOnce;

        expect(replayedNotification).toBe("not-waiting");
      }),
    ),
  );
});
