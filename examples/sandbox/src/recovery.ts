import { ThreadMutationGate } from "@yielded/agent-platform-cloudflare/alarm";
import { ThreadObjectIdentity } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import { DurableAgentRuntime } from "@yielded/agent/durable-agent-runtime";
import { readRunEvidenceSnapshot } from "@yielded/agent/run-continuation";
import {
  SubmissionLedger,
  SubmissionLookupById,
  submissionSettlementRecordId,
} from "@yielded/agent/submission-ledger";
import {
  getRecord,
  getRunInput,
  ThreadReader,
  ThreadTailRequest,
} from "@yielded/agent/thread-reader";
import type { ThreadWorkRequest } from "@yielded/agent/thread-work";
import {
  ReconciliationNeverStarted,
  ReconciliationUncertain,
  ToolReconciler,
  ToolReconcilerError,
} from "@yielded/agent/tool-reconciler";
import { Effect, Layer, Option } from "effect";
import * as Crypto from "effect/Crypto";

import { codingTools } from "./tools.ts";

export const codingReconciler = Layer.effect(
  ToolReconciler,
  Effect.gen(function* () {
    const { threadId } = yield* ThreadObjectIdentity;
    const reader = yield* ThreadReader;
    const crypto = yield* Crypto.Crypto;

    const uncertain = ReconciliationUncertain.make({
      reason: "The original native operation still needs its receipt.",
    });

    return ToolReconciler.of({
      reconcile: (evidence) =>
        Effect.gen(function* () {
          if (
            evidence.threadId !== threadId ||
            evidence.executionKind !== "ordinary" ||
            evidence.executionClass !== "idempotent" ||
            !Object.hasOwn(codingTools.tools, evidence.toolName)
          )
            return uncertain;
          const tail = yield* reader.inspectTail(ThreadTailRequest.make({ threadId }));

          const records = yield* readRunEvidenceSnapshot(
            threadId,
            evidence.submissionId,
            tail.tailSequence,
          );

          // These handlers dispatch only after committing allocation. A complete history with
          // no steps proves nonexecution; a reserved or dispatched ticket must retain its identity.
          return records.some(
            ({ record: { payload } }) =>
              payload._tag === "ToolStepSettled" &&
              payload.runId === evidence.runId &&
              payload.toolCallId === evidence.toolCallId,
          )
            ? uncertain
            : ReconciliationNeverStarted.make({});
        }).pipe(
          Effect.provideService(ThreadReader, reader),
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.mapError((cause) =>
            ToolReconcilerError.make({
              toolCallId: evidence.toolCallId,
              message: "The original coding-call history could not be verified.",
              cause,
            }),
          ),
        ),
    });
  }),
);

/** Recheck through the normal engine; a button cannot authorize replay or replace a receipt. */
export const recoverWorkspace = Effect.fn("Sandbox.recoverWorkspace")(function* () {
  const { threadId } = yield* ThreadObjectIdentity;
  const runtime = yield* DurableAgentRuntime;
  const mutations = yield* ThreadMutationGate;
  const result = yield* mutations.withMutation(runtime.runRecovery({ threadId }));

  if (result.blocked[0] !== undefined) return yield* result.blocked[0];

  return {
    recovered:
      result.workReports?.filter((report) => report.disposition === "repaired").length ?? 0,
    reviewRequired: yield* workspaceRecoveryRequired(),
  };
});

/** Observes the existing owners; the browser cannot infer that an operation is resolved. */
export const workspaceRecoveryRequired = Effect.fnUntraced(function* () {
  const { threadId } = yield* ThreadObjectIdentity;
  const runtime = yield* DurableAgentRuntime;
  const ledger = yield* SubmissionLedger;
  let cursor: ThreadWorkRequest["cursor"];

  do {
    const page = yield* runtime.discoverWork({
      threadId,
      limit: 32,
      ...(cursor === undefined ? {} : { cursor }),
    });

    for (const { owner } of page.entries) {
      if (owner._tag !== "Operation") continue;
      const input = yield* getRunInput({ threadId, runId: owner.runId });

      if (Option.isNone(input)) return true;
      const source = input.value.record.payload;

      if (source._tag !== "UserInputRecorded" || source.submissionId === undefined) return true;

      // Canonical settlement can precede the ledger's publication update.
      const terminal = yield* getRecord({
        threadId,
        recordId: submissionSettlementRecordId(source.submissionId),
      });

      if (Option.isSome(terminal)) return true;

      const original = yield* ledger.lookup(
        SubmissionLookupById.make({ submissionId: source.submissionId }),
      );

      if (
        Option.isNone(original) ||
        original.value.threadId !== threadId ||
        original.value.state === "settled" ||
        original.value.state === "unknown" ||
        original.value.state === "suspended"
      )
        return true;
    }
    cursor = page.cursor;
  } while (cursor !== undefined);

  return false;
});
