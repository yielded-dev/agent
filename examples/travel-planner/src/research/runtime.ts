import {
  CanonicalSequence,
  MAX_RUN_EVIDENCE_RECORDS,
  RUN_TERMINAL_RESERVE_RECORDS,
  MAX_RUN_RECOVERY_SUFFIX_RECORDS,
} from "@yielded/agent/records";
import { RunToolAuthorization } from "@yielded/agent/run-options";
import { SubmissionLedger, SubmissionLookupById } from "@yielded/agent/submission-ledger";
import {
  getRunInput,
  ThreadReader,
  ThreadStore,
  ThreadTailRequest,
} from "@yielded/agent/thread-store";
import { FrameworkMessage } from "@yielded/agent/worker";
import { Effect, Layer, Option, Schema, Stream } from "effect";

import { PlannerError, PlannerInput } from "../domain.ts";
import { PlannerAttempt, ProgressStore } from "../server/progress.ts";
import { publicationAuthorization } from "../server/security.ts";
import { ownerOfThread } from "../server/tenancy.ts";
import { CheckedFinishResearchLive } from "./completion.ts";
import { researchCoordinatorId, ScoutInput } from "./contracts.ts";
import { UpdatingResearchScout, updatingResearchScout } from "./scout.ts";

const unavailable = () =>
  new PlannerError({
    code: "unavailable",
    message: "The admitted research task could not be verified.",
  });

/** Load the canonical owner input, including for failed Runs and joined steering receipts. */
export const readScoutInput = Effect.fn("readScoutInput")(function* (
  submissionId: SubmissionLookupById["submissionId"],
) {
  const ledger = yield* SubmissionLedger;

  const found = yield* ledger
    .lookup(SubmissionLookupById.make({ submissionId }))
    .pipe(Effect.mapError(unavailable));

  if (Option.isNone(found)) return yield* unavailable();
  const submission = found.value;
  const origin = submission.workerAdmission?.origin;

  const input = yield* Schema.decodeUnknownEffect(ScoutInput)(submission.inputPayload).pipe(
    Effect.mapError(unavailable),
  );

  if (
    origin === undefined ||
    origin.worker.threadId !== submission.threadId ||
    origin.worker.targetAgentId !== updatingResearchScout.id ||
    origin.worker.delegationId !== UpdatingResearchScout.delegationId ||
    origin.source.agentId !== researchCoordinatorId ||
    origin.source.threadId !== input.sourceThreadId ||
    origin.depth !== 1
  )
    return yield* unavailable();

  return { input, submission, origin };
});

export const scoutAttemptLayer = (context: {
  readonly threadId: string;
  readonly submissionId: SubmissionLookupById["submissionId"];
  readonly attemptId: string;
}) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const progress = yield* ProgressStore;
      const ledger = yield* SubmissionLedger;

      const input = yield* Effect.cached(
        readScoutInput(context.submissionId).pipe(
          Effect.provideService(SubmissionLedger, ledger),
          Effect.flatMap((captured) =>
            captured.submission.threadId === context.threadId
              ? Effect.succeed(captured.input)
              : Effect.fail(unavailable()),
          ),
        ),
      );

      const writer = yield* Effect.acquireRelease(
        progress.begin(context.submissionId, context.attemptId),
        (writer) => writer.finish,
      );

      return Layer.mergeAll(
        CheckedFinishResearchLive,
        Layer.succeed(PlannerAttempt, {
          billingOwner: Effect.map(input, (input) => ownerOfThread(input.sourceThreadId)),
          settings: Effect.map(input, (input) => input.settings),
          progress: writer,
        }),
      );
    }),
  );

/** A completion report cannot create another research generation without a new canonical user input. */
export const ResearchAuthorizationLive = Layer.effect(
  RunToolAuthorization,
  Effect.gen(function* () {
    const store = yield* ThreadStore;

    return RunToolAuthorization.of({
      authorize: (request) => {
        if (
          request.frameworkMessage === undefined ||
          ![
            "research_scout_start",
            "research_scout_follow_up",
            "app_editor_start",
            "app_editor_follow_up",
          ].includes(request.call.toolName)
        )
          return publicationAuthorization.authorize(request);

        const denied = {
          _tag: "denied" as const,
          reason:
            "Report the completed research. A new user request is required to start or steer another research pass.",
        };

        return Effect.gen(function* () {
          const input = yield* getRunInput({
            threadId: request.threadId,
            runId: request.runId,
          }).pipe(Effect.provideService(ThreadReader, ThreadReader.fromStore(store)));

          if (
            Option.isNone(input) ||
            input.value.record.payload._tag !== "UserInputRecorded" ||
            input.value.record.payload.submissionId === undefined
          )
            return denied;
          const submissionId = input.value.record.payload.submissionId;

          const tail = yield* store.inspectTail(
            ThreadTailRequest.make({ threadId: request.threadId }),
          );

          let after = CanonicalSequence.make(0);
          let count = 0;

          const maximum =
            2 * (MAX_RUN_EVIDENCE_RECORDS + RUN_TERMINAL_RESERVE_RECORDS) +
            MAX_RUN_RECOVERY_SUFFIX_RECORDS;

          while (count < maximum) {
            const page = yield* store
              .read({
                threadId: request.threadId,
                selection: {
                  _tag: "RunEvidence",
                  runId: request.runId,
                  submissionId,
                  throughSequence: tail.tailSequence,
                },
                page: { limit: 8, afterSequence: after },
              })
              .pipe(Stream.runCollect);

            for (const entry of page) {
              if (entry.sequence <= after) return denied;
              after = entry.sequence;
              count++;
              const payload = entry.record.payload;

              if (
                payload._tag === "UserInputRecorded" &&
                payload.runId === request.runId &&
                !Schema.is(FrameworkMessage)(payload.messageAdmission) &&
                Schema.is(PlannerInput)(payload.input)
              )
                return { _tag: "allowed" as const };
            }
            if (page.length < 8) return denied;
          }

          return denied;
        }).pipe(Effect.orElseSucceed(() => denied));
      },
    });
  }),
);
