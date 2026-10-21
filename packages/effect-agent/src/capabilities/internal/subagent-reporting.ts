import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { utf8ByteLength } from "../../core/internal/utf8.ts";
import { Receipt } from "../../core/Receipt.ts";
import { type WorkerReport, WorkerCompletion } from "../../core/Worker.ts";
import {
  WorkerReportPreparationFailure,
  type WorkerRunReport,
  type WorkerReporting,
} from "../../engine/SubagentHost.ts";
import { type Declaration } from "./subagent-background.ts";
import { SubagentExecutionFailure } from "./subagent-contract.ts";

export { WorkerReport } from "../../core/Worker.ts";

export interface ReportingOptions {
  /**
   * Select canonical updates to notify the parent. Defaults to all. Decode the encoded value
   * with the child's update Schema. The first accepted emission freezes this decision.
   */
  readonly reportUpdate?: WorkerReporting["reportUpdate"];
  /**
   * Select settled Runs before projection. False retains a filtered decision without parent
   * delivery. Recovery reuses a committed decision, but may reevaluate before it commits.
   */
  readonly reportCompletion?: WorkerReporting["reportCompletion"];
}

const projectReport = <
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
>(
  declaration: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
) =>
  Effect.fnUntraced(function* (report: WorkerRunReport) {
    const { worker, observation } = report;
    const invalid = () => WorkerReportPreparationFailure.make({ stage: "projection" });

    if (
      worker.delegationId !== declaration.delegationId ||
      worker.targetAgentId !== declaration.target.id ||
      observation.receipt.threadId !== worker.threadId
    )
      return yield* invalid();

    const base = {
      _tag: "Settled" as const,
      worker,
      receipt: yield* Schema.decodeEffect(Receipt)(observation.receipt).pipe(
        Effect.mapError(invalid),
      ),
      runId: observation.runId,
      settlementId: observation.settlementId,
    };

    let projected: WorkerReport<Success>;
    let encodedResult: Schema.Json = null;

    if (observation.outcome === "completed") {
      const parameters = yield* Schema.decodeUnknownEffect(declaration.parameters)(
        observation.encodedParameters,
      ).pipe(Effect.mapError(invalid));

      const output = yield* Schema.decodeUnknownEffect(declaration.target.output)(
        observation.encodedResult,
      ).pipe(Effect.mapError(invalid));

      const result = yield* declaration.projectResult(
        output,
        { budgetExhausted: observation.budgetExhausted },
        parameters,
      );

      const encoded = yield* Schema.encodeEffect(declaration.success)(result).pipe(
        Effect.mapError(invalid),
      );

      const json = yield* Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
        Effect.mapError(invalid),
      );

      if (
        utf8ByteLength(JSON.stringify(json)) >
        (declaration.policy?.maxResultBytes ?? declaration.target.policy.toolResultBounds.maxBytes)
      )
        return yield* invalid();
      projected = { ...base, outcome: "completed", result };
      encodedResult = json;
    } else {
      projected = {
        ...base,
        outcome: observation.outcome,
        failure: SubagentExecutionFailure.make({
          delegationId: declaration.delegationId,
          targetAgentId: declaration.target.id,
          classification: observation.outcome === "failed" ? "child-failed" : "child-aborted",
          childThreadId: worker.threadId,
          childSubmissionId: observation.receipt.submissionId,
          childRunId: observation.runId,
          errorTag: observation.outcome === "failed" ? "WorkerFailed" : "WorkerAborted",
          message:
            observation.outcome === "failed" ? "Worker input failed" : "Worker input was aborted",
        }),
      };
    }

    return { projected, encodedResult };
  });

/** Register this standard completion projection on the parent; registration captures its services. */
export const reporting = <
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
>(
  declaration: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
  options: ReportingOptions = {},
) => ({
  delegationId: declaration.delegationId,
  target: declaration.target,
  ...options,
  prepare: Effect.fnUntraced(function* (report: WorkerRunReport) {
    const { projected, encodedResult } = yield* projectReport(declaration)(report);

    const encoded =
      projected.outcome === "completed"
        ? { ...projected, result: encodedResult }
        : {
            ...projected,
            failure: yield* Schema.encodeEffect(SubagentExecutionFailure)(projected.failure).pipe(
              Effect.mapError(() => WorkerReportPreparationFailure.make({ stage: "projection" })),
            ),
          };

    const message = yield* Schema.decodeEffect(WorkerCompletion)({
      _tag: "WorkerCompletion",
      schemaVersion: 1,
      report: encoded,
      budgetExhausted: report.observation.budgetExhausted,
    }).pipe(Effect.mapError(() => WorkerReportPreparationFailure.make({ stage: "projection" })));

    return { message };
  }),
});
