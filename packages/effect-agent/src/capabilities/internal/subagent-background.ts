import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import type * as Brand from "effect/Brand";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import type { AnyDefinition } from "../../core/Agent.ts";
import * as FailureDiagnostic from "../../core/FailureDiagnostic.ts";
import { RunId, SettlementId, ThreadId } from "../../core/Identifiers.ts";
import { utf8ByteLength, utf8Bytes } from "../../core/internal/utf8.ts";
import { MessageRef, MessageStatus } from "../../core/Messaging.ts";
import { IdempotencyKey, JoinedToHost, Receipt } from "../../core/Receipt.ts";
import {
  BackgroundSpawnTool,
  narrowSubagentGrant,
  SubagentGrant,
} from "../../core/SubagentContract.ts";
import {
  WorkerContext,
  type WorkerBudgetScope,
  WorkerError,
  WorkerOperationTool,
  WorkerPage,
  WorkerRef,
  WorkerStarted,
  WorkerStop,
  WorkerStopped,
  WorkerSummary,
} from "../../core/Worker.ts";
import { SubagentHost } from "../../engine/SubagentHost.ts";
import type { SubagentDefineOptions, SubagentPrepareContext } from "../Subagent.ts";
import {
  SubagentExecutionFailure,
  SubagentPrestartDenied,
  SubagentProjectionFailure,
} from "./subagent-contract.ts";

export type Declaration<
  Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
> = Pick<
  SubagentDefineOptions<
    Input,
    Output,
    unknown,
    Record<string, Tool.Any>,
    Parameters,
    Success,
    Failure,
    Prepare,
    Project
  >,
  | "parameters"
  | "success"
  | "failure"
  | "prepareInput"
  | "projectResult"
  | "policy"
  | "needsApproval"
> & {
  readonly name: Name;
  readonly delegationId: WorkerRef["delegationId"];
  readonly target: AnyDefinition & { readonly input: Input; readonly output: Output };
  readonly grant: SubagentGrant;
};

type WorkerDeclaration<Name extends string> = {
  readonly name: Name;
  readonly delegationId: WorkerRef["delegationId"];
  readonly target: Pick<AnyDefinition, "id">;
};

/** Portable identity checked against this declaration; possession grants no authority. */
export const Worker = <const Name extends string>(declaration: WorkerDeclaration<Name>) =>
  WorkerRef.pipe(
    Schema.refine(
      (worker): worker is WorkerRef & Brand.Brand<`@effect-agent/Worker/${Name}`> =>
        worker.delegationId === declaration.delegationId &&
        worker.targetAgentId === declaration.target.id,
    ),
  );

export type Worker<Name extends string> = ReturnType<typeof Worker<Name>>["Type"];

/** Model-facing projection deliberately excludes operator-private diagnostics. */
const toolObservation = <Success extends Schema.Top>(success: Success) =>
  Schema.Union([
    Schema.Struct({ _tag: Schema.Literal("Pending"), receipt: Receipt }),
    Schema.Struct({
      _tag: Schema.Literal("Settled"),
      receipt: Receipt,
      runId: RunId,
      settlementId: SettlementId,
      outcome: Schema.Literal("completed"),
      result: success,
    }),
    Schema.Struct({
      _tag: Schema.Literal("Settled"),
      receipt: Receipt,
      runId: Schema.optionalKey(RunId),
      settlementId: SettlementId,
      outcome: Schema.Literals(["failed", "aborted"]),
      failure: SubagentExecutionFailure,
    }),
  ]);

/** One exact Receipt, including private canonical failure evidence for programmatic callers. */
export const WorkerObservation = <Success extends Schema.Top>(success: Success) => {
  const projected = toolObservation(success);

  return Schema.Union([
    projected.members[0],
    projected.members[1],
    Schema.Struct({
      ...projected.members[2].fields,
      diagnostic: Schema.optionalKey(FailureDiagnostic.Failure),
    }),
  ]);
};

export type WorkerObservation<Success extends Schema.Top> = ReturnType<
  typeof WorkerObservation<Success>
>["Type"];

const host: Effect.Effect<SubagentHost["Service"], WorkerError, SubagentHost> =
  Effect.serviceOption(SubagentHost).pipe(
    Effect.flatMap((service) =>
      Option.match(service, {
        onNone: () => WorkerError.make({ operation: "context", reason: "unavailable" }),
        onSome: Effect.succeed,
      }),
    ),
  );

const receiptEqual = (left: Receipt, right: Receipt): boolean =>
  left.threadId === right.threadId &&
  left.submissionId === right.submissionId &&
  left.receiptId === right.receiptId &&
  left.queueSequence === right.queueSequence;

const operations = <
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
) => {
  const workerSchema = Worker(declaration);

  const projectionFailure = (stage: "input" | "result", cause?: unknown) =>
    SubagentProjectionFailure.make({
      delegationId: declaration.delegationId,
      stage,
      ...(cause === undefined ? {} : { cause }),
      message:
        stage === "input"
          ? "Worker input did not satisfy the declaration Schema"
          : "Worker result did not satisfy the declaration Schema",
    });

  const validateWorker = (worker: unknown, operation: WorkerError["operation"]) =>
    Schema.decodeUnknownEffect(workerSchema)(worker).pipe(
      Effect.mapError((cause) => WorkerError.make({ operation, reason: "worker-mismatch", cause })),
    );

  const validateReceipt = (
    worker: WorkerRef,
    receipt: Receipt,
    operation: WorkerError["operation"],
  ) =>
    Schema.decodeEffect(
      Receipt.check(Schema.makeFilter((value) => value.threadId === worker.threadId)),
    )(receipt).pipe(
      Effect.mapError((cause) =>
        WorkerError.make({ operation, reason: "receipt-mismatch", cause }),
      ),
    );

  const validateDelivery = Effect.fnUntraced(function* (
    worker: WorkerRef,
    delivery: MessageStatus,
    operation: WorkerError["operation"],
  ) {
    const validated = yield* Schema.decodeEffect(MessageStatus)(delivery).pipe(
      Effect.mapError((cause) => WorkerError.make({ operation, reason: "corrupt", cause })),
    );

    if (validated.receipt !== null) yield* validateReceipt(worker, validated.receipt, operation);

    return validated;
  });

  const context: Effect.Effect<WorkerContext, WorkerError, SubagentHost> = host.pipe(
    Effect.flatMap((service) => service.context),
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.toType(WorkerContext))),
    Effect.mapError((error) =>
      Schema.is(WorkerError)(error)
        ? error
        : WorkerError.make({ operation: "context", reason: "corrupt", cause: error }),
    ),
  );

  const prepare = Effect.fnUntraced(function* (
    parameters: Parameters["Type"],
    caller: WorkerContext,
  ) {
    const source = caller.source;

    const preparation: SubagentPrepareContext =
      source._tag === "tool"
        ? {
            source: "tool",
            delegationId: declaration.delegationId,
            toolCallId: source.toolCallId,
            parent: { agentId: source.agentId, threadId: source.threadId, runId: source.runId },
          }
        : {
            source: "programmatic",
            delegationId: declaration.delegationId,
            parent: { agentId: source.agentId, threadId: source.threadId },
          };

    const input = yield* declaration.prepareInput(parameters, preparation);

    const encodedInput = yield* Schema.encodeEffect(declaration.target.input)(input).pipe(
      Effect.mapError((cause) => projectionFailure("input", cause)),
    );

    return encodedInput;
  });

  const validateKey = (key: IdempotencyKey, operation: "start" | "followUp") =>
    Schema.decodeEffect(IdempotencyKey)(key).pipe(
      Effect.mapError((cause) =>
        WorkerError.make({ operation, reason: "idempotency-conflict", cause }),
      ),
    );

  const start = Effect.fnUntraced(function* (
    parameters: Parameters["Type"],
    options: {
      readonly idempotencyKey: IdempotencyKey;
      readonly budgetScope?: WorkerBudgetScope;
      readonly continuationOf?: WorkerRef;
    },
  ) {
    const service = yield* host;
    const caller = yield* context;
    const key = yield* validateKey(options.idempotencyKey, "start");

    const grant = narrowSubagentGrant(declaration.grant, caller.grant);

    if (caller.depth + 1 > grant.maxDepth) {
      return yield* SubagentPrestartDenied.make({
        delegationId: declaration.delegationId,
        targetAgentId: declaration.target.id,
        reason: "nested-delegation",
        message: "Worker start exceeds the inherited depth ceiling",
      });
    }
    if (!(caller.grant?.childLifetimes ?? ["attached", "background"]).includes("background")) {
      return yield* SubagentPrestartDenied.make({
        delegationId: declaration.delegationId,
        targetAgentId: declaration.target.id,
        reason: "grant-violation",
        message: "The inherited grant does not permit background children",
      });
    }

    const encodedParameters = yield* Schema.encodeEffect(declaration.parameters)(parameters).pipe(
      Effect.mapError((cause) => projectionFailure("input", cause)),
    );

    const encodedGrant = yield* Schema.encodeEffect(SubagentGrant)(grant).pipe(
      Effect.mapError((cause) => projectionFailure("input", cause)),
    );

    const started = yield* service.start({
      delegationId: declaration.delegationId,
      target: declaration.target,
      idempotencyKey: key,
      encodedParameters,
      encodedGrant,
      ...(options.continuationOf === undefined ? {} : { continuationOf: options.continuationOf }),
      ...(options.budgetScope === undefined ? {} : { budgetScope: options.budgetScope }),
      ...(declaration.policy === undefined ? {} : { declarationPolicy: declaration.policy }),
      prepare: Effect.map(prepare(parameters, caller), (encodedInput) => ({ encodedInput })),
    });

    const validated = yield* Schema.decodeEffect(WorkerStarted)(started).pipe(
      Effect.mapError((cause) =>
        WorkerError.make({ operation: "start", reason: "corrupt", cause }),
      ),
    );

    if (validated.delivery.message.ownerThreadId !== caller.source.threadId)
      return yield* WorkerError.make({ operation: "start", reason: "message-mismatch" });

    return {
      worker: yield* validateWorker(validated.worker, "start"),
      delivery: validated.delivery,
    };
  });

  const followUp = Effect.fnUntraced(function* (
    worker: Worker<Name>,
    parameters: Parameters["Type"],
    options: { readonly idempotencyKey: IdempotencyKey },
  ) {
    const service = yield* host;
    const validated = yield* validateWorker(worker, "followUp");
    const caller = yield* context;
    const key = yield* validateKey(options.idempotencyKey, "followUp");

    const encodedParameters = yield* Schema.encodeEffect(declaration.parameters)(parameters).pipe(
      Effect.mapError((cause) => projectionFailure("input", cause)),
    );

    const delivery = yield* validateDelivery(
      validated,
      yield* service.followUp({
        encodedParameters,
        prepare: prepare(parameters, caller).pipe(Effect.map((encodedInput) => ({ encodedInput }))),
        worker: validated,
        target: declaration.target,
        idempotencyKey: key,
      }),
      "followUp",
    );

    if (delivery.message.ownerThreadId !== caller.source.threadId)
      return yield* WorkerError.make({ operation: "followUp", reason: "message-mismatch" });

    return delivery;
  });

  const inspectMessage = Effect.fnUntraced(function* (worker: Worker<Name>, message: MessageRef) {
    const service = yield* host;
    const validated = yield* validateWorker(worker, "inspect");

    const requested = yield* Schema.decodeEffect(MessageRef)(message).pipe(
      Effect.mapError((cause) =>
        WorkerError.make({ operation: "inspect", reason: "message-mismatch", cause }),
      ),
    );

    const observed = yield* service.inspect({
      worker: validated,
      target: declaration.target,
      message: requested,
    });

    if (!("message" in observed))
      return yield* WorkerError.make({ operation: "inspect", reason: "corrupt" });
    const delivery = yield* validateDelivery(validated, observed, "inspect");

    if (!Schema.toEquivalence(MessageRef)(delivery.message, requested))
      return yield* WorkerError.make({ operation: "inspect", reason: "message-mismatch" });

    return delivery;
  });

  const observe = Effect.fnUntraced(function* (
    operation: "inspect" | "await",
    worker: Worker<Name>,
    receipt: Receipt,
  ): Effect.fn.Return<
    WorkerObservation<Success>,
    WorkerError | SubagentProjectionFailure | Failure["Type"],
    | SubagentHost
    | Parameters["DecodingServices"]
    | Output["DecodingServices"]
    | Success["EncodingServices"]
    | Project
  > {
    const service = yield* host;
    const validated = yield* validateWorker(worker, operation);
    const requested = yield* validateReceipt(validated, receipt, operation);

    const observed = yield* service[operation]({
      worker: validated,
      target: declaration.target,
      receipt: requested,
    });

    if (!("_tag" in observed)) return yield* WorkerError.make({ operation, reason: "corrupt" });

    const returned = yield* validateReceipt(validated, observed.receipt, operation);

    if (!receiptEqual(returned, requested))
      return yield* WorkerError.make({ operation, reason: "receipt-mismatch" });
    if (observed._tag === "Pending") {
      if (operation === "await") return yield* WorkerError.make({ operation, reason: "corrupt" });

      return { _tag: "Pending", receipt: returned };
    }

    const base = {
      _tag: "Settled" as const,
      receipt: returned,
      ...(observed.runId === undefined
        ? {}
        : {
            runId: yield* Schema.decodeEffect(RunId)(observed.runId).pipe(
              Effect.mapError((cause) => WorkerError.make({ operation, reason: "corrupt", cause })),
            ),
          }),
      settlementId: yield* Schema.decodeEffect(SettlementId)(observed.settlementId).pipe(
        Effect.mapError((cause) => WorkerError.make({ operation, reason: "corrupt", cause })),
      ),
    };

    if (observed.outcome === "failed" || observed.outcome === "aborted") {
      return {
        ...base,
        outcome: observed.outcome,
        ...(observed.diagnostic === undefined
          ? {}
          : {
              diagnostic: yield* Schema.decodeEffect(FailureDiagnostic.Failure)(
                observed.diagnostic,
              ).pipe(
                Effect.mapError((cause) =>
                  WorkerError.make({ operation, reason: "corrupt", cause }),
                ),
              ),
            }),
        failure: SubagentExecutionFailure.make({
          delegationId: declaration.delegationId,
          targetAgentId: declaration.target.id,
          classification: observed.outcome === "failed" ? "child-failed" : "child-aborted",
          childThreadId: worker.threadId,
          childSubmissionId: returned.submissionId,
          ...(base.runId === undefined ? {} : { childRunId: base.runId }),
          errorTag: observed.outcome === "failed" ? "WorkerFailed" : "WorkerAborted",
          message:
            observed.outcome === "failed" ? "Worker input failed" : "Worker input was aborted",
        }),
      };
    }
    if (
      observed.outcome !== "completed" ||
      base.runId === undefined ||
      typeof observed.budgetExhausted !== "boolean"
    ) {
      return yield* WorkerError.make({ operation, reason: "corrupt" });
    }

    const parameters = yield* Schema.decodeUnknownEffect(declaration.parameters)(
      observed.encodedParameters,
    ).pipe(Effect.mapError((cause) => projectionFailure("result", cause)));

    const output = yield* Schema.decodeUnknownEffect(declaration.target.output)(
      observed.encodedResult,
    ).pipe(Effect.mapError((cause) => projectionFailure("result", cause)));

    const result = yield* declaration.projectResult(
      output,
      { budgetExhausted: observed.budgetExhausted },
      parameters,
    );

    const encoded = yield* Schema.encodeEffect(declaration.success)(result).pipe(
      Effect.mapError((cause) => projectionFailure("result", cause)),
    );

    const json = yield* Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
      Effect.mapError((cause) => projectionFailure("result", cause)),
    );

    const caller = yield* context;

    if (
      utf8ByteLength(JSON.stringify(json)) >
      (declaration.policy?.maxResultBytes ?? caller.policy.toolResultBounds.maxBytes)
    ) {
      return yield* projectionFailure("result");
    }

    return yield* Schema.decodeUnknownEffect(Schema.toType(WorkerObservation(declaration.success)))(
      { ...base, outcome: "completed", result },
    ).pipe(Effect.mapError((cause) => projectionFailure("result", cause)));
  });

  const list = Effect.fnUntraced(function* (
    options: { readonly limit?: number; readonly after?: ThreadId } = {},
  ) {
    const service = yield* host;

    const limit = yield* Schema.decodeEffect(
      Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)),
    )(options.limit ?? 20).pipe(
      Effect.mapError(() => WorkerError.make({ operation: "list", reason: "capacity" })),
    );

    const page = yield* service.list({
      delegationId: declaration.delegationId,
      target: declaration.target,
      limit,
      ...(options.after === undefined ? {} : { after: options.after }),
    });

    const validated = yield* Schema.decodeEffect(WorkerPage)(page).pipe(
      Effect.mapError(() => WorkerError.make({ operation: "list", reason: "corrupt" })),
    );

    if (validated.items.length > limit)
      return yield* WorkerError.make({ operation: "list", reason: "corrupt" });
    const items = [];

    for (const item of validated.items) {
      const worker = yield* validateWorker(item.worker, "list");

      const latestReceipt =
        item.latestReceipt === null
          ? null
          : yield* validateReceipt(worker, item.latestReceipt, "list");

      items.push({ ...item, worker, latestReceipt });
    }

    return { items, next: validated.next };
  });

  const cancel = Effect.fnUntraced(function* (worker: Worker<Name>, receipt: Receipt) {
    const service = yield* host;
    const validated = yield* validateWorker(worker, "cancel");
    const requested = yield* validateReceipt(validated, receipt, "cancel");

    yield* service.cancel({ worker: validated, target: declaration.target, receipt: requested });
  });

  return { start, followUp, inspectMessage, observe, list, cancel };
};

/** Retain a first worker input. The same explicit key reuses its durable delivery identity. */
export const start = <
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
  parameters: Parameters["Type"],
  options: {
    readonly idempotencyKey: IdempotencyKey;
    readonly budgetScope?: WorkerBudgetScope;
    readonly continuationOf?: WorkerRef;
  },
) => operations(declaration).start(parameters, options);

/** Retain typed follow-up parameters for the same worker Thread; inspect its MessageRef for delivery. */
export const followUp = <
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
  worker: Worker<Name>,
  parameters: Parameters["Type"],
  options: { readonly idempotencyKey: IdempotencyKey },
) => operations(declaration).followUp(worker, parameters, options);

/** Inspect a continuing worker with the same summary contract returned by list. */
export const summary = Effect.fnUntraced(function* <const Name extends string>(
  declaration: WorkerDeclaration<Name> & { readonly target: AnyDefinition },
  worker: Worker<Name>,
) {
  const service = yield* host;

  const validated = yield* Schema.decodeEffect(Worker(declaration))(worker).pipe(
    Effect.mapError(() => WorkerError.make({ operation: "inspect", reason: "worker-mismatch" })),
  );

  const result = yield* service.summary({ worker: validated, target: declaration.target });

  const value = yield* Schema.decodeEffect(WorkerSummary)(result).pipe(
    Effect.mapError(() => WorkerError.make({ operation: "inspect", reason: "corrupt" })),
  );

  if (
    value.worker.threadId !== validated.threadId ||
    value.worker.delegationId !== validated.delegationId ||
    value.worker.targetAgentId !== validated.targetAgentId ||
    (value.latestReceipt !== null && value.latestReceipt.threadId !== validated.threadId)
  )
    return yield* WorkerError.make({ operation: "inspect", reason: "worker-mismatch" });

  return { ...value, worker: validated };
});

/** Inspect a worker summary, retained MessageRef, or the result of one exact accepted Receipt. */
export function inspect<
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
  worker: Worker<Name>,
): ReturnType<typeof summary<Name>>;

export function inspect<
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
  worker: Worker<Name>,
  receipt: Receipt,
): ReturnType<
  ReturnType<
    typeof operations<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>
  >["observe"]
>;

export function inspect<
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
  worker: Worker<Name>,
  message: MessageRef,
): Effect.Effect<MessageStatus, WorkerError, SubagentHost>;

export function inspect<
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
  worker: Worker<Name>,
  reference?: Receipt | MessageRef,
) {
  return reference === undefined
    ? summary(declaration, worker)
    : "messageId" in reference
      ? operations(declaration).inspectMessage(worker, reference)
      : operations(declaration).observe("inspect", worker, reference);
}

/** Read an authorized finite history snapshot; pass the last emitted sequence to resume. */
export const observe = <const Name extends string>(
  declaration: WorkerDeclaration<Name> & { readonly target: AnyDefinition },
  worker: Worker<Name>,
  options: { readonly after?: number } = {},
) =>
  Stream.unwrap(
    Effect.gen(function* () {
      const service = yield* host;

      const validated = yield* Schema.decodeEffect(Worker(declaration))(worker).pipe(
        Effect.mapError(() =>
          WorkerError.make({ operation: "observe", reason: "worker-mismatch" }),
        ),
      );

      const after =
        options.after === undefined
          ? undefined
          : yield* Schema.decodeEffect(Schema.Natural)(options.after).pipe(
              Effect.mapError(() => WorkerError.make({ operation: "observe", reason: "corrupt" })),
            );

      return service.observe({
        worker: validated,
        target: declaration.target,
        ...(after === undefined ? {} : { after }),
      });
    }),
  );

/** Seal a continuing worker, retaining a stable command identity across retries and Runs. */
export const stop = Effect.fnUntraced(function* <const Name extends string>(
  declaration: WorkerDeclaration<Name> & { readonly target: AnyDefinition },
  worker: Worker<Name>,
  options: { readonly idempotencyKey: IdempotencyKey },
) {
  const service = yield* host;

  const command = yield* Schema.decodeEffect(WorkerStop)({ worker, ...options }).pipe(
    Effect.mapError((cause) => WorkerError.make({ operation: "stop", reason: "corrupt", cause })),
  );

  if (
    worker.delegationId !== declaration.delegationId ||
    worker.targetAgentId !== declaration.target.id
  )
    return yield* WorkerError.make({ operation: "stop", reason: "worker-mismatch" });

  return yield* service.stop({ ...command, target: declaration.target }).pipe(
    Effect.flatMap(Schema.decodeEffect(WorkerStopped)),
    Effect.mapError((cause) =>
      Schema.is(WorkerError)(cause)
        ? cause
        : WorkerError.make({ operation: "stop", reason: "corrupt", cause }),
    ),
  );
});

/** Wait for the exact Receipt. Interrupting this Effect never cancels accepted work. */
export const awaitWorker = <
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
  worker: Worker<Name>,
  receipt: Receipt,
) => operations(declaration).observe("await", worker, receipt);

/** List only workers visible through the caller-bound host facet. */
export const list = <
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
  options: { readonly limit?: number; readonly after?: ThreadId } = {},
) => operations(declaration).list(options);

/** Request cancellation of exactly one Receipt; JoinedToHost remains a typed conflict. */
export const cancel = <
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
  worker: Worker<Name>,
  receipt: Receipt,
) => operations(declaration).cancel(worker, receipt);

/** Opt in to each model-facing operation. Waiting remains programmatic only. */
export interface StartToolOptions {
  /** Author-owned funding request; every native admission separately authorizes it. */
  readonly budgetScope?: WorkerBudgetScope;
}

const ReceiptParameters = <Name extends string>(declaration: WorkerDeclaration<Name>) =>
  Schema.Struct({ worker: Worker(declaration), receipt: Receipt });

const InspectParameters = <Name extends string>(declaration: WorkerDeclaration<Name>) =>
  Schema.Struct({
    worker: Worker(declaration),
    message: Schema.optionalKey(MessageRef),
    receipt: Schema.optionalKey(Receipt),
  }).check(
    Schema.makeFilter(
      ({ message, receipt }) => (message === undefined) !== (receipt === undefined),
      { message: "Provide exactly one retained message reference or accepted receipt" },
    ),
  );

const Inspection = <Success extends Schema.Top>(success: Success) =>
  Schema.Union([MessageStatus, toolObservation(success)]);

const WorkerParameters = <Name extends string>(declaration: WorkerDeclaration<Name>) =>
  Schema.Struct({ worker: Worker(declaration) });

const Summary = <Name extends string>(declaration: WorkerDeclaration<Name>) =>
  Schema.Struct({
    ...WorkerSummary.fields,
    worker: Worker(declaration),
  });

const FollowUpParameters = <Name extends string, Parameters extends Schema.Top>(
  declaration: WorkerDeclaration<Name>,
  parameters: Parameters,
) => Schema.Struct({ worker: Worker(declaration), parameters: Schema.Union([parameters]) });

const ListParameters = Schema.Struct({
  limit: Schema.optionalKey(
    Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)),
  ),
  after: Schema.optionalKey(ThreadId),
});

const Started = <Name extends string>(declaration: WorkerDeclaration<Name>) =>
  Schema.Struct({ worker: Worker(declaration), delivery: MessageStatus }).check(
    Schema.makeFilter(
      ({ worker, delivery }) =>
        delivery.receipt === null || worker.threadId === delivery.receipt.threadId,
    ),
  );

const Page = <Name extends string>(declaration: WorkerDeclaration<Name>) =>
  Schema.Struct({
    items: Schema.Array(Summary(declaration)).check(Schema.isMaxLength(100)),
    next: Schema.NullOr(ThreadId),
  });

const preparationFailure = <Failure extends Schema.Top>(failure: Failure) =>
  Schema.Union([failure, SubagentProjectionFailure, WorkerError]);

const startFailure = <Failure extends Schema.Top>(failure: Failure) =>
  Schema.Union([failure, SubagentProjectionFailure, SubagentPrestartDenied, WorkerError]);

const cancelFailure = Schema.Union([WorkerError, JoinedToHost]);

const modelKey = Effect.fnUntraced(function* (operation: "start" | "followUp") {
  const service = yield* host;
  const caller = yield* service.context;

  if (caller.source._tag !== "tool")
    return yield* WorkerError.make({ operation, reason: "denied" });
  const source = caller.source;
  const crypto = yield* Crypto.Crypto;

  const bytes = utf8Bytes(
    JSON.stringify([
      "worker-tool-v1",
      operation,
      source.agentId,
      source.threadId,
      source.runId,
      source.toolCallId,
    ]),
  );

  const digest = yield* crypto
    .digest("SHA-256", bytes)
    .pipe(Effect.mapError(() => WorkerError.make({ operation, reason: "unavailable" })));

  return yield* Schema.decodeEffect(IdempotencyKey)(`worker:${Hex.encode(digest)}`).pipe(
    Effect.mapError(() => WorkerError.make({ operation, reason: "unavailable" })),
  );
});

/** Pair one native Tool with its checked handler, retaining invocation service requirements. */
const action = <T extends Tool.Any, R>(
  tool: T,
  build: Effect.Effect<
    (
      parameters: Tool.Parameters<T>,
    ) => Effect.Effect<Tool.Success<T>, Tool.Failure<T>, Tool.HandlerServices<T>>,
    never,
    R
  >,
) => {
  const toolkit = Toolkit.make(tool);

  const layer = toolkit.toLayer(
    Effect.map(build, (handler) => {
      // The handler is checked above; only the computed single-tool name needs this bridge.
      return { [tool.name]: handler } as Toolkit.HandlersFrom<typeof toolkit.tools>;
    }),
  );

  return Object.freeze({ tool, layer });
};

/** Retain new worker input under the current Tool Call's stable identity. */
export const startTool = <
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
  options: StartToolOptions = {},
) => {
  const tool = Tool.make(`${declaration.name}_start` as const, {
    description: `Retain input for a background ${declaration.name} worker and return its delivery state. Pending confirms retention only; inspect the same message instead of starting another worker.`,
    parameters: declaration.parameters,
    success: Started(declaration),
    failure: startFailure(declaration.failure),
  })
    .annotate(WorkerOperationTool, true)
    .annotate(BackgroundSpawnTool, true)
    .addDependency(SubagentHost)
    .addDependency(Crypto.Crypto);

  const ops = operations(declaration);

  return action(
    tool,
    Effect.map(
      Effect.context<Prepare | Parameters["EncodingServices"] | Input["EncodingServices"]>(),
      (captured) => (parameters: Parameters["Type"]) =>
        Effect.gen(function* () {
          const service = yield* host;
          const idempotencyKey = yield* modelKey("start");

          return yield* ops
            .start(parameters, { ...options, idempotencyKey })
            .pipe(Effect.provideService(SubagentHost, service), Effect.provide(captured));
        }),
    ),
  );
};

/** Retain follow-up input for an existing worker under the current Tool Call's identity. */
export const followUpTool = <
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
) => {
  const tool = Tool.make(`${declaration.name}_follow_up` as const, {
    description: `Retain typed follow-up input for an existing ${declaration.name} worker. Pending is retained, not executed: inspect the same message instead of sending the command again.`,
    parameters: FollowUpParameters(declaration, declaration.parameters),
    success: MessageStatus,
    failure: preparationFailure(declaration.failure),
  })
    .annotate(WorkerOperationTool, true)
    .addDependency(SubagentHost)
    .addDependency(Crypto.Crypto);

  const ops = operations(declaration);

  return action(
    tool,
    Effect.map(
      Effect.context<Prepare | Parameters["EncodingServices"] | Input["EncodingServices"]>(),
      (captured) =>
        (parameters: { readonly worker: Worker<Name>; readonly parameters: Parameters["Type"] }) =>
          Effect.gen(function* () {
            const service = yield* host;
            const idempotencyKey = yield* modelKey("followUp");

            return yield* ops
              .followUp(parameters.worker, parameters.parameters, { idempotencyKey })
              .pipe(Effect.provideService(SubagentHost, service), Effect.provide(captured));
          }),
    ),
  );
};

/** Inspect one delivery or accepted Receipt without resending input. */
export const inspectTool = <
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
) => {
  const tool = Tool.make(`${declaration.name}_inspect` as const, {
    description: `Inspect one ${declaration.name} delivery by message reference, or its execution result by accepted receipt. Provide exactly one reference; inspection never resends input.`,
    parameters: InspectParameters(declaration),
    success: Inspection(declaration.success),
    failure: preparationFailure(declaration.failure),
  })
    .annotate(WorkerOperationTool, true)
    .addDependency(SubagentHost);

  const ops = operations(declaration);

  return action(
    tool,
    Effect.map(
      Effect.context<
        | Project
        | Parameters["DecodingServices"]
        | Output["DecodingServices"]
        | Success["EncodingServices"]
      >(),
      (captured) =>
        (parameters: {
          readonly worker: Worker<Name>;
          readonly message?: MessageRef;
          readonly receipt?: Receipt;
        }) =>
          Effect.gen(function* () {
            const service = yield* host;

            if (parameters.message !== undefined)
              return yield* ops.inspectMessage(parameters.worker, parameters.message);
            if (parameters.receipt === undefined)
              return yield* WorkerError.make({ operation: "inspect", reason: "message-mismatch" });

            return yield* ops.observe("inspect", parameters.worker, parameters.receipt).pipe(
              Effect.map((observed) => {
                if ("diagnostic" in observed) {
                  const { diagnostic: _diagnostic, ...projected } = observed;

                  return projected;
                }

                return observed;
              }),
              Effect.provideService(SubagentHost, service),
              Effect.provide(captured),
            );
          }),
    ),
  );
};

/** Inspect a continuing worker's state and latest accepted input. */
export const summaryTool = <
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
) => {
  const tool = Tool.make(`${declaration.name}_summary` as const, {
    description: `Inspect ${declaration.name}'s continuing worker state and latest accepted input.`,
    parameters: WorkerParameters(declaration),
    success: Summary(declaration),
    failure: WorkerError,
  })
    .annotate(WorkerOperationTool, true)
    .addDependency(SubagentHost);

  return action(
    tool,
    Effect.succeed((parameters: { readonly worker: Worker<Name> }) =>
      summary(declaration, parameters.worker),
    ),
  );
};

/** List workers visible through the current caller's host facet. */
export const listTool = <
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
) => {
  const tool = Tool.make(`${declaration.name}_list` as const, {
    description: `List visible ${declaration.name} workers.`,
    parameters: ListParameters,
    success: Page(declaration),
    failure: WorkerError,
  })
    .annotate(WorkerOperationTool, true)
    .addDependency(SubagentHost);

  const ops = operations(declaration);

  return action(
    tool,
    Effect.succeed((parameters: typeof ListParameters.Type) => ops.list(parameters)),
  );
};

/** Request cancellation of one exact worker Receipt. */
export const cancelTool = <
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
) => {
  const tool = Tool.make(`${declaration.name}_cancel` as const, {
    description: `Request cancellation of one exact ${declaration.name} Receipt.`,
    parameters: ReceiptParameters(declaration),
    success: Schema.Void,
    failure: cancelFailure,
  })
    .annotate(WorkerOperationTool, true)
    .addDependency(SubagentHost);

  const ops = operations(declaration);

  return action(
    tool,
    Effect.succeed((parameters: { readonly worker: Worker<Name>; readonly receipt: Receipt }) =>
      ops.cancel(parameters.worker, parameters.receipt),
    ),
  );
};
