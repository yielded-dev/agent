/**
 * Composite destination admission for host-authorized worker input. Retried admission completes the
 * same retained submission and materialization protocol.
 *
 * @since 0.1.0
 */

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { DurableRuntimeFailpoint, DurableRuntimeFailpointError } from "./DurableFailpoint.ts";
import { ensureThreadCreated, ensureWorkerOrigin } from "./internal/thread-initialization.ts";
import { ProducerId, WorkerAdmission } from "./Records.ts";
import {
  AdmissionConflict,
  AdmissionPolicyError,
  AdmissionRequest,
  LedgerError,
  MarkReadyRequest,
  SubmissionLedger,
} from "./SubmissionLedger.ts";
import {
  AppendConflict,
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadStore,
  ThreadStoreError,
} from "./ThreadStore.ts";
import { WakeScheduler } from "./WakeScheduler.ts";

/**
 * Storage admission after source authorization, input validation and capacity reservation.
 * This is an internal host port, not an authorization entrypoint. Hosts route the whole
 * operation to the destination owner and enroll recovery before executing it.
 *
 * @category models
 * @since 0.1.0
 */
export class WorkerAdmissionRequest extends Schema.Class<WorkerAdmissionRequest>(
  "@effect-agent/thread/WorkerAdmissionRequest",
)({
  ...AdmissionRequest.fields,
  workerAdmission: WorkerAdmission,
  producerId: ProducerId,
}) {}

/**
 * Expected admission, policy, ledger, Thread-storage, append, or injected failure from composite
 * worker admission.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WorkerAdmissionFailure = Schema.Union([
  AdmissionConflict,
  AdmissionPolicyError,
  LedgerError,
  ThreadStoreError,
  ThreadNotMaterialized,
  AppendConflict,
  DurableRuntimeFailpointError,
]);

/**
 * Local implementation shared by every host; retries finish the same admitted row.
 *
 * @category operations
 * @since 0.1.0
 */
export const admitWorker = Effect.fnUntraced(function* (request: WorkerAdmissionRequest) {
  const origin = request.workerAdmission.origin;

  if (
    origin.worker.threadId !== request.threadId ||
    origin.worker.targetAgentId !== request.agentId ||
    request.parentLinkage !== undefined
  )
    return yield* LedgerError.make({
      operation: "admit worker",
      message: "Worker admission does not belong to the destination Thread",
    });

  const ledger = yield* SubmissionLedger;
  const store = yield* ThreadStore;
  const wake = yield* WakeScheduler;
  const failpoint = yield* DurableRuntimeFailpoint;
  const admitted = yield* ledger.admit(AdmissionRequest.make(request));

  yield* failpoint.hit("submit:after-admit");
  if (admitted.replayed && admitted.state !== "admitted") return admitted;

  yield* store.materialize(
    ThreadMaterialization.make({
      threadId: request.threadId,
    }),
  );

  const config = {
    producerId: request.producerId,
    deploymentId: request.deploymentId,
  };

  yield* ensureThreadCreated(config, request.threadId, request.agentId, request.agentDigests);
  yield* ensureWorkerOrigin(config, origin).pipe(
    Effect.mapError((cause) =>
      LedgerError.make({ operation: "worker-origin", message: cause.reason, cause }),
    ),
  );
  yield* failpoint.hit("submit:after-materialize");
  yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
  yield* wake.notify(request.threadId);

  return admitted;
});

/**
 * Placement adapter for the composite operation; local hosts use the same implementation.
 *
 * @category services
 * @since 0.1.0
 */
export const WorkerAdmissionPort = Context.Reference<{ readonly admit: typeof admitWorker }>(
  "@effect-agent/thread/WorkerAdmissionPort",
  { defaultValue: () => ({ admit: admitWorker }) },
);
