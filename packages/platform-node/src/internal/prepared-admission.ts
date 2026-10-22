import { type DurableSubmitAgent } from "@yielded/agent/durable-agent-runtime";
import { type AgentId } from "@yielded/agent/identifiers";
import {
  PreparedInputAdmission,
  PreparedInputRetryable,
  PreparedInputRefused,
} from "@yielded/agent/prepared-input-admission";
import { PersistedJson } from "@yielded/agent/records";
import { ScheduleStorageError } from "@yielded/agent/schedule";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

import { type NodeDurableHost } from "../NodeDurableHost.ts";

const passthroughSubmitAgent = (agentId: AgentId): DurableSubmitAgent<typeof PersistedJson> => ({
  definition: { id: agentId, input: PersistedJson },
});

const ambiguous = (): PreparedInputRetryable =>
  PreparedInputRetryable.make({ reason: "ambiguous" });

const corrupt = (operation: string): ScheduleStorageError =>
  ScheduleStorageError.make({ operation, reason: "corrupt" });

/**
 * Host-owned admission gate, available while the host's worker pool is being assembled.
 *
 * @internal
 */
export class NodeAdmission extends Context.Service<
  NodeAdmission,
  Pick<NodeDurableHost["Service"], "submit" | "submissionStatus">
>()("@effect-agent/platform-node/internal/NodeAdmission") {}

/**
 * Acquire the gated source once; worker callers cannot replace its admission authority.
 *
 * @internal
 */
export const makeNodePreparedInputAdmission = Effect.gen(function* () {
  const host = yield* NodeAdmission;

  return PreparedInputAdmission.of({
    submissionStatus: (receipt) =>
      host
        .submissionStatus(receipt)
        .pipe(Effect.mapError(() => PreparedInputRetryable.make({ reason: "storage" }))),
    submit: (envelope) =>
      host
        .submit(passthroughSubmitAgent(envelope.agentId), envelope.input, {
          threadId: envelope.threadId,
          principal: envelope.deliveryPrincipal,
          idempotencyKey: envelope.admissionKey,
          ...(envelope.admissionGroup === undefined
            ? {}
            : { admissionGroup: envelope.admissionGroup }),
          ...(envelope.admissionFence === undefined
            ? {}
            : { admissionFence: envelope.admissionFence }),
          ...(envelope.workerAdmission === undefined
            ? {}
            : { workerAdmission: envelope.workerAdmission }),
          ...(envelope.messageAdmission === undefined
            ? {}
            : { messageAdmission: envelope.messageAdmission }),
          definitions: envelope.definitions,
        })
        .pipe(
          Effect.catchTags({
            AdmissionClosed: () =>
              Effect.fail(PreparedInputRetryable.make({ reason: "host-closed" })),
            AgentInputError: () => Effect.fail(corrupt("prepared admission input")),
            AdmissionConflict: () => Effect.fail(corrupt("prepared admission conflict")),
            DigestError: () => Effect.fail(ambiguous()),
            AdmissionPolicyError: (error) =>
              error.reason === "refused"
                ? PreparedInputRefused.make({ code: error.code })
                : PreparedInputRetryable.make({
                    reason: error.reason === "occupied" ? "capacity" : "storage",
                  }),
            LedgerError: () => PreparedInputRetryable.make({ reason: "storage" }),
            ThreadStoreError: () => Effect.fail(ambiguous()),
            ThreadNotMaterialized: () => Effect.fail(ambiguous()),
            AppendConflict: () => Effect.fail(ambiguous()),
            OwnershipLost: () => Effect.fail(ambiguous()),
            DurableRuntimeFailpointError: () => Effect.fail(ambiguous()),
          }),
        ),
  });
});
