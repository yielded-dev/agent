import { type DurableSubmitAgent } from "@yielded/agent/durable-agent-runtime";
import { type AgentId } from "@yielded/agent/identifiers";
import {
  PreparedInputAdmission,
  PreparedInputRetryable,
  PreparedInputRefused,
} from "@yielded/agent/prepared-input-admission";
import { PersistedJson } from "@yielded/agent/records";
import { ScheduleStorageError } from "@yielded/agent/schedule";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { CloudflareThreadClient, type ThreadClientError } from "../CloudflareThreadClient.ts";

const passthroughAgent = (agentId: AgentId): DurableSubmitAgent<typeof PersistedJson> => ({
  definition: { id: agentId, input: PersistedJson },
});

/** Ordinary prepared admission through one freshly addressed Thread Object call. */
export const cloudflarePreparedInputAdmissionLayer: Layer.Layer<
  PreparedInputAdmission,
  never,
  CloudflareThreadClient
> = Layer.effect(
  PreparedInputAdmission,
  Effect.gen(function* () {
    const client = yield* CloudflareThreadClient;

    return PreparedInputAdmission.of({
      submissionStatus: (receipt) =>
        client
          .submissionStatus(receipt)
          .pipe(Effect.mapError(() => PreparedInputRetryable.make({ reason: "storage" }))),
      submit: (envelope) =>
        client
          .submit(passthroughAgent(envelope.agentId), envelope.input, {
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
              AdmissionConflict: () =>
                ScheduleStorageError.make({ operation: "prepared admission", reason: "corrupt" }),
              AdmissionLimitExceeded: () => PreparedInputRetryable.make({ reason: "capacity" }),
              ThreadClientError: (error: ThreadClientError) =>
                PreparedInputRetryable.make({
                  reason: error.overloaded === true ? "capacity" : "transport",
                }),
              HostProtocolError: () => PreparedInputRetryable.make({ reason: "ambiguous" }),
              AdmissionPolicyError: (error) =>
                error.reason === "refused"
                  ? PreparedInputRefused.make({ code: error.code })
                  : PreparedInputRetryable.make({
                      reason: error.reason === "occupied" ? "capacity" : "storage",
                    }),
              LedgerError: () => PreparedInputRetryable.make({ reason: "storage" }),
              ThreadStoreError: () => PreparedInputRetryable.make({ reason: "storage" }),
              DurableAlarmError: () => PreparedInputRetryable.make({ reason: "storage" }),
              AgentInputError: () =>
                ScheduleStorageError.make({ operation: "prepared admission", reason: "corrupt" }),
              DigestError: () =>
                ScheduleStorageError.make({ operation: "prepared admission", reason: "corrupt" }),
              ThreadNotMaterialized: () => PreparedInputRetryable.make({ reason: "storage" }),
              AppendConflict: () => PreparedInputRetryable.make({ reason: "ambiguous" }),
              OwnershipLost: () => PreparedInputRetryable.make({ reason: "ambiguous" }),
              DurableRuntimeFailpointError: () =>
                PreparedInputRetryable.make({ reason: "ambiguous" }),
            }),
          ),
    });
  }),
);
