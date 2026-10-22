/**
 * Common immutable admission envelopes and the host port used by schedules and event deliveries.
 * Retryable failures may represent a lost acknowledgement after admission committed.
 *
 * @since 0.1.0
 */

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as FailureDiagnostic from "../core/FailureDiagnostic.ts";
import { AgentId, ThreadId } from "../core/Identifiers.ts";
import { InputMessage } from "../core/Messaging.ts";
import type { Receipt } from "../core/Receipt.ts";
import { DefinitionDigests, Digest, PersistedJson, WorkerAdmission } from "./Records.ts";
import type { ScheduleStorageError } from "./Schedule.ts";
import { AdmissionFence, AdmissionGroup, IdempotencyKey, Principal } from "./SubmissionLedger.ts";
import type { SubmissionStatus } from "./SubmissionStatus.ts";

const Name = Schema.NonEmptyString.check(
  Schema.isMaxLength(128),
  Schema.isPattern(/^[^\p{Surrogate}]*$/u),
);

/**
 * Bounded policy identifiers, never credentials or arbitrary diagnostics.
 *
 * @category schemas
 * @since 0.1.0
 */
export const PreparedInputAuthorizationDecision = Schema.Struct({
  policyId: Name,
  decisionId: Name,
});

/**
 * Decoded value of {@link PreparedInputAuthorizationDecision}.
 *
 * @category models
 * @since 0.1.0
 */
export type PreparedInputAuthorizationDecision = typeof PreparedInputAuthorizationDecision.Type;

/**
 * Common admission data; each producer retains its own cursor and delivery lifecycle.
 *
 * @category schemas
 * @since 0.1.0
 */
export const PreparedInput = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  threadId: ThreadId,
  deliveryPrincipal: Principal,
  admissionGroup: Schema.optionalKey(AdmissionGroup),
  admissionFence: Schema.optionalKey(AdmissionFence),
  agentId: AgentId,
  definitions: DefinitionDigests,
  input: PersistedJson,
  inputDigest: Digest,
  admissionKey: IdempotencyKey,
  authorization: PreparedInputAuthorizationDecision,
  workerAdmission: Schema.optionalKey(WorkerAdmission),
  messageAdmission: Schema.optionalKey(InputMessage),
});

/**
 * Decoded value of {@link PreparedInput}.
 *
 * @category models
 * @since 0.1.0
 */
export type PreparedInput = typeof PreparedInput.Type;

/**
 * Reasons to retry the unchanged prepared envelope, including ambiguous replies that may follow
 * committed admission.
 *
 * @category schemas
 * @since 0.1.0
 */
export const PreparedInputRetryReason = Schema.Literals([
  "transport",
  "capacity",
  "storage",
  "host-closed",
  "timeout",
  "ambiguous",
]);

/**
 * Decoded value of {@link PreparedInputRetryReason}.
 *
 * @category models
 * @since 0.1.0
 */
export type PreparedInputRetryReason = typeof PreparedInputRetryReason.Type;

/**
 * Only when the unchanged envelope was not admitted and cannot succeed, including on replay.
 *
 * @category errors
 * @since 0.1.0
 */
export class PreparedInputRefused extends Schema.TaggedError<PreparedInputRefused>()(
  "PreparedInputRefused",
  {
    code: Name,
    cause: Schema.optionalKey(FailureDiagnostic.Value),
    stack: Schema.optionalKey(Schema.String),
  },
) {}

/**
 * Includes lost replies: admission may already have committed.
 *
 * @category errors
 * @since 0.1.0
 */
export class PreparedInputRetryable extends Schema.TaggedError<PreparedInputRetryable>()(
  "PreparedInputRetryable",
  {
    reason: PreparedInputRetryReason,
    cause: Schema.optionalKey(FailureDiagnostic.Value),
    stack: Schema.optionalKey(Schema.String),
  },
) {}

/**
 * Conclusive refusal, retryable admission uncertainty, or scheduling-storage failure from a
 * prepared-input host.
 *
 * @category models
 * @since 0.1.0
 */
export type PreparedInputFailure =
  | PreparedInputRefused
  | PreparedInputRetryable
  | ScheduleStorageError;

/**
 * Host admission shared by event deliveries and scheduling adapters.
 *
 * @category services
 * @since 0.1.0
 */
export class PreparedInputAdmission extends Context.Service<
  PreparedInputAdmission,
  {
    /** Required when retention is configured; reports canonical settlement without releasing group ownership itself. */
    readonly submissionStatus?: (
      receipt: Receipt,
    ) => Effect.Effect<SubmissionStatus, PreparedInputFailure>;
    readonly submit: (envelope: PreparedInput) => Effect.Effect<Receipt, PreparedInputFailure>;
  }
>()("@effect-agent/thread/PreparedInputAdmission") {}

/**
 * Reduce one bounded admission attempt. The caller owns concurrency, durable retry state,
 * and completion fencing. Interruption and defects propagate without claiming a refusal.
 *
 * @category operations
 * @since 0.1.0
 */
export const admitPreparedInput = <R>(
  submit: Effect.Effect<Receipt, PreparedInputFailure, R>,
  timeoutMillis: number,
) =>
  submit.pipe(
    Effect.timeout(timeoutMillis),
    Effect.map((receipt) => ({ _tag: "Receipt" as const, receipt })),
    Effect.catchTag("PreparedInputRefused", (error) =>
      Effect.succeed({ _tag: "Refused" as const, error }),
    ),
    Effect.catchTag("PreparedInputRetryable", (error) =>
      Effect.succeed({
        _tag: "Retry" as const,
        reason: error.reason,
        diagnostic: FailureDiagnostic.capture(error),
      }),
    ),
    Effect.catchTag("TimeoutError", (error) =>
      Effect.succeed({
        _tag: "Retry" as const,
        reason: "timeout" as const,
        diagnostic: FailureDiagnostic.capture(error),
      }),
    ),
    Effect.catchTag("ScheduleStorageError", (error) =>
      error.reason === "unavailable"
        ? Effect.succeed({
            _tag: "Retry" as const,
            reason: "storage" as const,
            diagnostic: FailureDiagnostic.capture(error),
          })
        : Effect.fail(error),
    ),
  );
