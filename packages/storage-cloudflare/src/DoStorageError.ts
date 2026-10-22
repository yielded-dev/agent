/**
 * Typed storage failures and fault-injection locations for Durable Object adapters.
 *
 * @since 0.1.0
 */
import { CanonicalSequence } from "@yielded/agent/records";
import { ThreadStoreDiagnostic } from "@yielded/agent/thread-store";
import * as Schema from "effect/Schema";

/**
 * The stored layout version is incompatible with this adapter.
 *
 * @category errors
 * @since 0.1.0
 */
export class DoStorageCompatibilityError extends Schema.TaggedError<DoStorageCompatibilityError>()(
  "DoStorageCompatibilityError",
  {
    actualVersion: Schema.Int,
    message: Schema.String,
    supportedVersion: Schema.Int,
  },
) {}

/**
 * Stored bytes failed the current Schema and cannot be used as recovery truth.
 *
 * @category errors
 * @since 0.1.0
 */
export class DoStorageCorruptionError extends Schema.TaggedError<DoStorageCorruptionError>()(
  "DoStorageCorruptionError",
  {
    message: Schema.String,
    rowKey: Schema.String,
    table: Schema.String,
    diagnostic: Schema.optionalKey(ThreadStoreDiagnostic),
  },
) {}

/**
 * Durable Object SQLite infrastructure failed while opening or operating the store.
 *
 * @category errors
 * @since 0.1.0
 */
export class DoStorageError extends Schema.TaggedError<DoStorageError>()("DoStorageError", {
  cause: Schema.optionalKey(Schema.Defect()),
  message: Schema.String,
  operation: Schema.String,
  diagnostic: Schema.optionalKey(ThreadStoreDiagnostic),
}) {}

/**
 * Durable Object SQLite infrastructure failed while operating the Submission Ledger. Surfaces
 * at the SubmissionLedger port as the typed `LedgerError` with this error preserved as its
 * cause, so the adapter-level tag is never erased.
 *
 * @category errors
 * @since 0.1.0
 */
export class DoLedgerError extends Schema.TaggedError<DoLedgerError>()("DoLedgerError", {
  cause: Schema.optionalKey(Schema.Defect()),
  message: Schema.String,
  operation: Schema.String,
}) {}

/**
 * A stored value exceeds the configured per-value byte bound. The write is refused before mutation.
 *
 * @category errors
 * @since 0.1.0
 */
export class DoValueBoundExceeded extends Schema.TaggedError<DoValueBoundExceeded>()(
  "DoValueBoundExceeded",
  {
    actualBytes: Schema.Int,
    maxBytes: Schema.Int,
    operation: Schema.String,
  },
) {
  override get message() {
    return (
      `A stored value of ${this.actualBytes} bytes exceeds the Durable Object per-value bound ` +
      `of ${this.maxBytes} bytes during ${this.operation}. Nothing was written. Values of this ` +
      "size are the designed R2 AttachmentStore overflow path (deferred, deployment spec §3.1)."
    );
  }
}

/**
 * A canonical batch retry conflicts with existing append state. Tail conflicts carry the
 * actual committed tail as a diagnostic resume hint.
 *
 * @category errors
 * @since 0.1.0
 */
export class DoAppendConflict extends Schema.TaggedError<DoAppendConflict>()("DoAppendConflict", {
  message: Schema.String,
  reason: Schema.Literals(["batch-content", "record-identity", "tail", "stale-command"]),
  actualTailSequence: Schema.optionalKey(CanonicalSequence),
}) {}

/**
 * Fault-injection locations at storage operation boundaries, shared with the SQLite adapter.
 *
 * @category schemas
 * @since 0.1.0
 */
export const DoStorageFailpointLocation = Schema.Literals([
  "materialize:before",
  "materialize:after",
  "append:before",
  "append:after-batch-insert",
  "append:after-record-insert",
  "append:after-tail-update",
  "append:after",
  "export:after-thread-read",
  "ledger:admit:before",
  "ledger:admit:after",
  "ledger:mark-ready:before",
  "ledger:mark-ready:after",
  "ledger:claim:before",
  "ledger:claim:after",
  "ledger:mark-input-applied:before",
  "ledger:mark-input-applied:after",
  "ledger:finalize-settlement:before",
  "ledger:finalize-settlement:after",
  "ledger:request-abort:before",
  "ledger:request-abort:after",
  "ledger:claim-joining:before",
  "ledger:claim-joining:after",
  "ledger:mark-joined:before",
  "ledger:mark-joined:after",
  "ledger:revert-joining:before",
  "ledger:revert-joining:after",
  "ledger:suspend:before",
  "ledger:suspend:after",
  "ledger:approval-decision:before",
  "ledger:approval-decision:after",
  "ledger:mark-unknown:before",
  "ledger:mark-unknown:after",
  "ledger:unknown-resolution:before",
  "ledger:unknown-resolution:after",
  "ledger:child-reservation:before",
  "ledger:child-reservation:after",
  "ledger:child-attach:before",
  "ledger:child-attach:after",
  "ledger:child-release-pending:before",
  "ledger:child-release-pending:after",
  "ledger:child-release:before",
  "ledger:child-release:after",
  "ledger:child-settled:before",
  "ledger:child-settled:after",
]);

/**
 * Decoded value of the DoStorageFailpointLocation schema.
 *
 * @category models
 * @since 0.1.0
 */
export type DoStorageFailpointLocation = typeof DoStorageFailpointLocation.Type;

/**
 * Deterministic test-only fault or pause injected at a Durable Object storage boundary.
 *
 * @category errors
 * @since 0.1.0
 */
export class DoStorageFailpointError extends Schema.TaggedError<DoStorageFailpointError>()(
  "DoStorageFailpointError",
  {
    location: DoStorageFailpointLocation,
  },
) {
  override get message() {
    return `Injected Durable Object storage failure at ${this.location}.`;
  }
}
