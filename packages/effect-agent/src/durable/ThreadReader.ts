/**
 * Canonical observation and exact evidence reads without append or materialization authority.
 *
 * @since 0.1.0
 */

export {
  /**
   * Canonical observation without append or materialization authority.
   *
   * @category services
   * @since 0.1.0
   */
  ThreadReader,
  /**
   * Host-owned read: authenticate the Thread and exact locator before calling.
   *
   * @category operations
   * @since 0.1.0
   */
  getRecord,
  /**
   * Reads the exact original user-input record for a Run, rejecting incomplete or mismatched native
   * selections.
   *
   * @category operations
   * @since 0.1.0
   */
  getRunInput,
  /**
   * A bounded snapshot of source identity and the selected funding Run's accounting.
   *
   * @category operations
   * @since 0.1.0
   */
  readWorkerState,
  /**
   * Exact canonical identity; absence is not proof that an admission was never accepted.
   *
   * @category schemas
   * @since 0.1.0
   */
  ThreadRecordRequest,
  /**
   * The original user input for a Run, excluding later joined inputs.
   *
   * @category schemas
   * @since 0.1.0
   */
  ThreadRunInputRequest,
  /**
   * Native accounting for one funding Run; settled Thread history is never included.
   *
   * @category schemas
   * @since 0.1.0
   */
  ThreadWorkerStateRequest,
  /**
   * Bounded canonical read after an optional sequence, with a limit of 1–1,024 records.
   *
   * @category models
   * @since 0.1.0
   */
  ThreadRead,
  /**
   * Selects a bounded canonical range or a closed native evidence selection.
   *
   * @category schemas
   * @since 0.1.0
   */
  ThreadReadRequest,
  /**
   * Sparse ordered history page at a fixed tail; no record-count limit on the whole range.
   *
   * @category schemas
   * @since 0.1.0
   */
  ThreadPromptRead,
  /**
   * Starts observation of one Thread after an optional adapter-owned offset.
   *
   * @category models
   * @since 0.1.0
   */
  ThreadObservation,
  /**
   * Requests the first or next bounded page of an idle Thread transfer.
   *
   * @category models
   * @since 0.1.0
   */
  ThreadExportRequest,
  /**
   * One bounded, batch-aligned page. An absent cursor alone marks a complete transfer.
   *
   * @category models
   * @since 0.1.0
   */
  ThreadExport,
  /**
   * Requests the current committed canonical tail for one Thread.
   *
   * @category models
   * @since 0.1.0
   */
  ThreadTailRequest,
  /**
   * The committed tail of one Thread Log. A resuming producer composes its next
   * AppendRequest from this value instead of exporting the whole log.
   *
   * @category models
   * @since 0.1.0
   */
  ThreadTail,
  /**
   * Requests a consistent view of a Thread's root and lineage identity evidence.
   *
   * @category models
   * @since 0.1.0
   */
  ThreadIdentityRequest,
  /**
   * One consistent snapshot: Thread ID, canonical tail, admission presence, first record,
   * exact worker origin, then exact subagent lineage. A row selected twice appears only
   * once. Missing identity records remain absent; the snapshot grants no execution authority.
   *
   * @category models
   * @since 0.1.0
   */
  ThreadIdentity,
  /**
   * A Thread storage operation failed, optionally retaining bounded decoding or delivery
   * diagnostics.
   *
   * @category errors
   * @since 0.1.0
   */
  ThreadStoreError,
  /**
   * The requested Thread has no materialized durable storage.
   *
   * @category errors
   * @since 0.1.0
   */
  ThreadNotMaterialized,
} from "./ThreadStore.ts";
