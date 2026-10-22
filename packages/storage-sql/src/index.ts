/**
 * Shared SQL storage primitives for platform adapters.
 *
 * @since 0.1.0
 */
/**
 * SQL codecs, diagnostics, and transaction contracts shared by relational storage adapters.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlStorage from "./SqlStorage.ts";
/**
 * Named storage fault-injection locations for SQL adapter verification.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlStorageFailpoint from "./SqlStorageFailpoint.ts";
/**
 * Canonical SQL journal primitives for adapter-owned layouts and transactions.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlJournal from "./SqlJournal.ts";
/**
 * Thread storage and prepared canonical appends over a shared SQL journal.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlThreadStore from "./SqlThreadStore.ts";
/**
 * Submission transitions and retained admission facts shared by SQL adapters.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlSubmissionLedger from "./SqlSubmissionLedger.ts";
/**
 * Shared submission row fields with adapter-supplied text and sequence codecs.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlSubmissionRows from "./SqlSubmissionRows.ts";
/**
 * Observation offset encoding shared by SQL Thread store adapters.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlObservationOffset from "./SqlObservationOffset.ts";
/**
 * Decoding of accepted admission and control facts from validated SQL rows.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlAdmissionFacts from "./SqlAdmissionFacts.ts";
/**
 * SQL-backed activity checkpoints with adapter-owned initialization and storage bounds.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlActivityStore from "./SqlActivityStore.ts";
/**
 * SQL schedule transitions using adapter-provided write transactions.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlScheduleStore from "./SqlScheduleStore.ts";
/**
 * Inspection of adapter-owned SQLite layouts before storage operations begin.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqliteLayoutInspection from "./SqliteLayoutInspection.ts";
/**
 * Indexed canonical and execution reads shared by SQL storage adapters.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlThreadNativeReads from "./SqlThreadNativeReads.ts";
/**
 * SQL message delivery storage and atomic publication alongside canonical records.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlMessageDeliveryStore from "./SqlMessageDeliveryStore.ts";
/**
 * SQL subscription state and delivery obligations over adapter-owned tables.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlSubscriptionStore from "./SqlSubscriptionStore.ts";
/**
 * Retention and acknowledgement of lifecycle publications inside SQL source transactions.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlLifecyclePublication from "./SqlLifecyclePublication.ts";
/**
 * Transaction hooks for enrolling newly committed work in host maintenance.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlStorageProgress from "./SqlStorageProgress.ts";
/**
 * Composition of SQL journal, Thread store, and submission ledger under one storage owner.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlRunStorage from "./SqlRunStorage.ts";
/**
 * Paged canonical export and atomic import over an adapter-owned SQL layout.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlThreadImport from "./SqlThreadImport.ts";
/**
 * Rebuildable SQL indexes for bounded discovery of pending Thread work.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlThreadWork from "./SqlThreadWork.ts";
/**
 * Canonical archive range reads and publication over adapter-owned SQL tables.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlThreadArchiveRange from "./SqlThreadArchiveRange.ts";
/**
 * Canonical record encoding and SQL fragments shared by storage adapters.
 *
 * @category modules
 * @since 0.1.0
 */
export * as SqlCanonicalStorage from "./SqlCanonicalStorage.ts";
