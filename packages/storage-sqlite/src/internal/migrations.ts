import { makeSqlTransaction } from "@yielded/agent-storage-sql/sql-storage";
import { SQL_PROMPT_PREDICATE } from "@yielded/agent-storage-sql/sql-thread-native-reads";
import { createSqlThreadWorkTables } from "@yielded/agent-storage-sql/sql-thread-work";
import { makeSqliteLayoutInspection } from "@yielded/agent-storage-sql/sqlite-layout-inspection";
import { CURRENT_RECORD_FORMAT } from "@yielded/agent/records";
import { Effect, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

import {
  SqliteStorageCompatibilityError,
  SqliteStorageCorruptionError,
  SqliteStorageError,
} from "../SqliteStorageError.ts";

export const CurrentSqliteStorageVersion = 25;

/** Fresh layout only. Unsupported stores are rejected before these statements execute. */
const layoutStatements = [
  'CREATE TABLE "effect_agent_threads" ( thread_id TEXT PRIMARY KEY NOT NULL, created_at TEXT NOT NULL, tail_sequence INTEGER NOT NULL, tail_digest TEXT NOT NULL, producer_epoch INTEGER NOT NULL )',
  'CREATE TABLE "effect_agent_canonical_batches" ( thread_id TEXT NOT NULL, batch_id TEXT NOT NULL, first_sequence INTEGER NOT NULL, last_sequence INTEGER NOT NULL, tail_digest TEXT NOT NULL, batch_header_json TEXT NOT NULL, CONSTRAINT effect_agent_canonical_batches_span CHECK (first_sequence >= 1 AND last_sequence >= first_sequence AND last_sequence - first_sequence < 256), PRIMARY KEY (thread_id, batch_id), FOREIGN KEY (thread_id) REFERENCES "effect_agent_threads"(thread_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_canonical_records" ( thread_id TEXT NOT NULL, sequence INTEGER NOT NULL, record_id TEXT NOT NULL, batch_id TEXT NOT NULL, record_json TEXT, record_tag TEXT NOT NULL, run_id TEXT, tool_call_id TEXT, input_kind TEXT, source_submission_id TEXT, message_id TEXT, submission_id TEXT, application_input INTEGER NOT NULL, context_through INTEGER, context_kind TEXT, worker_thread_id TEXT, handoff INTEGER NOT NULL, PRIMARY KEY (thread_id, sequence), UNIQUE (thread_id, record_id), FOREIGN KEY (thread_id, batch_id) REFERENCES "effect_agent_canonical_batches"(thread_id, batch_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_record_runs" ( thread_id TEXT NOT NULL, run_id TEXT NOT NULL, sequence INTEGER NOT NULL, PRIMARY KEY (thread_id, run_id, sequence), FOREIGN KEY (thread_id, sequence) REFERENCES "effect_agent_canonical_records"(thread_id, sequence) ON DELETE RESTRICT )',
  "CREATE TABLE effect_agent_tool_declarations (thread_id TEXT NOT NULL, settlement_record_id TEXT NOT NULL, sequence INTEGER NOT NULL, PRIMARY KEY (thread_id, settlement_record_id, sequence), FOREIGN KEY (thread_id, sequence) REFERENCES effect_agent_canonical_records(thread_id, sequence) ON DELETE RESTRICT)",
  "CREATE INDEX effect_agent_tool_declarations_sequence ON effect_agent_tool_declarations(thread_id, sequence)",
  "CREATE TABLE effect_agent_record_refusals (thread_id TEXT NOT NULL, reservation_record_id TEXT NOT NULL, sequence INTEGER NOT NULL, PRIMARY KEY (thread_id, reservation_record_id, sequence), FOREIGN KEY (thread_id, sequence) REFERENCES effect_agent_canonical_records(thread_id, sequence) ON DELETE RESTRICT)",
  "CREATE INDEX effect_agent_record_refusals_sequence ON effect_agent_record_refusals(thread_id, sequence)",
  'CREATE INDEX effect_agent_canonical_records_batch ON "effect_agent_canonical_records" (thread_id, batch_id, sequence)',
  'CREATE TABLE "effect_agent_checkpoints" ( thread_id TEXT NOT NULL, through_sequence INTEGER NOT NULL, tail_digest TEXT NOT NULL, checkpoint_json TEXT NOT NULL, PRIMARY KEY (thread_id, through_sequence), FOREIGN KEY (thread_id) REFERENCES "effect_agent_threads"(thread_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_submissions" ( submission_id TEXT PRIMARY KEY NOT NULL, thread_id TEXT NOT NULL, queue_sequence INTEGER NOT NULL, principal TEXT NOT NULL, idempotency_key TEXT NOT NULL, agent_id TEXT NOT NULL, agent_digests_json TEXT NOT NULL, deployment_id TEXT NOT NULL, input_json TEXT NOT NULL, input_digest TEXT NOT NULL, receipt_id TEXT NOT NULL, state TEXT NOT NULL, settled_outcome TEXT, settled_record_id TEXT, finalized_at TEXT, created_at TEXT NOT NULL, ready_at TEXT, input_applied_record_id TEXT, input_applied_sequence INTEGER, joined_host_submission_id TEXT, suspended_reason_json TEXT, suspended_at TEXT, unknown_reason TEXT, unknown_tool_call_ids_json TEXT, parent_submission_id TEXT, parent_tool_call_id TEXT, admission_group TEXT, admission_fence_json TEXT, worker_admission_json TEXT, message_admission_json TEXT, UNIQUE (thread_id, principal, idempotency_key), UNIQUE (thread_id, queue_sequence) )',
  'CREATE INDEX effect_agent_submissions_group ON "effect_agent_submissions" (thread_id, admission_group, state)',
  'CREATE TABLE "effect_agent_submission_ownership" ( submission_id TEXT PRIMARY KEY NOT NULL, attempt_id TEXT NOT NULL, ownership_token TEXT NOT NULL, producer_epoch INTEGER NOT NULL, owner_producer_id TEXT NOT NULL, lease_expires_at TEXT NOT NULL, FOREIGN KEY (submission_id) REFERENCES "effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_attempts" ( attempt_id TEXT PRIMARY KEY NOT NULL, submission_id TEXT NOT NULL, thread_id TEXT NOT NULL, owner_producer_id TEXT NOT NULL, producer_epoch INTEGER NOT NULL, claimed_at TEXT NOT NULL, FOREIGN KEY (submission_id) REFERENCES "effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_abort_intents" ( submission_id TEXT PRIMARY KEY NOT NULL, author TEXT NOT NULL, reason TEXT NOT NULL, requested_at TEXT NOT NULL, canonical_record_id TEXT, FOREIGN KEY (submission_id) REFERENCES "effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE INDEX effect_agent_submissions_joined_host ON "effect_agent_submissions" (joined_host_submission_id)',
  'CREATE TABLE "effect_agent_approval_decisions" ( submission_id TEXT NOT NULL, tool_call_id TEXT NOT NULL, decision TEXT NOT NULL, resolver TEXT NOT NULL, reason TEXT NOT NULL, decided_at TEXT NOT NULL, PRIMARY KEY (submission_id, tool_call_id), FOREIGN KEY (submission_id) REFERENCES "effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_unknown_resolutions" ( submission_id TEXT NOT NULL, tool_call_id TEXT NOT NULL, resolution_kind TEXT NOT NULL, author TEXT NOT NULL, reason TEXT NOT NULL, resolution_json TEXT NOT NULL, resolved_at TEXT NOT NULL, PRIMARY KEY (submission_id, tool_call_id, resolution_kind), FOREIGN KEY (submission_id) REFERENCES "effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE INDEX effect_agent_submissions_parent ON "effect_agent_submissions" (parent_submission_id)',
  'CREATE TABLE "effect_agent_child_reservations" ( reservation_id TEXT PRIMARY KEY NOT NULL, parent_submission_id TEXT NOT NULL, parent_tool_call_id TEXT NOT NULL, child_submission_id TEXT, status TEXT NOT NULL, allocation_json TEXT NOT NULL, allocation_digest TEXT NOT NULL, accounting_json TEXT, reserved_at TEXT NOT NULL, release_began_at TEXT, released_at TEXT, UNIQUE (parent_submission_id, parent_tool_call_id), FOREIGN KEY (parent_submission_id) REFERENCES "effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_schedules" ( tenant_id TEXT NOT NULL, owner_id TEXT NOT NULL, schedule_id TEXT NOT NULL, deadline_at_millis INTEGER, record_json TEXT NOT NULL, PRIMARY KEY (tenant_id, owner_id, schedule_id) )',
  'CREATE INDEX effect_agent_schedules_deadline ON "effect_agent_schedules" (deadline_at_millis, tenant_id, owner_id, schedule_id) WHERE deadline_at_millis IS NOT NULL',
  'CREATE INDEX effect_agent_schedules_owner_deadline ON "effect_agent_schedules" (tenant_id, owner_id, deadline_at_millis, schedule_id) WHERE deadline_at_millis IS NOT NULL',
  'CREATE TABLE "effect_agent_subscription_sequences" ( tenant_id TEXT NOT NULL, source_address TEXT NOT NULL, sequence INTEGER NOT NULL, event_scan_cursor TEXT NOT NULL, delivery_scan_cursor TEXT NOT NULL, recovery_scan_cursor INTEGER NOT NULL, PRIMARY KEY (tenant_id, source_address) )',
  'CREATE TABLE "effect_agent_subscriptions" ( tenant_id TEXT NOT NULL, source_address TEXT NOT NULL, owner_id TEXT NOT NULL, subscription_id TEXT NOT NULL, ordinal INTEGER NOT NULL, source_name TEXT NOT NULL, source_version TEXT NOT NULL, matching_key TEXT NOT NULL, state TEXT NOT NULL, expires_at_millis INTEGER, recovery_at_millis INTEGER, recovery_present INTEGER NOT NULL DEFAULT 0, record_json TEXT NOT NULL, PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id), UNIQUE (tenant_id, source_address, ordinal) )',
  'CREATE INDEX effect_agent_subscriptions_owner ON "effect_agent_subscriptions" (tenant_id, source_address, owner_id, ordinal)',
  'CREATE INDEX effect_agent_subscriptions_candidates ON "effect_agent_subscriptions" (tenant_id, source_address, source_name, source_version, matching_key, ordinal)',
  'CREATE INDEX effect_agent_subscriptions_recovery ON "effect_agent_subscriptions" (tenant_id, source_address, recovery_at_millis, ordinal) WHERE recovery_at_millis IS NOT NULL',
  'CREATE TABLE "effect_agent_subscription_events" ( tenant_id TEXT NOT NULL, source_address TEXT NOT NULL, event_id TEXT NOT NULL, source_name TEXT NOT NULL, source_version TEXT NOT NULL, matching_key TEXT NOT NULL, payload_digest TEXT NOT NULL, cutoff INTEGER NOT NULL, cursor INTEGER NOT NULL, routing_complete INTEGER NOT NULL, tombstone INTEGER NOT NULL DEFAULT 0, next_attempt_at_millis INTEGER NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY (tenant_id, source_address, event_id) )',
  'CREATE INDEX effect_agent_subscription_events_pending ON "effect_agent_subscription_events" (tenant_id, source_address, routing_complete, next_attempt_at_millis, event_id)',
  'CREATE TABLE "effect_agent_subscription_deliveries" ( tenant_id TEXT NOT NULL, source_address TEXT NOT NULL, owner_id TEXT NOT NULL, subscription_id TEXT NOT NULL, event_id TEXT NOT NULL, delivery_key TEXT NOT NULL, state TEXT NOT NULL, next_attempt_at_millis INTEGER NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id, event_id), UNIQUE (tenant_id, source_address, delivery_key) )',
  'CREATE INDEX effect_agent_subscription_deliveries_pending ON "effect_agent_subscription_deliveries" (tenant_id, source_address, state, next_attempt_at_millis, delivery_key)',
  'CREATE INDEX effect_agent_subscription_deliveries_registration ON "effect_agent_subscription_deliveries" (tenant_id, source_address, owner_id, subscription_id, delivery_key)',
  'CREATE TABLE "effect_agent_message_deliveries" ( owner_thread_id TEXT NOT NULL, message_id TEXT NOT NULL, version INTEGER NOT NULL, state TEXT NOT NULL, deadline_at_millis INTEGER, record_json TEXT NOT NULL, PRIMARY KEY (owner_thread_id, message_id) )',
  'CREATE INDEX effect_agent_message_deliveries_due ON "effect_agent_message_deliveries" (deadline_at_millis, owner_thread_id, message_id) WHERE deadline_at_millis IS NOT NULL',
  "CREATE INDEX effect_agent_submissions_nonterminal ON \"effect_agent_submissions\" (thread_id, queue_sequence) WHERE state <> 'settled'",
  'CREATE INDEX effect_agent_records_call ON "effect_agent_canonical_records"(thread_id, record_tag, run_id, tool_call_id)',
  "CREATE INDEX effect_agent_records_run_input ON \"effect_agent_canonical_records\"(thread_id, run_id) WHERE record_tag = 'UserInputRecorded' AND input_kind = 'user'",
  "CREATE INDEX effect_agent_records_subtree ON \"effect_agent_canonical_records\"(thread_id, source_submission_id, sequence) WHERE record_tag = 'SubtreeBudgetReserved'",
  "CREATE INDEX effect_agent_records_worker_input ON \"effect_agent_canonical_records\"(thread_id, message_id) WHERE record_tag = 'WorkerInputRequested'",
  "CREATE INDEX effect_agent_message_deliveries_pending ON \"effect_agent_message_deliveries\"(owner_thread_id, message_id) WHERE state NOT IN ('processed', 'refused')",
  "CREATE TABLE effect_agent_worker_stops (thread_id TEXT PRIMARY KEY NOT NULL, terminal TEXT)",
  "CREATE INDEX effect_agent_worker_starts ON effect_agent_message_deliveries(owner_thread_id, json_extract(record_json, '$.envelope.workerAdmission.origin.worker.delegationId'), json_extract(record_json, '$.envelope.workerAdmission.origin.worker.targetAgentId'), message_id) WHERE message_id = json_extract(record_json, '$.envelope.workerAdmission.origin.firstMessageId')",
  "CREATE INDEX effect_agent_worker_pending ON effect_agent_message_deliveries(owner_thread_id, json_extract(record_json, '$.envelope.workerAdmission.origin.worker.threadId'), message_id) WHERE state IN ('pending', 'parked') AND json_extract(record_json, '$.receipt') IS NULL",
  'CREATE INDEX effect_agent_worker_execution ON "effect_agent_canonical_records"(thread_id, record_tag, sequence) WHERE run_id IS NOT NULL',
  "CREATE INDEX effect_agent_records_continuation ON \"effect_agent_canonical_records\"(thread_id, run_id, sequence) WHERE record_tag = 'RunContinuation'",
  'CREATE INDEX effect_agent_records_tag ON "effect_agent_canonical_records"(thread_id, record_tag, sequence)',
  'CREATE INDEX effect_agent_records_handoff ON "effect_agent_canonical_records"(thread_id, sequence) WHERE handoff = 1',
  "CREATE INDEX effect_agent_records_run_identity ON effect_agent_canonical_records(thread_id, run_id) WHERE run_id IS NOT NULL",
  "CREATE INDEX effect_agent_records_application_input ON effect_agent_canonical_records(thread_id, sequence) WHERE application_input = 1",
  "CREATE INDEX effect_agent_records_context ON effect_agent_canonical_records(thread_id, context_through, sequence) WHERE record_tag = 'RunContextRecorded'",
  `CREATE INDEX effect_agent_records_prompt ON effect_agent_canonical_records(thread_id, sequence) WHERE ${SQL_PROMPT_PREDICATE}`,
  "CREATE INDEX effect_agent_records_admitted_input ON effect_agent_canonical_records(thread_id, sequence) WHERE record_tag = 'UserInputRecorded' AND submission_id IS NOT NULL",
  // Prefix indexes let a coverage range produce at most 53 latest-visible candidates.
  "CREATE INDEX effect_agent_records_rollover ON effect_agent_canonical_records(thread_id, context_through, sequence) WHERE record_tag = 'CompactionCreated' AND context_kind = 'rollover'",
  ...Array.from(
    { length: 52 },
    (_, i) =>
      `CREATE INDEX effect_agent_records_rollover_bucket_${i + 1} ON effect_agent_canonical_records(thread_id, (context_through >> ${i + 1}), sequence) WHERE record_tag = 'CompactionCreated' AND context_kind = 'rollover'`,
  ),
  "CREATE INDEX effect_agent_records_worker_funding ON effect_agent_canonical_records(thread_id, source_submission_id, sequence, message_id) WHERE record_tag = 'WorkerInputRequested'",
  "CREATE INDEX effect_agent_records_worker_completed ON effect_agent_canonical_records(thread_id, message_id, sequence) WHERE record_tag = 'WorkerInputCompleted'",
  "CREATE INDEX effect_agent_records_worker_stop ON effect_agent_canonical_records(thread_id, worker_thread_id, sequence) WHERE record_tag = 'WorkerStopRequested'",
  "CREATE INDEX effect_agent_message_deliveries_identity ON effect_agent_message_deliveries(owner_thread_id, json_quote(message_id))",
  "CREATE INDEX effect_agent_message_deliveries_peer ON effect_agent_message_deliveries(owner_thread_id, message_id) WHERE state NOT IN ('processed', 'refused') AND json_extract(record_json, '$.envelope.messageAdmission.schemaVersion') = 1",
  "CREATE TABLE effect_agent_journal_ranges (thread_id TEXT NOT NULL, first_sequence INTEGER NOT NULL, last_sequence INTEGER NOT NULL, previous_tail_digest TEXT NOT NULL, tail_digest TEXT NOT NULL, record_count INTEGER NOT NULL, batch_count INTEGER NOT NULL, byte_count INTEGER NOT NULL, state TEXT NOT NULL, locator TEXT, PRIMARY KEY (thread_id, first_sequence), FOREIGN KEY (thread_id) REFERENCES effect_agent_threads(thread_id) ON DELETE RESTRICT)",
  "CREATE UNIQUE INDEX effect_agent_journal_ranges_open ON effect_agent_journal_ranges(thread_id) WHERE state = 'open'",
  "CREATE TABLE effect_agent_archive_batches (thread_id TEXT NOT NULL, batch_id TEXT NOT NULL, range_first_sequence INTEGER NOT NULL, batch_json TEXT NOT NULL, PRIMARY KEY (thread_id, batch_id), FOREIGN KEY (thread_id, batch_id) REFERENCES effect_agent_canonical_batches(thread_id, batch_id) ON DELETE RESTRICT, CONSTRAINT effect_agent_archive_batches_range_fkey FOREIGN KEY (thread_id, range_first_sequence) REFERENCES effect_agent_journal_ranges(thread_id, first_sequence) ON DELETE RESTRICT)",
  "CREATE TABLE effect_agent_archive_records (thread_id TEXT NOT NULL, sequence INTEGER NOT NULL, range_first_sequence INTEGER NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY (thread_id, sequence), FOREIGN KEY (thread_id, sequence) REFERENCES effect_agent_canonical_records(thread_id, sequence) ON DELETE RESTRICT, CONSTRAINT effect_agent_archive_records_range_fkey FOREIGN KEY (thread_id, range_first_sequence) REFERENCES effect_agent_journal_ranges(thread_id, first_sequence) ON DELETE RESTRICT)",
  "CREATE UNIQUE INDEX effect_agent_canonical_batches_sequence ON effect_agent_canonical_batches(thread_id, first_sequence)",
  "CREATE INDEX effect_agent_canonical_batches_last ON effect_agent_canonical_batches(thread_id, last_sequence)",
  "CREATE INDEX effect_agent_submissions_active_worker ON effect_agent_submissions(thread_id, submission_id) WHERE state <> 'settled' AND worker_admission_json IS NOT NULL",
  "CREATE INDEX effect_agent_submissions_active_parent ON effect_agent_submissions(thread_id, submission_id) WHERE state <> 'settled' AND parent_submission_id IS NOT NULL",
  "CREATE TABLE effect_agent_live_child_reservations (reservation_id TEXT PRIMARY KEY NOT NULL, thread_id TEXT NOT NULL)",
  "CREATE INDEX effect_agent_live_child_reservations_thread ON effect_agent_live_child_reservations(thread_id, reservation_id)",
  // Compact native positions keep accepted identities out of bounded export cursors.
  "CREATE TABLE effect_agent_transfer_commands (thread_id TEXT NOT NULL, command_kind TEXT NOT NULL, command_sequence INTEGER NOT NULL, submission_id TEXT NOT NULL, tool_call_id TEXT NOT NULL, resolution_kind TEXT NOT NULL, PRIMARY KEY (thread_id, command_kind, submission_id, tool_call_id, resolution_kind), CONSTRAINT effect_agent_transfer_commands_sequence_key UNIQUE (thread_id, command_kind, command_sequence), FOREIGN KEY (submission_id) REFERENCES effect_agent_submissions(submission_id) ON DELETE RESTRICT)",
  // Persistent scalar FIFO derivatives; rebuild from canonical input/settlement boundaries.
  "CREATE TABLE effect_agent_input_intervals (thread_id TEXT NOT NULL, sequence INTEGER NOT NULL, settlement_sequence INTEGER, PRIMARY KEY (thread_id, sequence), FOREIGN KEY (thread_id, sequence) REFERENCES effect_agent_canonical_records(thread_id, sequence) ON DELETE RESTRICT)",
  "CREATE INDEX effect_agent_input_intervals_open ON effect_agent_input_intervals(thread_id, sequence) WHERE settlement_sequence IS NULL",
  "CREATE TABLE effect_agent_settlement_spans (thread_id TEXT NOT NULL, height INTEGER NOT NULL, slot INTEGER NOT NULL, settlement_sequence INTEGER NOT NULL, input_sequence INTEGER NOT NULL, PRIMARY KEY (thread_id, height, slot, settlement_sequence, input_sequence), FOREIGN KEY (thread_id, input_sequence) REFERENCES effect_agent_input_intervals(thread_id, sequence) ON DELETE RESTRICT)",
  "CREATE INDEX effect_agent_settlement_spans_input ON effect_agent_settlement_spans(thread_id, input_sequence)",
  // Writer-local import indexes; rows are removed before publication.
  "CREATE TABLE effect_agent_import_fifo (thread_id TEXT NOT NULL, submission_id TEXT NOT NULL, queue_sequence INTEGER NOT NULL, PRIMARY KEY (thread_id, submission_id))",
  "CREATE INDEX effect_agent_import_fifo_active ON effect_agent_import_fifo(thread_id, queue_sequence)",
  "CREATE TABLE effect_agent_import_delivery_visits (thread_id TEXT NOT NULL, message_id TEXT NOT NULL, path_id TEXT NOT NULL, completed INTEGER NOT NULL, PRIMARY KEY (thread_id, message_id))",
  "CREATE INDEX effect_agent_import_delivery_visits_path ON effect_agent_import_delivery_visits(thread_id, path_id)",
  "CREATE TABLE effect_agent_transfer_state (thread_id TEXT PRIMARY KEY NOT NULL, revision INTEGER NOT NULL, admissions_count INTEGER NOT NULL, aborts_count INTEGER NOT NULL, approvals_count INTEGER NOT NULL, resolutions_count INTEGER NOT NULL, deliveries_count INTEGER NOT NULL)",
  "CREATE TRIGGER effect_agent_transfer_submissions_insert AFTER INSERT ON effect_agent_submissions FOR EACH ROW BEGIN INSERT INTO effect_agent_transfer_state (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count) VALUES (NEW.thread_id, 1, 1, 0, 0, 0, 0) ON CONFLICT(thread_id) DO UPDATE SET revision=revision+1, admissions_count=admissions_count+1; END",
  "CREATE TRIGGER effect_agent_transfer_abort_intents_insert AFTER INSERT ON effect_agent_abort_intents FOR EACH ROW BEGIN INSERT INTO effect_agent_transfer_state (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count) SELECT thread_id, 1, 0, 1, 0, 0, 0 FROM effect_agent_submissions WHERE submission_id=NEW.submission_id ON CONFLICT(thread_id) DO UPDATE SET revision=revision+1, aborts_count=aborts_count+1; INSERT INTO effect_agent_transfer_commands (thread_id, command_kind, command_sequence, submission_id, tool_call_id, resolution_kind) SELECT s.thread_id, 'aborts', t.aborts_count, NEW.submission_id, '', '' FROM effect_agent_submissions s JOIN effect_agent_transfer_state t ON t.thread_id=s.thread_id WHERE s.submission_id=NEW.submission_id; END",
  "CREATE TRIGGER effect_agent_transfer_approval_decisions_insert AFTER INSERT ON effect_agent_approval_decisions FOR EACH ROW BEGIN INSERT INTO effect_agent_transfer_state (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count) SELECT thread_id, 1, 0, 0, 1, 0, 0 FROM effect_agent_submissions WHERE submission_id=NEW.submission_id ON CONFLICT(thread_id) DO UPDATE SET revision=revision+1, approvals_count=approvals_count+1; INSERT INTO effect_agent_transfer_commands (thread_id, command_kind, command_sequence, submission_id, tool_call_id, resolution_kind) SELECT s.thread_id, 'approvals', t.approvals_count, NEW.submission_id, NEW.tool_call_id, '' FROM effect_agent_submissions s JOIN effect_agent_transfer_state t ON t.thread_id=s.thread_id WHERE s.submission_id=NEW.submission_id; END",
  "CREATE TRIGGER effect_agent_transfer_unknown_resolutions_insert AFTER INSERT ON effect_agent_unknown_resolutions FOR EACH ROW BEGIN INSERT INTO effect_agent_transfer_state (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count) SELECT thread_id, 1, 0, 0, 0, 1, 0 FROM effect_agent_submissions WHERE submission_id=NEW.submission_id ON CONFLICT(thread_id) DO UPDATE SET revision=revision+1, resolutions_count=resolutions_count+1; INSERT INTO effect_agent_transfer_commands (thread_id, command_kind, command_sequence, submission_id, tool_call_id, resolution_kind) SELECT s.thread_id, 'resolutions', t.resolutions_count, NEW.submission_id, NEW.tool_call_id, NEW.resolution_kind FROM effect_agent_submissions s JOIN effect_agent_transfer_state t ON t.thread_id=s.thread_id WHERE s.submission_id=NEW.submission_id; END",
  "CREATE TRIGGER effect_agent_transfer_message_deliveries_insert AFTER INSERT ON effect_agent_message_deliveries FOR EACH ROW BEGIN INSERT INTO effect_agent_transfer_state (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count) VALUES (NEW.owner_thread_id, 1, 0, 0, 0, 0, 1) ON CONFLICT(thread_id) DO UPDATE SET revision=revision+1, deliveries_count=deliveries_count+1; END",
  "CREATE TRIGGER effect_agent_transfer_message_deliveries_update AFTER UPDATE ON effect_agent_message_deliveries FOR EACH ROW WHEN OLD.owner_thread_id IS NOT NEW.owner_thread_id OR OLD.message_id IS NOT NEW.message_id OR OLD.version IS NOT NEW.version OR OLD.state IS NOT NEW.state OR OLD.deadline_at_millis IS NOT NEW.deadline_at_millis OR OLD.record_json IS NOT NEW.record_json BEGIN INSERT INTO effect_agent_transfer_state (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count) VALUES (NEW.owner_thread_id, 1, 0, 0, 0, 0, 0) ON CONFLICT(thread_id) DO UPDATE SET revision=revision+1; END",
  "CREATE TRIGGER effect_agent_transfer_child_reservations_insert AFTER INSERT ON effect_agent_child_reservations FOR EACH ROW BEGIN INSERT INTO effect_agent_live_child_reservations (reservation_id, thread_id) SELECT NEW.reservation_id, thread_id FROM effect_agent_submissions WHERE submission_id=NEW.parent_submission_id AND NEW.status<>'released'; INSERT INTO effect_agent_transfer_state (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count) SELECT thread_id, 1, 0, 0, 0, 0, 0 FROM effect_agent_submissions WHERE submission_id=NEW.parent_submission_id ON CONFLICT(thread_id) DO UPDATE SET revision=revision+1; END",
  "CREATE TRIGGER effect_agent_transfer_child_reservations_update AFTER UPDATE ON effect_agent_child_reservations FOR EACH ROW WHEN OLD.reservation_id IS NOT NEW.reservation_id OR OLD.parent_submission_id IS NOT NEW.parent_submission_id OR OLD.parent_tool_call_id IS NOT NEW.parent_tool_call_id OR OLD.child_submission_id IS NOT NEW.child_submission_id OR OLD.status IS NOT NEW.status OR OLD.allocation_json IS NOT NEW.allocation_json OR OLD.allocation_digest IS NOT NEW.allocation_digest OR OLD.accounting_json IS NOT NEW.accounting_json OR OLD.reserved_at IS NOT NEW.reserved_at OR OLD.release_began_at IS NOT NEW.release_began_at OR OLD.released_at IS NOT NEW.released_at BEGIN DELETE FROM effect_agent_live_child_reservations WHERE reservation_id=OLD.reservation_id; INSERT INTO effect_agent_live_child_reservations (reservation_id, thread_id) SELECT NEW.reservation_id, thread_id FROM effect_agent_submissions WHERE submission_id=NEW.parent_submission_id AND NEW.status<>'released'; INSERT INTO effect_agent_transfer_state (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count) SELECT thread_id, 1, 0, 0, 0, 0, 0 FROM effect_agent_submissions WHERE submission_id=NEW.parent_submission_id ON CONFLICT(thread_id) DO UPDATE SET revision=revision+1; END",
  "CREATE TRIGGER effect_agent_transfer_child_reservations_delete AFTER DELETE ON effect_agent_child_reservations FOR EACH ROW BEGIN DELETE FROM effect_agent_live_child_reservations WHERE reservation_id=OLD.reservation_id; INSERT INTO effect_agent_transfer_state (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count) SELECT thread_id, 1, 0, 0, 0, 0, 0 FROM effect_agent_submissions WHERE submission_id=OLD.parent_submission_id ON CONFLICT(thread_id) DO UPDATE SET revision=revision+1; END",
  "CREATE TRIGGER effect_agent_transfer_worker_stops_insert AFTER INSERT ON effect_agent_worker_stops FOR EACH ROW BEGIN INSERT INTO effect_agent_transfer_state (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count) VALUES (NEW.thread_id, 1, 0, 0, 0, 0, 0) ON CONFLICT(thread_id) DO UPDATE SET revision=revision+1; END",
  "CREATE TRIGGER effect_agent_transfer_worker_stops_update AFTER UPDATE ON effect_agent_worker_stops FOR EACH ROW WHEN OLD.thread_id IS NOT NEW.thread_id OR OLD.terminal IS NOT NEW.terminal BEGIN INSERT INTO effect_agent_transfer_state (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count) VALUES (NEW.thread_id, 1, 0, 0, 0, 0, 0) ON CONFLICT(thread_id) DO UPDATE SET revision=revision+1; END",
  "CREATE TRIGGER effect_agent_transfer_worker_stops_delete AFTER DELETE ON effect_agent_worker_stops FOR EACH ROW BEGIN INSERT INTO effect_agent_transfer_state (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count) VALUES (OLD.thread_id, 1, 0, 0, 0, 0, 0) ON CONFLICT(thread_id) DO UPDATE SET revision=revision+1; END",
  "CREATE INDEX effect_agent_message_deliveries_capacity ON effect_agent_message_deliveries(owner_thread_id, (COALESCE(json_extract(record_json, '$.envelope.messageAdmission._tag'), '') = 'WorkerUpdate'), message_id) WHERE state IN ('pending', 'accepted', 'parked')",
] as const;

const headerStatement =
  "CREATE TABLE effect_agent_schema (singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), layout_version INTEGER NOT NULL CHECK (layout_version > 0), record_format TEXT NOT NULL CHECK (length(record_format) > 0))";

const layoutObjects = layoutStatements.map((statement, index) => {
  const name = /^CREATE (TABLE|(?:UNIQUE )?INDEX|TRIGGER) "?([a-z_][a-z_0-9]*)"?/.exec(statement);

  if (name === null) throw new Error("Invalid fresh layout statement");

  return [name[1].replace("UNIQUE ", "").toLowerCase(), name[2], index] as const;
});

const Legacy = Schema.Tuple([Schema.Struct({ user_version: Schema.Int })]);

export interface SqliteStorageHeader {
  readonly layoutVersion: number;
  readonly recordFormat: string;
}

const incompatible = (actualVersion: number, message: string) =>
  SqliteStorageCompatibilityError.make({
    actualVersion,
    supportedVersion: CurrentSqliteStorageVersion,
    message: `${message} Keep the original file; the store was not changed.`,
  });

const storageError = (cause: SqlError) =>
  SqliteStorageError.make({ operation: "inspect storage layout", cause, message: cause.message });

const { decode, readObjects, readHeader, readManagedTriggers } = makeSqliteLayoutInspection({
  version: 25,
  statements: layoutStatements,
  objects: layoutObjects,
  headerStatement,
  incompatible,
  storageError,
  corruption: (table) =>
    SqliteStorageCorruptionError.make({
      table,
      rowKey: "schema",
      message: "Malformed storage layout or version header.",
    }),
});

const inspectStorage = Effect.fnUntraced(function* (): Effect.fn.Return<
  SqliteStorageHeader | undefined,
  SqliteStorageCompatibilityError | SqliteStorageCorruptionError | SqliteStorageError,
  SqlClient.SqlClient
> {
  const sql = (yield* SqlClient.SqlClient).withoutTransforms();

  const objects = yield* readObjects();

  const [legacy] = yield* decode(
    Legacy,
    yield* sql<Record<string, unknown>>`PRAGMA user_version`.pipe(Effect.mapError(storageError)),
    "pragma_user_version",
  );

  const version = legacy.user_version;

  if (version === 0 && objects.length === 0) return undefined;

  return yield* readHeader(objects, version);
});

/** The caller holds the dedicated client's exclusive lock across validation and ownership retirement. */
export const inspectManagedSqliteStorage = Effect.fnUntraced(function* () {
  const header = yield* inspectStorage();

  yield* readManagedTriggers(header !== undefined);
});

/** Read-only inspection. The caller owns a snapshot covering this check and its export reads. */
export const readSqliteStorageHeader = Effect.fnUntraced(function* () {
  const header = yield* inspectStorage();

  if (header === undefined)
    return yield* incompatible(0, "No initialized Thread storage to export.");

  return header;
});

/** Validate under the writer transaction before any DDL; initialize only a fresh store. */
export const ensureSqliteStorageLayout = Effect.fn("SqliteStorage.initializeLayout")(function* () {
  const sql = (yield* SqlClient.SqlClient).withoutTransforms();

  // Current storage opens through a read snapshot even while another connection is writing.
  // A fresh store acquires the writer lock and repeats inspection before initialization.
  const current = yield* makeSqlTransaction(sql, { begin: "BEGIN" })(inspectStorage()).pipe(
    Effect.catchTag("SqlError", storageError),
  );

  if (current?.layoutVersion === CurrentSqliteStorageVersion) return current;

  return yield* makeSqlTransaction(sql, { begin: "BEGIN IMMEDIATE" })(
    Effect.gen(function* () {
      const header = yield* inspectStorage();

      if (header !== undefined) return header;
      for (const statement of layoutStatements) yield* sql.unsafe(statement).withoutTransform;
      yield* createSqlThreadWorkTables().pipe(
        Effect.mapError((cause) =>
          SqliteStorageError.make({ operation: cause.operation, message: cause.message, cause }),
        ),
      );
      yield* sql.unsafe(headerStatement).withoutTransform;
      yield* sql`INSERT INTO effect_agent_schema (singleton, layout_version, record_format) VALUES (1, 25, ${CURRENT_RECORD_FORMAT})`;
      yield* sql.unsafe("PRAGMA user_version = 25").withoutTransform;

      return yield* readSqliteStorageHeader();
    }),
  ).pipe(Effect.catchTag("SqlError", storageError));
});
