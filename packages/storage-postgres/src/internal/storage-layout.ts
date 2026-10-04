import { makeSqlQuery, SqlInteger } from "@yielded/agent-storage-sql/sql-storage";
import { CURRENT_RECORD_FORMAT } from "@yielded/agent/records";
import { Effect, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";

import {
  PostgresStorageCompatibilityError,
  PostgresStorageCorruptionError,
} from "../PostgresStorageError.ts";

export const CurrentPostgresStorageVersion = 17;
const LEGACY_RECORD_FORMAT = "effect-agent/thread@1";

// Frozen statements captured while executing the shipped layout16 initializer on PostgreSQL 17.
const baseline16 = [
  'CREATE TABLE __NAMESPACE__."effect_agent_storage_version" ( id BOOLEAN PRIMARY KEY NOT NULL, version BIGINT NOT NULL, CONSTRAINT effect_agent_storage_version_single_row CHECK (id) )',
  'CREATE TABLE __NAMESPACE__."effect_agent_threads" ( thread_id TEXT COLLATE "C" PRIMARY KEY NOT NULL, created_at TEXT COLLATE "C" NOT NULL, tail_sequence BIGINT NOT NULL, tail_digest TEXT COLLATE "C" NOT NULL, producer_epoch BIGINT NOT NULL )',
  'CREATE TABLE __NAMESPACE__."effect_agent_canonical_batches" ( thread_id TEXT COLLATE "C" NOT NULL, batch_id TEXT COLLATE "C" NOT NULL, first_sequence BIGINT NOT NULL, last_sequence BIGINT NOT NULL, batch_digest TEXT COLLATE "C" NOT NULL, tail_digest TEXT COLLATE "C" NOT NULL, batch_json TEXT COLLATE "C" NOT NULL, PRIMARY KEY (thread_id, batch_id), FOREIGN KEY (thread_id) REFERENCES __NAMESPACE__."effect_agent_threads"(thread_id) ON DELETE RESTRICT )',
  'CREATE TABLE __NAMESPACE__."effect_agent_canonical_records" ( thread_id TEXT COLLATE "C" NOT NULL, sequence BIGINT NOT NULL, record_id TEXT COLLATE "C" NOT NULL, batch_id TEXT COLLATE "C" NOT NULL, record_json TEXT COLLATE "C" NOT NULL, PRIMARY KEY (thread_id, sequence), UNIQUE (thread_id, record_id), FOREIGN KEY (thread_id, batch_id) REFERENCES __NAMESPACE__."effect_agent_canonical_batches"(thread_id, batch_id) ON DELETE RESTRICT )',
  'CREATE INDEX effect_agent_canonical_records_batch ON __NAMESPACE__."effect_agent_canonical_records" (thread_id, batch_id, sequence)',
  'CREATE TABLE __NAMESPACE__."effect_agent_checkpoints" ( thread_id TEXT COLLATE "C" NOT NULL, through_sequence BIGINT NOT NULL, tail_digest TEXT COLLATE "C" NOT NULL, checkpoint_json TEXT COLLATE "C" NOT NULL, PRIMARY KEY (thread_id, through_sequence), FOREIGN KEY (thread_id) REFERENCES __NAMESPACE__."effect_agent_threads"(thread_id) ON DELETE RESTRICT )',
  'CREATE TABLE __NAMESPACE__."effect_agent_submissions" ( submission_id TEXT COLLATE "C" PRIMARY KEY NOT NULL, thread_id TEXT COLLATE "C" NOT NULL, queue_sequence BIGINT NOT NULL, principal TEXT COLLATE "C" NOT NULL, idempotency_key TEXT COLLATE "C" NOT NULL, agent_id TEXT COLLATE "C" NOT NULL, agent_digests_json TEXT COLLATE "C" NOT NULL, deployment_id TEXT COLLATE "C" NOT NULL, input_json TEXT COLLATE "C" NOT NULL, input_digest TEXT COLLATE "C" NOT NULL, receipt_id TEXT COLLATE "C" NOT NULL, state TEXT COLLATE "C" NOT NULL, settled_outcome TEXT COLLATE "C", settled_record_id TEXT COLLATE "C", finalized_at TEXT COLLATE "C", created_at TEXT COLLATE "C" NOT NULL, ready_at TEXT COLLATE "C", input_applied_record_id TEXT COLLATE "C", input_applied_sequence BIGINT, joined_host_submission_id TEXT COLLATE "C", suspended_reason_json TEXT COLLATE "C", suspended_at TEXT COLLATE "C", unknown_reason TEXT COLLATE "C", unknown_tool_call_ids_json TEXT COLLATE "C", parent_submission_id TEXT COLLATE "C", parent_tool_call_id TEXT COLLATE "C", admission_group TEXT COLLATE "C", admission_fence_json TEXT COLLATE "C", worker_admission_json TEXT COLLATE "C", message_admission_json TEXT COLLATE "C", UNIQUE (thread_id, principal, idempotency_key), UNIQUE (thread_id, queue_sequence) )',
  'CREATE INDEX effect_agent_submissions_group ON __NAMESPACE__."effect_agent_submissions" (thread_id, admission_group, state)',
  'CREATE TABLE __NAMESPACE__."effect_agent_submission_ownership" ( submission_id TEXT COLLATE "C" PRIMARY KEY NOT NULL, attempt_id TEXT COLLATE "C" NOT NULL, ownership_token TEXT COLLATE "C" NOT NULL, producer_epoch BIGINT NOT NULL, owner_producer_id TEXT COLLATE "C" NOT NULL, lease_expires_at TEXT COLLATE "C" NOT NULL, FOREIGN KEY (submission_id) REFERENCES __NAMESPACE__."effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE TABLE __NAMESPACE__."effect_agent_attempts" ( attempt_id TEXT COLLATE "C" PRIMARY KEY NOT NULL, submission_id TEXT COLLATE "C" NOT NULL, thread_id TEXT COLLATE "C" NOT NULL, owner_producer_id TEXT COLLATE "C" NOT NULL, producer_epoch BIGINT NOT NULL, claimed_at TEXT COLLATE "C" NOT NULL, FOREIGN KEY (submission_id) REFERENCES __NAMESPACE__."effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE TABLE __NAMESPACE__."effect_agent_abort_intents" ( submission_id TEXT COLLATE "C" PRIMARY KEY NOT NULL, author TEXT COLLATE "C" NOT NULL, reason TEXT COLLATE "C" NOT NULL, requested_at TEXT COLLATE "C" NOT NULL, canonical_record_id TEXT COLLATE "C", FOREIGN KEY (submission_id) REFERENCES __NAMESPACE__."effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE INDEX effect_agent_submissions_joined_host ON __NAMESPACE__."effect_agent_submissions" (joined_host_submission_id)',
  'CREATE TABLE __NAMESPACE__."effect_agent_approval_decisions" ( submission_id TEXT COLLATE "C" NOT NULL, tool_call_id TEXT COLLATE "C" NOT NULL, decision TEXT COLLATE "C" NOT NULL, resolver TEXT COLLATE "C" NOT NULL, reason TEXT COLLATE "C" NOT NULL, decided_at TEXT COLLATE "C" NOT NULL, PRIMARY KEY (submission_id, tool_call_id), FOREIGN KEY (submission_id) REFERENCES __NAMESPACE__."effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE TABLE __NAMESPACE__."effect_agent_unknown_resolutions" ( submission_id TEXT COLLATE "C" NOT NULL, tool_call_id TEXT COLLATE "C" NOT NULL, author TEXT COLLATE "C" NOT NULL, reason TEXT COLLATE "C" NOT NULL, resolution_json TEXT COLLATE "C" NOT NULL, resolved_at TEXT COLLATE "C" NOT NULL, PRIMARY KEY (submission_id, tool_call_id), FOREIGN KEY (submission_id) REFERENCES __NAMESPACE__."effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE INDEX effect_agent_submissions_parent ON __NAMESPACE__."effect_agent_submissions" (parent_submission_id)',
  'CREATE TABLE __NAMESPACE__."effect_agent_child_reservations" ( reservation_id TEXT COLLATE "C" PRIMARY KEY NOT NULL, parent_submission_id TEXT COLLATE "C" NOT NULL, parent_tool_call_id TEXT COLLATE "C" NOT NULL, child_submission_id TEXT COLLATE "C", status TEXT COLLATE "C" NOT NULL, allocation_json TEXT COLLATE "C" NOT NULL, allocation_digest TEXT COLLATE "C" NOT NULL, accounting_json TEXT COLLATE "C", reserved_at TEXT COLLATE "C" NOT NULL, release_began_at TEXT COLLATE "C", released_at TEXT COLLATE "C", UNIQUE (parent_submission_id, parent_tool_call_id), FOREIGN KEY (parent_submission_id) REFERENCES __NAMESPACE__."effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE TABLE __NAMESPACE__."effect_agent_schedules" ( tenant_id TEXT COLLATE "C" NOT NULL, owner_id TEXT COLLATE "C" NOT NULL, schedule_id TEXT COLLATE "C" NOT NULL, deadline_at_millis BIGINT, record_json TEXT COLLATE "C" NOT NULL, uses_capacity BOOLEAN NOT NULL, PRIMARY KEY (tenant_id, owner_id, schedule_id) )',
  'CREATE INDEX effect_agent_schedules_deadline ON __NAMESPACE__."effect_agent_schedules" (deadline_at_millis, tenant_id, owner_id, schedule_id) WHERE deadline_at_millis IS NOT NULL',
  'CREATE INDEX effect_agent_schedules_owner_deadline ON __NAMESPACE__."effect_agent_schedules" (tenant_id, owner_id, deadline_at_millis, schedule_id) WHERE deadline_at_millis IS NOT NULL',
  'CREATE TABLE __NAMESPACE__."effect_agent_subscription_sequences" ( tenant_id TEXT COLLATE "C" NOT NULL, source_address TEXT COLLATE "C" NOT NULL, sequence BIGINT NOT NULL, event_scan_cursor TEXT COLLATE "C" NOT NULL, delivery_scan_cursor TEXT COLLATE "C" NOT NULL, recovery_scan_cursor BIGINT NOT NULL, PRIMARY KEY (tenant_id, source_address) )',
  'CREATE TABLE __NAMESPACE__."effect_agent_subscriptions" ( tenant_id TEXT COLLATE "C" NOT NULL, source_address TEXT COLLATE "C" NOT NULL, owner_id TEXT COLLATE "C" NOT NULL, subscription_id TEXT COLLATE "C" NOT NULL, ordinal BIGINT NOT NULL, source_name TEXT COLLATE "C" NOT NULL, source_version TEXT COLLATE "C" NOT NULL, matching_key TEXT COLLATE "C" NOT NULL, state TEXT COLLATE "C" NOT NULL, expires_at_millis BIGINT, recovery_at_millis BIGINT, recovery_present BIGINT NOT NULL DEFAULT 0, record_json TEXT COLLATE "C" NOT NULL, PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id), UNIQUE (tenant_id, source_address, ordinal) )',
  'CREATE INDEX effect_agent_subscriptions_owner ON __NAMESPACE__."effect_agent_subscriptions" (tenant_id, source_address, owner_id, ordinal)',
  'CREATE INDEX effect_agent_subscriptions_candidates ON __NAMESPACE__."effect_agent_subscriptions" (tenant_id, source_address, source_name, source_version, matching_key, ordinal)',
  'CREATE INDEX effect_agent_subscriptions_recovery ON __NAMESPACE__."effect_agent_subscriptions" (tenant_id, source_address, recovery_at_millis, ordinal) WHERE recovery_at_millis IS NOT NULL',
  'CREATE TABLE __NAMESPACE__."effect_agent_subscription_events" ( tenant_id TEXT COLLATE "C" NOT NULL, source_address TEXT COLLATE "C" NOT NULL, event_id TEXT COLLATE "C" NOT NULL, source_name TEXT COLLATE "C" NOT NULL, source_version TEXT COLLATE "C" NOT NULL, matching_key TEXT COLLATE "C" NOT NULL, payload_digest TEXT COLLATE "C" NOT NULL, cutoff BIGINT NOT NULL, cursor BIGINT NOT NULL, routing_complete BIGINT NOT NULL, tombstone BIGINT NOT NULL DEFAULT 0, next_attempt_at_millis BIGINT NOT NULL, record_json TEXT COLLATE "C" NOT NULL, PRIMARY KEY (tenant_id, source_address, event_id) )',
  'CREATE INDEX effect_agent_subscription_events_pending ON __NAMESPACE__."effect_agent_subscription_events" (tenant_id, source_address, routing_complete, next_attempt_at_millis, event_id)',
  'CREATE TABLE __NAMESPACE__."effect_agent_subscription_deliveries" ( tenant_id TEXT COLLATE "C" NOT NULL, source_address TEXT COLLATE "C" NOT NULL, owner_id TEXT COLLATE "C" NOT NULL, subscription_id TEXT COLLATE "C" NOT NULL, event_id TEXT COLLATE "C" NOT NULL, delivery_key TEXT COLLATE "C" NOT NULL, state TEXT COLLATE "C" NOT NULL, next_attempt_at_millis BIGINT NOT NULL, record_json TEXT COLLATE "C" NOT NULL, retry_parked BOOLEAN NOT NULL, observe_settlement BOOLEAN NOT NULL, PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id, event_id), UNIQUE (tenant_id, source_address, delivery_key) )',
  'CREATE INDEX effect_agent_subscription_deliveries_pending ON __NAMESPACE__."effect_agent_subscription_deliveries" (tenant_id, source_address, state, next_attempt_at_millis, delivery_key)',
  'CREATE INDEX effect_agent_subscription_deliveries_registration ON __NAMESPACE__."effect_agent_subscription_deliveries" (tenant_id, source_address, owner_id, subscription_id, delivery_key)',
  'CREATE TABLE __NAMESPACE__."effect_agent_message_deliveries" ( owner_thread_id TEXT COLLATE "C" NOT NULL, message_id TEXT COLLATE "C" NOT NULL, version BIGINT NOT NULL, state TEXT COLLATE "C" NOT NULL, deadline_at_millis BIGINT, record_json TEXT COLLATE "C" NOT NULL, read_metadata JSONB NOT NULL, PRIMARY KEY (owner_thread_id, message_id) )',
  'CREATE INDEX effect_agent_message_deliveries_due ON __NAMESPACE__."effect_agent_message_deliveries" (deadline_at_millis, owner_thread_id, message_id) WHERE deadline_at_millis IS NOT NULL',
  'CREATE TABLE __NAMESPACE__."effect_agent_recovery_checkpoints" ( thread_id TEXT COLLATE "C" PRIMARY KEY NOT NULL, through_sequence BIGINT NOT NULL, tail_digest TEXT COLLATE "C" NOT NULL, checkpoint_json TEXT COLLATE "C" NOT NULL, FOREIGN KEY (thread_id) REFERENCES __NAMESPACE__."effect_agent_threads"(thread_id) ON DELETE RESTRICT )',
  "CREATE INDEX effect_agent_submissions_nonterminal ON __NAMESPACE__.\"effect_agent_submissions\" (thread_id, queue_sequence) WHERE state <> 'settled'",
  'CREATE TABLE __NAMESPACE__."effect_agent_worker_stops" (thread_id TEXT COLLATE "C" PRIMARY KEY NOT NULL, terminal TEXT COLLATE "C")',
  "CREATE INDEX effect_agent_worker_starts ON __NAMESPACE__.\"effect_agent_message_deliveries\"(owner_thread_id, (read_metadata ->> 'delegationId'), (read_metadata ->> 'targetAgentId'), message_id) WHERE (read_metadata ->> 'workerStart') = 'true'",
  "CREATE INDEX effect_agent_worker_pending ON __NAMESPACE__.\"effect_agent_message_deliveries\"(owner_thread_id, (read_metadata ->> 'threadId'), message_id) WHERE state IN ('pending', 'parked') AND (read_metadata ->> 'hasReceipt') = 'false'",
  'ALTER TABLE __NAMESPACE__."effect_agent_canonical_records" ADD COLUMN read_metadata JSONB NOT NULL',
  "CREATE INDEX effect_agent_records_call ON __NAMESPACE__.\"effect_agent_canonical_records\"(thread_id, (read_metadata ->> 'tag'), (read_metadata ->> 'runId'), (read_metadata ->> 'toolCallId'))",
  "CREATE INDEX effect_agent_records_run_input ON __NAMESPACE__.\"effect_agent_canonical_records\"(thread_id, (read_metadata ->> 'runId')) WHERE (read_metadata ->> 'tag') = 'UserInputRecorded' AND (read_metadata ->> 'kind') = 'user'",
  "CREATE INDEX effect_agent_records_subtree ON __NAMESPACE__.\"effect_agent_canonical_records\"(thread_id, (read_metadata ->> 'sourceSubmissionId'), sequence) WHERE (read_metadata ->> 'tag') = 'SubtreeBudgetReserved'",
  "CREATE INDEX effect_agent_records_worker_input ON __NAMESPACE__.\"effect_agent_canonical_records\"(thread_id, (read_metadata ->> 'messageId')) WHERE (read_metadata ->> 'tag') = 'WorkerInputRequested'",
  "CREATE INDEX effect_agent_worker_execution ON __NAMESPACE__.\"effect_agent_canonical_records\"(thread_id, (read_metadata ->> 'tag'), sequence) WHERE (read_metadata ->> 'runId') IS NOT NULL",
  "CREATE INDEX effect_agent_message_deliveries_pending ON __NAMESPACE__.\"effect_agent_message_deliveries\"(owner_thread_id, message_id) WHERE state NOT IN ('processed', 'refused')",
] as const;

const baselineShape: Readonly<
  Record<
    string,
    { readonly columns: ReadonlyArray<string>; readonly constraints: ReadonlyArray<string> }
  >
> = {
  effect_agent_abort_intents: {
    columns: [
      "submission_id:text:true",
      "author:text:true",
      "reason:text:true",
      "requested_at:text:true",
      "canonical_record_id:text:false",
    ],
    constraints: [
      "FOREIGN KEY (submission_id) REFERENCES effect_agent_submissions(submission_id) ON DELETE RESTRICT",
      "PRIMARY KEY (submission_id)",
    ],
  },
  effect_agent_approval_decisions: {
    columns: [
      "submission_id:text:true",
      "tool_call_id:text:true",
      "decision:text:true",
      "resolver:text:true",
      "reason:text:true",
      "decided_at:text:true",
    ],
    constraints: [
      "FOREIGN KEY (submission_id) REFERENCES effect_agent_submissions(submission_id) ON DELETE RESTRICT",
      "PRIMARY KEY (submission_id, tool_call_id)",
    ],
  },
  effect_agent_attempts: {
    columns: [
      "attempt_id:text:true",
      "submission_id:text:true",
      "thread_id:text:true",
      "owner_producer_id:text:true",
      "producer_epoch:bigint:true",
      "claimed_at:text:true",
    ],
    constraints: [
      "FOREIGN KEY (submission_id) REFERENCES effect_agent_submissions(submission_id) ON DELETE RESTRICT",
      "PRIMARY KEY (attempt_id)",
    ],
  },
  effect_agent_canonical_batches: {
    columns: [
      "thread_id:text:true",
      "batch_id:text:true",
      "first_sequence:bigint:true",
      "last_sequence:bigint:true",
      "batch_digest:text:true",
      "tail_digest:text:true",
      "batch_json:text:true",
    ],
    constraints: [
      "FOREIGN KEY (thread_id) REFERENCES effect_agent_threads(thread_id) ON DELETE RESTRICT",
      "PRIMARY KEY (thread_id, batch_id)",
    ],
  },
  effect_agent_canonical_records: {
    columns: [
      "thread_id:text:true",
      "sequence:bigint:true",
      "record_id:text:true",
      "batch_id:text:true",
      "record_json:text:true",
      "read_metadata:jsonb:true",
    ],
    constraints: [
      "FOREIGN KEY (thread_id, batch_id) REFERENCES effect_agent_canonical_batches(thread_id, batch_id) ON DELETE RESTRICT",
      "PRIMARY KEY (thread_id, sequence)",
      "UNIQUE (thread_id, record_id)",
    ],
  },
  effect_agent_checkpoints: {
    columns: [
      "thread_id:text:true",
      "through_sequence:bigint:true",
      "tail_digest:text:true",
      "checkpoint_json:text:true",
    ],
    constraints: [
      "FOREIGN KEY (thread_id) REFERENCES effect_agent_threads(thread_id) ON DELETE RESTRICT",
      "PRIMARY KEY (thread_id, through_sequence)",
    ],
  },
  effect_agent_child_reservations: {
    columns: [
      "reservation_id:text:true",
      "parent_submission_id:text:true",
      "parent_tool_call_id:text:true",
      "child_submission_id:text:false",
      "status:text:true",
      "allocation_json:text:true",
      "allocation_digest:text:true",
      "accounting_json:text:false",
      "reserved_at:text:true",
      "release_began_at:text:false",
      "released_at:text:false",
    ],
    constraints: [
      "FOREIGN KEY (parent_submission_id) REFERENCES effect_agent_submissions(submission_id) ON DELETE RESTRICT",
      "PRIMARY KEY (reservation_id)",
      "UNIQUE (parent_submission_id, parent_tool_call_id)",
    ],
  },
  effect_agent_message_deliveries: {
    columns: [
      "owner_thread_id:text:true",
      "message_id:text:true",
      "version:bigint:true",
      "state:text:true",
      "deadline_at_millis:bigint:false",
      "record_json:text:true",
      "read_metadata:jsonb:true",
    ],
    constraints: ["PRIMARY KEY (owner_thread_id, message_id)"],
  },
  effect_agent_recovery_checkpoints: {
    columns: [
      "thread_id:text:true",
      "through_sequence:bigint:true",
      "tail_digest:text:true",
      "checkpoint_json:text:true",
    ],
    constraints: [
      "FOREIGN KEY (thread_id) REFERENCES effect_agent_threads(thread_id) ON DELETE RESTRICT",
      "PRIMARY KEY (thread_id)",
    ],
  },
  effect_agent_schedules: {
    columns: [
      "tenant_id:text:true",
      "owner_id:text:true",
      "schedule_id:text:true",
      "deadline_at_millis:bigint:false",
      "record_json:text:true",
      "uses_capacity:boolean:true",
    ],
    constraints: ["PRIMARY KEY (tenant_id, owner_id, schedule_id)"],
  },
  effect_agent_storage_version: {
    columns: ["id:boolean:true", "version:bigint:true"],
    constraints: ["CHECK (id)", "PRIMARY KEY (id)"],
  },
  effect_agent_submission_ownership: {
    columns: [
      "submission_id:text:true",
      "attempt_id:text:true",
      "ownership_token:text:true",
      "producer_epoch:bigint:true",
      "owner_producer_id:text:true",
      "lease_expires_at:text:true",
    ],
    constraints: [
      "FOREIGN KEY (submission_id) REFERENCES effect_agent_submissions(submission_id) ON DELETE RESTRICT",
      "PRIMARY KEY (submission_id)",
    ],
  },
  effect_agent_submissions: {
    columns: [
      "submission_id:text:true",
      "thread_id:text:true",
      "queue_sequence:bigint:true",
      "principal:text:true",
      "idempotency_key:text:true",
      "agent_id:text:true",
      "agent_digests_json:text:true",
      "deployment_id:text:true",
      "input_json:text:true",
      "input_digest:text:true",
      "receipt_id:text:true",
      "state:text:true",
      "settled_outcome:text:false",
      "settled_record_id:text:false",
      "finalized_at:text:false",
      "created_at:text:true",
      "ready_at:text:false",
      "input_applied_record_id:text:false",
      "input_applied_sequence:bigint:false",
      "joined_host_submission_id:text:false",
      "suspended_reason_json:text:false",
      "suspended_at:text:false",
      "unknown_reason:text:false",
      "unknown_tool_call_ids_json:text:false",
      "parent_submission_id:text:false",
      "parent_tool_call_id:text:false",
      "admission_group:text:false",
      "admission_fence_json:text:false",
      "worker_admission_json:text:false",
      "message_admission_json:text:false",
    ],
    constraints: [
      "PRIMARY KEY (submission_id)",
      "UNIQUE (thread_id, principal, idempotency_key)",
      "UNIQUE (thread_id, queue_sequence)",
    ],
  },
  effect_agent_subscription_deliveries: {
    columns: [
      "tenant_id:text:true",
      "source_address:text:true",
      "owner_id:text:true",
      "subscription_id:text:true",
      "event_id:text:true",
      "delivery_key:text:true",
      "state:text:true",
      "next_attempt_at_millis:bigint:true",
      "record_json:text:true",
      "retry_parked:boolean:true",
      "observe_settlement:boolean:true",
    ],
    constraints: [
      "PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id, event_id)",
      "UNIQUE (tenant_id, source_address, delivery_key)",
    ],
  },
  effect_agent_subscription_events: {
    columns: [
      "tenant_id:text:true",
      "source_address:text:true",
      "event_id:text:true",
      "source_name:text:true",
      "source_version:text:true",
      "matching_key:text:true",
      "payload_digest:text:true",
      "cutoff:bigint:true",
      "cursor:bigint:true",
      "routing_complete:bigint:true",
      "tombstone:bigint:true",
      "next_attempt_at_millis:bigint:true",
      "record_json:text:true",
    ],
    constraints: ["PRIMARY KEY (tenant_id, source_address, event_id)"],
  },
  effect_agent_subscription_sequences: {
    columns: [
      "tenant_id:text:true",
      "source_address:text:true",
      "sequence:bigint:true",
      "event_scan_cursor:text:true",
      "delivery_scan_cursor:text:true",
      "recovery_scan_cursor:bigint:true",
    ],
    constraints: ["PRIMARY KEY (tenant_id, source_address)"],
  },
  effect_agent_subscriptions: {
    columns: [
      "tenant_id:text:true",
      "source_address:text:true",
      "owner_id:text:true",
      "subscription_id:text:true",
      "ordinal:bigint:true",
      "source_name:text:true",
      "source_version:text:true",
      "matching_key:text:true",
      "state:text:true",
      "expires_at_millis:bigint:false",
      "recovery_at_millis:bigint:false",
      "recovery_present:bigint:true",
      "record_json:text:true",
    ],
    constraints: [
      "PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id)",
      "UNIQUE (tenant_id, source_address, ordinal)",
    ],
  },
  effect_agent_threads: {
    columns: [
      "thread_id:text:true",
      "created_at:text:true",
      "tail_sequence:bigint:true",
      "tail_digest:text:true",
      "producer_epoch:bigint:true",
    ],
    constraints: ["PRIMARY KEY (thread_id)"],
  },
  effect_agent_unknown_resolutions: {
    columns: [
      "submission_id:text:true",
      "tool_call_id:text:true",
      "author:text:true",
      "reason:text:true",
      "resolution_json:text:true",
      "resolved_at:text:true",
    ],
    constraints: [
      "FOREIGN KEY (submission_id) REFERENCES effect_agent_submissions(submission_id) ON DELETE RESTRICT",
      "PRIMARY KEY (submission_id, tool_call_id)",
    ],
  },
  effect_agent_worker_stops: {
    columns: ["thread_id:text:true", "terminal:text:false"],
    constraints: ["PRIMARY KEY (thread_id)"],
  },
};

const baselineIndexes: Readonly<Record<string, string>> = {
  effect_agent_abort_intents_pkey:
    "CREATE UNIQUE INDEX effect_agent_abort_intents_pkey ON effect_agent_abort_intents USING btree (submission_id)",
  effect_agent_approval_decisions_pkey:
    "CREATE UNIQUE INDEX effect_agent_approval_decisions_pkey ON effect_agent_approval_decisions USING btree (submission_id, tool_call_id)",
  effect_agent_attempts_pkey:
    "CREATE UNIQUE INDEX effect_agent_attempts_pkey ON effect_agent_attempts USING btree (attempt_id)",
  effect_agent_canonical_batches_pkey:
    "CREATE UNIQUE INDEX effect_agent_canonical_batches_pkey ON effect_agent_canonical_batches USING btree (thread_id, batch_id)",
  effect_agent_canonical_records_batch:
    "CREATE INDEX effect_agent_canonical_records_batch ON effect_agent_canonical_records USING btree (thread_id, batch_id, sequence)",
  effect_agent_canonical_records_pkey:
    "CREATE UNIQUE INDEX effect_agent_canonical_records_pkey ON effect_agent_canonical_records USING btree (thread_id, sequence)",
  effect_agent_canonical_records_thread_id_record_id_key:
    "CREATE UNIQUE INDEX effect_agent_canonical_records_thread_id_record_id_key ON effect_agent_canonical_records USING btree (thread_id, record_id)",
  effect_agent_checkpoints_pkey:
    "CREATE UNIQUE INDEX effect_agent_checkpoints_pkey ON effect_agent_checkpoints USING btree (thread_id, through_sequence)",
  effect_agent_child_reservatio_parent_submission_id_parent_t_key:
    "CREATE UNIQUE INDEX effect_agent_child_reservatio_parent_submission_id_parent_t_key ON effect_agent_child_reservations USING btree (parent_submission_id, parent_tool_call_id)",
  effect_agent_child_reservations_pkey:
    "CREATE UNIQUE INDEX effect_agent_child_reservations_pkey ON effect_agent_child_reservations USING btree (reservation_id)",
  effect_agent_message_deliveries_due:
    "CREATE INDEX effect_agent_message_deliveries_due ON effect_agent_message_deliveries USING btree (deadline_at_millis, owner_thread_id, message_id) WHERE (deadline_at_millis IS NOT NULL)",
  effect_agent_message_deliveries_pending:
    "CREATE INDEX effect_agent_message_deliveries_pending ON effect_agent_message_deliveries USING btree (owner_thread_id, message_id) WHERE (state <> ALL (ARRAY['processed'::text, 'refused'::text]))",
  effect_agent_message_deliveries_pkey:
    "CREATE UNIQUE INDEX effect_agent_message_deliveries_pkey ON effect_agent_message_deliveries USING btree (owner_thread_id, message_id)",
  effect_agent_records_call:
    "CREATE INDEX effect_agent_records_call ON effect_agent_canonical_records USING btree (thread_id, ((read_metadata ->> 'tag'::text)), ((read_metadata ->> 'runId'::text)), ((read_metadata ->> 'toolCallId'::text)))",
  effect_agent_records_run_input:
    "CREATE INDEX effect_agent_records_run_input ON effect_agent_canonical_records USING btree (thread_id, ((read_metadata ->> 'runId'::text))) WHERE (((read_metadata ->> 'tag'::text) = 'UserInputRecorded'::text) AND ((read_metadata ->> 'kind'::text) = 'user'::text))",
  effect_agent_records_subtree:
    "CREATE INDEX effect_agent_records_subtree ON effect_agent_canonical_records USING btree (thread_id, ((read_metadata ->> 'sourceSubmissionId'::text)), sequence) WHERE ((read_metadata ->> 'tag'::text) = 'SubtreeBudgetReserved'::text)",
  effect_agent_records_worker_input:
    "CREATE INDEX effect_agent_records_worker_input ON effect_agent_canonical_records USING btree (thread_id, ((read_metadata ->> 'messageId'::text))) WHERE ((read_metadata ->> 'tag'::text) = 'WorkerInputRequested'::text)",
  effect_agent_recovery_checkpoints_pkey:
    "CREATE UNIQUE INDEX effect_agent_recovery_checkpoints_pkey ON effect_agent_recovery_checkpoints USING btree (thread_id)",
  effect_agent_schedules_deadline:
    "CREATE INDEX effect_agent_schedules_deadline ON effect_agent_schedules USING btree (deadline_at_millis, tenant_id, owner_id, schedule_id) WHERE (deadline_at_millis IS NOT NULL)",
  effect_agent_schedules_owner_deadline:
    "CREATE INDEX effect_agent_schedules_owner_deadline ON effect_agent_schedules USING btree (tenant_id, owner_id, deadline_at_millis, schedule_id) WHERE (deadline_at_millis IS NOT NULL)",
  effect_agent_schedules_pkey:
    "CREATE UNIQUE INDEX effect_agent_schedules_pkey ON effect_agent_schedules USING btree (tenant_id, owner_id, schedule_id)",
  effect_agent_storage_version_pkey:
    "CREATE UNIQUE INDEX effect_agent_storage_version_pkey ON effect_agent_storage_version USING btree (id)",
  effect_agent_submission_ownership_pkey:
    "CREATE UNIQUE INDEX effect_agent_submission_ownership_pkey ON effect_agent_submission_ownership USING btree (submission_id)",
  effect_agent_submissions_group:
    "CREATE INDEX effect_agent_submissions_group ON effect_agent_submissions USING btree (thread_id, admission_group, state)",
  effect_agent_submissions_joined_host:
    "CREATE INDEX effect_agent_submissions_joined_host ON effect_agent_submissions USING btree (joined_host_submission_id)",
  effect_agent_submissions_nonterminal:
    "CREATE INDEX effect_agent_submissions_nonterminal ON effect_agent_submissions USING btree (thread_id, queue_sequence) WHERE (state <> 'settled'::text)",
  effect_agent_submissions_parent:
    "CREATE INDEX effect_agent_submissions_parent ON effect_agent_submissions USING btree (parent_submission_id)",
  effect_agent_submissions_pkey:
    "CREATE UNIQUE INDEX effect_agent_submissions_pkey ON effect_agent_submissions USING btree (submission_id)",
  effect_agent_submissions_thread_id_principal_idempotency_ke_key:
    "CREATE UNIQUE INDEX effect_agent_submissions_thread_id_principal_idempotency_ke_key ON effect_agent_submissions USING btree (thread_id, principal, idempotency_key)",
  effect_agent_submissions_thread_id_queue_sequence_key:
    "CREATE UNIQUE INDEX effect_agent_submissions_thread_id_queue_sequence_key ON effect_agent_submissions USING btree (thread_id, queue_sequence)",
  effect_agent_subscription_del_tenant_id_source_address_deli_key:
    "CREATE UNIQUE INDEX effect_agent_subscription_del_tenant_id_source_address_deli_key ON effect_agent_subscription_deliveries USING btree (tenant_id, source_address, delivery_key)",
  effect_agent_subscription_deliveries_pending:
    "CREATE INDEX effect_agent_subscription_deliveries_pending ON effect_agent_subscription_deliveries USING btree (tenant_id, source_address, state, next_attempt_at_millis, delivery_key)",
  effect_agent_subscription_deliveries_pkey:
    "CREATE UNIQUE INDEX effect_agent_subscription_deliveries_pkey ON effect_agent_subscription_deliveries USING btree (tenant_id, source_address, owner_id, subscription_id, event_id)",
  effect_agent_subscription_deliveries_registration:
    "CREATE INDEX effect_agent_subscription_deliveries_registration ON effect_agent_subscription_deliveries USING btree (tenant_id, source_address, owner_id, subscription_id, delivery_key)",
  effect_agent_subscription_events_pending:
    "CREATE INDEX effect_agent_subscription_events_pending ON effect_agent_subscription_events USING btree (tenant_id, source_address, routing_complete, next_attempt_at_millis, event_id)",
  effect_agent_subscription_events_pkey:
    "CREATE UNIQUE INDEX effect_agent_subscription_events_pkey ON effect_agent_subscription_events USING btree (tenant_id, source_address, event_id)",
  effect_agent_subscription_sequences_pkey:
    "CREATE UNIQUE INDEX effect_agent_subscription_sequences_pkey ON effect_agent_subscription_sequences USING btree (tenant_id, source_address)",
  effect_agent_subscriptions_candidates:
    "CREATE INDEX effect_agent_subscriptions_candidates ON effect_agent_subscriptions USING btree (tenant_id, source_address, source_name, source_version, matching_key, ordinal)",
  effect_agent_subscriptions_owner:
    "CREATE INDEX effect_agent_subscriptions_owner ON effect_agent_subscriptions USING btree (tenant_id, source_address, owner_id, ordinal)",
  effect_agent_subscriptions_pkey:
    "CREATE UNIQUE INDEX effect_agent_subscriptions_pkey ON effect_agent_subscriptions USING btree (tenant_id, source_address, owner_id, subscription_id)",
  effect_agent_subscriptions_recovery:
    "CREATE INDEX effect_agent_subscriptions_recovery ON effect_agent_subscriptions USING btree (tenant_id, source_address, recovery_at_millis, ordinal) WHERE (recovery_at_millis IS NOT NULL)",
  effect_agent_subscriptions_tenant_id_source_address_ordinal_key:
    "CREATE UNIQUE INDEX effect_agent_subscriptions_tenant_id_source_address_ordinal_key ON effect_agent_subscriptions USING btree (tenant_id, source_address, ordinal)",
  effect_agent_threads_pkey:
    "CREATE UNIQUE INDEX effect_agent_threads_pkey ON effect_agent_threads USING btree (thread_id)",
  effect_agent_unknown_resolutions_pkey:
    "CREATE UNIQUE INDEX effect_agent_unknown_resolutions_pkey ON effect_agent_unknown_resolutions USING btree (submission_id, tool_call_id)",
  effect_agent_worker_execution:
    "CREATE INDEX effect_agent_worker_execution ON effect_agent_canonical_records USING btree (thread_id, ((read_metadata ->> 'tag'::text)), sequence) WHERE ((read_metadata ->> 'runId'::text) IS NOT NULL)",
  effect_agent_worker_pending:
    "CREATE INDEX effect_agent_worker_pending ON effect_agent_message_deliveries USING btree (owner_thread_id, ((read_metadata ->> 'threadId'::text)), message_id) WHERE ((state = ANY (ARRAY['pending'::text, 'parked'::text])) AND ((read_metadata ->> 'hasReceipt'::text) = 'false'::text))",
  effect_agent_worker_starts:
    "CREATE INDEX effect_agent_worker_starts ON effect_agent_message_deliveries USING btree (owner_thread_id, ((read_metadata ->> 'delegationId'::text)), ((read_metadata ->> 'targetAgentId'::text)), message_id) WHERE ((read_metadata ->> 'workerStart'::text) = 'true'::text)",
  effect_agent_worker_stops_pkey:
    "CREATE UNIQUE INDEX effect_agent_worker_stops_pkey ON effect_agent_worker_stops USING btree (thread_id)",
};

const headerStatement =
  "CREATE TABLE __NAMESPACE__.effect_agent_schema (singleton BIGINT PRIMARY KEY NOT NULL CHECK (singleton = 1), layout_version BIGINT NOT NULL CHECK (layout_version > 0), record_format TEXT NOT NULL CHECK (length(record_format) > 0))";

/** Ordered immutable DDL; namespace substitution only quotes an SQL identifier. */
export const postgresLayoutSteps = [
  {
    version: 16,
    statements: [
      ...baseline16,
      "INSERT INTO __NAMESPACE__.effect_agent_storage_version (id, version) VALUES (TRUE, 16)",
    ],
  },
  {
    version: 17,
    statements: [
      headerStatement,
      "INSERT INTO __NAMESPACE__.effect_agent_schema (singleton, layout_version, record_format) VALUES (1, 17, 'effect-agent/thread@1')",
      "UPDATE __NAMESPACE__.effect_agent_storage_version SET version = 17 WHERE id",
    ],
  },
] as const;

const quote = (namespace: string) => `"${namespace.replaceAll('"', '""')}"`;

const qualify = (statement: string, namespace: string) =>
  statement.replaceAll("__NAMESPACE__", quote(namespace));

const unqualify = (statement: string, namespace: string) =>
  statement.replaceAll(`${quote(namespace)}.`, "").replaceAll(`${namespace}.`, "");

const NameRows = Schema.Array(Schema.Struct({ name: Schema.String, kind: Schema.String }));

const ColumnRows = Schema.Array(
  Schema.Struct({
    name: Schema.String,
    column_name: Schema.String,
    type: Schema.String,
    not_null: Schema.Boolean,
  }),
);

const DefinitionRows = Schema.Array(
  Schema.Struct({ name: Schema.String, definition: Schema.String }),
);

const sameStrings = Schema.toEquivalence(Schema.Array(Schema.String));

const Header = Schema.Tuple([
  Schema.Struct({
    singleton: SqlInteger,
    layout_version: SqlInteger,
    record_format: Schema.NonEmptyString,
  }),
]);

const Legacy = Schema.Tuple([Schema.Struct({ id: Schema.Literal(true), version: SqlInteger })]);

const incompatible = (actualVersion: number, message: string) =>
  PostgresStorageCompatibilityError.make({
    actualVersion,
    supportedVersion: CurrentPostgresStorageVersion,
    message: `${message} Keep the original database; no layout upgrade was committed.`,
  });

const decode = <A, I>(schema: Schema.Codec<A, I>, value: unknown, table: string) =>
  Schema.decodeUnknownEffect(schema)(value, { onExcessProperty: "error" }).pipe(
    Effect.mapError(() =>
      PostgresStorageCorruptionError.make({
        table,
        rowKey: "schema",
        message: "Malformed storage layout or version header.",
      }),
    ),
  );

export interface PostgresStorageHeader {
  readonly layoutVersion: number;
  readonly recordFormat: string;
}

/** No DDL or transaction acquisition. The caller owns the snapshot/writer lock across subsequent work. */
export const inspectPostgresStorage = Effect.fnUntraced(function* (
  namespace: string,
  acceptedFormats: ReadonlyArray<string>,
) {
  const sql = yield* SqlClient.SqlClient;
  const { table, execute } = yield* makeSqlQuery(namespace);

  const names = yield* decode(
    NameRows,
    yield* execute(sql<Record<string, unknown>>`
    SELECT c.relname AS name, c.relkind::text AS kind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname=${namespace} AND starts_with(c.relname, 'effect_agent_')
  `),
    "pg_class",
  );

  const versionTable = names.find((row) => row.name === "effect_agent_storage_version");
  const schemaTable = names.find((row) => row.name === "effect_agent_schema");

  if (versionTable === undefined) {
    const independent = new Set([
      "effect_agent_activity_metadata",
      "effect_agent_activity_metadata_pkey",
      "effect_agent_activity_processor_state_v1",
      "effect_agent_activity_processor_state_v1_pkey",
    ]);

    if (names.every((row) => independent.has(row.name))) return undefined;

    return yield* incompatible(0, "Unversioned or incomplete Effect Agent storage.");
  }
  if (versionTable.kind !== "r") return yield* incompatible(0, "Malformed legacy version table.");

  const [legacy] = yield* decode(
    Legacy,
    yield* execute(
      sql<Record<string, unknown>>`SELECT * FROM ${table("effect_agent_storage_version")}`,
    ),
    "effect_agent_storage_version",
  );

  if (!postgresLayoutSteps.some((step) => step.version === legacy.version))
    return yield* incompatible(legacy.version, `Unsupported storage version ${legacy.version}.`);
  let header: PostgresStorageHeader;

  if (schemaTable === undefined) {
    if (legacy.version !== 16)
      return yield* incompatible(legacy.version, "Missing singleton schema header.");
    header = { layoutVersion: 16, recordFormat: LEGACY_RECORD_FORMAT };
  } else {
    if (schemaTable.kind !== "r")
      return yield* incompatible(legacy.version, "Malformed singleton schema table.");

    const [row] = yield* decode(
      Header,
      yield* execute(sql<Record<string, unknown>>`SELECT * FROM ${table("effect_agent_schema")}`),
      "effect_agent_schema",
    );

    if (
      row.singleton !== 1 ||
      row.layout_version < 17 ||
      !postgresLayoutSteps.some((step) => step.version === row.layout_version) ||
      row.layout_version !== legacy.version
    )
      return yield* incompatible(row.layout_version, "Unsupported or conflicting layout headers.");
    header = { layoutVersion: row.layout_version, recordFormat: row.record_format };
  }
  if (!acceptedFormats.includes(header.recordFormat))
    return yield* incompatible(
      header.layoutVersion,
      `Unsupported record format ${header.recordFormat}.`,
    );

  const columns = yield* decode(
    ColumnRows,
    yield* execute(sql<Record<string, unknown>>`
    SELECT c.relname AS name, a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS type, a.attnotnull AS not_null
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid
    WHERE n.nspname=${namespace} AND starts_with(c.relname, 'effect_agent_') AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped
    ORDER BY c.relname,a.attnum
  `),
    "pg_attribute",
  );

  const constraints = yield* decode(
    DefinitionRows,
    yield* execute(sql<Record<string, unknown>>`
    SELECT c.relname AS name, pg_get_constraintdef(k.oid) AS definition FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
    JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=${namespace} AND k.contype IN ('p','u','f','c','x')
    ORDER BY c.relname, pg_get_constraintdef(k.oid)
  `),
    "pg_constraint",
  );

  const indexes = yield* decode(
    DefinitionRows,
    yield* execute(sql<Record<string, unknown>>`
    SELECT indexname AS name, indexdef AS definition FROM pg_indexes WHERE schemaname=${namespace} ORDER BY indexname
  `),
    "pg_indexes",
  );

  const expectedShape =
    schemaTable === undefined
      ? baselineShape
      : {
          ...baselineShape,
          effect_agent_schema: {
            columns: [
              "singleton:bigint:true",
              "layout_version:bigint:true",
              "record_format:text:true",
            ],
            constraints: [
              "PRIMARY KEY (singleton)",
              "CHECK ((singleton = 1))",
              "CHECK ((layout_version > 0))",
              "CHECK ((length(record_format) > 0))",
            ],
          },
        };

  for (const [name, expected] of Object.entries(expectedShape)) {
    const actualColumns = columns
      .filter((row) => row.name === name)
      .map((row) => `${row.column_name}:${row.type}:${row.not_null}`);

    const actualConstraints = constraints
      .filter((row) => row.name === name)
      .map((row) => unqualify(row.definition, namespace))
      .sort();

    if (
      names.find((row) => row.name === name)?.kind !== "r" ||
      !sameStrings(actualColumns, expected.columns) ||
      !sameStrings(actualConstraints, [...expected.constraints].sort())
    )
      return yield* incompatible(legacy.version, `Missing or incompatible layout table ${name}.`);
  }
  for (const [name, definition] of Object.entries(baselineIndexes)) {
    const actual = indexes.find((row) => row.name === name);

    if (actual === undefined || unqualify(actual.definition, namespace) !== definition)
      return yield* incompatible(legacy.version, `Missing or incompatible layout index ${name}.`);
  }

  return header;
});

/** Export opening never creates a schema, runs layout steps, or initializes a journal. */
export const readPostgresStorageHeader = Effect.fnUntraced(function* (
  namespace: string,
  acceptedFormats: ReadonlyArray<string>,
) {
  const header = yield* inspectPostgresStorage(namespace, acceptedFormats);

  if (header === undefined)
    return yield* incompatible(0, "No initialized Thread storage to export.");

  return header;
});

/** The caller owns one writer transaction encompassing inspection, every step, and final validation. */
export const applyPostgresLayout = Effect.fnUntraced(function* (
  namespace: string,
  header: PostgresStorageHeader | undefined,
) {
  const sql = yield* SqlClient.SqlClient;
  const version = header?.layoutVersion ?? 0;

  for (const step of postgresLayoutSteps) {
    if (step.version <= version) continue;
    for (const statement of step.statements)
      yield* sql.unsafe(qualify(statement, namespace)).withoutTransform;
  }

  // Fresh storage uses today's record format; frozen steps retain the legacy format.
  if (header === undefined) {
    const { table, execute } = yield* makeSqlQuery(namespace);

    yield* execute(
      sql`UPDATE ${table("effect_agent_schema")} SET record_format = ${CURRENT_RECORD_FORMAT} WHERE singleton = 1`,
    );
  }

  return yield* readPostgresStorageHeader(namespace, [CURRENT_RECORD_FORMAT]);
});
