import { makeSqlQuery, SqlInteger } from "@yielded/agent-storage-sql/sql-storage";
import { CURRENT_RECORD_FORMAT } from "@yielded/agent/records";
import { Effect, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";

import {
  PostgresStorageCompatibilityError,
  PostgresStorageCorruptionError,
} from "../PostgresStorageError.ts";
import { matchesLayoutExpressions, type LayoutExpression } from "./layout-expression.ts";

export const CurrentPostgresStorageVersion = 18;

/** Fresh layout only; inspection rejects every predecessor before DDL. */
const layoutStatements = [
  'CREATE TABLE __NAMESPACE__."effect_agent_storage_version" ( id BOOLEAN PRIMARY KEY NOT NULL, version BIGINT NOT NULL, CONSTRAINT effect_agent_storage_version_single_row CHECK (id) )',
  'CREATE TABLE __NAMESPACE__."effect_agent_threads" ( thread_id TEXT COLLATE "C" PRIMARY KEY NOT NULL, created_at TEXT COLLATE "C" NOT NULL, tail_sequence BIGINT NOT NULL, tail_digest TEXT COLLATE "C" NOT NULL, producer_epoch BIGINT NOT NULL )',
  'CREATE TABLE __NAMESPACE__."effect_agent_canonical_batches" ( thread_id TEXT COLLATE "C" NOT NULL, batch_id TEXT COLLATE "C" NOT NULL, first_sequence BIGINT NOT NULL, last_sequence BIGINT NOT NULL, batch_digest TEXT COLLATE "C" NOT NULL, tail_digest TEXT COLLATE "C" NOT NULL, batch_json TEXT COLLATE "C" NOT NULL, PRIMARY KEY (thread_id, batch_id), FOREIGN KEY (thread_id) REFERENCES __NAMESPACE__."effect_agent_threads"(thread_id) ON DELETE RESTRICT )',
  'CREATE TABLE __NAMESPACE__."effect_agent_canonical_records" ( thread_id TEXT COLLATE "C" NOT NULL, sequence BIGINT NOT NULL, record_id TEXT COLLATE "C" NOT NULL, batch_id TEXT COLLATE "C" NOT NULL, record_json TEXT COLLATE "C" NOT NULL, record_tag TEXT COLLATE "C" NOT NULL, run_id TEXT COLLATE "C", tool_call_id TEXT COLLATE "C", input_kind TEXT COLLATE "C", source_submission_id TEXT COLLATE "C", message_id TEXT COLLATE "C", handoff BIGINT NOT NULL, PRIMARY KEY (thread_id, sequence), UNIQUE (thread_id, record_id), FOREIGN KEY (thread_id, batch_id) REFERENCES __NAMESPACE__."effect_agent_canonical_batches"(thread_id, batch_id) ON DELETE RESTRICT )',
  'CREATE TABLE __NAMESPACE__."effect_agent_record_runs" ( thread_id TEXT COLLATE "C" NOT NULL, run_id TEXT COLLATE "C" NOT NULL, sequence BIGINT NOT NULL, PRIMARY KEY (thread_id, run_id, sequence), FOREIGN KEY (thread_id, sequence) REFERENCES __NAMESPACE__."effect_agent_canonical_records"(thread_id, sequence) ON DELETE RESTRICT )',
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
  "CREATE INDEX effect_agent_submissions_nonterminal ON __NAMESPACE__.\"effect_agent_submissions\" (thread_id, queue_sequence) WHERE state <> 'settled'",
  'CREATE TABLE __NAMESPACE__."effect_agent_worker_stops" (thread_id TEXT COLLATE "C" PRIMARY KEY NOT NULL, terminal TEXT COLLATE "C")',
  "CREATE INDEX effect_agent_worker_starts ON __NAMESPACE__.\"effect_agent_message_deliveries\"(owner_thread_id, (read_metadata ->> 'delegationId'), (read_metadata ->> 'targetAgentId'), message_id) WHERE (read_metadata ->> 'workerStart') = 'true'",
  "CREATE INDEX effect_agent_worker_pending ON __NAMESPACE__.\"effect_agent_message_deliveries\"(owner_thread_id, (read_metadata ->> 'threadId'), message_id) WHERE state IN ('pending', 'parked') AND (read_metadata ->> 'hasReceipt') = 'false'",
  'CREATE INDEX effect_agent_records_call ON __NAMESPACE__."effect_agent_canonical_records"(thread_id, record_tag, run_id, tool_call_id)',
  "CREATE INDEX effect_agent_records_run_input ON __NAMESPACE__.\"effect_agent_canonical_records\"(thread_id, run_id) WHERE record_tag = 'UserInputRecorded' AND input_kind = 'user'",
  "CREATE INDEX effect_agent_records_subtree ON __NAMESPACE__.\"effect_agent_canonical_records\"(thread_id, source_submission_id, sequence) WHERE record_tag = 'SubtreeBudgetReserved'",
  "CREATE INDEX effect_agent_records_worker_input ON __NAMESPACE__.\"effect_agent_canonical_records\"(thread_id, message_id) WHERE record_tag = 'WorkerInputRequested'",
  'CREATE INDEX effect_agent_worker_execution ON __NAMESPACE__."effect_agent_canonical_records"(thread_id, record_tag, sequence) WHERE run_id IS NOT NULL',
  "CREATE INDEX effect_agent_message_deliveries_pending ON __NAMESPACE__.\"effect_agent_message_deliveries\"(owner_thread_id, message_id) WHERE state NOT IN ('processed', 'refused')",
  "CREATE INDEX effect_agent_records_continuation ON __NAMESPACE__.\"effect_agent_canonical_records\"(thread_id, run_id, sequence) WHERE record_tag = 'RunContinuation'",
  'CREATE INDEX effect_agent_records_tag ON __NAMESPACE__."effect_agent_canonical_records"(thread_id, record_tag, sequence)',
  'CREATE INDEX effect_agent_records_handoff ON __NAMESPACE__."effect_agent_canonical_records"(thread_id, sequence) WHERE handoff = 1',
] as const;

type LayoutConstraint =
  | readonly [kind: "p" | "u", columns: ReadonlyArray<string>]
  | readonly [
      kind: "f",
      columns: ReadonlyArray<string>,
      targetTable: string,
      targetColumns: ReadonlyArray<string>,
    ]
  | readonly [kind: "c", columns: ReadonlyArray<string>, expression: LayoutExpression];

interface LayoutIndex {
  readonly table: string;
  readonly columns: ReadonlyArray<string | null>;
  readonly unique?: boolean;
  readonly primary?: boolean;
  readonly expressions?: ReadonlyArray<string>;
  readonly predicate?: LayoutExpression;
}

// Logical layout expectations. Physical index storage parameters and tablespaces are not
// part of compatibility. Expression expectations are canonical data; a closed recognizer
// accepts the deparser spellings of these forms and rejects every other form.
const layoutShape: Readonly<
  Record<
    string,
    {
      readonly columns: ReadonlyArray<string>;
      readonly constraints: Readonly<Record<string, LayoutConstraint>>;
    }
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
    constraints: {
      effect_agent_abort_intents_pkey: ["p", ["submission_id"]],
      effect_agent_abort_intents_submission_id_fkey: [
        "f",
        ["submission_id"],
        "effect_agent_submissions",
        ["submission_id"],
      ],
    },
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
    constraints: {
      effect_agent_approval_decisions_pkey: ["p", ["submission_id", "tool_call_id"]],
      effect_agent_approval_decisions_submission_id_fkey: [
        "f",
        ["submission_id"],
        "effect_agent_submissions",
        ["submission_id"],
      ],
    },
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
    constraints: {
      effect_agent_attempts_pkey: ["p", ["attempt_id"]],
      effect_agent_attempts_submission_id_fkey: [
        "f",
        ["submission_id"],
        "effect_agent_submissions",
        ["submission_id"],
      ],
    },
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
    constraints: {
      effect_agent_canonical_batches_pkey: ["p", ["thread_id", "batch_id"]],
      effect_agent_canonical_batches_thread_id_fkey: [
        "f",
        ["thread_id"],
        "effect_agent_threads",
        ["thread_id"],
      ],
    },
  },
  effect_agent_record_runs: {
    columns: ["thread_id:text:true", "run_id:text:true", "sequence:bigint:true"],
    constraints: {
      effect_agent_record_runs_pkey: ["p", ["thread_id", "run_id", "sequence"]],
      effect_agent_record_runs_thread_id_sequence_fkey: [
        "f",
        ["thread_id", "sequence"],
        "effect_agent_canonical_records",
        ["thread_id", "sequence"],
      ],
    },
  },
  effect_agent_canonical_records: {
    columns: [
      "thread_id:text:true",
      "sequence:bigint:true",
      "record_id:text:true",
      "batch_id:text:true",
      "record_json:text:true",
      "record_tag:text:true",
      "run_id:text:false",
      "tool_call_id:text:false",
      "input_kind:text:false",
      "source_submission_id:text:false",
      "message_id:text:false",
      "handoff:bigint:true",
    ],
    constraints: {
      effect_agent_canonical_records_pkey: ["p", ["thread_id", "sequence"]],
      effect_agent_canonical_records_thread_id_batch_id_fkey: [
        "f",
        ["thread_id", "batch_id"],
        "effect_agent_canonical_batches",
        ["thread_id", "batch_id"],
      ],
      effect_agent_canonical_records_thread_id_record_id_key: ["u", ["thread_id", "record_id"]],
    },
  },
  effect_agent_checkpoints: {
    columns: [
      "thread_id:text:true",
      "through_sequence:bigint:true",
      "tail_digest:text:true",
      "checkpoint_json:text:true",
    ],
    constraints: {
      effect_agent_checkpoints_pkey: ["p", ["thread_id", "through_sequence"]],
      effect_agent_checkpoints_thread_id_fkey: [
        "f",
        ["thread_id"],
        "effect_agent_threads",
        ["thread_id"],
      ],
    },
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
    constraints: {
      effect_agent_child_reservatio_parent_submission_id_parent_t_key: [
        "u",
        ["parent_submission_id", "parent_tool_call_id"],
      ],
      effect_agent_child_reservations_parent_submission_id_fkey: [
        "f",
        ["parent_submission_id"],
        "effect_agent_submissions",
        ["submission_id"],
      ],
      effect_agent_child_reservations_pkey: ["p", ["reservation_id"]],
    },
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
    constraints: { effect_agent_message_deliveries_pkey: ["p", ["owner_thread_id", "message_id"]] },
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
    constraints: { effect_agent_schedules_pkey: ["p", ["tenant_id", "owner_id", "schedule_id"]] },
  },
  effect_agent_storage_version: {
    columns: ["id:boolean:true", "version:bigint:true"],
    constraints: {
      effect_agent_storage_version_pkey: ["p", ["id"]],
      effect_agent_storage_version_single_row: ["c", ["id"], ["column", "id"]],
    },
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
    constraints: {
      effect_agent_submission_ownership_pkey: ["p", ["submission_id"]],
      effect_agent_submission_ownership_submission_id_fkey: [
        "f",
        ["submission_id"],
        "effect_agent_submissions",
        ["submission_id"],
      ],
    },
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
    constraints: {
      effect_agent_submissions_pkey: ["p", ["submission_id"]],
      effect_agent_submissions_thread_id_principal_idempotency_ke_key: [
        "u",
        ["thread_id", "principal", "idempotency_key"],
      ],
      effect_agent_submissions_thread_id_queue_sequence_key: ["u", ["thread_id", "queue_sequence"]],
    },
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
    constraints: {
      effect_agent_subscription_del_tenant_id_source_address_deli_key: [
        "u",
        ["tenant_id", "source_address", "delivery_key"],
      ],
      effect_agent_subscription_deliveries_pkey: [
        "p",
        ["tenant_id", "source_address", "owner_id", "subscription_id", "event_id"],
      ],
    },
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
    constraints: {
      effect_agent_subscription_events_pkey: ["p", ["tenant_id", "source_address", "event_id"]],
    },
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
    constraints: {
      effect_agent_subscription_sequences_pkey: ["p", ["tenant_id", "source_address"]],
    },
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
    constraints: {
      effect_agent_subscriptions_pkey: [
        "p",
        ["tenant_id", "source_address", "owner_id", "subscription_id"],
      ],
      effect_agent_subscriptions_tenant_id_source_address_ordinal_key: [
        "u",
        ["tenant_id", "source_address", "ordinal"],
      ],
    },
  },
  effect_agent_threads: {
    columns: [
      "thread_id:text:true",
      "created_at:text:true",
      "tail_sequence:bigint:true",
      "tail_digest:text:true",
      "producer_epoch:bigint:true",
    ],
    constraints: { effect_agent_threads_pkey: ["p", ["thread_id"]] },
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
    constraints: {
      effect_agent_unknown_resolutions_pkey: ["p", ["submission_id", "tool_call_id"]],
      effect_agent_unknown_resolutions_submission_id_fkey: [
        "f",
        ["submission_id"],
        "effect_agent_submissions",
        ["submission_id"],
      ],
    },
  },
  effect_agent_worker_stops: {
    columns: ["thread_id:text:true", "terminal:text:false"],
    constraints: { effect_agent_worker_stops_pkey: ["p", ["thread_id"]] },
  },
};

const layoutIndexes: Readonly<Record<string, LayoutIndex>> = {
  effect_agent_records_continuation: {
    table: "effect_agent_canonical_records",
    columns: ["thread_id", "run_id", "sequence"],
    predicate: ["eq", ["column", "record_tag"], ["text", "RunContinuation"]],
  },
  effect_agent_records_tag: {
    table: "effect_agent_canonical_records",
    columns: ["thread_id", "record_tag", "sequence"],
  },
  effect_agent_records_handoff: {
    table: "effect_agent_canonical_records",
    columns: ["thread_id", "sequence"],
    predicate: ["eq", ["column", "handoff"], ["integer", 1]],
  },
  effect_agent_canonical_records_batch: {
    table: "effect_agent_canonical_records",
    columns: ["thread_id", "batch_id", "sequence"],
  },
  effect_agent_message_deliveries_due: {
    table: "effect_agent_message_deliveries",
    columns: ["deadline_at_millis", "owner_thread_id", "message_id"],
    predicate: ["notNull", ["column", "deadline_at_millis"]],
  },
  effect_agent_message_deliveries_pending: {
    table: "effect_agent_message_deliveries",
    columns: ["owner_thread_id", "message_id"],
    predicate: ["notIn", ["column", "state"], ["processed", "refused"]],
  },
  effect_agent_records_call: {
    table: "effect_agent_canonical_records",
    columns: ["thread_id", "record_tag", "run_id", "tool_call_id"],
  },
  effect_agent_records_run_input: {
    table: "effect_agent_canonical_records",
    columns: ["thread_id", "run_id"],
    predicate: [
      "and",
      [
        ["eq", ["column", "record_tag"], ["text", "UserInputRecorded"]],
        ["eq", ["column", "input_kind"], ["text", "user"]],
      ],
    ],
  },
  effect_agent_records_subtree: {
    table: "effect_agent_canonical_records",
    columns: ["thread_id", "source_submission_id", "sequence"],
    predicate: ["eq", ["column", "record_tag"], ["text", "SubtreeBudgetReserved"]],
  },
  effect_agent_records_worker_input: {
    table: "effect_agent_canonical_records",
    columns: ["thread_id", "message_id"],
    predicate: ["eq", ["column", "record_tag"], ["text", "WorkerInputRequested"]],
  },
  effect_agent_schedules_deadline: {
    table: "effect_agent_schedules",
    columns: ["deadline_at_millis", "tenant_id", "owner_id", "schedule_id"],
    predicate: ["notNull", ["column", "deadline_at_millis"]],
  },
  effect_agent_schedules_owner_deadline: {
    table: "effect_agent_schedules",
    columns: ["tenant_id", "owner_id", "deadline_at_millis", "schedule_id"],
    predicate: ["notNull", ["column", "deadline_at_millis"]],
  },
  effect_agent_submissions_group: {
    table: "effect_agent_submissions",
    columns: ["thread_id", "admission_group", "state"],
  },
  effect_agent_submissions_joined_host: {
    table: "effect_agent_submissions",
    columns: ["joined_host_submission_id"],
  },
  effect_agent_submissions_nonterminal: {
    table: "effect_agent_submissions",
    columns: ["thread_id", "queue_sequence"],
    predicate: ["ne", ["column", "state"], ["text", "settled"]],
  },
  effect_agent_submissions_parent: {
    table: "effect_agent_submissions",
    columns: ["parent_submission_id"],
  },
  effect_agent_subscription_deliveries_pending: {
    table: "effect_agent_subscription_deliveries",
    columns: ["tenant_id", "source_address", "state", "next_attempt_at_millis", "delivery_key"],
  },
  effect_agent_subscription_deliveries_registration: {
    table: "effect_agent_subscription_deliveries",
    columns: ["tenant_id", "source_address", "owner_id", "subscription_id", "delivery_key"],
  },
  effect_agent_subscription_events_pending: {
    table: "effect_agent_subscription_events",
    columns: [
      "tenant_id",
      "source_address",
      "routing_complete",
      "next_attempt_at_millis",
      "event_id",
    ],
  },
  effect_agent_subscriptions_candidates: {
    table: "effect_agent_subscriptions",
    columns: [
      "tenant_id",
      "source_address",
      "source_name",
      "source_version",
      "matching_key",
      "ordinal",
    ],
  },
  effect_agent_subscriptions_owner: {
    table: "effect_agent_subscriptions",
    columns: ["tenant_id", "source_address", "owner_id", "ordinal"],
  },
  effect_agent_subscriptions_recovery: {
    table: "effect_agent_subscriptions",
    columns: ["tenant_id", "source_address", "recovery_at_millis", "ordinal"],
    predicate: ["notNull", ["column", "recovery_at_millis"]],
  },
  effect_agent_worker_execution: {
    table: "effect_agent_canonical_records",
    columns: ["thread_id", "record_tag", "sequence"],
    predicate: ["notNull", ["column", "run_id"]],
  },
  effect_agent_worker_pending: {
    table: "effect_agent_message_deliveries",
    columns: ["owner_thread_id", null, "message_id"],
    expressions: ["threadId"],
    predicate: [
      "and",
      [
        ["in", ["column", "state"], ["pending", "parked"]],
        ["eq", ["json", "hasReceipt"], ["text", "false"]],
      ],
    ],
  },
  effect_agent_worker_starts: {
    table: "effect_agent_message_deliveries",
    columns: ["owner_thread_id", null, null, "message_id"],
    expressions: ["delegationId", "targetAgentId"],
    predicate: ["eq", ["json", "workerStart"], ["text", "true"]],
  },
};

const headerStatement =
  "CREATE TABLE __NAMESPACE__.effect_agent_schema (singleton BIGINT PRIMARY KEY NOT NULL CHECK (singleton = 1), layout_version BIGINT NOT NULL CHECK (layout_version > 0), record_format TEXT NOT NULL CHECK (length(record_format) > 0))";

const quote = (namespace: string) => `"${namespace.replaceAll('"', '""')}"`;

const qualify = (statement: string, namespace: string) =>
  statement.replaceAll("__NAMESPACE__", quote(namespace));

const NameRows = Schema.Array(Schema.Struct({ name: Schema.String, kind: Schema.String }));

const ColumnRows = Schema.Array(
  Schema.Struct({
    name: Schema.String,
    column_name: Schema.String,
    type: Schema.String,
    not_null: Schema.Boolean,
    collation: Schema.NullOr(Schema.String),
  }),
);

const ConstraintRows = Schema.Array(
  Schema.Struct({
    table_name: Schema.String,
    name: Schema.String,
    kind: Schema.String,
    columns: Schema.Array(Schema.String),
    target_table: Schema.NullOr(Schema.String),
    target_schema: Schema.NullOr(Schema.String),
    target_columns: Schema.Array(Schema.String),
    expression: Schema.NullOr(Schema.String),
    enforcement_ok: Schema.Boolean,
  }),
);

const IndexRows = Schema.Array(
  Schema.Struct({
    name: Schema.String,
    table_name: Schema.String,
    columns: Schema.Array(Schema.NullOr(Schema.String)),
    is_unique: Schema.Boolean,
    is_primary: Schema.Boolean,
    expressions: Schema.NullOr(Schema.String),
    predicate: Schema.NullOr(Schema.String),
    structure_ok: Schema.Boolean,
  }),
);

const sameIndexColumns = Schema.toEquivalence(Schema.Array(Schema.NullOr(Schema.String)));

const matchesIndex = (actual: (typeof IndexRows.Type)[number] | undefined, expected: LayoutIndex) =>
  actual !== undefined &&
  actual.structure_ok &&
  actual.table_name === expected.table &&
  actual.is_unique === (expected.unique ?? false) &&
  actual.is_primary === (expected.primary ?? false) &&
  sameIndexColumns(actual.columns, expected.columns) &&
  matchesLayoutExpressions(
    actual.expressions,
    (expected.expressions ?? []).map((key) => ["json", key]),
  ) &&
  matchesLayoutExpressions(
    actual.predicate,
    expected.predicate === undefined ? [] : [expected.predicate],
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
    message: `${message} Keep the original database; the store was not changed.`,
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
export const inspectPostgresStorage = Effect.fnUntraced(function* (namespace: string) {
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
  if (versionTable.kind !== "r") return yield* incompatible(0, "Malformed version table.");

  const [legacy] = yield* decode(
    Legacy,
    yield* execute(
      sql<Record<string, unknown>>`SELECT * FROM ${table("effect_agent_storage_version")}`,
    ),
    "effect_agent_storage_version",
  );

  if (legacy.version !== CurrentPostgresStorageVersion)
    return yield* incompatible(legacy.version, `Unsupported storage version ${legacy.version}.`);
  if (schemaTable?.kind !== "r")
    return yield* incompatible(legacy.version, "Missing or malformed singleton schema table.");

  const [row] = yield* decode(
    Header,
    yield* execute(sql<Record<string, unknown>>`SELECT * FROM ${table("effect_agent_schema")}`),
    "effect_agent_schema",
  );

  if (
    row.singleton !== 1 ||
    row.layout_version !== CurrentPostgresStorageVersion ||
    row.layout_version !== legacy.version
  )
    return yield* incompatible(row.layout_version, "Unsupported or conflicting layout headers.");
  const header = { layoutVersion: row.layout_version, recordFormat: row.record_format };

  if (header.recordFormat !== CURRENT_RECORD_FORMAT)
    return yield* incompatible(
      header.layoutVersion,
      `Unsupported record format ${header.recordFormat}.`,
    );

  const columns = yield* decode(
    ColumnRows,
    yield* execute(sql<Record<string, unknown>>`
    SELECT c.relname AS name, a.attname AS column_name,
      CASE WHEN a.atttypmod <> -1 THEN ''
        WHEN a.atttypid='pg_catalog.int8'::regtype THEN 'bigint'
        WHEN a.atttypid='pg_catalog.bool'::regtype THEN 'boolean'
        WHEN a.atttypid='pg_catalog.text'::regtype THEN 'text'
        WHEN a.atttypid='pg_catalog.jsonb'::regtype THEN 'jsonb' ELSE '' END AS type,
      a.attnotnull AS not_null,
      cn.nspname || '.' || co.collname AS collation
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid
    LEFT JOIN pg_collation co ON co.oid=a.attcollation LEFT JOIN pg_namespace cn ON cn.oid=co.collnamespace
    WHERE n.nspname=${namespace} AND starts_with(c.relname, 'effect_agent_') AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped
    ORDER BY c.relname,a.attnum
  `),
    "pg_attribute",
  );

  // Shipped expressions use only pinned built-ins. Any function/operator dependency
  // denotes an extension or user-defined overload, even if its deparsed spelling matches.
  const constraints = yield* decode(
    ConstraintRows,
    yield* execute(sql<Record<string, unknown>>`
      SELECT c.relname AS table_name, k.conname AS name, k.contype::text AS kind,
        to_jsonb(ARRAY(SELECT a.attname FROM unnest(k.conkey) WITH ORDINALITY v(attnum, position)
          JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attnum=v.attnum ORDER BY v.position)) AS columns,
        r.relname AS target_table, rn.nspname AS target_schema,
        to_jsonb(ARRAY(SELECT a.attname FROM unnest(k.confkey) WITH ORDINALITY v(attnum, position)
          JOIN pg_attribute a ON a.attrelid=k.confrelid AND a.attnum=v.attnum ORDER BY v.position)) AS target_columns,
        pg_get_expr(k.conbin, k.conrelid, false) AS expression,
        (k.convalidated AND NOT k.condeferrable AND NOT k.condeferred
          AND k.conislocal AND k.coninhcount=0 AND k.conparentid=0
          AND (k.contype<>'c' OR NOT k.connoinherit)
          AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid='pg_constraint'::regclass
            AND d.objid=k.oid AND d.refclassid IN ('pg_proc'::regclass, 'pg_operator'::regclass))
          AND (k.contype NOT IN ('p','u') OR ic.relname=k.conname)
          AND (k.contype<>'f' OR (k.confupdtype='a' AND k.confdeltype='r' AND k.confmatchtype='s'
            AND k.conpfeqop <@ ARRAY['pg_catalog.=(text,text)'::regoperator, 'pg_catalog.=(bigint,bigint)'::regoperator]::oid[]
            AND k.conppeqop <@ ARRAY['pg_catalog.=(text,text)'::regoperator, 'pg_catalog.=(bigint,bigint)'::regoperator]::oid[]
            AND k.conffeqop <@ ARRAY['pg_catalog.=(text,text)'::regoperator, 'pg_catalog.=(bigint,bigint)'::regoperator]::oid[]))) AS enforcement_ok
      FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
      JOIN pg_namespace n ON n.oid=c.relnamespace
      LEFT JOIN pg_class r ON r.oid=k.confrelid LEFT JOIN pg_namespace rn ON rn.oid=r.relnamespace
      LEFT JOIN pg_class ic ON ic.oid=k.conindid
      WHERE n.nspname=${namespace} AND k.contype IN ('p','u','f','c','x')
    `),
    "pg_constraint",
  );

  // Frozen keys use btree ASC NULLS LAST (indoption=0), default built-in opclasses,
  // and the column collation, or the database default for JSON text expressions.
  const indexes = yield* decode(
    IndexRows,
    yield* execute(sql<Record<string, unknown>>`
      SELECT ic.relname AS name, c.relname AS table_name,
        to_jsonb(ARRAY(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY v(attnum, position)
          LEFT JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=v.attnum ORDER BY v.position)) AS columns,
        i.indisunique AS is_unique, i.indisprimary AS is_primary,
        pg_get_expr(i.indexprs, i.indrelid, false) AS expressions,
        pg_get_expr(i.indpred, i.indrelid, false) AS predicate,
        (ic.relkind='i' AND am.amname='btree' AND i.indisvalid AND i.indisready AND i.indislive
          AND NOT i.indisexclusion AND i.indimmediate AND NOT i.indnullsnotdistinct
          AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid='pg_class'::regclass
            AND d.objid=i.indexrelid AND d.refclassid IN ('pg_proc'::regclass, 'pg_operator'::regclass))
          AND i.indnatts=i.indnkeyatts AND NOT EXISTS (
            SELECT 1 FROM unnest(i.indkey, i.indcollation, i.indclass, i.indoption)
              WITH ORDINALITY v(attnum, collation_oid, opclass, options, position)
            JOIN pg_attribute ia ON ia.attrelid=i.indexrelid AND ia.attnum=v.position
            LEFT JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=v.attnum
            JOIN pg_opclass op ON op.oid=v.opclass
            WHERE v.options<>0 OR NOT op.opcdefault OR op.opcnamespace<>'pg_catalog'::regnamespace
              OR op.opcintype<>ia.atttypid OR op.opcmethod<>ic.relam
              OR v.collation_oid<>CASE WHEN v.attnum=0 THEN 'pg_catalog.default'::regcollation ELSE a.attcollation END
          )) AS structure_ok
      FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid JOIN pg_class c ON c.oid=i.indrelid
      JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_am am ON am.oid=ic.relam
      WHERE n.nspname=${namespace}
    `),
    "pg_index",
  );

  const expectedShape: typeof layoutShape = {
    ...layoutShape,
    effect_agent_schema: {
      columns: ["singleton:bigint:true", "layout_version:bigint:true", "record_format:text:true"],
      constraints: {
        effect_agent_schema_pkey: ["p", ["singleton"]],
        effect_agent_schema_singleton_check: [
          "c",
          ["singleton"],
          ["eq", ["column", "singleton"], ["integer", 1]],
        ],
        effect_agent_schema_layout_version_check: [
          "c",
          ["layout_version"],
          ["gt", ["column", "layout_version"], ["integer", 0]],
        ],
        effect_agent_schema_record_format_check: [
          "c",
          ["record_format"],
          ["gt", ["length", "record_format"], ["integer", 0]],
        ],
      },
    },
  };

  for (const [name, expected] of Object.entries(expectedShape)) {
    const tableColumns = columns.filter((row) => row.name === name);
    const actualConstraints = constraints.filter((row) => row.table_name === name);

    if (
      names.find((row) => row.name === name)?.kind !== "r" ||
      !sameStrings(
        tableColumns.map((row) => `${row.column_name}:${row.type}:${row.not_null}`),
        expected.columns,
      ) ||
      tableColumns.some(
        (row) =>
          row.collation !==
          (row.type === "text"
            ? name === "effect_agent_schema"
              ? "pg_catalog.default"
              : "pg_catalog.C"
            : null),
      ) ||
      actualConstraints.length !== Object.keys(expected.constraints).length
    )
      return yield* incompatible(legacy.version, `Missing or incompatible layout table ${name}.`);

    for (const [constraintName, constraint] of Object.entries(expected.constraints)) {
      const actual = actualConstraints.find((row) => row.name === constraintName);
      const [kind, keyColumns] = constraint;

      if (
        actual === undefined ||
        !actual.enforcement_ok ||
        actual.kind !== kind ||
        !sameStrings(actual.columns, keyColumns) ||
        actual.target_table !== (constraint[0] === "f" ? constraint[2] : null) ||
        actual.target_schema !== (kind === "f" ? namespace : null) ||
        !sameStrings(actual.target_columns, constraint[0] === "f" ? constraint[3] : []) ||
        !matchesLayoutExpressions(actual.expression, constraint[0] === "c" ? [constraint[2]] : [])
      )
        return yield* incompatible(
          legacy.version,
          `Missing or incompatible layout constraint ${constraintName}.`,
        );

      if (
        (kind === "p" || kind === "u") &&
        !matchesIndex(
          indexes.find((row) => row.name === constraintName),
          {
            table: name,
            columns: keyColumns,
            unique: true,
            primary: kind === "p",
          },
        )
      )
        return yield* incompatible(
          legacy.version,
          `Missing or incompatible layout index ${constraintName}.`,
        );
    }
  }
  for (const [name, expected] of Object.entries(layoutIndexes)) {
    if (
      !matchesIndex(
        indexes.find((row) => row.name === name),
        expected,
      )
    )
      return yield* incompatible(legacy.version, `Missing or incompatible layout index ${name}.`);
  }

  return header;
});

/** Export opening never creates a schema, runs layout steps, or initializes a journal. */
export const readPostgresStorageHeader = Effect.fnUntraced(function* (namespace: string) {
  const header = yield* inspectPostgresStorage(namespace);

  if (header === undefined)
    return yield* incompatible(0, "No initialized Thread storage to export.");

  return header;
});

/** The caller owns one writer transaction encompassing inspection, fresh initialization, and final validation. */
export const applyPostgresLayout = Effect.fnUntraced(function* (
  namespace: string,
  header: PostgresStorageHeader | undefined,
) {
  const sql = yield* SqlClient.SqlClient;

  if (header !== undefined) return header;
  for (const statement of layoutStatements)
    yield* sql.unsafe(qualify(statement, namespace)).withoutTransform;
  yield* sql.unsafe(qualify(headerStatement, namespace)).withoutTransform;
  const { table, execute } = yield* makeSqlQuery(namespace);

  yield* execute(
    sql`INSERT INTO ${table("effect_agent_storage_version")} (id, version) VALUES (TRUE, 18)`,
  );
  yield* execute(
    sql`INSERT INTO ${table("effect_agent_schema")} (singleton, layout_version, record_format) VALUES (1, 18, ${CURRENT_RECORD_FORMAT})`,
  );

  return yield* readPostgresStorageHeader(namespace);
});
