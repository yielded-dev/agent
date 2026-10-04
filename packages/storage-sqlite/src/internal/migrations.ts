import { makeSqlTransaction } from "@yielded/agent-storage-sql/sql-storage";
import { makeSqliteLayoutInspection } from "@yielded/agent-storage-sql/sqlite-layout-inspection";
import { CURRENT_RECORD_FORMAT } from "@yielded/agent/records";
import { Effect, Schema } from "effect";
import type * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

import {
  SqliteStorageCompatibilityError,
  SqliteStorageCorruptionError,
  SqliteStorageError,
} from "../SqliteStorageError.ts";

export const CurrentSqliteStorageVersion = 17;
// Frozen legacy record format: never replace this with a future CURRENT_RECORD_FORMAT.
const LEGACY_RECORD_FORMAT = "effect-agent/thread@1";

// Captured from the shipped layout16 initializer. Historical statements never call current DDL helpers.
const baseline16 = [
  'CREATE TABLE "effect_agent_threads" ( thread_id TEXT PRIMARY KEY NOT NULL, created_at TEXT NOT NULL, tail_sequence INTEGER NOT NULL, tail_digest TEXT NOT NULL, producer_epoch INTEGER NOT NULL )',
  'CREATE TABLE "effect_agent_canonical_batches" ( thread_id TEXT NOT NULL, batch_id TEXT NOT NULL, first_sequence INTEGER NOT NULL, last_sequence INTEGER NOT NULL, batch_digest TEXT NOT NULL, tail_digest TEXT NOT NULL, batch_json TEXT NOT NULL, PRIMARY KEY (thread_id, batch_id), FOREIGN KEY (thread_id) REFERENCES "effect_agent_threads"(thread_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_canonical_records" ( thread_id TEXT NOT NULL, sequence INTEGER NOT NULL, record_id TEXT NOT NULL, batch_id TEXT NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY (thread_id, sequence), UNIQUE (thread_id, record_id), FOREIGN KEY (thread_id, batch_id) REFERENCES "effect_agent_canonical_batches"(thread_id, batch_id) ON DELETE RESTRICT )',
  'CREATE INDEX effect_agent_canonical_records_batch ON "effect_agent_canonical_records" (thread_id, batch_id, sequence)',
  'CREATE TABLE "effect_agent_checkpoints" ( thread_id TEXT NOT NULL, through_sequence INTEGER NOT NULL, tail_digest TEXT NOT NULL, checkpoint_json TEXT NOT NULL, PRIMARY KEY (thread_id, through_sequence), FOREIGN KEY (thread_id) REFERENCES "effect_agent_threads"(thread_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_submissions" ( submission_id TEXT PRIMARY KEY NOT NULL, thread_id TEXT NOT NULL, queue_sequence INTEGER NOT NULL, principal TEXT NOT NULL, idempotency_key TEXT NOT NULL, agent_id TEXT NOT NULL, agent_digests_json TEXT NOT NULL, deployment_id TEXT NOT NULL, input_json TEXT NOT NULL, input_digest TEXT NOT NULL, receipt_id TEXT NOT NULL, state TEXT NOT NULL, settled_outcome TEXT, settled_record_id TEXT, finalized_at TEXT, created_at TEXT NOT NULL, ready_at TEXT, input_applied_record_id TEXT, input_applied_sequence INTEGER, joined_host_submission_id TEXT, suspended_reason_json TEXT, suspended_at TEXT, unknown_reason TEXT, unknown_tool_call_ids_json TEXT, parent_submission_id TEXT, parent_tool_call_id TEXT, admission_group TEXT, admission_fence_json TEXT, worker_admission_json TEXT, message_admission_json TEXT, UNIQUE (thread_id, principal, idempotency_key), UNIQUE (thread_id, queue_sequence) )',
  'CREATE INDEX effect_agent_submissions_group ON "effect_agent_submissions" (thread_id, admission_group, state)',
  'CREATE TABLE "effect_agent_submission_ownership" ( submission_id TEXT PRIMARY KEY NOT NULL, attempt_id TEXT NOT NULL, ownership_token TEXT NOT NULL, producer_epoch INTEGER NOT NULL, owner_producer_id TEXT NOT NULL, lease_expires_at TEXT NOT NULL, FOREIGN KEY (submission_id) REFERENCES "effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_attempts" ( attempt_id TEXT PRIMARY KEY NOT NULL, submission_id TEXT NOT NULL, thread_id TEXT NOT NULL, owner_producer_id TEXT NOT NULL, producer_epoch INTEGER NOT NULL, claimed_at TEXT NOT NULL, FOREIGN KEY (submission_id) REFERENCES "effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_abort_intents" ( submission_id TEXT PRIMARY KEY NOT NULL, author TEXT NOT NULL, reason TEXT NOT NULL, requested_at TEXT NOT NULL, canonical_record_id TEXT, FOREIGN KEY (submission_id) REFERENCES "effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE INDEX effect_agent_submissions_joined_host ON "effect_agent_submissions" (joined_host_submission_id)',
  'CREATE TABLE "effect_agent_approval_decisions" ( submission_id TEXT NOT NULL, tool_call_id TEXT NOT NULL, decision TEXT NOT NULL, resolver TEXT NOT NULL, reason TEXT NOT NULL, decided_at TEXT NOT NULL, PRIMARY KEY (submission_id, tool_call_id), FOREIGN KEY (submission_id) REFERENCES "effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_unknown_resolutions" ( submission_id TEXT NOT NULL, tool_call_id TEXT NOT NULL, author TEXT NOT NULL, reason TEXT NOT NULL, resolution_json TEXT NOT NULL, resolved_at TEXT NOT NULL, PRIMARY KEY (submission_id, tool_call_id), FOREIGN KEY (submission_id) REFERENCES "effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
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
  'CREATE TABLE "effect_agent_recovery_checkpoints" ( thread_id TEXT PRIMARY KEY NOT NULL, through_sequence INTEGER NOT NULL, tail_digest TEXT NOT NULL, checkpoint_json TEXT NOT NULL, FOREIGN KEY (thread_id) REFERENCES "effect_agent_threads"(thread_id) ON DELETE RESTRICT )',
  "CREATE INDEX effect_agent_submissions_nonterminal ON \"effect_agent_submissions\" (thread_id, queue_sequence) WHERE state <> 'settled'",
  "CREATE INDEX effect_agent_records_call ON \"effect_agent_canonical_records\"(thread_id, json_extract(record_json, '$.payload._tag'), json_extract(record_json, '$.payload.runId'), json_extract(record_json, '$.payload.toolCallId'))",
  "CREATE INDEX effect_agent_records_run_input ON \"effect_agent_canonical_records\"(thread_id, json_extract(record_json, '$.payload.runId')) WHERE json_extract(record_json, '$.payload._tag') = 'UserInputRecorded' AND json_extract(record_json, '$.payload.kind') = 'user'",
  "CREATE INDEX effect_agent_records_subtree ON \"effect_agent_canonical_records\"(thread_id, json_extract(record_json, '$.payload.sourceSubmissionId'), sequence) WHERE json_extract(record_json, '$.payload._tag') = 'SubtreeBudgetReserved'",
  "CREATE INDEX effect_agent_records_worker_input ON \"effect_agent_canonical_records\"(thread_id, json_extract(record_json, '$.payload.admission.messageId')) WHERE json_extract(record_json, '$.payload._tag') = 'WorkerInputRequested'",
  "CREATE INDEX effect_agent_message_deliveries_pending ON \"effect_agent_message_deliveries\"(owner_thread_id, message_id) WHERE state NOT IN ('processed', 'refused')",
  "CREATE TABLE effect_agent_worker_stops (thread_id TEXT PRIMARY KEY NOT NULL, terminal TEXT)",
  "CREATE INDEX effect_agent_worker_starts ON effect_agent_message_deliveries(owner_thread_id, json_extract(record_json, '$.envelope.workerAdmission.origin.worker.delegationId'), json_extract(record_json, '$.envelope.workerAdmission.origin.worker.targetAgentId'), message_id) WHERE message_id = json_extract(record_json, '$.envelope.workerAdmission.origin.firstMessageId')",
  "CREATE INDEX effect_agent_worker_pending ON effect_agent_message_deliveries(owner_thread_id, json_extract(record_json, '$.envelope.workerAdmission.origin.worker.threadId'), message_id) WHERE state IN ('pending', 'parked') AND json_extract(record_json, '$.receipt') IS NULL",
  "CREATE INDEX effect_agent_worker_execution ON effect_agent_canonical_records(thread_id, json_extract(record_json, '$.payload._tag'), sequence) WHERE json_extract(record_json, '$.payload.runId') IS NOT NULL",
] as const;

const baselineObjects = [
  ["table", "effect_agent_abort_intents", 9],
  ["table", "effect_agent_approval_decisions", 11],
  ["table", "effect_agent_attempts", 8],
  ["table", "effect_agent_canonical_batches", 1],
  ["table", "effect_agent_canonical_records", 2],
  ["index", "effect_agent_canonical_records_batch", 3],
  ["table", "effect_agent_checkpoints", 4],
  ["table", "effect_agent_child_reservations", 14],
  ["table", "effect_agent_message_deliveries", 28],
  ["index", "effect_agent_message_deliveries_due", 29],
  ["index", "effect_agent_message_deliveries_pending", 36],
  ["index", "effect_agent_records_call", 32],
  ["index", "effect_agent_records_run_input", 33],
  ["index", "effect_agent_records_subtree", 34],
  ["index", "effect_agent_records_worker_input", 35],
  ["table", "effect_agent_recovery_checkpoints", 30],
  ["table", "effect_agent_schedules", 15],
  ["index", "effect_agent_schedules_deadline", 16],
  ["index", "effect_agent_schedules_owner_deadline", 17],
  ["table", "effect_agent_submission_ownership", 7],
  ["table", "effect_agent_submissions", 5],
  ["index", "effect_agent_submissions_group", 6],
  ["index", "effect_agent_submissions_joined_host", 10],
  ["index", "effect_agent_submissions_nonterminal", 31],
  ["index", "effect_agent_submissions_parent", 13],
  ["table", "effect_agent_subscription_deliveries", 25],
  ["index", "effect_agent_subscription_deliveries_pending", 26],
  ["index", "effect_agent_subscription_deliveries_registration", 27],
  ["table", "effect_agent_subscription_events", 23],
  ["index", "effect_agent_subscription_events_pending", 24],
  ["table", "effect_agent_subscription_sequences", 18],
  ["table", "effect_agent_subscriptions", 19],
  ["index", "effect_agent_subscriptions_candidates", 21],
  ["index", "effect_agent_subscriptions_owner", 20],
  ["index", "effect_agent_subscriptions_recovery", 22],
  ["table", "effect_agent_threads", 0],
  ["table", "effect_agent_unknown_resolutions", 12],
  ["index", "effect_agent_worker_execution", 40],
  ["index", "effect_agent_worker_pending", 39],
  ["index", "effect_agent_worker_starts", 38],
  ["table", "effect_agent_worker_stops", 37],
] as const;

const headerStatement =
  "CREATE TABLE effect_agent_schema (singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), layout_version INTEGER NOT NULL CHECK (layout_version > 0), record_format TEXT NOT NULL CHECK (length(record_format) > 0))";

/** Ordered, immutable adapter layout steps. Record payloads are never rewritten. */
export const sqliteLayoutSteps = [
  { version: 16, statements: [...baseline16, "PRAGMA user_version = 16"] },
  {
    version: 17,
    statements: [
      headerStatement,
      "INSERT INTO effect_agent_schema (singleton, layout_version, record_format) VALUES (1, 17, 'effect-agent/thread@1')",
      "PRAGMA user_version = 17",
    ],
  },
] as const;

const Legacy = Schema.Tuple([Schema.Struct({ user_version: Schema.Int })]);

export interface SqliteStorageHeader {
  readonly layoutVersion: number;
  readonly recordFormat: string;
}

const incompatible = (actualVersion: number, message: string) =>
  SqliteStorageCompatibilityError.make({
    actualVersion,
    supportedVersion: CurrentSqliteStorageVersion,
    message: `${message} Keep the original file; no layout upgrade was committed.`,
  });

const storageError = (cause: SqlError) =>
  SqliteStorageError.make({ operation: "inspect storage layout", cause, message: cause.message });

const { decode, readObjects, readHeader } = makeSqliteLayoutInspection({
  baseline: {
    version: 16,
    recordFormat: LEGACY_RECORD_FORMAT,
    statements: baseline16,
    objects: baselineObjects,
  },
  steps: sqliteLayoutSteps,
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

const inspectStorage = Effect.fnUntraced(function* (
  client: SqlClient.SqlClient,
): Effect.fn.Return<
  SqliteStorageHeader | undefined,
  SqliteStorageCompatibilityError | SqliteStorageCorruptionError | SqliteStorageError
> {
  const sql = client.withoutTransforms();

  const objects = yield* readObjects(sql);

  const [legacy] = yield* decode(
    Legacy,
    yield* sql<Record<string, unknown>>`PRAGMA user_version`.pipe(Effect.mapError(storageError)),
    "pragma_user_version",
  );

  const version = legacy.user_version;

  if (version === 0 && objects.length === 0) return undefined;

  return yield* readHeader(sql, objects, version);
});

/** Read-only inspection. The caller owns a snapshot covering this check and its export reads. */
export const readSqliteStorageHeader = Effect.fnUntraced(function* (sql: SqlClient.SqlClient) {
  const header = yield* inspectStorage(sql);

  if (header === undefined)
    return yield* incompatible(0, "No initialized Thread storage to export.");

  return header;
});

/** Validate under the writer transaction before any DDL; commit every pending step together. */
export const ensureSqliteStorageLayout = Effect.fn("SqliteStorage.upgradeLayout")(function* (
  client: SqlClient.SqlClient,
) {
  const sql = client.withoutTransforms();

  // Current storage opens through a read snapshot even while another connection is writing.
  // Only a pending layout acquires the writer lock, then repeats every check under that lock.
  const current = yield* makeSqlTransaction(sql, { begin: "BEGIN" })(inspectStorage(sql)).pipe(
    Effect.catchTag("SqlError", storageError),
  );

  if (current?.layoutVersion === CurrentSqliteStorageVersion) return current;

  return yield* makeSqlTransaction(sql, { begin: "BEGIN IMMEDIATE" })(
    Effect.gen(function* () {
      const header = yield* inspectStorage(sql);
      const version = header?.layoutVersion ?? 0;

      for (const step of sqliteLayoutSteps) {
        if (step.version <= version) continue;
        for (const statement of step.statements) yield* sql.unsafe(statement).withoutTransform;
      }

      // Fresh storage uses today's record format; frozen steps retain the legacy format.
      if (header === undefined)
        yield* sql`UPDATE effect_agent_schema SET record_format = ${CURRENT_RECORD_FORMAT} WHERE singleton = 1`;

      return yield* readSqliteStorageHeader(sql);
    }),
  ).pipe(Effect.catchTag("SqlError", storageError));
});
