import { makeSqlTransaction } from "@yielded/agent-storage-sql/sql-storage";
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

export const CurrentSqliteStorageVersion = 18;

/** Fresh layout only. Unsupported stores are rejected before these statements execute. */
const layoutStatements = [
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
  "CREATE INDEX effect_agent_records_run ON effect_agent_canonical_records(thread_id, json_extract(record_json, '$.payload.runId'), json_extract(record_json, '$.payload._tag'), sequence)",
  "CREATE INDEX effect_agent_records_run_sequence ON effect_agent_canonical_records(thread_id, json_extract(record_json, '$.payload.runId'), sequence)",
  "CREATE INDEX effect_agent_records_tag ON effect_agent_canonical_records(thread_id, json_extract(record_json, '$.payload._tag'), sequence)",
  "CREATE INDEX effect_agent_records_submission ON effect_agent_canonical_records(thread_id, json_extract(record_json, '$.payload.submissionId'), sequence)",
  "CREATE INDEX effect_agent_records_source ON effect_agent_canonical_records(thread_id, json_extract(record_json, '$.payload.sourceSubmissionId'), sequence)",
  "CREATE INDEX effect_agent_records_worker_source ON effect_agent_canonical_records(thread_id, json_extract(record_json, '$.payload.admission.sourceSubmissionId'), sequence)",
  "CREATE INDEX effect_agent_records_worker_run ON effect_agent_canonical_records(thread_id, json_extract(record_json, '$.payload.admission.origin.source.runId'), sequence) WHERE json_extract(record_json, '$.payload.admission.origin.source._tag') = 'tool'",
  "CREATE INDEX effect_agent_records_update_run ON effect_agent_canonical_records(thread_id, json_extract(record_json, '$.payload.update.runId'), sequence)",
  "CREATE INDEX effect_agent_records_peer_run ON effect_agent_canonical_records(thread_id, json_extract(record_json, '$.payload.source.runId'), sequence) WHERE json_extract(record_json, '$.payload.source._tag') = 'tool'",
] as const;

const headerStatement =
  "CREATE TABLE effect_agent_schema (singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), layout_version INTEGER NOT NULL CHECK (layout_version > 0), record_format TEXT NOT NULL CHECK (length(record_format) > 0))";

const layoutObjects = layoutStatements.map((statement, index) => {
  const name = /^CREATE (TABLE|INDEX) "?([a-z_]+)"?/.exec(statement);

  if (name === null) throw new Error("Invalid fresh layout statement");

  return [name[1].toLowerCase(), name[2], index] as const;
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

const { decode, readObjects, readHeader } = makeSqliteLayoutInspection({
  version: 18,
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
      yield* sql.unsafe(headerStatement).withoutTransform;
      yield* sql`INSERT INTO effect_agent_schema (singleton, layout_version, record_format) VALUES (1, 18, ${CURRENT_RECORD_FORMAT})`;
      yield* sql.unsafe("PRAGMA user_version = 18").withoutTransform;

      return yield* readSqliteStorageHeader();
    }),
  ).pipe(Effect.catchTag("SqlError", storageError));
});
