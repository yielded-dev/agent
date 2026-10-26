import {
  assertLegacyMessagePublicationDrained,
  backfillPeerMessageRetention,
} from "@yielded/agent-storage-sql/sql-message-delivery-store";
import { makeSqlTransaction } from "@yielded/agent-storage-sql/sql-storage";
import { backfillSubscriptionSettlementObservation } from "@yielded/agent-storage-sql/sql-subscription-store";
import {
  SQL_PROMPT_PREDICATE,
  upgradeExecutionCheckpoints,
} from "@yielded/agent-storage-sql/sql-thread-native-reads";
import {
  createSqlThreadWorkTables,
  upgradeSqlThreadWorkIndex,
} from "@yielded/agent-storage-sql/sql-thread-work";
import { makeSqliteLayoutInspection } from "@yielded/agent-storage-sql/sqlite-layout-inspection";
import { CURRENT_RECORD_FORMAT } from "@yielded/agent/records";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type * as SqlError from "effect/sql/SqlError";

import {
  SqliteStorageCompatibilityError,
  SqliteStorageCorruptionError,
  SqliteStorageError,
} from "../SqliteStorageError.ts";

export const CurrentSqliteStorageVersion = 31;

/** Exact released layout, retained for predecessor validation. */
const layout27Statements = [
  "CREATE TABLE \"effect_agent_threads\" ( thread_id TEXT PRIMARY KEY NOT NULL, created_at TEXT, tail_sequence INTEGER NOT NULL, work_version INTEGER NOT NULL DEFAULT 3, work_state TEXT NOT NULL DEFAULT 'ready', work_through_sequence INTEGER NOT NULL DEFAULT 0, work_reporting INTEGER NOT NULL DEFAULT 0, work_entry_count INTEGER NOT NULL DEFAULT 0, worker_input_sequence INTEGER, worker_settlement_sequence INTEGER )",
  "CREATE TABLE effect_agent_envelopes (envelope_key INTEGER PRIMARY KEY NOT NULL, content_digest TEXT UNIQUE NOT NULL, prefix TEXT NOT NULL, before_payload TEXT NOT NULL, before_record_id TEXT NOT NULL, suffix TEXT NOT NULL)",
  "CREATE TABLE effect_agent_run_keys (run_key INTEGER PRIMARY KEY NOT NULL, thread_id TEXT NOT NULL, run_id TEXT NOT NULL, UNIQUE (thread_id, run_id), FOREIGN KEY (thread_id) REFERENCES effect_agent_threads(thread_id) ON DELETE RESTRICT)",
  "CREATE TABLE effect_agent_definitions (definition_key INTEGER PRIMARY KEY NOT NULL, content_digest TEXT UNIQUE NOT NULL, value_json TEXT NOT NULL)",
  "CREATE TABLE effect_agent_executions (thread_id TEXT NOT NULL, run_id TEXT NOT NULL, input_sequence INTEGER NOT NULL, revision INTEGER NOT NULL, checkpoint_json TEXT NOT NULL, last_commit_id TEXT, last_commit_digest TEXT, PRIMARY KEY (thread_id, run_id), FOREIGN KEY (thread_id) REFERENCES effect_agent_threads(thread_id) ON DELETE RESTRICT)",
  'CREATE TABLE "effect_agent_canonical_batches" ( thread_id TEXT NOT NULL, batch_id TEXT NOT NULL, first_sequence INTEGER NOT NULL, last_sequence INTEGER NOT NULL, producer_id_json TEXT NOT NULL, CONSTRAINT effect_agent_canonical_batches_span CHECK (first_sequence >= 1 AND last_sequence >= first_sequence AND last_sequence - first_sequence < 256), PRIMARY KEY (thread_id, batch_id), FOREIGN KEY (thread_id) REFERENCES "effect_agent_threads"(thread_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_canonical_records" ( thread_id TEXT NOT NULL, sequence INTEGER NOT NULL, record_id TEXT NOT NULL, created_at_json TEXT NOT NULL, envelope_key INTEGER NOT NULL, payload_json TEXT, record_tag INTEGER NOT NULL, run_id INTEGER, tool_call_id TEXT, input_kind TEXT, source_submission_id TEXT, message_id TEXT, submission_id TEXT, application_input INTEGER NOT NULL, context_through INTEGER, context_kind TEXT, worker_thread_id TEXT, handoff INTEGER NOT NULL, PRIMARY KEY (thread_id, sequence), UNIQUE (thread_id, record_id), FOREIGN KEY (thread_id) REFERENCES "effect_agent_threads"(thread_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_record_runs" ( thread_id TEXT NOT NULL, run_id INTEGER NOT NULL, sequence INTEGER NOT NULL, PRIMARY KEY (thread_id, run_id, sequence), FOREIGN KEY (thread_id, sequence) REFERENCES "effect_agent_canonical_records"(thread_id, sequence) ON DELETE RESTRICT ) WITHOUT ROWID',
  "CREATE TABLE effect_agent_rollover_buckets (thread_id TEXT NOT NULL, height INTEGER NOT NULL, bucket INTEGER NOT NULL, sequence INTEGER NOT NULL, PRIMARY KEY (thread_id, height, bucket, sequence), FOREIGN KEY (thread_id, sequence) REFERENCES effect_agent_canonical_records(thread_id, sequence) ON DELETE RESTRICT) WITHOUT ROWID",
  "CREATE TABLE effect_agent_tool_declarations (thread_id TEXT NOT NULL, settlement_record_id TEXT NOT NULL, sequence INTEGER NOT NULL, PRIMARY KEY (thread_id, settlement_record_id, sequence), FOREIGN KEY (thread_id, sequence) REFERENCES effect_agent_canonical_records(thread_id, sequence) ON DELETE RESTRICT)",
  "CREATE INDEX effect_agent_tool_declarations_sequence ON effect_agent_tool_declarations(thread_id, sequence)",
  "CREATE TABLE effect_agent_record_refusals (thread_id TEXT NOT NULL, reservation_record_id TEXT NOT NULL, sequence INTEGER NOT NULL, PRIMARY KEY (thread_id, reservation_record_id, sequence), FOREIGN KEY (thread_id, sequence) REFERENCES effect_agent_canonical_records(thread_id, sequence) ON DELETE RESTRICT)",
  "CREATE INDEX effect_agent_record_refusals_sequence ON effect_agent_record_refusals(thread_id, sequence)",
  'CREATE TABLE "effect_agent_checkpoints" ( thread_id TEXT NOT NULL, through_sequence INTEGER NOT NULL, checkpoint_json TEXT NOT NULL, PRIMARY KEY (thread_id, through_sequence), FOREIGN KEY (thread_id) REFERENCES "effect_agent_threads"(thread_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_submissions" ( submission_id TEXT PRIMARY KEY NOT NULL, thread_id TEXT NOT NULL, queue_sequence INTEGER NOT NULL, principal TEXT NOT NULL, idempotency_key TEXT NOT NULL, agent_id TEXT NOT NULL, definition_key INTEGER NOT NULL, deployment_id TEXT NOT NULL, input_json TEXT NOT NULL, input_digest TEXT NOT NULL, receipt_id TEXT NOT NULL, state TEXT NOT NULL, settled_outcome TEXT, settled_record_id TEXT, finalized_at TEXT, created_at TEXT NOT NULL, ready_at TEXT, input_applied_record_id TEXT, input_applied_sequence INTEGER, joined_host_submission_id TEXT, suspended_reason_json TEXT, suspended_at TEXT, unknown_reason TEXT, unknown_tool_call_ids_json TEXT, parent_submission_id TEXT, parent_tool_call_id TEXT, admission_group TEXT, admission_fence_json TEXT, worker_admission_json TEXT, message_admission_json TEXT, UNIQUE (thread_id, principal, idempotency_key), UNIQUE (thread_id, queue_sequence) )',
  "CREATE INDEX effect_agent_submissions_group ON \"effect_agent_submissions\" (thread_id, admission_group) WHERE admission_group IS NOT NULL AND state <> 'settled'",
  'CREATE TABLE "effect_agent_abort_intents" ( submission_id TEXT PRIMARY KEY NOT NULL, author TEXT NOT NULL, reason TEXT NOT NULL, requested_at TEXT NOT NULL, canonical_record_id TEXT, FOREIGN KEY (submission_id) REFERENCES "effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE INDEX effect_agent_submissions_joined_host ON "effect_agent_submissions" (joined_host_submission_id) WHERE joined_host_submission_id IS NOT NULL',
  'CREATE TABLE "effect_agent_approval_decisions" ( submission_id TEXT NOT NULL, tool_call_id TEXT NOT NULL, decision TEXT NOT NULL, resolver TEXT NOT NULL, reason TEXT NOT NULL, decided_at TEXT NOT NULL, PRIMARY KEY (submission_id, tool_call_id), FOREIGN KEY (submission_id) REFERENCES "effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_unknown_resolutions" ( submission_id TEXT NOT NULL, tool_call_id TEXT NOT NULL, resolution_kind TEXT NOT NULL, author TEXT NOT NULL, reason TEXT NOT NULL, resolution_json TEXT NOT NULL, resolved_at TEXT NOT NULL, PRIMARY KEY (submission_id, tool_call_id, resolution_kind), FOREIGN KEY (submission_id) REFERENCES "effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE INDEX effect_agent_submissions_parent ON "effect_agent_submissions" (parent_submission_id) WHERE parent_submission_id IS NOT NULL',
  'CREATE TABLE "effect_agent_child_reservations" ( reservation_id TEXT PRIMARY KEY NOT NULL, parent_submission_id TEXT NOT NULL, parent_tool_call_id TEXT NOT NULL, child_submission_id TEXT, status TEXT NOT NULL, allocation_json TEXT NOT NULL, allocation_digest TEXT NOT NULL, accounting_json TEXT, reserved_at TEXT NOT NULL, release_began_at TEXT, released_at TEXT, UNIQUE (parent_submission_id, parent_tool_call_id), FOREIGN KEY (parent_submission_id) REFERENCES "effect_agent_submissions"(submission_id) ON DELETE RESTRICT )',
  'CREATE TABLE "effect_agent_schedules" ( tenant_id TEXT NOT NULL, owner_id TEXT NOT NULL, schedule_id TEXT NOT NULL, deadline_at_millis INTEGER, record_json TEXT NOT NULL, PRIMARY KEY (tenant_id, owner_id, schedule_id) )',
  'CREATE INDEX effect_agent_schedules_deadline ON "effect_agent_schedules" (deadline_at_millis, tenant_id, owner_id, schedule_id) WHERE deadline_at_millis IS NOT NULL',
  'CREATE INDEX effect_agent_schedules_owner_deadline ON "effect_agent_schedules" (tenant_id, owner_id, deadline_at_millis, schedule_id) WHERE deadline_at_millis IS NOT NULL',
  'CREATE TABLE "effect_agent_subscription_sequences" ( tenant_id TEXT NOT NULL, source_address TEXT NOT NULL, sequence INTEGER NOT NULL, event_scan_cursor TEXT NOT NULL, delivery_scan_cursor TEXT NOT NULL, recovery_scan_cursor INTEGER NOT NULL, PRIMARY KEY (tenant_id, source_address) )',
  'CREATE TABLE "effect_agent_subscriptions" ( tenant_id TEXT NOT NULL, source_address TEXT NOT NULL, owner_id TEXT NOT NULL, subscription_id TEXT NOT NULL, ordinal INTEGER NOT NULL, source_name TEXT NOT NULL, source_version TEXT NOT NULL, matching_key TEXT NOT NULL, state TEXT NOT NULL, expires_at_millis INTEGER, recovery_at_millis INTEGER, recovery_present INTEGER NOT NULL DEFAULT 0, record_json TEXT NOT NULL, PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id), UNIQUE (tenant_id, source_address, ordinal) )',
  'CREATE INDEX effect_agent_subscriptions_owner ON "effect_agent_subscriptions" (tenant_id, source_address, owner_id, ordinal)',
  "CREATE INDEX effect_agent_subscriptions_capacity ON \"effect_agent_subscriptions\" (tenant_id, source_address, expires_at_millis) WHERE state IN ('active', 'paused')",
  "CREATE INDEX effect_agent_subscriptions_owner_capacity ON \"effect_agent_subscriptions\" (tenant_id, source_address, owner_id, expires_at_millis) WHERE state IN ('active', 'paused')",
  'CREATE INDEX effect_agent_subscriptions_candidates ON "effect_agent_subscriptions" (tenant_id, source_address, source_name, source_version, matching_key, ordinal)',
  'CREATE INDEX effect_agent_subscriptions_recovery ON "effect_agent_subscriptions" (tenant_id, source_address, recovery_at_millis, ordinal) WHERE recovery_at_millis IS NOT NULL',
  'CREATE TABLE "effect_agent_subscription_events" ( tenant_id TEXT NOT NULL, source_address TEXT NOT NULL, event_id TEXT NOT NULL, source_name TEXT NOT NULL, source_version TEXT NOT NULL, matching_key TEXT NOT NULL, payload_digest TEXT NOT NULL, cutoff INTEGER NOT NULL, cursor INTEGER NOT NULL, routing_complete INTEGER NOT NULL, tombstone INTEGER NOT NULL DEFAULT 0, next_attempt_at_millis INTEGER NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY (tenant_id, source_address, event_id) )',
  'CREATE INDEX effect_agent_subscription_events_pending ON "effect_agent_subscription_events" (tenant_id, source_address, routing_complete, next_attempt_at_millis, event_id)',
  'CREATE TABLE "effect_agent_subscription_deliveries" ( tenant_id TEXT NOT NULL, source_address TEXT NOT NULL, owner_id TEXT NOT NULL, subscription_id TEXT NOT NULL, event_id TEXT NOT NULL, delivery_key TEXT NOT NULL, state TEXT NOT NULL, next_attempt_at_millis INTEGER NOT NULL, record_json TEXT NOT NULL, observe_settlement INTEGER NOT NULL DEFAULT 0 CHECK (observe_settlement IN (0, 1)), PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id, event_id), UNIQUE (tenant_id, source_address, delivery_key) )',
  'CREATE INDEX effect_agent_subscription_deliveries_pending ON "effect_agent_subscription_deliveries" (tenant_id, source_address, state, next_attempt_at_millis, delivery_key)',
  'CREATE INDEX effect_agent_subscription_deliveries_registration ON "effect_agent_subscription_deliveries" (tenant_id, source_address, owner_id, subscription_id, delivery_key)',
  "CREATE INDEX effect_agent_subscription_deliveries_capacity ON \"effect_agent_subscription_deliveries\" (tenant_id, source_address, owner_id) WHERE state IN ('selected', 'prepared')",
  "CREATE INDEX effect_agent_subscription_deliveries_observing ON \"effect_agent_subscription_deliveries\" (tenant_id, source_address, owner_id) WHERE state='delivered' AND observe_settlement=1",
  'CREATE TABLE "effect_agent_message_deliveries" ( owner_thread_id TEXT NOT NULL, message_id TEXT NOT NULL, version INTEGER NOT NULL, state TEXT NOT NULL, deadline_at_millis INTEGER, record_json TEXT NOT NULL, peer_retained_until_millis INTEGER, PRIMARY KEY (owner_thread_id, message_id) )',
  'CREATE INDEX effect_agent_message_deliveries_due ON "effect_agent_message_deliveries" (deadline_at_millis, owner_thread_id, message_id) WHERE deadline_at_millis IS NOT NULL',
  "CREATE INDEX effect_agent_submissions_nonterminal ON \"effect_agent_submissions\" (thread_id, queue_sequence) WHERE state <> 'settled'",
  'CREATE INDEX effect_agent_records_call ON "effect_agent_canonical_records"(thread_id, record_tag, run_id, tool_call_id) WHERE tool_call_id IS NOT NULL',
  "CREATE INDEX effect_agent_records_run_input ON \"effect_agent_canonical_records\"(thread_id, run_id) WHERE record_tag = 30 AND input_kind = 'user'",
  'CREATE INDEX effect_agent_records_subtree ON "effect_agent_canonical_records"(thread_id, source_submission_id, sequence) WHERE record_tag = 22',
  'CREATE INDEX effect_agent_records_worker_input ON "effect_agent_canonical_records"(thread_id, message_id) WHERE record_tag = 34',
  "CREATE INDEX effect_agent_message_deliveries_pending ON \"effect_agent_message_deliveries\"(owner_thread_id, message_id) WHERE state NOT IN ('processed', 'refused')",
  "CREATE TABLE effect_agent_worker_stops (thread_id TEXT PRIMARY KEY NOT NULL, terminal TEXT)",
  "CREATE INDEX effect_agent_worker_starts ON effect_agent_message_deliveries(owner_thread_id, json_extract(record_json, '$.envelope.workerAdmission.origin.worker.delegationId'), json_extract(record_json, '$.envelope.workerAdmission.origin.worker.targetAgentId'), message_id) WHERE message_id = json_extract(record_json, '$.envelope.workerAdmission.origin.firstMessageId')",
  "CREATE INDEX effect_agent_worker_pending ON effect_agent_message_deliveries(owner_thread_id, json_extract(record_json, '$.envelope.workerAdmission.origin.worker.threadId'), message_id) WHERE state IN ('pending', 'parked') AND json_extract(record_json, '$.receipt') IS NULL",
  'CREATE INDEX effect_agent_records_tag ON "effect_agent_canonical_records"(thread_id, record_tag, sequence)',
  'CREATE INDEX effect_agent_records_handoff ON "effect_agent_canonical_records"(thread_id, sequence) WHERE handoff = 1',
  "CREATE INDEX effect_agent_records_application_input ON effect_agent_canonical_records(thread_id, sequence) WHERE application_input = 1",
  `CREATE INDEX effect_agent_records_prompt ON effect_agent_canonical_records(thread_id, sequence) WHERE ${SQL_PROMPT_PREDICATE}`,
  "CREATE INDEX effect_agent_records_admitted_input ON effect_agent_canonical_records(thread_id, sequence) WHERE record_tag = 30 AND submission_id IS NOT NULL",
  "CREATE INDEX effect_agent_records_worker_funding ON effect_agent_canonical_records(thread_id, source_submission_id, sequence, message_id) WHERE record_tag = 34",
  "CREATE INDEX effect_agent_records_worker_completed ON effect_agent_canonical_records(thread_id, message_id, sequence) WHERE record_tag = 32",
  "CREATE INDEX effect_agent_records_worker_stop ON effect_agent_canonical_records(thread_id, worker_thread_id, sequence) WHERE record_tag = 38",
  "CREATE INDEX effect_agent_message_deliveries_identity ON effect_agent_message_deliveries(owner_thread_id, json_quote(message_id))",
  "CREATE INDEX effect_agent_message_deliveries_peer_retained ON effect_agent_message_deliveries(owner_thread_id, peer_retained_until_millis, message_id) WHERE peer_retained_until_millis IS NOT NULL",
  "CREATE INDEX effect_agent_message_deliveries_peer ON effect_agent_message_deliveries(owner_thread_id, message_id) WHERE state NOT IN ('processed', 'refused') AND json_extract(record_json, '$.envelope.messageAdmission.peerName') IS NOT NULL",
  "CREATE TABLE effect_agent_journal_ranges (thread_id TEXT NOT NULL, first_sequence INTEGER NOT NULL, last_sequence INTEGER NOT NULL, record_count INTEGER NOT NULL, batch_count INTEGER NOT NULL, byte_count INTEGER NOT NULL, state TEXT NOT NULL, locator TEXT, PRIMARY KEY (thread_id, first_sequence), FOREIGN KEY (thread_id) REFERENCES effect_agent_threads(thread_id) ON DELETE RESTRICT)",
  "CREATE TABLE effect_agent_archive_batches (thread_id TEXT NOT NULL, batch_id TEXT NOT NULL, range_first_sequence INTEGER NOT NULL, batch_json TEXT NOT NULL, PRIMARY KEY (thread_id, batch_id), FOREIGN KEY (thread_id, batch_id) REFERENCES effect_agent_canonical_batches(thread_id, batch_id) ON DELETE RESTRICT, CONSTRAINT effect_agent_archive_batches_range_fkey FOREIGN KEY (thread_id, range_first_sequence) REFERENCES effect_agent_journal_ranges(thread_id, first_sequence) ON DELETE RESTRICT)",
  "CREATE TABLE effect_agent_archive_records (thread_id TEXT NOT NULL, sequence INTEGER NOT NULL, range_first_sequence INTEGER NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY (thread_id, sequence), FOREIGN KEY (thread_id, sequence) REFERENCES effect_agent_canonical_records(thread_id, sequence) ON DELETE RESTRICT, CONSTRAINT effect_agent_archive_records_range_fkey FOREIGN KEY (thread_id, range_first_sequence) REFERENCES effect_agent_journal_ranges(thread_id, first_sequence) ON DELETE RESTRICT)",
  "CREATE UNIQUE INDEX effect_agent_canonical_batches_sequence ON effect_agent_canonical_batches(thread_id, first_sequence)",
  "CREATE INDEX effect_agent_canonical_batches_last ON effect_agent_canonical_batches(thread_id, last_sequence)",
  "CREATE TABLE effect_agent_live_child_reservations (reservation_id TEXT PRIMARY KEY NOT NULL, thread_id TEXT NOT NULL)",
  "CREATE INDEX effect_agent_live_child_reservations_thread ON effect_agent_live_child_reservations(thread_id, reservation_id)",
  // Compact native positions keep accepted identities out of bounded export cursors.
  // Persistent scalar FIFO derivatives; rebuild from canonical input/settlement boundaries.
  // Writer-local import indexes; rows are removed before publication.
  "CREATE TABLE effect_agent_import_delivery_visits (thread_id TEXT NOT NULL, message_id TEXT NOT NULL, path_id TEXT NOT NULL, completed INTEGER NOT NULL, PRIMARY KEY (thread_id, message_id))",
  "CREATE INDEX effect_agent_import_delivery_visits_path ON effect_agent_import_delivery_visits(thread_id, path_id)",
  "CREATE TRIGGER effect_agent_live_child_insert AFTER INSERT ON effect_agent_child_reservations FOR EACH ROW BEGIN INSERT INTO effect_agent_live_child_reservations (reservation_id, thread_id) SELECT NEW.reservation_id, thread_id FROM effect_agent_submissions WHERE submission_id=NEW.parent_submission_id AND NEW.status<>'released'; END",
  "CREATE TRIGGER effect_agent_live_child_update AFTER UPDATE ON effect_agent_child_reservations FOR EACH ROW WHEN OLD.reservation_id IS NOT NEW.reservation_id OR OLD.parent_submission_id IS NOT NEW.parent_submission_id OR OLD.parent_tool_call_id IS NOT NEW.parent_tool_call_id OR OLD.child_submission_id IS NOT NEW.child_submission_id OR OLD.status IS NOT NEW.status OR OLD.allocation_json IS NOT NEW.allocation_json OR OLD.allocation_digest IS NOT NEW.allocation_digest OR OLD.accounting_json IS NOT NEW.accounting_json OR OLD.reserved_at IS NOT NEW.reserved_at OR OLD.release_began_at IS NOT NEW.release_began_at OR OLD.released_at IS NOT NEW.released_at BEGIN DELETE FROM effect_agent_live_child_reservations WHERE reservation_id=OLD.reservation_id; INSERT INTO effect_agent_live_child_reservations (reservation_id, thread_id) SELECT NEW.reservation_id, thread_id FROM effect_agent_submissions WHERE submission_id=NEW.parent_submission_id AND NEW.status<>'released'; END",
  "CREATE TRIGGER effect_agent_live_child_delete AFTER DELETE ON effect_agent_child_reservations FOR EACH ROW BEGIN DELETE FROM effect_agent_live_child_reservations WHERE reservation_id=OLD.reservation_id; END",
  "CREATE INDEX effect_agent_message_deliveries_capacity ON effect_agent_message_deliveries(owner_thread_id, (COALESCE(json_extract(record_json, '$.envelope.messageAdmission._tag'), '') = 'WorkerUpdate'), message_id) WHERE state IN ('pending', 'accepted', 'parked')",
] as const;

const layout28Statements = layout27Statements.filter(
  (statement) => !statement.startsWith("CREATE INDEX effect_agent_canonical_batches_last "),
);

const layoutStatements = layout28Statements.filter(
  (statement) => !statement.startsWith('CREATE TABLE "effect_agent_checkpoints" '),
);

const headerStatement =
  "CREATE TABLE effect_agent_schema (singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), layout_version INTEGER NOT NULL CHECK (layout_version > 0), record_format TEXT NOT NULL CHECK (length(record_format) > 0))";

const layoutObjects = (statements: ReadonlyArray<string>) =>
  statements.map((statement, index) => {
    const name = /^CREATE (TABLE|(?:UNIQUE )?INDEX|TRIGGER) "?([a-z_][a-z_0-9]*)"?/.exec(statement);

    // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- The synchronous SQL layout parser rejects malformed developer-authored declarations before storage mutation.
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

const storageError = (cause: SqlError.SqlError) =>
  SqliteStorageError.make({ operation: "inspect storage layout", cause, message: cause.message });

const inspection = (version: number, statements: ReadonlyArray<string>) =>
  makeSqliteLayoutInspection({
    version,
    statements,
    objects: layoutObjects(statements),
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

// Released predecessors retain the same record format and canonical tables.
const addedIndexes = new Set([
  "effect_agent_message_deliveries_peer_retained",
  "effect_agent_subscriptions_capacity",
  "effect_agent_subscriptions_owner_capacity",
  "effect_agent_subscription_deliveries_capacity",
  "effect_agent_subscription_deliveries_observing",
]);

const layout25Statements = layoutObjects(layout27Statements)
  .filter(([, name]) => !addedIndexes.has(name))
  .map(([, , index]) =>
    layout27Statements[index]
      .replace(", peer_retained_until_millis INTEGER", "")
      .replace(
        ", observe_settlement INTEGER NOT NULL DEFAULT 0 CHECK (observe_settlement IN (0, 1))",
        "",
      )
      .replace(
        "'$.envelope.messageAdmission.peerName') IS NOT NULL",
        "'$.envelope.messageAdmission.schemaVersion') = 1",
      ),
  );

const { decode, readObjects, readHeader, readManagedTriggers } = inspection(
  CurrentSqliteStorageVersion,
  layoutStatements,
);

const layout25 = inspection(25, layout25Statements);
const layout26 = inspection(26, layout27Statements);
const layout27 = inspection(27, layout27Statements);
const layout28 = inspection(28, layout28Statements);
const layout29 = inspection(29, layoutStatements);
const layout30 = inspection(30, layoutStatements);

const inspectStorage = Effect.fnUntraced(function* (
  allowUpgrade = false,
): Effect.fn.Return<
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

  if (version >= 28 && objects.some(({ name }) => name === "effect_agent_canonical_batches_last"))
    return yield* incompatible(version, "Layout contains the retired batch-end index.");
  if (version >= 29 && objects.some(({ name }) => name === "effect_agent_checkpoints"))
    return yield* incompatible(version, "Current layout contains retired generic checkpoints.");

  if (
    allowUpgrade &&
    (version === 25 ||
      version === 26 ||
      version === 27 ||
      version === 28 ||
      version === 29 ||
      version === 30)
  ) {
    if (version === 25 && objects.some(({ name }) => addedIndexes.has(name)))
      return yield* incompatible(version, "Mixed layout 25 and 26 objects.");

    const predecessor =
      version === 25
        ? layout25
        : version === 26
          ? layout26
          : version === 27
            ? layout27
            : version === 28
              ? layout28
              : version === 29
                ? layout29
                : layout30;

    const header = yield* predecessor.readHeader(objects, version);

    yield* predecessor.readManagedTriggers(true);

    return header;
  }

  return yield* readHeader(objects, version);
});

/** The caller holds the dedicated client's exclusive lock across validation and ownership retirement. */
export const inspectManagedSqliteStorage = Effect.fnUntraced(function* () {
  const header = yield* inspectStorage(true);

  yield* readManagedTriggers(header !== undefined);
});

/** Read-only inspection. The caller owns a snapshot covering this check and its export reads. */
export const readSqliteStorageHeader = Effect.fnUntraced(function* () {
  const header = yield* inspectStorage();

  if (header === undefined)
    return yield* incompatible(0, "No initialized Thread storage to export.");

  return header;
});

/** Initialize fresh storage or atomically upgrade the exact released layouts 25–30. */
export const ensureSqliteStorageLayout = Effect.fn("SqliteStorage.initializeLayout")(function* () {
  const sql = (yield* SqlClient.SqlClient).withoutTransforms();

  // Current storage opens through a read snapshot even while another connection is writing.
  // Fresh and predecessor stores acquire the writer lock and repeat validation before DDL.
  const current = yield* makeSqlTransaction(sql, { begin: "BEGIN" })(inspectStorage(true)).pipe(
    Effect.catchTag("SqlError", storageError),
  );

  if (current?.layoutVersion === CurrentSqliteStorageVersion) return current;

  return yield* makeSqlTransaction(sql, { begin: "BEGIN IMMEDIATE" })(
    Effect.gen(function* () {
      const header = yield* inspectStorage(true);

      if (header !== undefined && header.layoutVersion < 30)
        yield* assertLegacyMessagePublicationDrained().pipe(
          Effect.mapError((cause) =>
            cause.reason === "conflict"
              ? incompatible(
                  header.layoutVersion,
                  "Recover work discovery and drain peer, report and update publication using the previous library version before upgrading.",
                )
              : SqliteStorageError.make({
                  operation: cause.operation,
                  message: cause.message,
                  cause,
                }),
          ),
        );

      if (header?.layoutVersion === 25) {
        yield* sql`ALTER TABLE effect_agent_message_deliveries ADD COLUMN peer_retained_until_millis INTEGER`;
        yield* backfillPeerMessageRetention().pipe(
          Effect.catchTag("MessageDeliveryError", () =>
            SqliteStorageCorruptionError.make({
              table: "effect_agent_message_deliveries",
              rowKey: "upgrade",
              message: "Invalid layout-25 delivery evidence; upgrade rolled back.",
            }),
          ),
        );
        yield* sql`ALTER TABLE effect_agent_subscription_deliveries ADD COLUMN observe_settlement INTEGER NOT NULL DEFAULT 0 CHECK (observe_settlement IN (0, 1))`;
        yield* backfillSubscriptionSettlementObservation().pipe(
          Effect.catchTag("SubscriptionError", () =>
            SqliteStorageCorruptionError.make({
              table: "effect_agent_subscription_deliveries",
              rowKey: "upgrade",
              message: "Invalid layout-25 subscription delivery evidence; upgrade rolled back.",
            }),
          ),
        );
        yield* sql`DROP INDEX effect_agent_message_deliveries_peer`;
        for (const [, name, index] of layoutObjects(layoutStatements))
          if (addedIndexes.has(name) || name === "effect_agent_message_deliveries_peer")
            yield* sql.unsafe(layoutStatements[index]).withoutTransform;
      }
      if (header?.layoutVersion === 25 || header?.layoutVersion === 26) {
        yield* upgradeExecutionCheckpoints(undefined).pipe(
          Effect.catchTag("ThreadStoreError", (cause) =>
            SqliteStorageCorruptionError.make({
              table: "effect_agent_executions",
              rowKey: "upgrade",
              message: `Invalid retained execution; upgrade rolled back: ${cause.message}`,
            }),
          ),
        );
      }
      if (header !== undefined && header.layoutVersion < CurrentSqliteStorageVersion) {
        yield* upgradeSqlThreadWorkIndex().pipe(
          Effect.mapError((cause) =>
            SqliteStorageError.make({ operation: cause.operation, message: cause.message, cause }),
          ),
        );
        if (header.layoutVersion < 30) {
          if (header.layoutVersion <= 27)
            yield* sql`DROP INDEX effect_agent_canonical_batches_last`;
          // Retire only the derived generic snapshot cache. This step preserves the exact
          // canonical and operational bytes; earlier layout conversions remain independent.
          if (header.layoutVersion < 29) yield* sql`DROP TABLE effect_agent_checkpoints`;
          yield* sql`DROP INDEX IF EXISTS effect_agent_records_delivery_predecessor`;
          yield* sql`DROP INDEX IF EXISTS effect_agent_work_entries_message`;
          yield* sql`DROP INDEX IF EXISTS effect_agent_work_entries_peer`;
          yield* createSqlThreadWorkTables().pipe(
            Effect.mapError((cause) =>
              SqliteStorageError.make({
                operation: cause.operation,
                message: cause.message,
                cause,
              }),
            ),
          );
        }
        yield* sql`UPDATE effect_agent_schema SET layout_version=${CurrentSqliteStorageVersion} WHERE singleton=1`;
        yield* sql.unsafe(`PRAGMA user_version = ${CurrentSqliteStorageVersion}`).withoutTransform;

        return yield* readSqliteStorageHeader();
      }
      if (header !== undefined) return header;
      for (const statement of layoutStatements) yield* sql.unsafe(statement).withoutTransform;
      yield* createSqlThreadWorkTables().pipe(
        Effect.mapError((cause) =>
          SqliteStorageError.make({ operation: cause.operation, message: cause.message, cause }),
        ),
      );
      yield* sql.unsafe(headerStatement).withoutTransform;
      yield* sql`INSERT INTO effect_agent_schema (singleton, layout_version, record_format) VALUES (1, ${CurrentSqliteStorageVersion}, ${CURRENT_RECORD_FORMAT})`;
      yield* sql.unsafe(`PRAGMA user_version = ${CurrentSqliteStorageVersion}`).withoutTransform;

      return yield* readSqliteStorageHeader();
    }),
  ).pipe(Effect.catchTag("SqlError", storageError));
});
