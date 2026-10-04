import { makeSqliteLayoutInspection } from "@yielded/agent-storage-sql/sqlite-layout-inspection";
import { CURRENT_RECORD_FORMAT } from "@yielded/agent/records";
import { Effect, Schema } from "effect";
import type * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

import {
  DoStorageCompatibilityError,
  DoStorageCorruptionError,
  DoStorageError,
} from "../DoStorageError.ts";

export const CurrentDoStorageVersion = 17;
// Frozen legacy record format: never replace this with a future CURRENT_RECORD_FORMAT.
const LEGACY_RECORD_FORMAT = "effect-agent/thread@1";

// Captured from the shipped layout16 initializer. Historical statements never call current DDL helpers.
const baseline16 = [
  "CREATE TABLE effect_agent_threads ( thread_id TEXT PRIMARY KEY NOT NULL, created_at TEXT NOT NULL, tail_sequence INTEGER NOT NULL, tail_digest TEXT NOT NULL, producer_epoch INTEGER NOT NULL )",
  "CREATE TABLE effect_agent_canonical_batches ( thread_id TEXT NOT NULL, batch_id TEXT NOT NULL, first_sequence INTEGER NOT NULL, last_sequence INTEGER NOT NULL, batch_digest TEXT NOT NULL, tail_digest TEXT NOT NULL, batch_json TEXT NOT NULL, PRIMARY KEY (thread_id, batch_id), FOREIGN KEY (thread_id) REFERENCES effect_agent_threads(thread_id) ON DELETE RESTRICT )",
  "CREATE TABLE effect_agent_canonical_records ( thread_id TEXT NOT NULL, sequence INTEGER NOT NULL, record_id TEXT NOT NULL, batch_id TEXT NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY (thread_id, sequence), UNIQUE (thread_id, record_id), FOREIGN KEY (thread_id, batch_id) REFERENCES effect_agent_canonical_batches(thread_id, batch_id) ON DELETE RESTRICT )",
  "CREATE INDEX effect_agent_canonical_records_batch ON effect_agent_canonical_records (thread_id, batch_id, sequence)",
  "CREATE TABLE effect_agent_checkpoints ( thread_id TEXT NOT NULL, through_sequence INTEGER NOT NULL, tail_digest TEXT NOT NULL, checkpoint_json TEXT NOT NULL, PRIMARY KEY (thread_id, through_sequence), FOREIGN KEY (thread_id) REFERENCES effect_agent_threads(thread_id) ON DELETE RESTRICT )",
  "CREATE TABLE effect_agent_submissions ( submission_id TEXT PRIMARY KEY NOT NULL, thread_id TEXT NOT NULL, queue_sequence INTEGER NOT NULL, principal TEXT NOT NULL, idempotency_key TEXT NOT NULL, agent_id TEXT NOT NULL, agent_digests_json TEXT NOT NULL, deployment_id TEXT NOT NULL, input_json TEXT NOT NULL, input_digest TEXT NOT NULL, receipt_id TEXT NOT NULL, state TEXT NOT NULL, settled_outcome TEXT, settled_record_id TEXT, finalized_at TEXT, created_at TEXT NOT NULL, ready_at TEXT, input_applied_record_id TEXT, input_applied_sequence INTEGER, joined_host_submission_id TEXT, suspended_reason_json TEXT, suspended_at TEXT, unknown_reason TEXT, unknown_tool_call_ids_json TEXT, parent_submission_id TEXT, parent_tool_call_id TEXT, admission_group TEXT, admission_fence_json TEXT, worker_admission_json TEXT, message_admission_json TEXT, UNIQUE (thread_id, principal, idempotency_key), UNIQUE (thread_id, queue_sequence) )",
  "CREATE INDEX effect_agent_submissions_joined_host ON effect_agent_submissions (joined_host_submission_id)",
  "CREATE INDEX effect_agent_submissions_parent ON effect_agent_submissions (parent_submission_id)",
  "CREATE INDEX effect_agent_submissions_group ON effect_agent_submissions (thread_id, admission_group, state)",
  "CREATE TABLE effect_agent_submission_ownership ( submission_id TEXT PRIMARY KEY NOT NULL, attempt_id TEXT NOT NULL, ownership_token TEXT NOT NULL, producer_epoch INTEGER NOT NULL, owner_producer_id TEXT NOT NULL, lease_expires_at TEXT NOT NULL, FOREIGN KEY (submission_id) REFERENCES effect_agent_submissions(submission_id) ON DELETE RESTRICT )",
  "CREATE TABLE effect_agent_attempts ( attempt_id TEXT PRIMARY KEY NOT NULL, submission_id TEXT NOT NULL, thread_id TEXT NOT NULL, owner_producer_id TEXT NOT NULL, producer_epoch INTEGER NOT NULL, claimed_at TEXT NOT NULL, FOREIGN KEY (submission_id) REFERENCES effect_agent_submissions(submission_id) ON DELETE RESTRICT )",
  "CREATE TABLE effect_agent_abort_intents ( submission_id TEXT PRIMARY KEY NOT NULL, author TEXT NOT NULL, reason TEXT NOT NULL, requested_at TEXT NOT NULL, canonical_record_id TEXT, FOREIGN KEY (submission_id) REFERENCES effect_agent_submissions(submission_id) ON DELETE RESTRICT )",
  "CREATE TABLE effect_agent_approval_decisions ( submission_id TEXT NOT NULL, tool_call_id TEXT NOT NULL, decision TEXT NOT NULL, resolver TEXT NOT NULL, reason TEXT NOT NULL, decided_at TEXT NOT NULL, PRIMARY KEY (submission_id, tool_call_id), FOREIGN KEY (submission_id) REFERENCES effect_agent_submissions(submission_id) ON DELETE RESTRICT )",
  "CREATE TABLE effect_agent_unknown_resolutions ( submission_id TEXT NOT NULL, tool_call_id TEXT NOT NULL, author TEXT NOT NULL, reason TEXT NOT NULL, resolution_json TEXT NOT NULL, resolved_at TEXT NOT NULL, PRIMARY KEY (submission_id, tool_call_id), FOREIGN KEY (submission_id) REFERENCES effect_agent_submissions(submission_id) ON DELETE RESTRICT )",
  "CREATE TABLE effect_agent_child_reservations ( reservation_id TEXT PRIMARY KEY NOT NULL, parent_submission_id TEXT NOT NULL, parent_tool_call_id TEXT NOT NULL, child_submission_id TEXT, status TEXT NOT NULL, allocation_json TEXT NOT NULL, allocation_digest TEXT NOT NULL, accounting_json TEXT, reserved_at TEXT NOT NULL, release_began_at TEXT, released_at TEXT, UNIQUE (parent_submission_id, parent_tool_call_id), FOREIGN KEY (parent_submission_id) REFERENCES effect_agent_submissions(submission_id) ON DELETE RESTRICT )",
  "CREATE TABLE effect_agent_child_settlements ( parent_submission_id TEXT NOT NULL, child_submission_id TEXT NOT NULL, child_outcome TEXT, recorded_at TEXT NOT NULL, PRIMARY KEY (parent_submission_id, child_submission_id) )",
  "CREATE INDEX effect_agent_records_call ON \"effect_agent_canonical_records\"(thread_id, json_extract(record_json, '$.payload._tag'), json_extract(record_json, '$.payload.runId'), json_extract(record_json, '$.payload.toolCallId'))",
  "CREATE INDEX effect_agent_records_run_input ON \"effect_agent_canonical_records\"(thread_id, json_extract(record_json, '$.payload.runId')) WHERE json_extract(record_json, '$.payload._tag') = 'UserInputRecorded' AND json_extract(record_json, '$.payload.kind') = 'user'",
  "CREATE INDEX effect_agent_records_subtree ON \"effect_agent_canonical_records\"(thread_id, json_extract(record_json, '$.payload.sourceSubmissionId'), sequence) WHERE json_extract(record_json, '$.payload._tag') = 'SubtreeBudgetReserved'",
  "CREATE INDEX effect_agent_records_worker_input ON \"effect_agent_canonical_records\"(thread_id, json_extract(record_json, '$.payload.admission.messageId')) WHERE json_extract(record_json, '$.payload._tag') = 'WorkerInputRequested'",
  "CREATE INDEX effect_agent_submissions_nonterminal ON effect_agent_submissions (thread_id, queue_sequence) WHERE state <> 'settled'",
  "CREATE TABLE effect_agent_message_deliveries ( owner_thread_id TEXT NOT NULL, message_id TEXT NOT NULL, version INTEGER NOT NULL, state TEXT NOT NULL, deadline_at_millis INTEGER, record_json TEXT NOT NULL, PRIMARY KEY (owner_thread_id, message_id) )",
  "CREATE INDEX effect_agent_message_deliveries_due ON effect_agent_message_deliveries (deadline_at_millis, owner_thread_id, message_id) WHERE deadline_at_millis IS NOT NULL",
  "CREATE INDEX effect_agent_message_deliveries_pending ON \"effect_agent_message_deliveries\"(owner_thread_id, message_id) WHERE state NOT IN ('processed', 'refused')",
  "CREATE TABLE effect_agent_recovery_checkpoints ( thread_id TEXT PRIMARY KEY NOT NULL, through_sequence INTEGER NOT NULL, tail_digest TEXT NOT NULL, checkpoint_json TEXT NOT NULL, FOREIGN KEY (thread_id) REFERENCES effect_agent_threads(thread_id) ON DELETE RESTRICT )",
  "CREATE TABLE effect_agent_worker_stops (thread_id TEXT PRIMARY KEY NOT NULL, terminal TEXT)",
  "CREATE INDEX effect_agent_worker_starts ON effect_agent_message_deliveries(owner_thread_id, json_extract(record_json, '$.envelope.workerAdmission.origin.worker.delegationId'), json_extract(record_json, '$.envelope.workerAdmission.origin.worker.targetAgentId'), message_id) WHERE message_id = json_extract(record_json, '$.envelope.workerAdmission.origin.firstMessageId')",
  "CREATE INDEX effect_agent_worker_pending ON effect_agent_message_deliveries(owner_thread_id, json_extract(record_json, '$.envelope.workerAdmission.origin.worker.threadId'), message_id) WHERE state IN ('pending', 'parked') AND json_extract(record_json, '$.receipt') IS NULL",
  "CREATE INDEX effect_agent_worker_execution ON effect_agent_canonical_records(thread_id, json_extract(record_json, '$.payload._tag'), sequence) WHERE json_extract(record_json, '$.payload.runId') IS NOT NULL",
  "CREATE TABLE effect_agent_meta ( key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL )",
] as const;

const baselineObjects = [
  ["table", "effect_agent_abort_intents", 11],
  ["table", "effect_agent_approval_decisions", 12],
  ["table", "effect_agent_attempts", 10],
  ["table", "effect_agent_canonical_batches", 1],
  ["table", "effect_agent_canonical_records", 2],
  ["index", "effect_agent_canonical_records_batch", 3],
  ["table", "effect_agent_checkpoints", 4],
  ["table", "effect_agent_child_reservations", 14],
  ["table", "effect_agent_child_settlements", 15],
  ["table", "effect_agent_message_deliveries", 21],
  ["index", "effect_agent_message_deliveries_due", 22],
  ["index", "effect_agent_message_deliveries_pending", 23],
  ["table", "effect_agent_meta", 29],
  ["index", "effect_agent_records_call", 16],
  ["index", "effect_agent_records_run_input", 17],
  ["index", "effect_agent_records_subtree", 18],
  ["index", "effect_agent_records_worker_input", 19],
  ["table", "effect_agent_recovery_checkpoints", 24],
  ["table", "effect_agent_submission_ownership", 9],
  ["table", "effect_agent_submissions", 5],
  ["index", "effect_agent_submissions_group", 8],
  ["index", "effect_agent_submissions_joined_host", 6],
  ["index", "effect_agent_submissions_nonterminal", 20],
  ["index", "effect_agent_submissions_parent", 7],
  ["table", "effect_agent_threads", 0],
  ["table", "effect_agent_unknown_resolutions", 13],
  ["index", "effect_agent_worker_execution", 28],
  ["index", "effect_agent_worker_pending", 27],
  ["index", "effect_agent_worker_starts", 26],
  ["table", "effect_agent_worker_stops", 25],
] as const;

const headerStatement =
  "CREATE TABLE effect_agent_schema (singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), layout_version INTEGER NOT NULL CHECK (layout_version > 0), record_format TEXT NOT NULL CHECK (length(record_format) > 0))";

/** Ordered, immutable adapter layout steps. Record payloads are never rewritten. */
export const doLayoutSteps = [
  {
    version: 16,
    statements: [
      ...baseline16,
      "INSERT INTO effect_agent_meta (key, value) VALUES ('storage_version', '16')",
    ],
  },
  {
    version: 17,
    statements: [
      headerStatement,
      "INSERT INTO effect_agent_schema (singleton, layout_version, record_format) VALUES (1, 17, 'effect-agent/thread@1')",
      "UPDATE effect_agent_meta SET value = '17' WHERE key = 'storage_version'",
    ],
  },
] as const;

const Legacy = Schema.Tuple([
  Schema.Struct({
    key: Schema.Literal("storage_version"),
    value: Schema.String.check(Schema.isPattern(/^(0|[1-9][0-9]*)$/)).pipe(
      Schema.decodeTo(Schema.FiniteFromString),
      Schema.decodeTo(Schema.Int),
    ),
  }),
]);

export interface DoStorageHeader {
  readonly layoutVersion: number;
  readonly recordFormat: string;
}

const incompatible = (actualVersion: number, message: string) =>
  DoStorageCompatibilityError.make({
    actualVersion,
    supportedVersion: CurrentDoStorageVersion,
    message: `${message} Keep the original store; no layout upgrade was committed.`,
  });

const storageError = (cause: SqlError) =>
  DoStorageError.make({ operation: "inspect storage layout", cause, message: cause.message });

const { decode, readObjects, readHeader } = makeSqliteLayoutInspection({
  baseline: {
    version: 16,
    recordFormat: LEGACY_RECORD_FORMAT,
    statements: baseline16,
    objects: baselineObjects,
  },
  steps: doLayoutSteps,
  headerStatement,
  incompatible,
  storageError,
  corruption: (table) =>
    DoStorageCorruptionError.make({
      table,
      rowKey: "schema",
      message: "Malformed storage layout or version header.",
    }),
});

const inspectStorage = Effect.fnUntraced(function* (
  client: SqlClient.SqlClient,
): Effect.fn.Return<
  DoStorageHeader | undefined,
  DoStorageCompatibilityError | DoStorageCorruptionError | DoStorageError
> {
  const sql = client.withoutTransforms();

  const objects = yield* readObjects(sql);

  const meta = objects.find((row) => row.name === "effect_agent_meta");

  if (meta === undefined) {
    if (objects.length === 0) return undefined;

    return yield* incompatible(0, "Unversioned or incomplete Effect Agent storage.");
  }

  const [legacy] = yield* decode(
    Legacy,
    yield* sql<Record<string, unknown>>`
    SELECT * FROM effect_agent_meta
  `.pipe(Effect.mapError(storageError)),
    "effect_agent_meta",
  );

  const version = legacy.value;

  return yield* readHeader(sql, objects, version);
});

/** Read-only inspection. The caller owns a snapshot covering this check and its export reads. */
export const readDoStorageHeader = Effect.fnUntraced(function* (sql: SqlClient.SqlClient) {
  const header = yield* inspectStorage(sql);

  if (header === undefined)
    return yield* incompatible(0, "No initialized Thread storage to export.");

  return header;
});

/** Validate under the writer transaction before any DDL; commit every pending step together. */
export const ensureDoStorageLayout = Effect.fn("DoStorage.upgradeLayout")(function* (
  client: SqlClient.SqlClient,
) {
  const sql = client.withoutTransforms();

  return yield* sql
    .withTransaction(
      Effect.gen(function* () {
        const header = yield* inspectStorage(sql);
        const version = header?.layoutVersion ?? 0;

        for (const step of doLayoutSteps) {
          if (step.version <= version) continue;
          for (const statement of step.statements) yield* sql.unsafe(statement).withoutTransform;
        }

        // Fresh storage uses today's record format; frozen steps retain the legacy format.
        if (header === undefined)
          yield* sql`UPDATE effect_agent_schema SET record_format = ${CURRENT_RECORD_FORMAT} WHERE singleton = 1`;

        return yield* readDoStorageHeader(sql);
      }),
    )
    .pipe(Effect.catchTag("SqlError", storageError));
});
