import type { RawAppendRequest } from "@yielded/agent-storage-sql/sql-journal";
import { canonicalRecordPointers } from "@yielded/agent-storage-sql/sql-thread-native-reads";
import type { RunId } from "@yielded/agent/identifiers";
import { ExportRecord } from "@yielded/agent/record-format";
import {
  type EvidenceReference,
  CanonicalSequence,
  Digest,
  ProducerEpoch,
  RecordId,
  type CanonicalRecord,
  type RunContinuation,
  MAX_RUN_CONTINUATION_BYTES,
  MAX_RUN_RECOVERY_SUFFIX_RECORDS,
  MAX_RUN_RECOVERY_SUFFIX_BYTES,
} from "@yielded/agent/records";
import { canonicalRecordBytes, reference } from "@yielded/agent/run-continuation";
import {
  submissionAbortRecordId,
  submissionInputRecordId,
  submissionSettlementRecordId,
} from "@yielded/agent/submission-ledger";
import {
  MAX_ARCHIVE_RANGE_BYTES,
  MAX_ARCHIVE_RANGE_RECORDS,
  MAX_CANONICAL_BATCH_BYTES,
  ThreadArchiveRange,
} from "@yielded/agent/thread-archive-range";
import { ThreadStoreDiagnostic } from "@yielded/agent/thread-store";
import {
  CanonicalWorkEntry,
  MAX_WORK_ENTRY_BYTES,
  WORK_INDEX_VERSION,
  advanceWorkEntry,
  type WorkerReportingMode,
} from "@yielded/agent/thread-work";
import { Effect, Result, Schema } from "effect";
import { classifySqliteError, SqlError } from "effect/sql/SqlError";

import {
  DoAppendConflict,
  DoFenceRejected,
  DoStorageCorruptionError,
  DoStorageError,
} from "../DoStorageError.ts";
import type { RawAppendResult } from "./do-journal.ts";
import {
  AppendRefusal,
  decode,
  failure,
  reject,
  validateProgress,
  workChanges,
  workReferenceTags,
  type SyncAppendError,
} from "./sync-append-model.ts";

const Text = Schema.String.check(Schema.isMaxLength(2_000_000));
const Identifier = Schema.NonEmptyString.check(Schema.isMaxLength(1024));

export const SyncThreadRow = Schema.Struct({
  thread_id: Identifier,
  created_at: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
  producer_epoch: ProducerEpoch,
  tail_digest: Text,
  tail_sequence: CanonicalSequence,
});

export const SyncRecordRow = Schema.Struct({
  batch_id: Identifier,
  thread_id: Identifier,
  record_id: Identifier,
  record_json: Text,
  sequence: CanonicalSequence,
});

const BatchRow = Schema.Struct({
  thread_id: Identifier,
  batch_id: Identifier,
  first_sequence: CanonicalSequence,
  last_sequence: CanonicalSequence,
  batch_digest: Text,
  tail_digest: Text,
});

const RangeRow = Schema.Struct({
  thread_id: ThreadArchiveRange.fields.threadId,
  first_sequence: CanonicalSequence,
  last_sequence: CanonicalSequence,
  previous_tail_digest: Digest,
  tail_digest: Digest,
  record_count: Schema.Int,
  batch_count: Schema.Int,
  byte_count: Schema.Int,
  state: Schema.Literals(["open", "sealed", "archived"]),
  locator: Schema.NullOr(Schema.String),
});

const Header = Schema.Struct({
  version: Schema.Int,
  state: Schema.Literals(["ready", "rebuilding"]),
  through_sequence: CanonicalSequence,
  reporting: Schema.Literals([0, 1, 2]),
  entry_count: Schema.Natural,
  actual_count: Schema.Natural,
});

const EntryRows = Schema.Array(
  Schema.Struct({ id: Schema.String, entry_json: Schema.NullOr(Schema.String) }),
);

const WorkJson = Schema.fromJsonString(CanonicalWorkEntry);
const RecordJson = Schema.fromJsonString(ExportRecord);

type Binding = string | number | null;
type References = ReadonlyMap<RecordId, EvidenceReference>;
type Query = (
  query: string,
  ...bindings: ReadonlyArray<Binding>
) => Array<Record<string, SqlStorageValue>>;

/** Crypto remains a real effect, resolved before the synchronous SQL core. */
export const prepareSyncReferences = Effect.fnUntraced(function* (request: RawAppendRequest) {
  const entries = yield* Effect.forEach(
    request.records.filter(({ canonical }) => workReferenceTags.has(canonical.payload._tag)),
    ({ canonical }) => reference(canonical),
  );

  return new Map(entries.map((entry) => [entry.recordId, entry]));
});

const rowDecode = <A, I>(
  schema: Schema.Codec<A, I>,
  table: string,
  rowKey: string,
  value: unknown,
): A => {
  const result = Schema.decodeUnknownResult(schema)(value);

  if (Result.isFailure(result))
    return reject(
      DoStorageCorruptionError.make({
        table,
        rowKey,
        message: "Stored rows do not satisfy the storage schema",
        diagnostic: ThreadStoreDiagnostic.make({
          causeTag: "SchemaError",
          operation: "decode storage rows",
          decoder: table,
          issueTag: result.failure.issue._tag,
        }),
      }),
    );

  return result.success;
};

const groups = <A>(items: ReadonlyArray<A>, size: number): Array<ReadonlyArray<A>> => {
  const result: Array<ReadonlyArray<A>> = [];

  for (let offset = 0; offset < items.length; offset += size)
    result.push(items.slice(offset, offset + size));

  return result;
};

const storedRecord = (alias: string) => `COALESCE(${alias}.record_json, (
  SELECT a.record_json FROM effect_agent_archive_records a
  WHERE a.thread_id=${alias}.thread_id AND a.sequence=${alias}.sequence
    AND (SELECT r.state='archived' AND r.locator IS NOT NULL FROM effect_agent_journal_ranges r
      WHERE r.thread_id=a.thread_id AND r.first_sequence=a.range_first_sequence)))`;

const rangeDescriptor = (row: typeof RangeRow.Type) => {
  if (
    (row.state === "archived") !== (row.locator !== null) ||
    row.last_sequence - row.first_sequence + 1 !== row.record_count ||
    row.batch_count > row.record_count
  )
    return reject(failure("archive descriptor identity"));
  decode(ThreadArchiveRange, {
    format: "effect-agent/thread-range@1",
    threadId: row.thread_id,
    firstSequence: row.first_sequence,
    lastSequence: row.last_sequence,
    previousTailDigest: row.previous_tail_digest,
    tailDigest: row.tail_digest,
    recordCount: row.record_count,
    batchCount: row.batch_count,
    byteCount: row.byte_count,
    state: row.state === "archived" ? "archived" : "sealed",
    ...(row.locator === null ? {} : { locator: row.locator }),
  });
};

const appendRange = (
  exec: Query,
  request: RawAppendRequest,
  first: CanonicalSequence,
  last: CanonicalSequence,
) => {
  const byteCount =
    request.batchBytes + request.records.reduce((sum, record) => sum + record.recordBytes, 0);

  if (
    request.batchBytes > MAX_CANONICAL_BATCH_BYTES ||
    byteCount > MAX_ARCHIVE_RANGE_BYTES ||
    request.records.length > MAX_ARCHIVE_RANGE_RECORDS
  )
    return reject(failure("append archive range bound"));
  if (request.expectedTailSequence > 0) {
    const advanced = decode(
      Schema.Array(RangeRow),
      exec(
        `UPDATE effect_agent_journal_ranges
      SET last_sequence=?, tail_digest=?, record_count=record_count+?, batch_count=batch_count+1, byte_count=byte_count+?
      WHERE thread_id=? AND state='open' AND locator IS NULL AND last_sequence=? AND tail_digest=?
        AND first_sequence>0 AND record_count=last_sequence-first_sequence+1
        AND record_count BETWEEN 1 AND ? AND batch_count BETWEEN 1 AND record_count AND byte_count BETWEEN 1 AND ? RETURNING *`,
        last,
        request.tailDigest,
        request.records.length,
        byteCount,
        request.threadId,
        first - 1,
        request.expectedTailDigest,
        MAX_ARCHIVE_RANGE_RECORDS - request.records.length,
        MAX_ARCHIVE_RANGE_BYTES - byteCount,
      ),
    );

    if (advanced.length > 1) return reject(failure("ambiguous open archive range"));
    if (advanced[0] !== undefined) {
      rangeDescriptor(advanced[0]);

      return;
    }
  }

  const rows = decode(
    Schema.Array(RangeRow),
    exec(
      "SELECT * FROM effect_agent_journal_ranges WHERE thread_id=? AND state='open'",
      request.threadId,
    ),
  );

  if (rows.length > 1) return reject(failure("ambiguous open archive range"));
  const current = rows[0];

  if (current !== undefined) {
    rangeDescriptor(current);
    if (
      current.last_sequence !== first - 1 ||
      current.tail_digest !== request.expectedTailDigest ||
      (current.record_count + request.records.length <= MAX_ARCHIVE_RANGE_RECORDS &&
        current.byte_count + byteCount <= MAX_ARCHIVE_RANGE_BYTES)
    )
      return reject(failure("archive range append frontier"));
    exec(
      "UPDATE effect_agent_journal_ranges SET state='sealed' WHERE thread_id=? AND first_sequence=?",
      request.threadId,
      current.first_sequence,
    );
  }
  exec(
    `INSERT INTO effect_agent_journal_ranges (thread_id, first_sequence, last_sequence, previous_tail_digest, tail_digest, record_count, batch_count, byte_count, state, locator) VALUES (?, ?, ?, ?, ?, ?, 1, ?, 'open', NULL)`,
    request.threadId,
    first,
    last,
    request.expectedTailDigest,
    request.tailDigest,
    request.records.length,
    byteCount,
  );
};

const makeProgressReader = (exec: Query, request: RawAppendRequest) => ({
  previous(runId: RunId): RunContinuation | undefined {
    const rows = decode(
      Schema.Array(Schema.Struct({ record_id: RecordId, record_json: Schema.String })),
      exec(
        `SELECT record_id, ${storedRecord("effect_agent_canonical_records")} AS record_json FROM effect_agent_canonical_records INDEXED BY effect_agent_records_continuation WHERE thread_id=? AND run_id=? AND record_tag='RunContinuation' ORDER BY sequence DESC LIMIT 1`,
        request.threadId,
        JSON.stringify(runId),
      ),
    );

    const row = rows[0];

    if (row === undefined) return;
    const record = decode(RecordJson, row.record_json);

    if (
      record.recordId !== row.record_id ||
      record.payload._tag !== "RunContinuation" ||
      record.payload.runId !== runId ||
      canonicalRecordBytes(record) > MAX_RUN_CONTINUATION_BYTES
    )
      return reject(failure("invalid newest Run continuation"));

    return record.payload;
  },
  initial(next: RunContinuation): ReadonlyArray<CanonicalRecord> {
    const records: Array<CanonicalRecord> = [];
    let after = 0;
    let bytes = 0;

    const controls = [
      submissionInputRecordId(next.submissionId),
      submissionAbortRecordId(next.submissionId),
      submissionSettlementRecordId(next.submissionId),
    ];

    while (true) {
      const page = decode(
        Schema.Array(
          Schema.Struct({
            thread_id: Identifier,
            sequence: CanonicalSequence,
            record_id: RecordId,
            batch_id: Identifier,
            record_json: Schema.String,
          }),
        ),
        exec(
          `WITH evidence AS MATERIALIZED (
        SELECT * FROM (SELECT * FROM (SELECT sequence FROM effect_agent_record_runs WHERE thread_id=? AND run_id=? AND sequence>? AND sequence<=? ORDER BY sequence LIMIT 8) AS membership
          UNION SELECT sequence FROM effect_agent_canonical_records WHERE thread_id=? AND record_id=? AND sequence>? AND sequence<=?
          UNION SELECT sequence FROM effect_agent_canonical_records WHERE thread_id=? AND record_id=? AND sequence>? AND sequence<=?
          UNION SELECT sequence FROM effect_agent_canonical_records WHERE thread_id=? AND record_id=? AND sequence>? AND sequence<=? ORDER BY sequence LIMIT 8) ORDER BY sequence LIMIT 8)
        SELECT canonical.thread_id, canonical.sequence, canonical.record_id, canonical.batch_id, ${storedRecord("canonical")} AS record_json FROM evidence LEFT JOIN effect_agent_canonical_records canonical ON canonical.thread_id=? AND canonical.sequence=evidence.sequence ORDER BY evidence.sequence`,
          request.threadId,
          JSON.stringify(next.runId),
          after,
          request.expectedTailSequence,
          ...controls.flatMap((id) => [request.threadId, id, after, request.expectedTailSequence]),
          request.threadId,
        ),
      );

      for (const row of page) {
        const record = decode(RecordJson, row.record_json);

        if (
          record.recordId !== row.record_id ||
          row.thread_id !== request.threadId ||
          row.sequence <= after ||
          row.sequence > request.expectedTailSequence
        )
          return reject(failure("invalid initial preparation identity"));
        records.push(record);
        bytes += canonicalRecordBytes(record);
        if (
          records.length > MAX_RUN_RECOVERY_SUFFIX_RECORDS ||
          bytes > MAX_RUN_RECOVERY_SUFFIX_BYTES
        )
          return reject(failure("Initial Run preparation exceeds its recovery bound"));
        after = row.sequence;
      }
      if (page.length < 8) return records;
    }
  },
});

const applyInterval = (
  exec: Query,
  threadId: string,
  sequence: CanonicalSequence,
  record: CanonicalRecord,
) => {
  const payload = record.payload;

  if (
    payload._tag !== "SubmissionSettled" &&
    (payload._tag !== "UserInputRecorded" || payload.submissionId === undefined)
  )
    return;

  const boundary = (id: string, tag: string) => {
    const rows = decode(
      Schema.Array(Schema.Struct({ sequence: CanonicalSequence, record_tag: Schema.String })),
      exec(
        "SELECT sequence, record_tag FROM effect_agent_canonical_records WHERE thread_id=? AND record_id=?",
        threadId,
        id,
      ),
    );

    if (rows.length > 1 || (rows[0] !== undefined && rows[0].record_tag !== tag))
      return reject(failure("Interval boundary has conflicting canonical identity"));

    return rows[0]?.sequence;
  };

  const input =
    payload._tag === "SubmissionSettled"
      ? boundary(submissionInputRecordId(payload.submissionId), "UserInputRecorded")
      : sequence;

  if (input === undefined) return;

  const settlement =
    payload._tag === "SubmissionSettled"
      ? sequence
      : payload.submissionId === undefined
        ? null
        : (boundary(submissionSettlementRecordId(payload.submissionId), "SubmissionSettled") ??
          null);

  if (settlement !== null && input >= settlement)
    return reject(failure("Canonical settlement does not follow its input"));
  exec(
    "INSERT INTO effect_agent_input_intervals (thread_id, sequence, settlement_sequence) VALUES (?, ?, ?) ON CONFLICT (thread_id, sequence) DO UPDATE SET settlement_sequence=excluded.settlement_sequence",
    threadId,
    input,
    settlement,
  );
  exec(
    "DELETE FROM effect_agent_settlement_spans WHERE thread_id=? AND input_sequence=?",
    threadId,
    input,
  );
  if (settlement === null) return;
  const cover: Array<{ height: number; slot: number }> = [];

  for (let at = input + 1; at < settlement;) {
    let height = Math.floor(Math.log2(settlement - at));

    while (2 ** height > settlement - at || at % 2 ** height !== 0) height--;
    cover.push({ height, slot: at / 2 ** height });
    at += 2 ** height;
  }
  for (const group of groups(cover, 20))
    exec(
      `INSERT INTO effect_agent_settlement_spans (thread_id, height, slot, settlement_sequence, input_sequence) VALUES ${group.map(() => "(?,?,?,?,?)").join(",")}`,
      ...group.flatMap(({ height, slot }) => [threadId, height, slot, settlement, input]),
    );
};

const makeWork = (exec: Query) => {
  let tablesPresent = false;

  const invalidate = () => {
    tablesPresent = false;
  };

  const getEntry = (threadId: string, id: string): CanonicalWorkEntry | undefined => {
    const [row] = decode(
      EntryRows,
      exec(
        "SELECT id, CASE WHEN length(CAST(entry_json AS BLOB)) <= ? THEN entry_json ELSE NULL END AS entry_json FROM effect_agent_work_entries WHERE thread_id=? AND id=?",
        MAX_WORK_ENTRY_BYTES,
        threadId,
        id,
      ),
    );

    if (row === undefined) return;
    if (
      row.entry_json === null ||
      new TextEncoder().encode(row.entry_json).length > MAX_WORK_ENTRY_BYTES
    )
      return reject(failure("work entry byte bound"));
    const entry = decode(WorkJson, row.entry_json);

    if (entry.id !== row.id) return reject(failure("canonical work identity"));

    return entry;
  };

  const apply = (request: RawAppendRequest, first: CanonicalSequence, references: References) => {
    for (const [index, { canonical: record }] of request.records.entries()) {
      const sequence = CanonicalSequence.make(first + index);

      applyInterval(exec, request.threadId, sequence, record);
      const pointers = canonicalRecordPointers(record);

      for (const settlement of pointers.toolSettlements)
        exec(
          "INSERT INTO effect_agent_tool_declarations (thread_id, settlement_record_id, sequence) VALUES (?, ?, ?)",
          request.threadId,
          settlement,
          sequence,
        );
      if (pointers.refusalReservation !== undefined)
        exec(
          "INSERT INTO effect_agent_record_refusals (thread_id, reservation_record_id, sequence) VALUES (?, ?, ?)",
          request.threadId,
          pointers.refusalReservation,
          sequence,
        );
    }
    if (!tablesPresent)
      tablesPresent =
        decode(
          Schema.Tuple([Schema.Struct({ count: Schema.Natural })]),
          exec(
            "SELECT COUNT(*) AS count FROM sqlite_master WHERE type='table' AND name IN ('effect_agent_work_index', 'effect_agent_work_entries')",
          ),
        )[0].count === 2;
    if (!tablesPresent) return;

    const decoded = Schema.decodeUnknownResult(Schema.Array(Header))(
      exec(
        "SELECT version, state, through_sequence, reporting, entry_count, (SELECT COUNT(*) FROM effect_agent_work_entries WHERE thread_id=?) AS actual_count FROM effect_agent_work_index WHERE thread_id=?",
        request.threadId,
        request.threadId,
      ),
    );

    if (Result.isFailure(decoded) || decoded.success.length !== 1) return;
    const current = decoded.success[0];

    if (
      current === undefined ||
      current.version !== WORK_INDEX_VERSION ||
      current.entry_count !== current.actual_count ||
      current.state !== "ready" ||
      current.through_sequence !== first - 1
    )
      return;

    let mode: WorkerReportingMode =
      current.reporting === 1 ? "standard" : current.reporting === 2 ? "private" : "none";

    let through = current.through_sequence;

    for (const { canonical: record } of request.records) {
      through = CanonicalSequence.make(through + 1);
      if (!workReferenceTags.has(record.payload._tag)) continue;
      const ref = references.get(record.recordId);

      if (ref === undefined) return reject(failure("Missing prepared work reference"));
      for (const change of workChanges(record, through, mode, ref)) {
        if (change._tag === "WorkerMode") {
          mode = change.mode;
          continue;
        }
        if (change._tag === "Remove") {
          if (change.stateRecordId !== undefined) {
            const previous = getEntry(request.threadId, change.id);

            if (
              previous?.stateReference._tag !== "Canonical" ||
              previous.stateReference.recordId !== change.stateRecordId
            )
              continue;
          }
          exec(
            "DELETE FROM effect_agent_work_entries WHERE thread_id=? AND id=?",
            request.threadId,
            change.id,
          );
          continue;
        }

        const messageId =
          change.entry.owner._tag === "Handoff" ? change.entry.owner.messageId : undefined;

        if (
          messageId !== undefined &&
          exec(
            "SELECT 1 FROM effect_agent_message_deliveries WHERE owner_thread_id=? AND message_id=?",
            request.threadId,
            messageId,
          ).length > 0
        )
          continue;
        const entry = advanceWorkEntry(getEntry(request.threadId, change.entry.id), change.entry);
        const encoded = Schema.encodeResult(WorkJson)(entry);

        if (Result.isFailure(encoded)) return reject(failure("encode work entry", encoded.failure));
        if (new TextEncoder().encode(encoded.success).length > MAX_WORK_ENTRY_BYTES)
          return reject(failure("work entry byte bound"));
        const worker = entry.owner._tag === "WorkerInput" ? entry.owner : undefined;
        const handoff = entry.owner._tag === "Handoff" ? entry.owner.kind : null;

        exec(
          `INSERT INTO effect_agent_work_entries (thread_id, id, message_id, entry_json, created_sequence, owner_tag, worker_thread_id, worker_update, handoff_kind) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (thread_id, id) DO UPDATE SET message_id=excluded.message_id, entry_json=excluded.entry_json, created_sequence=excluded.created_sequence, owner_tag=excluded.owner_tag, worker_thread_id=excluded.worker_thread_id, worker_update=excluded.worker_update, handoff_kind=excluded.handoff_kind`,
          request.threadId,
          entry.id,
          messageId === undefined ? null : JSON.stringify(messageId),
          encoded.success,
          entry.createdSequence,
          entry.owner._tag,
          worker === undefined ? null : JSON.stringify(worker.workerThreadId),
          worker === undefined ? null : worker.update ? 1 : 0,
          handoff,
        );
      }
    }
    decode(
      Schema.Tuple([Schema.Struct({ count: Schema.Natural })]),
      exec(
        `INSERT INTO effect_agent_work_index (thread_id, version, state, through_sequence, reporting, entry_count) VALUES (?, ?, 'ready', ?, ?, (SELECT COUNT(*) FROM effect_agent_work_entries WHERE thread_id=?)) ON CONFLICT (thread_id) DO UPDATE SET version=excluded.version, state=excluded.state, through_sequence=excluded.through_sequence, reporting=excluded.reporting, entry_count=excluded.entry_count RETURNING entry_count AS count`,
        request.threadId,
        WORK_INDEX_VERSION,
        through,
        mode === "standard" ? 1 : mode === "private" ? 2 : 0,
        request.threadId,
      ),
    );
  };

  return { apply, invalidate };
};

/** Own only synchronous SQLite and its bounded derived views. The caller retains its
 * source transaction through asynchronous alarm enrollment and optional lifecycle hooks. */
export const makeSyncAppend = (
  storage: Pick<DurableObjectStorage, "sql" | "transactionSync">,
  views: {
    readonly thread: (id: string) => ReadonlyArray<typeof SyncThreadRow.Type> | undefined;
    readonly acceptThread: (row: typeof SyncThreadRow.Type) => void;
    readonly prefix: (
      id: string,
      through: number,
    ) => ReadonlyArray<typeof SyncRecordRow.Type> | undefined;
    readonly acceptRecord: (row: typeof SyncRecordRow.Type, bytes: number) => void;
  },
) => {
  const sql = storage.sql;

  const exec: Query = (statement, ...bindings) => {
    try {
      return sql.exec(statement, ...bindings).toArray();
    } catch (cause) {
      const error = SqlError.make({
        reason: classifySqliteError(cause, {
          operation: "execute",
          message: "Failed to execute statement",
        }),
      });

      return reject(
        DoStorageError.make({
          operation: "synchronous append SQL",
          message: error.message,
          cause: error,
        }),
      );
    }
  };

  const work = makeWork(exec);

  const append = (
    request: RawAppendRequest,
    references: References,
  ): Result.Result<RawAppendResult, SyncAppendError> => {
    try {
      // A refusal discovered after a write must unwind through transactionSync before
      // it becomes data. Returning a failure value from inside the callback would commit.
      const result = storage.transactionSync((): RawAppendResult => {
        const threads =
          views.thread(request.threadId) ??
          rowDecode(
            Schema.Array(SyncThreadRow),
            "effect_agent_threads",
            request.threadId,
            exec("SELECT * FROM effect_agent_threads WHERE thread_id=?", request.threadId),
          );

        const thread = threads.length === 1 ? threads[0] : undefined;

        if (thread === undefined)
          return reject(
            DoStorageCorruptionError.make({
              table: "effect_agent_threads",
              rowKey: request.threadId,
              message: `Expected exactly one row but found ${threads.length}.`,
            }),
          );
        if (request.producerEpoch !== thread.producer_epoch)
          return reject(
            DoFenceRejected.make({
              producerEpoch: request.producerEpoch,
              actualEpoch: thread.producer_epoch,
              message: `Producer epoch ${request.producerEpoch} is not the current epoch ${thread.producer_epoch}.`,
            }),
          );
        const prefix = views.prefix(request.threadId, thread.tail_sequence);

        const batches =
          prefix !== undefined && !prefix.some((row) => row.batch_id === request.batchId)
            ? []
            : rowDecode(
                Schema.Array(BatchRow),
                "effect_agent_canonical_batches",
                `${request.threadId}/${request.batchId}`,
                exec(
                  "SELECT thread_id, batch_id, first_sequence, last_sequence, batch_digest, tail_digest FROM effect_agent_canonical_batches WHERE thread_id=? AND batch_id=?",
                  request.threadId,
                  request.batchId,
                ),
              );

        if (batches.length > 1)
          return reject(
            DoStorageCorruptionError.make({
              table: "effect_agent_canonical_batches",
              rowKey: `${request.threadId}/${request.batchId}`,
              message: "A canonical batch primary key returned more than one row.",
            }),
          );
        const existing = batches[0];

        if (existing !== undefined) {
          if (existing.batch_digest !== request.batchDigest)
            return reject(
              DoAppendConflict.make({
                message: `Batch ${request.batchId} already exists with different canonical content.`,
                reason: "batch-digest",
              }),
            );

          return {
            firstSequence: existing.first_sequence,
            lastSequence: existing.last_sequence,
            replayed: true,
            tailDigest: existing.tail_digest,
          };
        }
        if (
          request.expectedTailSequence !== thread.tail_sequence ||
          request.expectedTailDigest !== thread.tail_digest
        )
          return reject(
            DoAppendConflict.make({
              message: "The canonical tail has advanced.",
              reason: "tail",
              actualTailSequence: thread.tail_sequence,
              actualTailDigest: thread.tail_digest,
            }),
          );
        const ids = request.records.map((record) => record.recordId);
        const duplicates = prefix?.filter((row) => ids.some((id) => id === row.record_id)) ?? [];

        if (prefix === undefined)
          for (const group of groups(ids, 90))
            duplicates.push(
              ...rowDecode(
                Schema.Array(SyncRecordRow),
                "effect_agent_canonical_records",
                `${request.threadId}/record_ids`,
                exec(
                  `SELECT thread_id, sequence, record_id, batch_id, ${storedRecord("effect_agent_canonical_records")} AS record_json FROM effect_agent_canonical_records WHERE thread_id=? AND record_id IN (${group.map(() => "?").join(",")})`,
                  request.threadId,
                  ...group,
                ),
              ),
            );
        if (duplicates.length > 0)
          return reject(
            DoAppendConflict.make({
              message: `Canonical record ID ${duplicates[0]?.record_id} already exists.`,
              reason: "record-identity",
            }),
          );
        const first = decode(CanonicalSequence, thread.tail_sequence + 1);
        const last = decode(CanonicalSequence, first + request.records.length - 1);

        validateProgress(request.progress, makeProgressReader(exec, request));
        appendRange(exec, request, first, last);
        exec(
          "INSERT INTO effect_agent_canonical_batches (thread_id, batch_id, first_sequence, last_sequence, batch_digest, tail_digest, batch_header_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
          request.threadId,
          request.batchId,
          first,
          last,
          request.batchDigest,
          request.tailDigest,
          request.batchHeaderJson,
        );

        const records = request.records.map((record, index) => ({
          record,
          row: {
            thread_id: request.threadId,
            sequence: CanonicalSequence.make(first + index),
            record_id: record.recordId,
            batch_id: request.batchId,
            record_json: record.recordJson,
            ...record.readMetadata.columns,
          },
        }));

        for (const group of groups(records, 5)) {
          exec(
            `INSERT INTO effect_agent_canonical_records (thread_id, sequence, record_id, batch_id, record_json, record_tag, run_id, tool_call_id, input_kind, source_submission_id, message_id, submission_id, application_input, context_through, context_kind, worker_thread_id, handoff) VALUES ${group.map(() => "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").join(",")}`,
            ...group.flatMap(({ row }) => [
              row.thread_id,
              row.sequence,
              row.record_id,
              row.batch_id,
              row.record_json,
              row.record_tag,
              row.run_id,
              row.tool_call_id,
              row.input_kind,
              row.source_submission_id,
              row.message_id,
              row.submission_id,
              row.application_input,
              row.context_through,
              row.context_kind,
              row.worker_thread_id,
              row.handoff,
            ]),
          );

          const memberships = group.flatMap(({ record, row }) =>
            record.readMetadata.runIds.map(
              (runId) => [request.threadId, runId, row.sequence] as const,
            ),
          );

          for (const members of groups(memberships, 33))
            exec(
              `INSERT INTO effect_agent_record_runs (thread_id, run_id, sequence) VALUES ${members.map(() => "(?,?,?)").join(",")}`,
              ...members.flat(),
            );
          for (const { record, row } of group) views.acceptRecord(row, record.recordBytes);
        }

        const updated = rowDecode(
          Schema.Tuple([SyncThreadRow]),
          "effect_agent_threads",
          request.threadId,
          exec(
            "UPDATE effect_agent_threads SET tail_sequence=?, tail_digest=?, producer_epoch=? WHERE thread_id=? RETURNING *",
            last,
            request.tailDigest,
            request.producerEpoch,
            request.threadId,
          ),
        );

        views.acceptThread(updated[0]);
        work.apply(request, first, references);

        return {
          firstSequence: first,
          lastSequence: last,
          replayed: false,
          tailDigest: request.tailDigest,
        };
      });

      return Result.succeed(result);
    } catch (cause) {
      work.invalidate();
      if (cause instanceof AppendRefusal) return Result.fail(cause.error);
      throw cause;
    }
  };

  return { append, invalidate: work.invalidate };
};
