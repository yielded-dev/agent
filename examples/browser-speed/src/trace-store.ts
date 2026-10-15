import { Clock, Effect, Schema, Semaphore } from "effect";

import { LabError, Report, TraceArchive, TraceEvent } from "./contract.ts";

export interface TraceSink {
  readonly append: (event: TraceEvent) => void;
  readonly checkpoint: (report: Report) => void;
}

const reportCodec = Schema.fromJsonString(Report);
const eventCodec = Schema.fromJsonString(TraceEvent);
const archiveCodec = Schema.fromJsonString(TraceArchive);
const maxTraceBytes = 32 * 1024 * 1024;

/** SQLite retains incremental evidence before R2 publication, including interrupted requests. */
export const makeTraceStore = (sql: SqlStorage, ownerId: string, bucket: R2Bucket | undefined) => {
  const uploads = Semaphore.makeUnsafe(1);

  sql.exec(`CREATE TABLE IF NOT EXISTS lab_traces (
    id TEXT PRIMARY KEY, report TEXT NOT NULL, bytes INTEGER NOT NULL DEFAULT 0,
    dropped INTEGER NOT NULL DEFAULT 0, archived INTEGER NOT NULL DEFAULT 0
  )`);
  sql.exec(`CREATE TABLE IF NOT EXISTS lab_trace_events (
    run_id TEXT NOT NULL, sequence INTEGER NOT NULL, value TEXT NOT NULL,
    PRIMARY KEY (run_id, sequence)
  )`);

  const checkpoint = (report: Report) => {
    sql.exec(
      `INSERT INTO lab_traces(id, report) VALUES (?, ?)
       ON CONFLICT(id) DO UPDATE SET report=excluded.report, archived=0`,
      report.input.id,
      Schema.encodeSync(reportCodec)(report),
    );
  };

  const sink = (id: string): TraceSink => ({
    checkpoint,
    append: (event) => {
      const value = Schema.encodeSync(eventCodec)(event);
      const size = new TextEncoder().encode(value).byteLength;
      const row = sql.exec<{ bytes: number }>("SELECT bytes FROM lab_traces WHERE id=?", id).one();

      if (row.bytes + size > maxTraceBytes) {
        sql.exec("UPDATE lab_traces SET dropped=dropped+1 WHERE id=?", id);

        return;
      }
      sql.exec("INSERT INTO lab_trace_events VALUES (?, ?, ?)", id, event.sequence, value);
      sql.exec("UPDATE lab_traces SET bytes=bytes+?, archived=0 WHERE id=?", size, id);
    },
  });

  const reports = Effect.try({
    try: () =>
      sql
        .exec<{ report: string }>("SELECT report FROM lab_traces ORDER BY rowid")
        .toArray()
        .map((row) => Schema.decodeSync(reportCodec)(row.report)),
    catch: () => LabError.make({ code: "storage", message: "Could not load saved reports." }),
  });

  const read = Effect.fnUntraced(function* (id: string) {
    return yield* Effect.try({
      try: (): TraceArchive | null => {
        const row = sql
          .exec<{ report: string; dropped: number; archived: number }>(
            "SELECT report, dropped, archived FROM lab_traces WHERE id=?",
            id,
          )
          .toArray()[0];

        if (row === undefined) return null;
        const report = Schema.decodeSync(reportCodec)(row.report);

        const events = sql
          .exec<{ value: string }>(
            "SELECT value FROM lab_trace_events WHERE run_id=? ORDER BY sequence",
            id,
          )
          .toArray()
          .map((entry) => Schema.decodeSync(eventCodec)(entry.value));

        return {
          version: 1,
          runId: id,
          conversationId: report.input.chatId ?? id,
          report,
          events: events.filter((event) => event.kind !== "otlp"),
          otlp: events.filter((event) => event.kind === "otlp").map((event) => event.data),
          droppedEvents: row.dropped,
          storage: row.archived ? "r2" : "durable-object",
        };
      },
      catch: () => LabError.make({ code: "storage", message: "Could not read this trace." }),
    }).pipe(
      Effect.filterOrFail(
        (archive) => archive !== null,
        () => LabError.make({ code: "invalid", message: "This session does not own that trace." }),
      ),
    );
  });

  const archive = Effect.fnUntraced(function* (id: string) {
    const stamp = sql
      .exec<{ report: string; bytes: number; dropped: number }>(
        "SELECT report, bytes, dropped FROM lab_traces WHERE id=?",
        id,
      )
      .toArray()[0];

    const value = yield* read(id);

    if (
      stamp === undefined ||
      bucket === undefined ||
      value.storage === "r2" ||
      value.report.status === "running"
    )
      return value;
    const saved: TraceArchive = { ...value, storage: "r2" };

    yield* Effect.tryPromise({
      try: () =>
        bucket.put(
          `browser-use/v1/${ownerId}/${value.conversationId}/${id}.json`,
          Schema.encodeSync(archiveCodec)(saved),
          { httpMetadata: { contentType: "application/json" } },
        ),
      catch: () =>
        LabError.make({
          code: "storage",
          message: "R2 upload failed. The trace is retained in this session; download it to retry.",
        }),
    }).pipe(
      Effect.timeoutOrElse({
        duration: "10 seconds",
        orElse: () =>
          Effect.fail(
            LabError.make({
              code: "storage",
              message:
                "R2 upload timed out. The trace is retained in this session; download it to retry.",
            }),
          ),
      }),
    );
    // Evidence can arrive while R2 is writing. Never mark a newer snapshot as archived.
    sql.exec(
      "UPDATE lab_traces SET archived=1 WHERE id=? AND report=? AND bytes=? AND dropped=?",
      id,
      stamp.report,
      stamp.bytes,
      stamp.dropped,
    );

    return saved;
  }, uploads.withPermits(1));

  const control = Effect.fnUntraced(function* (report: Report, mode: "watch" | "control") {
    const now = yield* Clock.currentTimeMillis;

    checkpoint(report);

    const sequence = sql
      .exec<{ next: number }>(
        "SELECT COALESCE(MAX(sequence)+1, 0) AS next FROM lab_trace_events WHERE run_id=?",
        report.input.id,
      )
      .one().next;

    sink(report.input.id).append({
      sequence,
      at: now - Date.parse(report.startedAt),
      kind: "browser.control",
      data: { mode },
    });
    yield* archive(report.input.id);
  });

  return { sink, reports, read, archive, control };
};
