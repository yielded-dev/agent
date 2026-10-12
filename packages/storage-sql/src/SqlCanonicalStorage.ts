import { canonicalJson, digestCanonicalJson } from "@yielded/agent/digest";
import { Digest, RecordJson, type CanonicalRecordPayload } from "@yielded/agent/records";
import { ThreadStoreError } from "@yielded/agent/thread-store";
import { Crypto, Effect, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { Fragment, Statement } from "effect/sql/Statement";

import { makeSqlQuery, SqlInteger } from "./SqlStorage.ts";
import type { CanonicalRecordMetadata } from "./SqlThreadNativeReads.ts";

const Text = Schema.String.check(Schema.isMaxLength(16 * 1024 * 1024));
const Key = SqlInteger.pipe(Schema.decodeTo(Schema.Int.check(Schema.isGreaterThan(0))));

/** Literal canonical JSON between createdAt, payload and recordId, in lexical key order. */
export const EnvelopeFragments = Schema.Struct({
  prefix: Text,
  before_payload: Text,
  before_record_id: Text,
  suffix: Text,
});

export const CapturedRecordStorage = Schema.Struct({
  created_at_json: Text,
  record_id_json: Text,
  payload_json: Text,
  envelope: EnvelopeFragments,
  envelope_digest: Digest,
});

export type CapturedRecordStorage = typeof CapturedRecordStorage.Type;

/** Codes belong to this fresh physical layout, not the public record format. */
export const RECORD_TAGS: Readonly<Record<CanonicalRecordPayload["_tag"], number>> = Object.freeze({
  AbortRequested: 1,
  AgentUpdateEmitted: 2,
  CompactionCreated: 3,
  ModelCallAborted: 4,
  ModelCompleted: 5,
  ModelResponseInterrupted: 6,
  ModelResponseRecorded: 7,
  PeerMessagePrepared: 8,
  RepairAnnotated: 9,
  RunCompleted: 10,
  RunContextRecorded: 11,
  RunContinuation: 12,
  RunDurationExhausted: 13,
  RunFailed: 14,
  RunPolicyUsageReserved: 15,
  RunStarted: 16,
  SubagentJoined: 17,
  SubagentLineageRecorded: 18,
  SubagentRequested: 19,
  SubagentStarted: 20,
  SubmissionSettled: 21,
  SubtreeBudgetReserved: 22,
  ThreadCreated: 23,
  ToolApprovalDecided: 24,
  ToolApprovalRequested: 25,
  ToolCallResolved: 26,
  ToolCallSettled: 27,
  ToolCallUnknown: 28,
  ToolStepSettled: 29,
  UserInputRecorded: 30,
  WorkHandoffCompleted: 31,
  WorkerInputCompleted: 32,
  WorkerInputRefused: 33,
  WorkerInputRequested: 34,
  WorkerOriginRecorded: 35,
  WorkerReportPrepared: 36,
  WorkerReportRefused: 37,
  WorkerStopRequested: 38,
});

const sameEnvelope = Schema.toEquivalence(EnvelopeFragments);

const failure = (operation: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation,
    message: "Canonical physical representation is incomplete or corrupt",
    ...(cause === undefined ? {} : { cause }),
  });

const decodeWire = Schema.decodeUnknownEffect(Schema.Record(Schema.String, RecordJson));
const decodeText = Schema.decodeUnknownEffect(Schema.String);

/** Capture exact wire before any transaction suspension; no timestamp or JSON dialect conversion. */
export const captureRecordStorage = Effect.fnUntraced(
  function* (wire: RecordJson, json: string) {
    const object = yield* decodeWire(wire);
    const createdAt = yield* decodeText(object.createdAt);
    const recordId = yield* decodeText(object.recordId);
    const payload = yield* Schema.decodeEffect(RecordJson)(object.payload);
    const fragments = ["{", "", "", ""];
    let at = 0;
    let count = 0;

    for (const key of Object.keys(object).sort()) {
      fragments[at] += `${count++ === 0 ? "" : ","}${JSON.stringify(key)}:`;
      if (key === "createdAt" || key === "payload" || key === "recordId") at++;
      else fragments[at] += canonicalJson(object[key]);
    }
    fragments[at] += "}";

    const envelope = yield* Schema.decodeEffect(EnvelopeFragments)({
      prefix: fragments[0],
      before_payload: fragments[1],
      before_record_id: fragments[2],
      suffix: fragments[3],
    });

    const created_at_json = JSON.stringify(createdAt);
    const payload_json = canonicalJson(payload);

    if (
      at !== 3 ||
      envelope.prefix +
        created_at_json +
        envelope.before_payload +
        payload_json +
        envelope.before_record_id +
        JSON.stringify(recordId) +
        envelope.suffix !==
        json
    )
      return yield* failure("capture canonical wire");

    return CapturedRecordStorage.make({
      created_at_json,
      record_id_json: JSON.stringify(recordId),
      payload_json,
      envelope,
      envelope_digest: yield* digestCanonicalJson(canonicalJson(envelope)),
    });
  },
  Effect.mapError((cause) => failure("capture canonical storage", cause)),
);

/** SQL concatenation preserves additive envelope members and original scalar spelling. */
export const hotRecordJson = (sql: SqlClient.SqlClient, namespace?: string, alias?: string) => {
  const owner = sql(alias ?? "effect_agent_canonical_records");

  const envelopes = sql(
    namespace === undefined ? "effect_agent_envelopes" : `${namespace}.effect_agent_envelopes`,
  );

  return sql`(SELECT e.prefix || ${owner}.created_at_json || e.before_payload || ${owner}.payload_json
    || e.before_record_id || ${owner}.record_id || e.suffix
    FROM ${envelopes} e WHERE e.envelope_key=${owner}.envelope_key)`;
};

/** Count stored wire bytes from column metadata; missing fragments still produce NULL. */
export const hotRecordBytes = (sql: SqlClient.SqlClient, namespace?: string, alias?: string) => {
  const owner = sql(alias ?? "effect_agent_canonical_records");

  const envelopes = sql(
    namespace === undefined ? "effect_agent_envelopes" : `${namespace}.effect_agent_envelopes`,
  );

  return sql`(SELECT octet_length(e.prefix) + octet_length(${owner}.created_at_json)
    + octet_length(e.before_payload) + octet_length(${owner}.payload_json)
    + octet_length(e.before_record_id) + octet_length(${owner}.record_id) + octet_length(e.suffix)
    FROM ${envelopes} e WHERE e.envelope_key=${owner}.envelope_key)`;
};

/** A sequence belongs to the nearest preceding commit only when that commit covers it. */
export const recordBatchId = (sql: SqlClient.SqlClient, namespace?: string, alias?: string) => {
  const owner = sql(alias ?? "effect_agent_canonical_records");

  const batches = sql(
    namespace === undefined
      ? "effect_agent_canonical_batches"
      : `${namespace}.effect_agent_canonical_batches`,
  );

  return sql`(SELECT CASE WHEN ${owner}.sequence<=b.last_sequence THEN b.batch_id END FROM ${batches} b
    WHERE b.thread_id=${owner}.thread_id AND b.first_sequence<=${owner}.sequence
    ORDER BY b.first_sequence DESC LIMIT 1)`;
};

export const storedBatchHeader = (sql: SqlClient.SqlClient, alias?: string) => {
  const owner = sql(alias ?? "effect_agent_canonical_batches");

  return sql`('{"batchId":' || ${owner}.batch_id || ',"producerId":' || ${owner}.producer_id_json || '}')`;
};

export const runKey = (
  sql: SqlClient.SqlClient,
  thread: Fragment,
  run: Fragment,
  namespace?: string,
) => {
  const table = sql(
    namespace === undefined ? "effect_agent_run_keys" : `${namespace}.effect_agent_run_keys`,
  );

  return sql`(SELECT run_key FROM ${table} WHERE thread_id=${thread} AND run_id=${run})`;
};

export const runIdentity = (sql: SqlClient.SqlClient, key: Fragment, namespace?: string) => {
  const table = sql(
    namespace === undefined ? "effect_agent_run_keys" : `${namespace}.effect_agent_run_keys`,
  );

  return sql`(SELECT run_id FROM ${table} WHERE run_key=${key})`;
};

export const definitionJson = (sql: SqlClient.SqlClient, namespace?: string, alias?: string) => {
  const owner = sql(alias ?? "effect_agent_submissions");

  const table = sql(
    namespace === undefined ? "effect_agent_definitions" : `${namespace}.effect_agent_definitions`,
  );

  return sql`(SELECT value_json FROM ${table} WHERE definition_key=${owner}.definition_key)`;
};

/** These writes run only inside the caller's existing fenced/admission/import transaction. */
export const makeCanonicalStorage = Effect.fnUntraced(function* (namespace?: string) {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const { table, execute } = yield* makeSqlQuery(namespace);

  const query = <A extends object>(statement: Statement<A>) =>
    execute(statement).pipe(
      Effect.mapError((cause) => failure("write canonical dictionary", cause)),
    );

  const readKey = Schema.decodeUnknownEffect(
    Schema.Array(Schema.Struct({ key: Key })).check(Schema.isMaxLength(1)),
  );

  const lookupKey = (statement: Statement<object>) =>
    query(statement).pipe(
      Effect.flatMap(readKey),
      Effect.map((rows) => rows[0]?.key),
      Effect.mapError((cause) => failure("read canonical dictionary key", cause)),
    );

  const storage = {
    envelope: Effect.fnUntraced(function* (value: CapturedRecordStorage) {
      const read = Effect.fnUntraced(function* () {
        const [row] = yield* Schema.decodeUnknownEffect(
          Schema.Array(
            Schema.Struct({
              key: Key,
              ...EnvelopeFragments.fields,
            }),
          ).check(Schema.isMaxLength(1)),
        )(
          yield* query(sql`
          SELECT envelope_key AS key, prefix, before_payload, before_record_id, suffix
          FROM ${table("effect_agent_envelopes")} WHERE content_digest=${value.envelope_digest}`),
        ).pipe(Effect.mapError((cause) => failure("read envelope dictionary", cause)));

        if (
          row !== undefined &&
          (row.prefix !== value.envelope.prefix ||
            row.before_payload !== value.envelope.before_payload ||
            row.before_record_id !== value.envelope.before_record_id ||
            row.suffix !== value.envelope.suffix)
        )
          return yield* failure("envelope dictionary collision");

        return row?.key;
      });

      const prior = yield* read();

      if (prior !== undefined) return prior;
      yield* query(sql`INSERT INTO ${table("effect_agent_envelopes")}
        (content_digest, prefix, before_payload, before_record_id, suffix)
        VALUES (${value.envelope_digest}, ${value.envelope.prefix}, ${value.envelope.before_payload}, ${value.envelope.before_record_id}, ${value.envelope.suffix})
        ON CONFLICT(content_digest) DO NOTHING`);
      const inserted = yield* read();

      return inserted === undefined ? yield* failure("missing envelope dictionary") : inserted;
    }),
    run: Effect.fnUntraced(function* (threadId: string, runId: string) {
      const statement = sql`SELECT run_key AS key FROM ${table("effect_agent_run_keys")}
        WHERE thread_id=${threadId} AND run_id=${runId}`;

      const prior = yield* lookupKey(statement);

      if (prior !== undefined) return prior;
      yield* query(sql`INSERT INTO ${table("effect_agent_run_keys")} (thread_id, run_id)
        VALUES (${threadId}, ${runId}) ON CONFLICT(thread_id, run_id) DO NOTHING`);
      const inserted = yield* lookupKey(statement);

      return inserted === undefined ? yield* failure("missing Run dictionary") : inserted;
    }),
    definition: Effect.fnUntraced(function* (json: string) {
      const digest = yield* digestCanonicalJson(json).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.mapError((cause) => failure("digest admission definitions", cause)),
      );

      const read = Effect.fnUntraced(function* () {
        const [row] = yield* Schema.decodeUnknownEffect(
          Schema.Array(
            Schema.Struct({
              key: Key,
              value_json: Text,
            }),
          ).check(Schema.isMaxLength(1)),
        )(
          yield* query(sql`
          SELECT definition_key AS key, value_json FROM ${table("effect_agent_definitions")}
          WHERE content_digest=${digest}`),
        ).pipe(Effect.mapError((cause) => failure("read definition dictionary", cause)));

        if (row !== undefined && row.value_json !== json)
          return yield* failure("definition dictionary collision");

        return row?.key;
      });

      const prior = yield* read();

      if (prior !== undefined) return prior;
      yield* query(sql`INSERT INTO ${table("effect_agent_definitions")} (content_digest, value_json)
        VALUES (${digest}, ${json}) ON CONFLICT(content_digest) DO NOTHING`);
      const inserted = yield* read();

      return inserted === undefined ? yield* failure("missing definition dictionary") : inserted;
    }),
  };

  return {
    ...storage,
    records: Effect.fnUntraced(function* (
      threadId: string,
      first: number,
      records: ReadonlyArray<{
        readonly storage: CapturedRecordStorage;
        readonly readMetadata: CanonicalRecordMetadata;
      }>,
    ) {
      const envelopes = new Map<
        string,
        { readonly key: number; readonly value: typeof EnvelopeFragments.Type }
      >();

      const runs = new Map<string, number>();

      const resolveRun = Effect.fnUntraced(function* (run: string) {
        const prior = runs.get(run);

        if (prior !== undefined) return prior;
        const value = yield* storage.run(threadId, run);

        runs.set(run, value);

        return value;
      });

      return yield* Effect.forEach(
        records,
        Effect.fnUntraced(function* (record, index) {
          const known = envelopes.get(record.storage.envelope_digest);

          if (known !== undefined && !sameEnvelope(known.value, record.storage.envelope))
            return yield* failure("envelope dictionary collision");
          let envelope = known?.key;

          if (envelope === undefined) {
            envelope = yield* storage.envelope(record.storage);
            envelopes.set(record.storage.envelope_digest, {
              key: envelope,
              value: record.storage.envelope,
            });
          }
          const run = record.readMetadata.columns.run_id;

          return {
            columns: {
              ...record.readMetadata.columns,
              thread_id: threadId,
              sequence: first + index,
              record_id: record.storage.record_id_json,
              created_at_json: record.storage.created_at_json,
              payload_json: record.storage.payload_json,
              envelope_key: envelope,
              run_id: run === null ? null : yield* resolveRun(run),
            },
            memberships: yield* Effect.forEach(record.readMetadata.runIds, (run) =>
              resolveRun(run).pipe(
                Effect.map((key) => ({
                  thread_id: threadId,
                  sequence: first + index,
                  run_id: key,
                })),
              ),
            ),
          };
        }),
      );
    }),
  };
});
