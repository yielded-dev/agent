import {
  ActivityBusy,
  ActivityMutationFailpoint,
  ActivityOwnershipLost,
  ActivityProcessorKey,
  ActivityProcessorStore,
  ActivityProgress,
  ActivityStoreError,
  ActivityWorkConflict,
  PreparedActivity,
  type ActivitySession,
} from "@yielded/agent/activity-store";
import { CanonicalSequence, Digest } from "@yielded/agent/records";
import { Clock, Effect, Schema } from "effect";
import * as SqlClientService from "effect/sql/SqlClient";

import { sqlStorageOwner } from "./internal/SqlOwner.ts";
import {
  makeRowDecoder,
  makeSqlQuery,
  SqlInteger,
  type SqlWriteTransaction,
} from "./SqlStorage.ts";

const STORAGE_VERSION = 2 as const;
const activeSessions = new WeakMap<object, Map<string, object>>();
const StoredJson = Schema.String.check(Schema.isMaxLength(16 * 1024 * 1024));
const sameKey = Schema.toEquivalence(ActivityProcessorKey);
const sameWork = Schema.toEquivalence(PreparedActivity);

class ActivityStateRow extends Schema.Class<ActivityStateRow>(
  "@effect-agent/storage-sql/ActivityStateRow",
)({
  processor_id: ActivityProcessorKey.fields.processorId,
  processor_version: ActivityProcessorKey.fields.processorVersion,
  thread_id: ActivityProcessorKey.fields.threadId,
  format_version: SqlInteger,
  through_sequence: SqlInteger.pipe(Schema.decodeTo(ActivityProgress.fields.throughSequence)),
  progress_json: StoredJson,
}) {}

class ActivityChangeCountRow extends Schema.Class<ActivityChangeCountRow>(
  "@effect-agent/storage-sql/ActivityChangeCountRow",
)({
  changed: SqlInteger,
}) {}

const StoredVersionHeader = Schema.Struct({ version: Schema.Int });
const decodeVersionHeader = Schema.decodeEffect(Schema.fromJsonString(StoredVersionHeader));
const decodeProgressJson = Schema.decodeEffect(Schema.fromJsonString(ActivityProgress));

const storeError = (
  operation: string,
  reason: ActivityStoreError["reason"] = "unavailable",
): ActivityStoreError => ActivityStoreError.make({ operation, reason });

const { decodeRows } = makeRowDecoder(({ rowKey }) => storeError(rowKey, "corrupt"));

const decodeInput = <A, I>(
  schema: Schema.Codec<A, I, never>,
  value: unknown,
  operation: string,
): Effect.Effect<A, ActivityStoreError> =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError(() => storeError(operation, "invalid-input")),
  );

const encodeProgress = (
  progress: ActivityProgress,
  operation: string,
): Effect.Effect<string, ActivityStoreError> =>
  Schema.encodeEffect(Schema.fromJsonString(ActivityProgress))(progress).pipe(
    Effect.mapError(() => storeError(operation, "corrupt")),
    Effect.flatMap((encoded) =>
      Schema.decodeEffect(StoredJson)(encoded).pipe(
        Effect.mapError(() => storeError(operation, "invalid-input")),
      ),
    ),
  );

const decodeProgress = Effect.fnUntraced(function* (
  value: string,
  operation: string,
): Effect.fn.Return<ActivityProgress, ActivityStoreError> {
  const header = yield* decodeVersionHeader(value).pipe(
    Effect.mapError(() => storeError(operation, "corrupt")),
  );

  if (header.version !== STORAGE_VERSION) {
    return yield* storeError(operation, "incompatible");
  }

  const progress = yield* decodeProgressJson(value).pipe(
    Effect.mapError(() => storeError(operation, "corrupt")),
  );

  const canonical = yield* encodeProgress(progress, operation);

  if (canonical !== value) return yield* storeError(operation, "corrupt");

  return progress;
});

const validateProgress = Effect.fnUntraced(function* (
  progress: ActivityProgress,
  operation: string,
): Effect.fn.Return<ActivityProgress, ActivityStoreError> {
  if (
    progress.pending !== null &&
    (!sameKey(progress.pending.key, progress.key) ||
      progress.pending.sequence !== progress.throughSequence + 1)
  ) {
    return yield* storeError(operation, "corrupt");
  }

  return progress;
});

/**
 * Initialize standalone activity progress and provide its transitions. The adapter supplies
 * its transaction semantics and optional namespace setup, which runs in the initialization transaction.
 */
export const makeSqlActivityStore = Effect.fnUntraced(function* (
  transact: SqlWriteTransaction,
  initializeNamespace: Effect.Effect<
    void,
    ActivityStoreError,
    SqlClientService.SqlClient
  > = Effect.void,
  namespace?: string,
) {
  const sql = yield* SqlClientService.SqlClient;
  const owner = yield* sqlStorageOwner(sql, namespace);
  const withWriteTransaction: SqlWriteTransaction = (body) => owner.withMutation(transact(body));
  let registry = activeSessions.get(owner.identity);

  if (registry === undefined) {
    registry = new Map();
    activeSessions.set(owner.identity, registry);
  }
  const active = registry;
  const granted = new Map<string, object>();
  let closed = false;

  yield* Effect.addFinalizer(() =>
    owner.withMutation(
      Effect.sync(() => {
        closed = true;
        for (const [id, token] of granted) {
          if (active.get(id) === token) active.delete(id);
        }
        granted.clear();
      }),
    ),
  );

  const { table: relation, execute } = yield* makeSqlQuery(namespace);
  const failpoint = yield* ActivityMutationFailpoint;
  const decodeMetadataRows = decodeRows(Schema.Array(Schema.Struct({ version: SqlInteger })));
  const decodeTableRows = decodeRows(Schema.Array(Schema.Struct({ name: Schema.NonEmptyString })));
  const decodeStateRows = decodeRows(Schema.Array(ActivityStateRow));
  const decodeChangeCountRows = decodeRows(Schema.Array(ActivityChangeCountRow));

  yield* failpoint.hit("activity:initialize:before");
  yield* withWriteTransaction(
    Effect.gen(function* () {
      yield* initializeNamespace;

      const integer = sql.literal(
        sql.onDialectOrElse({ pg: () => "BIGINT", orElse: () => "INTEGER" }),
      );

      yield* sql`
        CREATE TABLE IF NOT EXISTS ${relation("effect_agent_activity_metadata")} (
          component TEXT PRIMARY KEY NOT NULL,
          version ${integer} NOT NULL
        )
      `.pipe(execute);

      const metadataRows = yield* sql<Record<string, unknown>>`
        SELECT version FROM ${relation("effect_agent_activity_metadata")}
        WHERE component = ${"activity"}
      `.pipe(execute);

      const metadata = yield* decodeMetadataRows(
        "activity",
        "decode activity schema version",
        metadataRows,
      );

      if (metadata.length > 1) {
        return yield* storeError("decode activity schema version", "corrupt");
      }
      const currentVersion = metadata[0]?.version;

      if (currentVersion !== undefined && currentVersion !== STORAGE_VERSION) {
        return yield* storeError("initialize activity schema", "incompatible");
      }
      if (currentVersion === undefined) {
        const tableRows = yield* sql.onDialectOrElse({
          pg: () =>
            sql<Record<string, unknown>>`
            SELECT c.relname AS name FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p')
              AND c.relname LIKE 'effect_agent_activity_processor_state_%'
              AND n.nspname = ${namespace ?? sql`current_schema()`}
          `.pipe(execute),
          orElse: () =>
            sql<Record<string, unknown>>`
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name LIKE 'effect_agent_activity_processor_state_%'
          `.pipe(execute),
        });

        const existing = yield* decodeTableRows("activity", "inspect activity schema", tableRows);

        if (existing.length > 0) {
          return yield* storeError("initialize activity schema", "incompatible");
        }
        yield* sql`
          CREATE TABLE ${relation("effect_agent_activity_processor_state_v2")} (
            processor_id TEXT NOT NULL,
            processor_version TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            format_version ${integer} NOT NULL,
            through_sequence ${integer} NOT NULL,
            progress_json TEXT NOT NULL,
            PRIMARY KEY (processor_id, processor_version, thread_id)
          )
        `.pipe(execute);
        yield* sql`
          INSERT INTO ${relation("effect_agent_activity_metadata")} (component, version)
          VALUES (${"activity"}, ${STORAGE_VERSION})
        `.pipe(execute);
      }
      yield* sql`
        SELECT processor_id, processor_version, thread_id, format_version,
          through_sequence, progress_json
        FROM ${relation("effect_agent_activity_processor_state_v2")}
        LIMIT 0
      `.pipe(execute);
    }),
  ).pipe(Effect.catchTag("SqlError", () => Effect.fail(storeError("initialize activity schema"))));
  yield* failpoint.hit("activity:initialize:after");

  const readProgress = Effect.fnUntraced(function* (
    key: ActivityProcessorKey,
    operation: string,
  ): Effect.fn.Return<ActivityProgress | null, ActivityStoreError> {
    const rawRows = yield* sql<Record<string, unknown>>`
        SELECT processor_id, processor_version, thread_id, format_version,
          through_sequence, progress_json
        FROM ${relation("effect_agent_activity_processor_state_v2")}
        WHERE processor_id = ${key.processorId}
          AND processor_version = ${key.processorVersion}
          AND thread_id = ${key.threadId}
      `.pipe(
      execute,
      Effect.mapError(() => storeError(operation)),
    );

    const rows = yield* decodeStateRows("activity", operation, rawRows);

    if (rows.length === 0) return null;
    if (rows.length !== 1) return yield* storeError(operation, "corrupt");
    const row = rows[0];

    if (row.format_version !== STORAGE_VERSION) {
      return yield* storeError(operation, "incompatible");
    }
    const progress = yield* decodeProgress(row.progress_json, operation);

    yield* validateProgress(progress, operation);
    if (
      !sameKey(progress.key, key) ||
      row.processor_id !== key.processorId ||
      row.processor_version !== key.processorVersion ||
      row.thread_id !== key.threadId ||
      row.through_sequence !== progress.throughSequence
    ) {
      return yield* storeError(operation, "corrupt");
    }

    return progress;
  });

  const checkChanged = Effect.fnUntraced(function* (
    rawRows: ReadonlyArray<Record<string, unknown>>,
    operation: string,
  ): Effect.fn.Return<void, ActivityStoreError> {
    const rows = yield* decodeChangeCountRows("activity", operation, rawRows);

    if (rows.length !== 1 || rows[0].changed !== 1) {
      return yield* storeError(operation, "corrupt");
    }
  });

  const insertProgress = Effect.fnUntraced(function* (
    progress: ActivityProgress,
    operation: string,
  ) {
    const progressJson = yield* encodeProgress(progress, operation);

    const changed = yield* sql<Record<string, unknown>>`
      INSERT INTO ${relation("effect_agent_activity_processor_state_v2")} (
        processor_id, processor_version, thread_id, format_version, through_sequence, progress_json
      ) VALUES (
        ${progress.key.processorId}, ${progress.key.processorVersion}, ${progress.key.threadId},
        ${STORAGE_VERSION}, ${progress.throughSequence}, ${progressJson}
      )
      RETURNING 1 AS changed
    `.pipe(execute);

    yield* checkChanged(changed, operation);
  });

  const updateProgress = Effect.fnUntraced(function* (
    current: ActivityProgress,
    next: ActivityProgress,
    operation: string,
  ) {
    const progressJson = yield* encodeProgress(next, operation);

    const changed = yield* sql<Record<string, unknown>>`
      UPDATE ${relation("effect_agent_activity_processor_state_v2")}
      SET format_version = ${STORAGE_VERSION},
          through_sequence = ${next.throughSequence},
          progress_json = ${progressJson}
      WHERE processor_id = ${current.key.processorId}
        AND processor_version = ${current.key.processorVersion}
        AND thread_id = ${current.key.threadId}
        AND through_sequence = ${current.throughSequence}
      RETURNING 1 AS changed
    `.pipe(execute);

    yield* checkChanged(changed, operation);
  });

  const inspect: ActivityProcessorStore["Service"]["inspect"] = Effect.fnUntraced(function* (key) {
    const decoded = yield* decodeInput(ActivityProcessorKey, key, "inspect activity progress");

    return yield* owner.withMutation(
      Effect.gen(function* () {
        if (closed || owner.isClosed()) return yield* storeError("inspect activity progress");

        return yield* readProgress(decoded, "inspect activity progress");
      }),
    );
  });

  const acquire: ActivityProcessorStore["Service"]["acquire"] = Effect.fn(
    "SqlActivityStore.acquire",
  )(function* (rawKey) {
    const decoded = yield* decodeInput(ActivityProcessorKey, rawKey, "acquire activity progress");
    const key = ActivityProcessorKey.make({ ...decoded });
    const id = JSON.stringify([key.processorId, key.processorVersion, key.threadId]);

    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const token = {};

        const initial = yield* owner.withMutation(
          Effect.gen(function* () {
            if (closed || owner.isClosed()) return yield* storeError("acquire activity progress");
            if (active.has(id)) return yield* ActivityBusy.make({ key });
            const current = yield* readProgress(key, "acquire activity progress");

            active.set(id, token);
            granted.set(id, token);

            return (
              current ??
              ActivityProgress.make({
                version: STORAGE_VERSION,
                key,
                throughSequence: CanonicalSequence.make(0),
                pending: null,
                advancedAt: null,
              })
            );
          }),
        );

        const checkActive = Effect.suspend(() =>
          closed || owner.isClosed() || active.get(id) !== token
            ? Effect.fail(ActivityOwnershipLost.make({ key }))
            : Effect.void,
        );

        // Scope cleanup is local. A stale finalizer cannot retire a newer session.
        yield* Effect.addFinalizer(() =>
          owner.withMutation(
            Effect.sync(() => {
              if (active.get(id) === token) active.delete(id);
              if (granted.get(id) === token) granted.delete(id);
            }),
          ),
        );

        const mutate = <A, E, R>(body: Effect.Effect<A, E, R>) =>
          owner.withMutation(Effect.andThen(checkActive, transact(body)));

        const session: ActivitySession = {
          progress: initial,
          checkActive,
          prepare: Effect.fnUntraced(function* (input) {
            const operation = "prepare activity output";
            const work = yield* decodeInput(PreparedActivity, input, operation);

            yield* failpoint.hit("activity:prepare:before");

            const result = yield* mutate(
              Effect.gen(function* () {
                yield* checkActive;
                const stored = yield* readProgress(key, operation);

                const current =
                  stored ??
                  ActivityProgress.make({
                    version: STORAGE_VERSION,
                    key,
                    throughSequence: CanonicalSequence.make(0),
                    pending: null,
                    advancedAt: null,
                  });

                if (current.pending !== null) {
                  if (sameWork(current.pending, work))
                    return { work: current.pending, changed: false } as const;

                  return yield* ActivityWorkConflict.make({ key, workId: work.workId });
                }
                if (!sameKey(work.key, key) || work.sequence !== current.throughSequence + 1)
                  return yield* ActivityWorkConflict.make({ key, workId: work.workId });
                const next = ActivityProgress.make({ ...current, pending: work });

                if (stored === null) yield* insertProgress(next, operation);
                else yield* updateProgress(current, next, operation);
                yield* failpoint.hit("activity:prepare:after-state");

                return { work, changed: true } as const;
              }),
            ).pipe(Effect.catchTag("SqlError", () => Effect.fail(storeError(operation))));

            if (result.changed) yield* failpoint.hit("activity:prepare:after");

            return result.work;
          }),
          advance: Effect.fnUntraced(function* (input) {
            const operation = "advance activity progress";
            const workId = yield* decodeInput(Digest, input, operation);

            yield* failpoint.hit("activity:advance:before");

            const next = yield* mutate(
              Effect.gen(function* () {
                yield* checkActive;
                const current = yield* readProgress(key, operation);

                if (
                  current === null ||
                  current.pending === null ||
                  current.pending.workId !== workId
                )
                  return yield* ActivityWorkConflict.make({ key, workId });

                const next = ActivityProgress.make({
                  ...current,
                  throughSequence: current.pending.sequence,
                  pending: null,
                  advancedAt: yield* Clock.currentTimeMillis,
                });

                yield* updateProgress(current, next, operation);
                yield* failpoint.hit("activity:advance:after-state");

                return next;
              }),
            ).pipe(Effect.catchTag("SqlError", () => Effect.fail(storeError(operation))));

            yield* failpoint.hit("activity:advance:after");

            return next;
          }),
        };

        return yield* restore(Effect.andThen(checkActive, Effect.succeed(session)));
      }),
    );
  });

  return ActivityProcessorStore.of({ inspect, acquire });
});
