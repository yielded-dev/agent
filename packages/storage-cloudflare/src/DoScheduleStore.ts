import {
  makeSqlScheduleStore,
  type SqlScheduleWriteTransaction,
} from "@yielded/agent-storage-sql/sql-schedule-store";
import {
  ScheduleFailpoint,
  type ScheduleFailpointError,
  ScheduleInstant,
  ScheduleStorageError,
  ScheduleStore,
} from "@yielded/agent/schedule";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClientService from "effect/sql/SqlClient";

const CURRENT_SCHEDULE_STORE_VERSION = 3;
const MAX_STORED_SCHEDULE_LENGTH = 1_900_000;

class ScheduleDeadlineRow extends Schema.Class<ScheduleDeadlineRow>(
  "@effect-agent/storage-cloudflare/ScheduleDeadlineRow",
)({
  deadline_at_millis: Schema.NullOr(ScheduleInstant),
}) {}

class ScheduleStoreStateRow extends Schema.Class<ScheduleStoreStateRow>(
  "@effect-agent/storage-cloudflare/ScheduleStoreStateRow",
)({
  storage_version: Schema.Int.check(Schema.isGreaterThan(0)),
  alarm_generation: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}) {}

class ScheduleTableNameRow extends Schema.Class<ScheduleTableNameRow>(
  "@effect-agent/storage-cloudflare/ScheduleTableNameRow",
)({
  name: Schema.String,
}) {}

export interface DoScheduleAlarmReplacement {
  readonly deadlineAtMillis: number | null;
  /** Included in every logical alarm payload so equal-time replacement stays distinguishable. */
  readonly generation: number;
}

export type DoScheduleReplaceAlarm = (
  replacement: DoScheduleAlarmReplacement,
) => Effect.Effect<void, ScheduleStorageError>;

/**
 * Platform-owned transaction boundary for schedule SQL and logical alarm mutation. The callback
 * and its `replaceAlarm` capability belong to one fiber and must not escape or fork.
 */
export class DoScheduleTransaction extends Context.Service<
  DoScheduleTransaction,
  {
    readonly run: <A, E, R>(
      body: (replaceAlarm: DoScheduleReplaceAlarm) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | ScheduleStorageError, R>;
  }
>()("@effect-agent/storage-cloudflare/DoScheduleTransaction") {}

/** Platform driver operations that use the same schedule-state and alarm transaction. */
export class DoScheduleAlarmControl extends Context.Service<
  DoScheduleAlarmControl,
  {
    /** Establish a future recovery wake before cross-Object admission starts. */
    readonly prearm: (
      deadlineAtMillis: number,
    ) => Effect.Effect<void, ScheduleStorageError | ScheduleFailpointError>;
    /** Replace or cancel the wake from the object's authoritative indexed deadline. */
    readonly reconcile: Effect.Effect<void, ScheduleStorageError | ScheduleFailpointError>;
  }
>()("@effect-agent/storage-cloudflare/DoScheduleAlarmControl") {}

const unavailable = (operation: string): ScheduleStorageError =>
  ScheduleStorageError.make({ operation, reason: "unavailable" });

const corrupt = (operation: string): ScheduleStorageError =>
  ScheduleStorageError.make({ operation, reason: "corrupt" });

const decodeRows = <A, I, R>(
  schema: Schema.Codec<A, I, R>,
  rows: ReadonlyArray<unknown>,
  operation: string,
): Effect.Effect<A, ScheduleStorageError, R> =>
  Schema.decodeUnknownEffect(schema)(rows).pipe(Effect.mapError(() => corrupt(operation)));

const decodeBoundary = <A, I, R>(
  schema: Schema.Codec<A, I, R>,
  value: unknown,
  operation: string,
): Effect.Effect<A, ScheduleStorageError, R> =>
  Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError(() => corrupt(operation)));

const initializeScheduleStore = Effect.fnUntraced(function* () {
  const sql = yield* SqlClientService.SqlClient;
  const operation = "initialize schedule store";

  const rawTables = yield* sql<Record<string, unknown>>`
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
      AND name IN (
        'effect_agent_schedule_store_state',
        'effect_agent_schedules'
      )
    ORDER BY name
  `.pipe(Effect.mapError(() => unavailable(operation)));

  const tables = yield* decodeRows(Schema.Array(ScheduleTableNameRow), rawTables, operation);
  const hasState = tables.some((row) => row.name === "effect_agent_schedule_store_state");
  const hasSchedules = tables.some((row) => row.name === "effect_agent_schedules");

  if (!hasState) {
    if (hasSchedules) return yield* corrupt(operation);
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`
            CREATE TABLE effect_agent_schedule_store_state (
              singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
              storage_version INTEGER NOT NULL,
              alarm_generation INTEGER NOT NULL
            )
          `.withoutTransform;
          yield* sql`
            CREATE TABLE effect_agent_schedules (
              tenant_id TEXT NOT NULL,
              owner_id TEXT NOT NULL,
              schedule_id TEXT NOT NULL,
              deadline_at_millis INTEGER,
              record_json TEXT NOT NULL,
              PRIMARY KEY (tenant_id, owner_id, schedule_id)
            )
          `.withoutTransform;
          yield* sql`
            CREATE INDEX effect_agent_schedules_deadline
              ON effect_agent_schedules (deadline_at_millis, tenant_id, owner_id, schedule_id)
              WHERE deadline_at_millis IS NOT NULL
          `.withoutTransform;
          yield* sql`
            CREATE INDEX effect_agent_schedules_owner_deadline
              ON effect_agent_schedules (tenant_id, owner_id, deadline_at_millis, schedule_id)
              WHERE deadline_at_millis IS NOT NULL
          `.withoutTransform;
          yield* sql`
            INSERT INTO effect_agent_schedule_store_state (
              singleton, storage_version, alarm_generation
            ) VALUES (1, ${CURRENT_SCHEDULE_STORE_VERSION}, 0)
          `.withoutTransform;
        }),
      )
      .pipe(Effect.mapError(() => unavailable(operation)));

    return;
  }

  if (!hasSchedules) return yield* corrupt(operation);

  const rawState = yield* sql<Record<string, unknown>>`
    SELECT storage_version, alarm_generation
    FROM effect_agent_schedule_store_state
    WHERE singleton = 1
  `.pipe(Effect.mapError(() => unavailable(operation)));

  const state = yield* decodeRows(Schema.Array(ScheduleStoreStateRow), rawState, operation);

  if (state.length !== 1 || state[0].storage_version !== CURRENT_SCHEDULE_STORE_VERSION) {
    return yield* corrupt(
      state.length === 1
        ? `${operation}: incompatible storage version ${state[0].storage_version}; expected ${CURRENT_SCHEDULE_STORE_VERSION}`
        : `${operation}: invalid storage version row`,
    );
  }
});

const makeServices = Effect.gen(function* () {
  const sql = yield* SqlClientService.SqlClient;
  const transactions = yield* DoScheduleTransaction;
  const scheduleFailpoint = yield* ScheduleFailpoint;

  yield* initializeScheduleStore();

  const readNextDeadline = Effect.fnUntraced(function* (
    operation: string,
  ): Effect.fn.Return<number | null, ScheduleStorageError> {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT MIN(deadline_at_millis) AS deadline_at_millis
      FROM effect_agent_schedules
      WHERE deadline_at_millis IS NOT NULL
    `.pipe(Effect.mapError(() => unavailable(operation)));

    const decoded = yield* decodeRows(Schema.Array(ScheduleDeadlineRow), rows, operation);

    if (decoded.length !== 1) return yield* corrupt(operation);

    return decoded[0].deadline_at_millis;
  });

  const replaceAlarm = Effect.fnUntraced(function* (
    replace: DoScheduleReplaceAlarm,
    deadlineAtMillis: number | null,
    operation: string,
  ) {
    const rawState = yield* sql<Record<string, unknown>>`
      UPDATE effect_agent_schedule_store_state
      SET alarm_generation = alarm_generation + 1
      WHERE singleton = 1
      RETURNING storage_version, alarm_generation
    `.pipe(Effect.mapError(() => unavailable(operation)));

    const state = yield* decodeRows(Schema.Array(ScheduleStoreStateRow), rawState, operation);

    if (state.length !== 1 || state[0].storage_version !== CURRENT_SCHEDULE_STORE_VERSION) {
      return yield* corrupt(operation);
    }
    yield* scheduleFailpoint.hit("schedule:alarm:before");
    yield* replace({ deadlineAtMillis, generation: state[0].alarm_generation });
    yield* scheduleFailpoint.hit("schedule:alarm:after");
  });

  const transact: SqlScheduleWriteTransaction = (body, operation) =>
    transactions.run((replace) =>
      Effect.gen(function* () {
        const result = yield* body;

        if (result.changed) {
          yield* replaceAlarm(replace, yield* readNextDeadline(operation), operation);
        }

        return result;
      }),
    );

  const store = yield* makeSqlScheduleStore(transact, undefined, MAX_STORED_SCHEDULE_LENGTH);

  const prearm = Effect.fnUntraced(function* (
    deadlineAtMillis: number,
  ): Effect.fn.Return<void, ScheduleStorageError | ScheduleFailpointError> {
    yield* decodeBoundary(ScheduleInstant, deadlineAtMillis, "pre-arm schedule recovery");
    yield* transactions.run((replace) =>
      replaceAlarm(replace, deadlineAtMillis, "pre-arm schedule recovery"),
    );
    yield* scheduleFailpoint.hit("schedule:prearm:after");
  });

  const reconcile = Effect.gen(function* () {
    yield* transactions.run((replace) =>
      Effect.gen(function* () {
        const deadline = yield* readNextDeadline("reconcile schedule alarm");

        yield* replaceAlarm(replace, deadline, "reconcile schedule alarm");
      }),
    );
    yield* scheduleFailpoint.hit("schedule:reconcile:after");
  });

  return Context.make(ScheduleStore, store).pipe(
    Context.add(DoScheduleAlarmControl, { prearm, reconcile }),
  );
});

/**
 * Durable Object SQLite ScheduleStore. Platform code supplies the one transaction owner that
 * combines these SQL mutations with its logical and native alarm lifecycle.
 */
export const scheduleStoreLayer: Layer.Layer<
  ScheduleStore | DoScheduleAlarmControl,
  ScheduleStorageError,
  SqlClientService.SqlClient | DoScheduleTransaction
> = Layer.effectContext(makeServices);
