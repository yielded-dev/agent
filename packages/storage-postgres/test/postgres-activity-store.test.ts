import { describe, expect, it } from "@effect/vitest";
import * as PostgresStorage from "@yielded/agent-storage-postgres/postgres-storage";
import type { ActivityMutationFailpoint } from "@yielded/agent/activity-store";
import {
  ActivityBusy,
  ActivityMutationFailure,
  ActivityOwnershipLost,
  ActivityProcessorKey,
  ActivityProcessorStore,
  ActivityStoreError,
  ActivityWorkConflict,
  PreparedActivity,
} from "@yielded/agent/activity-store";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Result,
  Schema,
  Scope,
} from "effect";
import * as SqlClientService from "effect/sql/SqlClient";
import { TestClock } from "effect/testing";

import { clientLayer, storage, withTemporaryDatabase } from "./harness.ts";

const key = Schema.decodeSync(ActivityProcessorKey)({
  processorId: "profile",
  processorVersion: "v1",
  threadId: "thread-1",
});

const independentKeys = [
  key,
  Schema.decodeSync(ActivityProcessorKey)({ ...key, threadId: "thread-2" }),
  Schema.decodeSync(ActivityProcessorKey)({ ...key, processorId: "audit" }),
  Schema.decodeSync(ActivityProcessorKey)({ ...key, processorVersion: "v2" }),
];

const work = (
  sequence: number,
  marker: string,
  activityKey = key,
  output: Schema.Json = { marker },
) =>
  Schema.decodeSync(PreparedActivity)({
    version: 1,
    key: activityKey,
    sequence,
    workId: marker.repeat(64),
    recordId: `record-${sequence}`,

    output,
  });

const storeLayer = (url: string) => storage(url).activityStore;

const failpointLayer = (url: string, handler: ActivityMutationFailpoint["Service"]["hit"]) =>
  storage(url, { activityFailpoint: handler }).activityStore;

const runStore = <A, E>(
  url: string,
  effect: Effect.Effect<A, E, ActivityProcessorStore | Scope.Scope>,
) => effect.pipe(Effect.scoped, Effect.provide(storeLayer(url)));

const inspect = (url: string, activityKey = key) =>
  runStore(
    url,
    Effect.gen(function* () {
      const store = yield* ActivityProcessorStore;

      return yield* store.inspect(activityKey);
    }),
  );

const runRaw = <A, E>(url: string, effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
  effect.pipe(Effect.provide(clientLayer(url)));

const sharedActivityLayer = PostgresStorage.activityStoreLayer();
const sharedClientLayer = clientLayer;

describe("Postgres activity processor store", () => {
  it.effect("preserves a live local owner across elapsed time and rejects closed sessions", () =>
    withTemporaryDatabase((location) =>
      Effect.gen(function* () {
        expect(yield* inspect(location)).toBeNull();
        yield* TestClock.setTime(1_000);
        const prepared = work(1, "a");

        const first = yield* runStore(
          location,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;

            const lifetime = yield* Effect.acquireRelease(Scope.make(), (scope) =>
              Scope.close(scope, Exit.void),
            );

            const session = yield* store.acquire(key).pipe(Scope.provide(lifetime));

            yield* session.prepare(prepared);
            yield* TestClock.setTime(12_000);
            const contender = yield* store.acquire(key).pipe(Effect.result);

            expect(Result.isFailure(contender)).toBe(true);
            if (Result.isFailure(contender))
              expect(contender.failure).toEqual(ActivityBusy.make({ key }));
            yield* session.checkActive;
            yield* Scope.close(lifetime, Exit.void);

            const resumed = yield* store.acquire(key);

            expect(resumed.progress.pending).toEqual(prepared);
            expect(yield* session.prepare(work(1, "b")).pipe(Effect.flip)).toEqual(
              ActivityOwnershipLost.make({ key }),
            );
            expect(yield* session.advance(prepared.workId).pipe(Effect.flip)).toEqual(
              ActivityOwnershipLost.make({ key }),
            );
            expect((yield* store.inspect(key))?.pending).toEqual(prepared);
            expect((yield* store.inspect(key))?.throughSequence).toBe(0);

            return resumed;
          }),
        );

        const advanced = yield* runStore(
          location,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;
            const session = yield* store.acquire(key);

            expect(session.progress.pending).toEqual(prepared);
            yield* TestClock.setTime(13_000);
            expect(yield* first.advance(prepared.workId).pipe(Effect.flip)).toEqual(
              ActivityOwnershipLost.make({ key }),
            );

            return yield* session.advance(prepared.workId);
          }),
        );

        expect(advanced.throughSequence).toBe(1);
        expect(advanced.pending).toBeNull();
        expect(yield* inspect(location)).toMatchObject({
          throughSequence: 1,
          pending: null,
          advancedAt: 13_000,
        });
      }),
    ),
  );

  it.effect("pins one exact next output and rejects divergent preparation", () =>
    withTemporaryDatabase((location) =>
      runStore(
        location,
        Effect.gen(function* () {
          const store = yield* ActivityProcessorStore;
          const session = yield* store.acquire(key);
          const wrongSequence = work(2, "1");

          expect(yield* session.prepare(wrongSequence).pipe(Effect.flip)).toEqual(
            ActivityWorkConflict.make({ key, workId: wrongSequence.workId }),
          );
          const wrongKey = work(1, "2", independentKeys[1]);

          expect(yield* session.prepare(wrongKey).pipe(Effect.flip)).toEqual(
            ActivityWorkConflict.make({ key, workId: wrongKey.workId }),
          );
          const pinned = work(1, "b", key, { value: "pinned" });

          expect(yield* session.prepare(pinned)).toEqual(pinned);
          expect(yield* session.prepare(pinned)).toEqual(pinned);
          const divergent = work(1, "c", key, { value: "different" });

          expect(yield* session.prepare(divergent).pipe(Effect.flip)).toEqual(
            ActivityWorkConflict.make({ key, workId: divergent.workId }),
          );
          const wrongWorkId = work(1, "3").workId;

          expect(yield* session.advance(wrongWorkId).pipe(Effect.flip)).toEqual(
            ActivityWorkConflict.make({ key, workId: wrongWorkId }),
          );
        }),
      ),
    ),
  );

  it.effect("shares local activity authority across live storage facets", () =>
    withTemporaryDatabase((location) =>
      Effect.scoped(
        Effect.gen(function* () {
          const firstScope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
            Scope.close(scope, Exit.void),
          );

          const firstContext = yield* Layer.build(sharedActivityLayer).pipe(
            Scope.provide(firstScope),
          );

          const secondContext = yield* Layer.build(sharedActivityLayer);
          const first = Context.get(firstContext, ActivityProcessorStore);
          const second = Context.get(secondContext, ActivityProcessorStore);
          const old = yield* first.acquire(key);
          const independent = yield* second.acquire(independentKeys[1]);

          expect(yield* second.acquire(key).pipe(Effect.flip)).toEqual(ActivityBusy.make({ key }));
          yield* Scope.close(firstScope, Exit.void);
          yield* independent.checkActive;

          const current = yield* second.acquire(key);
          const pinned = work(1, "d");

          yield* current.prepare(pinned);
          expect(yield* old.prepare(work(1, "e")).pipe(Effect.flip)).toEqual(
            ActivityOwnershipLost.make({ key }),
          );
          expect((yield* second.inspect(key))?.pending).toEqual(pinned);
          yield* current.advance(pinned.workId);
          yield* independent.prepare(work(1, "f", independentKeys[1]));
        }),
      ).pipe(Effect.provide(sharedClientLayer(location))),
    ),
  );

  it.effect("recovers prepared output and an advanced cursor after lost acknowledgements", () =>
    withTemporaryDatabase((location) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(1_000);
        const prepared = work(1, "e");

        const prepareFailure = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;
            const session = yield* store.acquire(key);

            return yield* session.prepare(prepared);
          }),
        ).pipe(
          Effect.provide(
            failpointLayer(location, (point) =>
              point === "activity:prepare:after"
                ? Effect.fail(ActivityMutationFailure.make({ point }))
                : Effect.void,
            ),
          ),
          Effect.flip,
        );

        expect(prepareFailure).toEqual(
          ActivityMutationFailure.make({ point: "activity:prepare:after" }),
        );
        expect((yield* inspect(location))?.pending).toEqual(prepared);
        yield* TestClock.setTime(2_000);

        const advanceFailure = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;
            const session = yield* store.acquire(key);

            expect(session.progress.pending).toEqual(prepared);

            return yield* session.advance(prepared.workId);
          }),
        ).pipe(
          Effect.provide(
            failpointLayer(location, (point) =>
              point === "activity:advance:after"
                ? Effect.fail(ActivityMutationFailure.make({ point }))
                : Effect.void,
            ),
          ),
          Effect.flip,
        );

        expect(advanceFailure).toEqual(
          ActivityMutationFailure.make({ point: "activity:advance:after" }),
        );
        expect(yield* inspect(location)).toMatchObject({
          throughSequence: 1,
          pending: null,
          advancedAt: 2_000,
        });
        yield* runStore(
          location,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;
            const session = yield* store.acquire(key);

            expect(session.progress.throughSequence).toBe(1);
            expect(yield* session.advance(prepared.workId).pipe(Effect.flip)).toEqual(
              ActivityWorkConflict.make({ key, workId: prepared.workId }),
            );
            expect((yield* store.inspect(key))?.throughSequence).toBe(1);
          }),
        );
      }),
    ),
  );

  it.effect("rolls back defects, timeout, and interruption inside state transactions", () =>
    withTemporaryDatabase((location) =>
      Effect.gen(function* () {
        const prepared = work(1, "f");

        const defect = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;
            const session = yield* store.acquire(key);

            return yield* session.prepare(prepared);
          }),
        ).pipe(
          Effect.provide(
            failpointLayer(location, (point) =>
              point === "activity:prepare:after-state" ? Effect.die("prepare defect") : Effect.void,
            ),
          ),
          Effect.exit,
        );

        expect(Exit.isFailure(defect) && Cause.hasDies(defect.cause)).toBe(true);
        expect((yield* inspect(location))?.pending ?? null).toBeNull();

        yield* runStore(
          location,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;
            const session = yield* store.acquire(key);

            yield* session.prepare(prepared);
          }),
        );
        const reached = yield* Deferred.make<void>();

        const advancing = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;
            const session = yield* store.acquire(key);

            return yield* session.advance(prepared.workId);
          }),
        ).pipe(
          Effect.provide(
            failpointLayer(location, (point) =>
              point === "activity:advance:after-state"
                ? Deferred.succeed(reached, undefined).pipe(Effect.andThen(Effect.never))
                : Effect.void,
            ),
          ),
          Effect.timeout("1 second"),
          Effect.forkChild,
        );

        yield* Deferred.await(reached);
        yield* TestClock.adjust("1 second");
        expect(Exit.isFailure(yield* Fiber.await(advancing))).toBe(true);
        expect(yield* inspect(location)).toMatchObject({ throughSequence: 0, pending: prepared });

        const interruptedAt = yield* Deferred.make<void>();

        const interrupted = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;
            const session = yield* store.acquire(key);

            return yield* session.advance(prepared.workId);
          }),
        ).pipe(
          Effect.provide(
            failpointLayer(location, (point) =>
              point === "activity:advance:after-state"
                ? Deferred.succeed(interruptedAt, undefined).pipe(Effect.andThen(Effect.never))
                : Effect.void,
            ),
          ),
          Effect.forkChild,
        );

        yield* Deferred.await(interruptedAt);
        yield* Fiber.interrupt(interrupted);
        expect(Exit.isFailure(yield* Fiber.await(interrupted))).toBe(true);
        expect(yield* inspect(location)).toMatchObject({ throughSequence: 0, pending: prepared });
      }),
    ),
  );

  it.effect("keeps thread, processor, and processor-version progress independent", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        yield* runStore(
          url,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;

            for (const [index, activityKey] of independentKeys.entries()) {
              const session = yield* store.acquire(activityKey);
              const prepared = work(1, String(index), activityKey);

              yield* session.prepare(prepared);
              if (index === 0) yield* session.advance(prepared.workId);
            }
          }),
        );
        expect((yield* inspect(url, independentKeys[0]))?.throughSequence).toBe(1);
        for (const activityKey of independentKeys.slice(1)) {
          expect((yield* inspect(url, activityKey))?.throughSequence).toBe(0);
          expect((yield* inspect(url, activityKey))?.pending?.key).toEqual(activityKey);
        }
      }),
    ),
  );

  it.effect("rejects incompatible stored formats", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        yield* runStore(
          url,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;
            const session = yield* store.acquire(key);

            yield* session.prepare(work(1, "a"));
          }),
        );
        yield* runRaw(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            yield* sql.unsafe(
              "UPDATE effect_agent_activity_processor_state_v2 SET format_version = 3",
            );
          }),
        );
        expect(yield* inspect(url).pipe(Effect.flip)).toEqual(
          ActivityStoreError.make({
            operation: "inspect activity progress",
            reason: "incompatible",
          }),
        );
      }),
    ),
  );
});
