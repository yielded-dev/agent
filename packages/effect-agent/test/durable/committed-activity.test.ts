import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { type PreparedActivity } from "@yielded/agent/activity-store";
import {
  ActivityProgress,
  ActivityOwnershipLost,
  ActivityMutationFailure,
  ActivityProcessorKey,
  ActivityProcessorStore,
} from "@yielded/agent/activity-store";
import { ActivityPassLimits, processCommittedActivity } from "@yielded/agent/committed-activity";
import { ThreadId } from "@yielded/agent/identifiers";
import {
  BatchId,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  DeploymentId,
  ObservationOffset,
  RecordEnvelope,
  RecordId,
  UserInputRecorded,
} from "@yielded/agent/records";
import { ThreadStore, ThreadTail } from "@yielded/agent/thread-store";
import {
  Cause,
  Clock,
  DateTime,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";

const threadId = Schema.decodeSync(ThreadId)("dan-chad");
const sequence = Schema.decodeSync(CanonicalSequence);

const key = ActivityProcessorKey.make({
  processorId: "discussion",
  processorVersion: "1",
  threadId,
});

const limits = ActivityPassLimits.make({
  maxRecords: 2,
  pageSize: 1,
  timeoutMillis: 1_000,
});

const record = (number: number, text = `statement ${number}`, offset = `offset:${number}`) =>
  CanonicalRecordEnvelope.make({
    threadId,
    batchId: Schema.decodeSync(BatchId)(`batch:${number}`),
    sequence: sequence(number),
    offset: Schema.decodeSync(ObservationOffset)(offset),
    record: RecordEnvelope.make({
      recordId: Schema.decodeSync(RecordId)(`record:${number}`),
      family: "thread",
      schemaVersion: 1,
      createdAt: DateTime.makeUnsafe(10),
      deploymentId: Schema.decodeSync(DeploymentId)("test"),
      payload: UserInputRecorded.make({ kind: "user", input: text }),
    }),
  });

class ExtractionUnavailable extends Schema.TaggedError<ExtractionUnavailable>()(
  "ExtractionUnavailable",
  {},
) {}

/** A workflow probe, not a storage implementation. SQLite tests own transaction/fencing semantics. */
const probe = (initial: ReadonlyArray<CanonicalRecordEnvelope> = [record(1)]) => {
  const state = {
    records: [...initial],
    through: sequence(0),
    pending: null as PreparedActivity | null,
    advancedAt: null as number | null,
    prepared: [] as Array<PreparedActivity>,
    released: 0,
    pages: [] as Array<number>,
    prepareLostAck: false,
    advanceLostAck: false,
  };

  const progress = () =>
    ActivityProgress.make({
      version: 2,
      key,
      throughSequence: state.through,
      pending: state.pending,
      advancedAt: state.advancedAt,
    });

  const store = ActivityProcessorStore.of({
    inspect: () => Effect.die("The processor must use its session, not a separate progress read"),
    acquire: () =>
      Effect.gen(function* () {
        let active = true;

        const checkActive = Effect.suspend(() =>
          active ? Effect.void : Effect.fail(ActivityOwnershipLost.make({ key })),
        );

        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            active = false;
            state.released += 1;
          }),
        );

        return {
          progress: progress(),
          checkActive,
          prepare: (work) =>
            checkActive.pipe(
              Effect.andThen(
                Effect.gen(function* () {
                  state.pending = work;
                  state.prepared.push(work);
                  if (state.prepareLostAck) {
                    state.prepareLostAck = false;

                    return yield* ActivityMutationFailure.make({ point: "activity:prepare:after" });
                  }

                  return work;
                }),
              ),
            ),
          advance: () =>
            checkActive.pipe(
              Effect.andThen(
                Effect.gen(function* () {
                  state.through = sequence(state.through + 1);
                  state.pending = null;
                  state.advancedAt = yield* Clock.currentTimeMillis;
                  if (state.advanceLostAck) {
                    state.advanceLostAck = false;

                    return yield* ActivityMutationFailure.make({ point: "activity:advance:after" });
                  }

                  return progress();
                }),
              ),
            ),
        };
      }),
  });

  const threads = ThreadStore.of({
    readExecution: () => Effect.die("Ingestion does not read active execution state"),
    readPrompt: () => Stream.die("Ingestion requires full canonical evidence"),
    readIdentity: () => Effect.die("Ingestion must not read worker identity"),
    materialize: () => Effect.die("Ingestion must not materialize Threads"),
    append: () => Effect.die("Ingestion must not append canonical records"),
    export: () => Effect.die("Ingestion must use bounded reads"),
    observe: () => Stream.die("Ingestion must not start an unbounded observation"),
    inspectTail: () =>
      Effect.succeed(
        ThreadTail.make({
          threadId,
          tailSequence: sequence(state.records.length),
        }),
      ),
    read: (request) => {
      if ("selection" in request)
        return Stream.die("History fixture does not implement selected reads");
      state.pages.push(request.limit);

      return Stream.fromIterable(
        state.records.slice(
          request.afterSequence ?? 0,
          (request.afterSequence ?? 0) + request.limit,
        ),
      );
    },
  });

  const layer = Layer.mergeAll(
    Layer.succeed(ActivityProcessorStore, store),
    Layer.succeed(ThreadStore, threads),
    NodeCrypto.layer,
  );

  return { state, store, threads, layer };
};

describe("finite committed activity processing", () => {
  it.effect(
    "reuses pinned output after lost prepare acknowledgement even when the observation cursor changes",
    () => {
      const p = probe();

      p.state.prepareLostAck = true;
      let extractions = 0;
      const applied: Array<PreparedActivity> = [];

      const options = {
        key,
        limits,
        extract: () =>
          Effect.sync(() => {
            extractions += 1;

            return `extraction ${extractions}`;
          }),
        apply: (work: PreparedActivity) =>
          Effect.sync(() => {
            applied.push(work);
          }),
      };

      return Effect.gen(function* () {
        expect(yield* processCommittedActivity(options).pipe(Effect.flip)).toEqual(
          ActivityMutationFailure.make({ point: "activity:prepare:after" }),
        );
        expect(p.state.through).toBe(0);
        p.state.records = [record(1, "statement 1", "restored-cursor")];
        yield* processCommittedActivity(options);
        expect(extractions).toBe(1);
        expect(applied).toEqual(p.state.prepared);
        expect(applied[0].output).toBe("extraction 1");
      }).pipe(Effect.provide(p.layer));
    },
  );

  it.effect(
    "replays applied work before progress and resumes beyond an acknowledged-lost advance",
    () => {
      const p = probe();
      let extractions = 0;
      let attempts = 0;
      const receipts = new Map<string, PreparedActivity>();

      const options = {
        key,
        limits,
        extract: () =>
          Effect.sync(() => {
            extractions += 1;

            return "pinned";
          }),
        apply: (work: PreparedActivity) =>
          Effect.gen(function* () {
            attempts += 1;
            receipts.set(work.workId, work);
            if (attempts === 1) return yield* new ExtractionUnavailable();
          }),
      };

      return Effect.gen(function* () {
        expect(yield* processCommittedActivity(options).pipe(Effect.flip)).toEqual(
          new ExtractionUnavailable(),
        );
        expect(p.state.through).toBe(0);
        p.state.advanceLostAck = true;
        expect(yield* processCommittedActivity(options).pipe(Effect.flip)).toEqual(
          ActivityMutationFailure.make({ point: "activity:advance:after" }),
        );
        const resumed = yield* processCommittedActivity(options);

        expect(resumed.processed).toBe(0);
        expect(resumed.throughSequence).toBe(1);
        expect(extractions).toBe(1);
        expect(attempts).toBe(2);
        expect(receipts.size).toBe(1);
      }).pipe(Effect.provide(p.layer));
    },
  );

  it.effect("rejects gaps and truncated tails before applying pinned output", () =>
    Effect.gen(function* () {
      const gap = probe([record(2)]);
      let applied = 0;

      const options = {
        key,
        limits,
        extract: () => Effect.succeed(null),
        apply: () =>
          Effect.sync(() => {
            applied += 1;
          }),
      };

      const gapError = yield* processCommittedActivity(options).pipe(
        Effect.provide(gap.layer),
        Effect.flip,
      );

      expect(gapError).toMatchObject({
        _tag: "ActivityProcessingError",
        reason: "noncontiguous",
      });
      const changed = probe();

      changed.state.prepareLostAck = true;
      yield* processCommittedActivity(options).pipe(Effect.provide(changed.layer), Effect.exit);
      const pending = changed.state.pending;

      changed.state.records = [];
      expect(
        yield* processCommittedActivity(options).pipe(Effect.provide(changed.layer), Effect.flip),
      ).toMatchObject({ _tag: "ActivityProcessingError", reason: "noncontiguous" });
      expect(changed.state.pending).toEqual(pending);
      expect(changed.state.through).toBe(0);
      expect(changed.state.released).toBe(2);
      expect(changed.state.pages).toEqual([1]);
      expect(applied).toBe(0);
      expect(changed.state.through).toBe(0);
    }),
  );

  it.effect("finalizes scoped callbacks on timeout and interruption without advancing", () =>
    Effect.gen(function* () {
      for (const boundary of ["extract", "apply"] as const) {
        for (const mode of ["timeout", "interrupt"] as const) {
          const p = probe();
          const entered = yield* Deferred.make<void>();
          let released = 0;

          const waiting = Effect.gen(function* () {
            yield* Effect.acquireRelease(Deferred.succeed(entered, undefined), () =>
              Effect.sync(() => {
                released += 1;
              }),
            );

            return yield* Effect.never;
          });

          const fiber = yield* processCommittedActivity({
            key,
            limits,
            extract: () => (boundary === "extract" ? waiting : Effect.succeed(null)),
            apply: () =>
              boundary === "apply" ? waiting : Effect.die("No interrupted extraction may apply"),
          }).pipe(Effect.provide(p.layer), Effect.forkChild);

          yield* Deferred.await(entered);
          if (mode === "timeout") yield* TestClock.adjust(1_000);
          else yield* Fiber.interrupt(fiber);
          const exit = yield* Fiber.await(fiber);

          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isFailure(exit)) {
            if (mode === "interrupt") expect(Cause.hasInterrupts(exit.cause)).toBe(true);
            else
              expect(Cause.squash(exit.cause)).toMatchObject({
                _tag: "ActivityProcessingError",
                reason: "timeout",
              });
          }
          expect(released).toBe(1);
          expect(p.state.released).toBe(1);
          expect(p.state.through).toBe(0);
          expect(p.state.pending !== null).toBe(boundary === "apply");
        }
      }
    }),
  );
});
