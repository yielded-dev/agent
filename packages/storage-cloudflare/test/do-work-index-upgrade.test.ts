import { layer } from "@yielded/agent-storage-cloudflare/do-thread-store";
import { SqlStorageProgress } from "@yielded/agent-storage-sql/sql-storage-progress";
import { RunId, ToolCallId } from "@yielded/agent/identifiers";
import { CanonicalRecord, ToolCallUnknown } from "@yielded/agent/records";
import { AppendRequest, ThreadMaterialization, ThreadStore } from "@yielded/agent/thread-store";
import * as Effect from "effect/Effect";
import { expect, it } from "vite-plus/test";

import { batch, inputRecord } from "./canonical-fixtures.ts";
import { sequence, thread, withThreadStorage } from "./harness.ts";

const unknown = (id: string) =>
  CanonicalRecord.make({
    ...inputRecord(id, "retained input"),
    payload: ToolCallUnknown.make({
      runId: RunId.make("upgrade-run"),
      turn: 1,
      toolCallId: ToolCallId.make(id),
      toolName: "external_change",
      reason: "The external result is not yet confirmed.",
    }),
  });

const snapshot = (storage: DurableObjectStorage) =>
  storage.sql
    .exec("SELECT * FROM effect_agent_canonical_records ORDER BY thread_id, sequence")
    .toArray();

// Regression: https://github.com/yielded-dev/agent/commit/2899b5bfbf3f7959f54eabedf8b05fa48e5d96a5
it("upgrades a released work index without losing unknown operations or rewriting history", () => {
  const name = `work-upgrade-${crypto.randomUUID()}`;
  const threadId = thread(name);

  return withThreadStorage(name, (storage) =>
    Effect.gen(function* () {
      yield* Effect.gen(function* () {
        const store = yield* ThreadStore;

        yield* store.materialize(ThreadMaterialization.make({ threadId }));
        yield* store.append(
          AppendRequest.make({
            threadId,
            expectedTailSequence: sequence(0),
            batch: batch("before-upgrade", [unknown("unconfirmed")]),
          }),
        );
      }).pipe(Effect.provide(layer({ storage })));

      // The released predecessor has this exact extra derived cache; the rest of
      // its authoritative DDL is retained verbatim by the adapter's layout check.
      storage.sql.exec(
        "CREATE TABLE effect_agent_checkpoints ( thread_id TEXT NOT NULL, through_sequence INTEGER NOT NULL, checkpoint_json TEXT NOT NULL, PRIMARY KEY (thread_id, through_sequence), FOREIGN KEY (thread_id) REFERENCES effect_agent_threads(thread_id) ON DELETE RESTRICT )",
      );
      storage.sql.exec("UPDATE effect_agent_schema SET layout_version=28");
      storage.sql.exec("UPDATE effect_agent_meta SET value='28' WHERE key='storage_version'");
      storage.sql.exec("UPDATE effect_agent_threads SET work_version=3");
      const before = snapshot(storage);

      const page = yield* Effect.gen(function* () {
        const store = yield* ThreadStore;

        if (store.work === undefined) return yield* Effect.fail("Work discovery is unavailable");

        return yield* store.work.page({ threadId, limit: 32 });
      }).pipe(Effect.provide(layer({ storage })));

      expect(page.entries).toMatchObject([
        { owner: { _tag: "Operation", toolCallId: "unconfirmed" }, state: "unknown" },
      ]);
      expect(snapshot(storage)).toEqual(before);
    }),
  );
});

// Regression: the same release skips later appends while its old index is unrecognized.
it("continues a long upgrade after cold opens and records only actual page progress", () => {
  const name = `work-upgrade-suffix-${crypto.randomUUID()}`;
  const threadId = thread(name);
  const suffixRecords = 2_305;
  const progress: string[] = [];

  return withThreadStorage(name, (storage) =>
    Effect.gen(function* () {
      yield* Effect.gen(function* () {
        const store = yield* ThreadStore;

        yield* store.materialize(ThreadMaterialization.make({ threadId }));
        yield* store.append(
          AppendRequest.make({
            threadId,
            expectedTailSequence: sequence(0),
            batch: batch("prefix", [unknown("prefix-unknown")]),
          }),
        );
        storage.sql.exec("UPDATE effect_agent_schema SET layout_version=30");
        storage.sql.exec("UPDATE effect_agent_meta SET value='30' WHERE key='storage_version'");
        storage.sql.exec("UPDATE effect_agent_threads SET work_version=3");
        for (let index = 0; index < suffixRecords; index += 256) {
          const records = Array.from({ length: Math.min(256, suffixRecords - index) }, (_, item) =>
            inputRecord(`suffix-${index + item}`, "Unindexed retained history"),
          );

          yield* store.append(
            AppendRequest.make({
              threadId,
              expectedTailSequence: sequence(index + 1),
              batch: batch(`suffix-${index}`, [records[0]!, ...records.slice(1)]),
            }),
          );
        }
      }).pipe(Effect.provide(layer({ storage })));

      const before = snapshot(storage);
      let previousThrough = 1;

      for (let pass = 0; pass < 10; pass++) {
        // Each provide opens/closes the storage owner, retaining only real SQLite state.
        const page = yield* Effect.gen(function* () {
          const store = yield* ThreadStore;

          if (store.work === undefined) return yield* Effect.fail("Work discovery is unavailable");

          return yield* store.work.page({ threadId, limit: 32 }).pipe(Effect.result);
        }).pipe(
          Effect.provide(layer({ storage })),
          Effect.provideService(SqlStorageProgress, {
            committed: (kind) =>
              Effect.sync(() => {
                progress.push(kind);
              }),
          }),
        );

        const row = storage.sql
          .exec<{ work_through_sequence: number; work_state: string }>(
            "SELECT work_through_sequence, work_state FROM effect_agent_threads",
          )
          .one();

        expect(row.work_through_sequence).toBeGreaterThan(previousThrough);
        expect(row.work_through_sequence - previousThrough).toBeLessThanOrEqual(256);
        expect(progress).toHaveLength(pass + 2);
        previousThrough = row.work_through_sequence;
        if (pass < 9) {
          expect(page).toMatchObject({
            _tag: "Failure",
            failure: { _tag: "WorkDiscoveryUnavailable", reason: "incomplete-rebuild" },
          });
        } else {
          expect(page).toMatchObject({
            _tag: "Success",
            success: {
              entries: [
                { owner: { _tag: "Operation", toolCallId: "prefix-unknown" }, state: "unknown" },
              ],
            },
          });
          expect(row.work_state).toBe("ready");
        }
      }

      expect(progress.every((kind) => kind === "work-index")).toBe(true);
      expect(snapshot(storage)).toEqual(before);
      yield* Effect.gen(function* () {
        const store = yield* ThreadStore;

        if (store.work === undefined) return yield* Effect.fail("Work discovery is unavailable");
        yield* store.work.page({ threadId, limit: 32 });
      }).pipe(
        Effect.provide(layer({ storage })),
        Effect.provideService(SqlStorageProgress, {
          committed: (kind) =>
            Effect.sync(() => {
              progress.push(kind);
            }),
        }),
      );
      expect(progress).toHaveLength(11);
    }),
  );
}, 60_000);
