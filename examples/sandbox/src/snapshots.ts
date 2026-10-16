import { ThreadMutationGate } from "@yielded/agent-platform-cloudflare/alarm";
import { ThreadObjectIdentity } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import { ThreadStore, ThreadTailRequest } from "@yielded/agent/thread-store";
import { ThreadWorkDiscovery, type ThreadWorkRequest } from "@yielded/agent/thread-work";
import { WakeScheduler } from "@yielded/agent/wake-scheduler";
import { Clock, Context, Effect, Layer, Schema } from "effect";
import { DurableObjectState } from "effect-cf";

import type { SavedFiles } from "./contract.ts";
import { SHELL_ACTIVE_KEY, SHELL_COMMAND_PREFIX, ShellCommand } from "./contract.ts";
import { Workspace } from "./workspace.ts";

export const snapshotLane = "sandbox-files";
const key = "workspace-snapshot";

const Backup = Schema.Struct({
  id: Schema.String.check(Schema.isUUID()),
  dir: Schema.Literal("/workspace"),
  size: Schema.Natural,
  sha256: Schema.NonEmptyString,
  format: Schema.Literal("tar+zstd/1"),
});

const Snapshot = Schema.Struct({ backup: Backup, source: Schema.String, savedAt: Schema.Natural });

const Operation = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("saving"),
    id: Schema.String,
    source: Schema.String,
    instanceId: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("restoring"),
    id: Schema.String,
    instanceId: Schema.NullOr(Schema.String),
  }),
]);

const SnapshotState = Schema.Struct({
  snapshot: Schema.NullOr(Snapshot),
  previous: Schema.NullOr(Backup),
  operation: Schema.NullOr(Operation),
  failedSource: Schema.NullOr(Schema.String),
  restoreFailed: Schema.Boolean,
});

type SnapshotState = typeof SnapshotState.Type;

const empty: SnapshotState = {
  snapshot: null,
  previous: null,
  operation: null,
  failedSource: null,
  restoreFailed: false,
};

const readSnapshotState = Effect.gen(function* () {
  const { storage } = yield* DurableObjectState.DurableObjectState;
  const saved = yield* storage.kv.get(key);

  return saved === undefined ? empty : yield* Schema.decodeUnknownEffect(SnapshotState)(saved);
});

/** Read inside the same source transaction as fresh admission. Replays keep their receipts. */
export const snapshotBusy = readSnapshotState.pipe(Effect.map((state) => state.operation !== null));

const make = Effect.gen(function* () {
  const durableState = yield* DurableObjectState.DurableObjectState;
  const { storage } = durableState;

  const read = readSnapshotState.pipe(
    Effect.provideService(DurableObjectState.DurableObjectState, durableState),
  );

  const { threadId } = yield* ThreadObjectIdentity;
  const workspace = yield* Workspace;
  const mutations = yield* ThreadMutationGate;
  const work = yield* ThreadWorkDiscovery;
  const store = yield* ThreadStore;
  const wake = yield* WakeScheduler;

  const put = (state: SnapshotState) => storage.kv.put(key, state);
  const notify = wake.notify(threadId, "progress");

  const status = read.pipe(
    Effect.map((state): typeof SavedFiles.Type => ({
      savedAt: state.snapshot?.savedAt ?? null,
      phase: state.operation?.kind ?? "idle",
      saveFailed: state.failedSource !== null,
      restoreFailed: state.restoreFailed,
    })),
  );

  const idle = Effect.fnUntraced(function* (allowUnknown = false) {
    const active = yield* storage.kv.get(SHELL_ACTIVE_KEY);

    if (active !== undefined) {
      const id = yield* Schema.decodeUnknownEffect(Schema.String)(active);

      const command = yield* Schema.decodeUnknownEffect(ShellCommand)(
        yield* storage.kv.get(`${SHELL_COMMAND_PREFIX}${id}`),
      );

      if (!allowUnknown || command.status !== "unknown") return false;
    }
    let cursor: ThreadWorkRequest["cursor"];

    do {
      const page = yield* work.page({
        threadId,
        limit: 64,
        ...(cursor === undefined ? {} : { cursor }),
      });

      if (
        page.entries.some(
          ({ owner, state, wait }) =>
            !(
              allowUnknown &&
              owner._tag === "Operation" &&
              (state === "unknown" || wait === "effect-resolution")
            ),
        )
      )
        return false;
      cursor = page.cursor;
    } while (cursor !== undefined);

    return true;
  });

  const cleanup = Effect.gen(function* () {
    const state = yield* read;

    if (state.previous === null) return;
    const sandbox = yield* workspace.bind();

    yield* sandbox.deleteDirectoryBackup(state.previous);
    yield* mutations.withTransaction(
      Effect.gen(function* () {
        const latest = yield* read;

        if (latest.previous?.id === state.previous?.id) yield* put({ ...latest, previous: null });
      }),
    );
  });

  const save = Effect.gen(function* () {
    const inspection = yield* workspace.inspect;

    if (inspection.sandbox === null || inspection.expired) return;

    const operation = yield* mutations.withTransaction(
      Effect.gen(function* () {
        const state = yield* read;

        if (state.operation !== null || !(yield* idle())) return null;
        const { tailSequence } = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));
        const commands = yield* storage.kv.list({ prefix: SHELL_COMMAND_PREFIX, limit: 100 });
        const source = `${tailSequence}:${commands.length}`;

        if (state.snapshot?.source === source || state.failedSource === source) return null;
        const instanceId = yield* workspace.instanceId;

        if (instanceId === null) return null;
        const operation = { kind: "saving" as const, id: crypto.randomUUID(), source, instanceId };

        yield* put({ ...state, operation });

        return operation;
      }),
    );

    if (operation === null) return;
    yield* notify;
    const sandbox = yield* workspace.bind(operation.instanceId);

    const options = {
      dir: "/workspace",
      exclude: ["/.sandbox-agent/native/**", "/.sandbox-agent/processes/**"],
    };

    const result = yield* sandbox.backupDirectory(options).pipe(Effect.result);

    if (result._tag === "Failure") {
      yield* mutations.withTransaction(
        Effect.gen(function* () {
          const state = yield* read;

          if (state.operation?.id === operation.id)
            yield* put({ ...state, operation: null, failedSource: operation.source });
        }),
      );
      yield* notify;

      return yield* result.failure;
    }
    const backup = yield* Schema.decodeUnknownEffect(Backup)(result.success);
    const savedAt = yield* Clock.currentTimeMillis;

    const retained = yield* mutations.withTransaction(
      Effect.gen(function* () {
        const state = yield* read;

        if (state.operation?.id !== operation.id) return false;
        yield* put({
          snapshot: { backup, source: operation.source, savedAt },
          previous: state.snapshot?.backup ?? null,
          operation: null,
          failedSource: null,
          restoreFailed: false,
        });

        return true;
      }),
    );

    if (!retained) yield* sandbox.deleteDirectoryBackup(backup);
    yield* notify;
    yield* cleanup;
  });

  const requestRestore = mutations.withMutation(
    Effect.gen(function* () {
      const expiredInstance = yield* workspace.instanceId;
      const inspection = yield* workspace.inspect;

      if (!inspection.expired) return;
      yield* mutations.withTransaction(
        Effect.gen(function* () {
          const state = yield* read;

          // An inspection of the old container must not replace a concurrently restored one.
          if ((yield* workspace.instanceId) !== expiredInstance) return;
          if (state.operation?.kind === "restoring") {
            if (state.restoreFailed) {
              yield* put({ ...state, restoreFailed: false });
              yield* mutations.recordProgress([snapshotLane]);
            }

            return;
          }
          if (state.operation !== null || !(yield* idle(true)))
            return yield* SnapshotRejected.make({
              message: "Wait for the current workspace operation to finish.",
            });
          if (state.snapshot === null)
            return yield* SnapshotRejected.make({
              message: "This workspace has no saved files to restore.",
            });
          yield* put({
            ...state,
            restoreFailed: false,
            operation: { kind: "restoring", id: crypto.randomUUID(), instanceId: null },
          });
          yield* mutations.recordProgress([snapshotLane]);
        }),
      );
      yield* notify;
    }),
    { invalidatesRecovery: false, lanes: [snapshotLane] },
  );

  const requestSave = mutations.withMutation(
    mutations.withTransaction(
      Effect.gen(function* () {
        const state = yield* read;

        if (state.operation !== null || !(yield* idle()))
          return yield* SnapshotRejected.make({
            message: "Wait for the current workspace operation to finish.",
          });
        // A new owner request may retry a failed capture; alarm redelivery may not.
        if (state.failedSource !== null) yield* put({ ...state, failedSource: null });
        yield* mutations.recordProgress([snapshotLane]);
      }),
    ),
    { invalidatesRecovery: false, lanes: [snapshotLane] },
  );

  const restore = Effect.gen(function* () {
    let state = yield* read;

    if (state.operation?.kind !== "restoring" || state.snapshot === null) return;
    const operationId = state.operation.id;
    let instanceId = state.operation.instanceId;

    if (instanceId === null) {
      const initial = yield* workspace.bind();
      const current = yield* initial.ensure();

      instanceId = current.instanceId;
      yield* mutations.withTransaction(
        Effect.gen(function* () {
          state = yield* read;
          if (state.operation?.kind !== "restoring" || state.operation.id !== operationId)
            return yield* SnapshotRejected.make({ message: "The restore request changed." });
          yield* put({ ...state, operation: { ...state.operation, instanceId } });
        }),
      );
    }
    const sandbox = yield* workspace.bind(instanceId);
    const description = yield* sandbox.describe;

    if (!description.running || description.current?.instanceId !== instanceId) {
      yield* mutations.withTransaction(
        Effect.gen(function* () {
          const latest = yield* read;

          if (latest.operation?.kind === "restoring" && latest.operation.id === operationId)
            yield* put({ ...latest, operation: { ...latest.operation, instanceId: null } });
        }),
      );

      return yield* SnapshotRejected.make({
        message: "The replacement container stopped before restoration finished.",
      });
    }
    yield* sandbox.restoreDirectory(state.snapshot.backup);
    yield* sandbox.writeFile(workspace.identity.path, workspace.identity.id);
    yield* mutations.withTransaction(
      Effect.gen(function* () {
        const latest = yield* read;

        if (latest.operation?.id !== operationId)
          return yield* SnapshotRejected.make({ message: "The restore request changed." });
        yield* workspace.retain(instanceId);
        yield* put({ ...latest, operation: null, restoreFailed: false });
      }),
    );
    yield* notify;
  });

  const run = Effect.gen(function* () {
    const state = yield* read;

    if (state.operation?.kind === "restoring")
      return yield* restore.pipe(
        Effect.tapError(() =>
          mutations
            .withTransaction(
              Effect.gen(function* () {
                const latest = yield* read;

                if (latest.operation?.id === state.operation?.id)
                  yield* put({ ...latest, restoreFailed: true });
              }),
            )
            .pipe(Effect.andThen(notify)),
        ),
      );
    if (state.operation?.kind === "saving") {
      // The SDK chooses its upload ID. After interruption keep the last committed
      // backup; repeating this capture could create another untracked object.
      yield* mutations.withTransaction(
        put({ ...state, operation: null, failedSource: state.operation.source }),
      );
      yield* notify;

      return;
    }
    yield* cleanup;
    yield* save;
  });

  return { status, requestRestore, requestSave, run };
});

export class SnapshotRejected extends Schema.TaggedError<SnapshotRejected>()("SnapshotRejected", {
  message: Schema.String,
}) {}

export class WorkspaceSnapshots extends Context.Service<
  WorkspaceSnapshots,
  Effect.Success<typeof make>
>()("sandbox-example/WorkspaceSnapshots") {
  static readonly layer = Layer.effect(WorkspaceSnapshots, make);
}
