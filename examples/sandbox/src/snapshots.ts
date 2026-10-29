import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto";
import { ThreadMutationGate } from "@yielded/agent-platform-cloudflare/alarm";
import { ThreadObjectIdentity } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import { ThreadStore, ThreadTailRequest } from "@yielded/agent/thread-store";
import { ThreadWorkDiscovery, type ThreadWorkRequest } from "@yielded/agent/thread-work";
import { WakeScheduler } from "@yielded/agent/wake-scheduler";
import { CheckpointInput, SandboxCheckpoint } from "@yielded/sandbox/cloudflare/NativeSandbox";
import { reportSanitizedCause } from "@yielded/sandbox/observability/RuntimeDiagnostics";
import { Cause, Clock, Context, Effect, Fiber, Layer, Schema } from "effect";
import { DurableObjectState } from "effect-cf";
import * as SecureRandom from "effect/crypto/SecureRandom";

import type { SavedFiles } from "./contract.ts";
import { SHELL_ACTIVE_KEY, SHELL_COMMAND_PREFIX, ShellCommand } from "./contract.ts";
import { Workspace } from "./workspace.ts";

const randomUUID = SecureRandom.SecureRandom.use((crypto) => crypto.uuidV4()).pipe(
  Effect.provide(BrowserCrypto.layer),
  Effect.orDie,
);

export const snapshotLane = "sandbox-files";
const key = "workspace-snapshot";

const Backup = Schema.Struct({
  id: Schema.String.check(Schema.isUUID()),
  dir: Schema.Literal("/workspace"),
  size: Schema.Natural,
  sha256: Schema.NonEmptyString,
  format: Schema.Literal("tar+zstd/1"),
});

const Snapshot = Schema.Struct({
  backup: Backup,
  checkpoint: Schema.optionalKey(SandboxCheckpoint),
  source: Schema.String,
  savedAt: Schema.Natural,
});

const Operation = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("saving"),
    id: Schema.String,
    source: Schema.String,
    instanceId: Schema.String,
    sequence: Schema.optionalKey(CheckpointInput.fields.sequence),
    backup: Schema.optionalKey(Backup),
  }),
  Schema.Struct({
    kind: Schema.Literal("restoring"),
    id: Schema.String,
    instanceId: Schema.NullOr(Schema.String),
    mode: Schema.optionalKey(Schema.Literals(["checkpoint", "backup"])),
  }),
]);

const SnapshotState = Schema.Struct({
  snapshot: Schema.NullOr(Snapshot),
  previous: Schema.NullOr(Backup),
  operation: Schema.NullOr(Operation),
  failedSource: Schema.NullOr(Schema.String),
  restoreFailed: Schema.Boolean,
  sequence: Schema.optionalKey(CheckpointInput.fields.sequence),
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

/** Admission can queue during a save; execution reconciles it before touching the workspace. */
export const snapshotRestoring = readSnapshotState.pipe(
  Effect.map((state) => state.operation?.kind === "restoring"),
);

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

  const idle = Effect.fnUntraced(function* (allowUnresolved = false) {
    const active = yield* storage.kv.get(SHELL_ACTIVE_KEY);

    if (active !== undefined) {
      const id = yield* Schema.decodeUnknownEffect(Schema.String)(active);

      const command = yield* Schema.decodeUnknownEffect(ShellCommand)(
        yield* storage.kv.get(`${SHELL_COMMAND_PREFIX}${id}`),
      );

      if (!allowUnresolved || command.status !== "unknown") return false;
    }
    let cursor: ThreadWorkRequest["cursor"];

    do {
      const page = yield* work.page({
        threadId,
        limit: 64,
        ...(cursor === undefined ? {} : { cursor }),
      });

      // Nonterminal submissions retain Admission owners. After settlement,
      // unresolved Operations can still be indexed as ready; restore preserves them.
      if (page.entries.some(({ owner }) => !allowUnresolved || owner._tag !== "Operation"))
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

  const finishSave = Effect.gen(function* () {
    const state = yield* read;
    const operation = state.operation;

    if (operation?.kind !== "saving") return;
    const backup = operation.backup;
    const sandbox = yield* workspace.bind(operation.instanceId);

    // Only the native capture has bounded idempotency. A retry uses its original
    // instance and sequence; the already retained R2 upload is never repeated.
    const captured = yield* Effect.gen(function* () {
      if (backup === undefined || operation.sequence === undefined) return;

      return yield* sandbox.checkpoint({
        instanceId: operation.instanceId,
        sequence: operation.sequence,
      });
    }).pipe(
      Effect.tapError((error) =>
        reportSanitizedCause(Cause.fail(error), { operation: "WorkspaceSnapshots.checkpoint" }),
      ),
      Effect.result,
    );

    if (backup === undefined || captured._tag === "Failure") {
      // A lost reply can leave native IO running after this Thread is evicted.
      // Exact ensure waits for the native admission gate without replaying an
      // uncertain upload or allowing the next model call through.
      const settled = yield* sandbox.ensure().pipe(Effect.result);

      if (settled._tag === "Failure") {
        const description = yield* sandbox.describe;

        if (description.running && description.current?.instanceId === operation.instanceId)
          return yield* settled.failure;
      }
    }
    const savedAt = yield* Clock.currentTimeMillis;

    yield* mutations.withTransaction(
      Effect.gen(function* () {
        const latest = yield* read;

        if (latest.operation?.id !== operation.id) return;
        // The SDK chooses its upload ID. Without a receipt, keep the last save
        // and require an explicit retry, including for pre-checkpoint records.
        if (backup === undefined)
          return yield* put({ ...latest, operation: null, failedSource: operation.source });
        yield* put({
          ...latest,
          snapshot: {
            backup,
            ...(captured._tag === "Success" && captured.success !== undefined
              ? { checkpoint: captured.success }
              : {}),
            source: operation.source,
            savedAt,
          },
          previous: latest.snapshot?.backup ?? null,
          operation: null,
          failedSource: null,
          restoreFailed: false,
        });
      }),
    );
    yield* notify;
  });

  const beforeExecution = Effect.gen(function* () {
    const state = yield* read;

    if (state.operation?.kind === "restoring")
      return yield* SnapshotRejected.make({
        message: "Wait for workspace restoration to finish.",
      });
    yield* finishSave;
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

        const sequence = yield* Schema.decodeEffect(CheckpointInput.fields.sequence)(
          (state.sequence ?? 0) + 1,
        );

        const operation = {
          kind: "saving" as const,
          id: yield* randomUUID,
          source,
          instanceId,
          sequence,
        };

        yield* put({ ...state, operation, sequence });

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
      yield* finishSave;

      return yield* result.failure;
    }
    const backup = yield* Schema.decodeUnknownEffect(Backup)(result.success);

    const retained = yield* mutations.withTransaction(
      Effect.gen(function* () {
        const state = yield* read;

        if (state.operation?.kind !== "saving" || state.operation.id !== operation.id) return false;
        yield* put({
          ...state,
          operation: { ...state.operation, backup },
        });

        return true;
      }),
    );

    if (!retained) {
      yield* sandbox.deleteDirectoryBackup(backup);

      return;
    }
    yield* finishSave;
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
            operation: {
              kind: "restoring",
              id: yield* randomUUID,
              instanceId: null,
              mode: state.snapshot.checkpoint === undefined ? "backup" : "checkpoint",
            },
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
    const snapshot = state.snapshot;
    const operationId = state.operation.id;
    let instanceId = state.operation.instanceId;
    let mode = state.operation.mode ?? "backup";

    if (mode === "checkpoint" && snapshot.checkpoint === undefined)
      return yield* SnapshotRejected.make({
        message: "The saved native checkpoint is unavailable.",
      });

    if (instanceId === null) {
      const initial = yield* workspace.bind();

      const restored = yield* (
        mode === "checkpoint" && snapshot.checkpoint !== undefined
          ? initial.restore(snapshot.checkpoint)
          : initial.ensure()
      ).pipe(Effect.result);

      if (restored._tag === "Failure") {
        if (mode !== "checkpoint" || restored.failure.code !== "CHECKPOINT_UNAVAILABLE")
          return yield* restored.failure;
        // Only an explicit incompatibility permits the portable restore. Persist
        // the choice before boot so redelivery cannot switch back to native.
        mode = "backup";
        yield* mutations.withTransaction(
          Effect.gen(function* () {
            state = yield* read;
            if (state.operation?.kind !== "restoring" || state.operation.id !== operationId)
              return yield* SnapshotRejected.make({ message: "The restore request changed." });
            yield* put({ ...state, operation: { ...state.operation, mode } });
          }),
        );
      }
      const current = restored._tag === "Success" ? restored.success : yield* initial.ensure();

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
    if (mode === "backup") yield* sandbox.restoreDirectory(snapshot.backup);
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
    yield* Effect.gen(function* () {
      if (state.operation?.kind === "saving") return yield* finishSave;
      yield* cleanup;
      yield* save;
    }).pipe(
      // Incoming work yields the after-native phase. Join this finite save
      // before releasing the maintenance pass; admission can still accept it.
      // The child stays interruptible so its timeout remains effective.
      Effect.timeout("40 seconds"),
      Effect.forkChild({ uninterruptible: false }),
      Effect.flatMap(Fiber.join),
      Effect.uninterruptible,
    );
  });

  return { status, requestRestore, requestSave, run, beforeExecution };
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
