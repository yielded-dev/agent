import { ThreadMutationGate } from "@yielded/agent-platform-cloudflare/alarm";
import { ThreadObjectIdentity } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import { SandboxOperationError } from "@yielded/sandbox/cloudflare";
import { Cause, Clock, Context, Effect, Layer, Schema, Semaphore } from "effect";
import { DurableObjectState } from "effect-cf";

import { PREVIEW_LIFETIME_MS, type WorkspacePreviewGrant } from "./contract.ts";
import {
  STATIC_PREVIEW_HEALTH_PATH,
  STATIC_PREVIEW_PORT,
  STATIC_PREVIEW_SERVER,
} from "./preview-server.ts";
import { snapshotBusy } from "./snapshots.ts";
import { Workspace } from "./workspace.ts";

export class PreviewRejected extends Schema.TaggedError<PreviewRejected>()("PreviewRejected", {
  message: Schema.String,
}) {}

const key = "workspace-preview";

const Receipt = Schema.Struct({
  version: Schema.Literal(1),
  generation: Schema.Natural.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
  instanceId: Schema.NonEmptyString,
  materializationId: Schema.NonEmptyString,
  deadlineAt: Schema.Natural,
  processId: Schema.NullOr(Schema.NonEmptyString),
});

type Receipt = typeof Receipt.Type;

const make = Effect.gen(function* () {
  const workspace = yield* Workspace;
  const state = yield* DurableObjectState.DurableObjectState;
  const { threadId } = yield* ThreadObjectIdentity;
  const mutations = yield* ThreadMutationGate;
  const serial = yield* Semaphore.make(1);

  const filesBusy = snapshotBusy.pipe(
    Effect.provideService(DurableObjectState.DurableObjectState, state),
  );

  const read = state.storage.kv
    .get(key)
    .pipe(
      Effect.flatMap((value) =>
        value === undefined ? Effect.succeed(null) : Schema.decodeUnknownEffect(Receipt)(value),
      ),
    );

  const available = Effect.gen(function* () {
    if (yield* filesBusy)
      return yield* PreviewRejected.make({ message: "Wait for saving or restoration to finish." });
    const instanceId = yield* workspace.instanceId;

    if (instanceId === null || workspace.identity.id !== threadId)
      return yield* PreviewRejected.make({
        message: "Open an existing workspace before previewing files.",
      });

    return instanceId;
  });

  const check = Effect.fnUntraced(function* (receipt: Receipt) {
    const instanceId = yield* available;
    const current = yield* read;

    if (
      instanceId !== receipt.instanceId ||
      current?.generation !== receipt.generation ||
      current.instanceId !== receipt.instanceId ||
      current.materializationId !== threadId ||
      current.materializationId !== receipt.materializationId ||
      current.processId !== receipt.processId ||
      current.deadlineAt !== receipt.deadlineAt ||
      receipt.deadlineAt <= (yield* Clock.currentTimeMillis)
    )
      return yield* PreviewRejected.make({
        message: "The preview expired or its workspace changed. Open it again.",
      });
  });

  const open = Effect.gen(function* () {
    const instanceId = yield* mutations.withTransaction(available);
    const sandbox = yield* workspace.bind(instanceId);
    const description = yield* sandbox.describe;

    if (!description.running || description.current?.instanceId !== instanceId)
      return yield* PreviewRejected.make({
        message: "Restore the expired workspace before previewing files.",
      });
    let receipt = yield* read;

    const observed =
      receipt?.instanceId === instanceId && receipt.processId !== null
        ? yield* sandbox.observeProcess(receipt.processId, { maxBytes: 0 })
        : null;

    if (observed?.state === "lost")
      return yield* PreviewRejected.make({ message: "The preview process outcome is unknown." });
    if (
      receipt === null ||
      receipt.instanceId !== instanceId ||
      receipt.deadlineAt <= (yield* Clock.currentTimeMillis) ||
      observed?.state === "exited"
    ) {
      if (observed?.state === "running") {
        yield* sandbox.signalProcess(observed.id, 9);

        const stopped = yield* sandbox.observeProcess(observed.id, {
          maxBytes: 0,
          waitMillis: 10_000,
        });

        if (stopped?.state !== "exited")
          return yield* PreviewRejected.make({
            message: "The previous preview process has not stopped.",
          });
      }
      if (observed !== null) yield* sandbox.acknowledgeProcess(observed.id);
      const previous = receipt;

      receipt = yield* mutations.withTransaction(
        Effect.gen(function* () {
          if (
            (yield* available) !== instanceId ||
            (yield* read)?.generation !== previous?.generation
          )
            return yield* PreviewRejected.make({
              message: "The workspace changed while opening its preview.",
            });

          const next = yield* Schema.decodeEffect(Receipt)({
            version: 1,
            generation: (previous?.generation ?? 0) + 1,
            instanceId,
            materializationId: threadId,
            deadlineAt: (yield* Clock.currentTimeMillis) + PREVIEW_LIFETIME_MS,
            processId: null,
          });

          yield* state.storage.kv.put(key, next);

          return next;
        }),
      );
    }
    if (receipt.processId === null) {
      const pending = receipt;
      const processId = yield* sandbox.allocateProcess();

      if (!processId.startsWith(`process:${instanceId}:`))
        return yield* PreviewRejected.make({
          message: "The preview allocation belongs to a different workspace instance.",
        });
      receipt = { ...pending, processId };
      yield* mutations.withTransaction(
        check(pending).pipe(Effect.andThen(state.storage.kv.put(key, receipt))),
      );
    }
    const allocated = receipt;
    const processId = allocated.processId;

    if (processId === null) return yield* Effect.die("Preview allocation was not retained");
    yield* mutations.withTransaction(check(allocated));

    // Replaying this exact ticket/configuration observes the original process and deadline.
    const process = yield* sandbox.exec(
      ["/usr/bin/python3", "-I", "-S", "-B", "-u", "-c", STATIC_PREVIEW_SERVER, processId],
      {
        processId,
        cwd: "/",
        deadlineAt: allocated.deadlineAt,
        workspaceIdentity: { path: workspace.identity.path, id: allocated.materializationId },
      },
    );

    yield* process.waitForHttp(STATIC_PREVIEW_PORT, {
      path: STATIC_PREVIEW_HEALTH_PATH,
      timeout: Math.min(
        10_000,
        Math.max(1, allocated.deadlineAt - (yield* Clock.currentTimeMillis)),
      ),
      status: { min: 204, max: 204 },
    });

    const ready = yield* sandbox.forwardPort(
      STATIC_PREVIEW_PORT,
      new Request(`http://preview.invalid${STATIC_PREVIEW_HEALTH_PATH}`, { method: "HEAD" }),
    );

    if (ready.status !== 204 || ready.headers.get("x-yielded-static-preview") !== processId)
      return yield* PreviewRejected.make({
        message: "The static preview server did not acquire its port.",
      });
    yield* mutations.withTransaction(check(allocated));

    const token = Array.from(crypto.getRandomValues(new Uint8Array(16)), (byte) =>
      byte.toString(16).padStart(2, "0"),
    ).join("");

    yield* sandbox.grantPreview({
      port: STATIC_PREVIEW_PORT,
      token,
      materializationId: allocated.materializationId,
      generation: allocated.generation,
      instanceId,
      processId,
      expiresAt: allocated.deadlineAt,
    });
    yield* mutations.withTransaction(check(allocated)).pipe(
      Effect.catchCause((cause) =>
        sandbox.revokePreview.pipe(
          Effect.exit,
          Effect.flatMap((cleanup) =>
            Effect.failCause(
              cleanup._tag === "Failure" ? Cause.combine(cause, cleanup.cause) : cause,
            ),
          ),
        ),
      ),
    );

    return { token, expiresAt: allocated.deadlineAt };
  });

  return {
    open: serial.withPermit(mutations.withMutation(open, { invalidatesRecovery: false })).pipe(
      Effect.timeoutOrElse({
        duration: "30 seconds",
        orElse: () =>
          Effect.fail(
            PreviewRejected.make({ message: "Preview preparation timed out. Try again." }),
          ),
      }),
      Effect.mapError((cause) =>
        cause._tag === "PreviewRejected" || cause._tag === "SandboxOperationError"
          ? cause
          : new SandboxOperationError({ operation: "preview.open", cause }),
      ),
    ),
  };
});

/** One bounded, read-only server per retained workspace. The host authorizes the POST. */
export class WorkspacePreviews extends Context.Service<
  WorkspacePreviews,
  {
    readonly open: Effect.Effect<
      typeof WorkspacePreviewGrant.Type,
      PreviewRejected | SandboxOperationError
    >;
  }
>()("sandbox-example/WorkspacePreviews") {
  static readonly layer = Layer.effect(this, make);
}
