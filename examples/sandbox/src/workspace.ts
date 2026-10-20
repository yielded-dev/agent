import { ThreadObjectIdentity } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import {
  NativeSandboxNamespace,
  makeNativeSandbox,
  SandboxOperationError,
  type SessionSandbox,
} from "@yielded/sandbox/cloudflare";
import { Context, Effect, Layer, Schema, Semaphore, type Stream, SubscriptionRef } from "effect";
import { DurableObjectState } from "effect-cf";

/** Admitted runs and commands may create a workspace; readers require its retained instance. */
export class Workspace extends Context.Service<
  Workspace,
  {
    readonly identity: { readonly path: string; readonly id: string };
    readonly starting: Stream.Stream<boolean>;
    readonly instanceId: Effect.Effect<string | null, SandboxOperationError>;
    readonly bind: (
      instanceId?: string,
    ) => Effect.Effect<SessionSandbox["Service"], SandboxOperationError>;
    readonly retain: (instanceId: string) => Effect.Effect<void, SandboxOperationError>;
    readonly open: Effect.Effect<SessionSandbox["Service"], SandboxOperationError>;
    readonly existing: Effect.Effect<SessionSandbox["Service"] | null, SandboxOperationError>;
    readonly inspect: Effect.Effect<
      { readonly sandbox: SessionSandbox["Service"] | null; readonly expired: boolean },
      SandboxOperationError
    >;
  }
>()("sandbox-example/Workspace") {
  static readonly layer = Layer.effect(
    Workspace,
    Effect.gen(function* () {
      const { threadId } = yield* ThreadObjectIdentity;
      const state = yield* DurableObjectState.DurableObjectState;
      const namespace = yield* NativeSandboxNamespace;
      const identity = { path: "/workspace/.sandbox-agent-materialization", id: threadId };
      const starting = yield* SubscriptionRef.make(false);
      const creation = yield* Semaphore.make(1);

      const bind = (instanceId?: string) =>
        makeNativeSandbox(threadId, {
          instance: "standard-3",
          idleMillis: 1_800_000,
          ...(instanceId === undefined
            ? {}
            : {
                expectedInstanceId: instanceId,
                recovery: "exactWorkspaceRequired" as const,
              }),
        }).pipe(Effect.provideService(NativeSandboxNamespace, namespace));

      const saved = state.storage.kv
        .get("workspace-instance")
        .pipe(
          Effect.flatMap((value) =>
            value === undefined
              ? Effect.succeed(null)
              : Schema.decodeUnknownEffect(Schema.NonEmptyString)(value),
          ),
        );

      const failure = (cause: unknown) =>
        new SandboxOperationError({ operation: "workspace", cause });

      const existing = saved.pipe(
        Effect.flatMap((instanceId) =>
          instanceId === null ? Effect.succeed(null) : bind(instanceId),
        ),
        Effect.mapError(failure),
      );

      const inspect = Effect.gen(function* () {
        const instanceId = yield* saved;

        if (instanceId === null) return { sandbox: null, expired: false };
        const sandbox = yield* bind(instanceId);
        const description = yield* sandbox.describe;

        return {
          sandbox,
          expired: !description.running || description.current?.instanceId !== instanceId,
        };
      }).pipe(Effect.mapError(failure));

      const create = Effect.gen(function* () {
        yield* SubscriptionRef.set(starting, true);
        const initial = yield* bind();
        const { instanceId } = yield* initial.ensure();
        const exact = yield* bind(instanceId);

        yield* exact.writeFile(identity.path, identity.id);
        yield* state.storage.kv.put("workspace-instance", instanceId);

        return exact;
      }).pipe(Effect.ensuring(SubscriptionRef.set(starting, false)));

      const open = Effect.gen(function* () {
        const retained = yield* existing;

        return retained ?? (yield* create);
      }).pipe(creation.withPermits(1), Effect.mapError(failure));

      return Workspace.of({
        identity,
        starting: SubscriptionRef.changes(starting),
        open,
        existing,
        inspect,
        instanceId: saved.pipe(Effect.mapError(failure)),
        bind,
        retain: (instanceId) =>
          state.storage.kv.put("workspace-instance", instanceId).pipe(Effect.mapError(failure)),
      });
    }),
  ).pipe(Layer.provide(NativeSandboxNamespace.layer({ binding: "SANDBOXES" })));
}
