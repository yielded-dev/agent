import { ThreadObjectIdentity } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import {
  NativeSandboxNamespace,
  makeNativeSandbox,
  SandboxOperationError,
  type SessionSandbox,
} from "@yielded/sandbox/cloudflare";
import { Context, Effect, Layer, Schema } from "effect";
import { DurableObjectState } from "effect-cf";

/** Only a coding tool may create a workspace; readers require its retained instance. */
export class Workspace extends Context.Service<
  Workspace,
  {
    readonly identity: { readonly path: string; readonly id: string };
    readonly open: Effect.Effect<SessionSandbox["Service"], SandboxOperationError>;
    readonly existing: Effect.Effect<SessionSandbox["Service"] | null, SandboxOperationError>;
  }
>()("sandbox-example/Workspace") {
  static readonly layer = Layer.effect(
    Workspace,
    Effect.gen(function* () {
      const { threadId } = yield* ThreadObjectIdentity;
      const state = yield* DurableObjectState.DurableObjectState;
      const namespace = yield* NativeSandboxNamespace;
      const identity = { path: "/workspace/.sandbox-agent-materialization", id: threadId };

      const bind = (instanceId?: string) =>
        makeNativeSandbox(threadId, {
          instance: "standard-1",
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

      const open = Effect.gen(function* () {
        const retained = yield* existing;

        if (retained !== null) return retained;
        const initial = yield* bind();
        const { instanceId } = yield* initial.ensure();
        const exact = yield* bind(instanceId);

        yield* exact.writeFile(identity.path, identity.id);
        yield* state.storage.kv.put("workspace-instance", instanceId);

        return exact;
      }).pipe(Effect.mapError(failure));

      return Workspace.of({ identity, open, existing });
    }),
  ).pipe(Layer.provide(NativeSandboxNamespace.layer({ binding: "SANDBOXES" })));
}
