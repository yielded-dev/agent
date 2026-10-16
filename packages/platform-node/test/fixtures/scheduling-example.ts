import { type NodeDurableAgentRuntimeOptions } from "@yielded/agent-platform-node/node-durable-agent-runtime";
import { NodeDurableHost } from "@yielded/agent-platform-node/node-durable-host";
import { NodeScheduling } from "@yielded/agent-platform-node/node-scheduling";
import { type ResolvedBinding } from "@yielded/agent/agent-registration";
import { type DurableSubmitAgent } from "@yielded/agent/durable-agent-runtime";
import { type ScheduleCreateOptions, Scheduling } from "@yielded/agent/scheduling";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Schema from "effect/Schema";

/** The caller supplies registered bindings, their real digests, and an explicit authorizer. */
export const schedulingRuntimeLayer = (
  bindings: ReadonlyArray<ResolvedBinding>,
  runtimeOptions: NodeDurableAgentRuntimeOptions,
) =>
  NodeScheduling.layer().pipe(
    Layer.provideMerge(NodeDurableHost.layerStack({ ...runtimeOptions, bindings })),
  );

/** Provide the runtime and application authorizer Layers once around this process workflow. */
export const runScheduledHost = Effect.fn("Example.runScheduledHost")(function* <
  InputSchema extends Schema.Top,
>(
  agent: DurableSubmitAgent<InputSchema>,
  input: InputSchema["Type"],
  options: ScheduleCreateOptions,
) {
  const scheduling = yield* Scheduling;
  const host = yield* NodeDurableHost;

  yield* scheduling.create(agent, input, options);

  return yield* host.runResolvedWorkers;
});
