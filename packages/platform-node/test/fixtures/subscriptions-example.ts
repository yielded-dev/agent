import { type NodeDurableAgentRuntimeOptions } from "@yielded/agent-platform-node/node-durable-agent-runtime";
import { NodeDurableHost } from "@yielded/agent-platform-node/node-durable-host";
import { NodeSubscriptions } from "@yielded/agent-platform-node/node-subscriptions";
import { subscriptionStoreLayer } from "@yielded/agent-storage-sqlite/sqlite-subscription-store";
import { EventSources, type EventSource } from "@yielded/agent/event-source";
import { type SourcePartition, SubscriptionAuthorizer } from "@yielded/agent/subscription";
import {
  SubscriptionInputBindings,
  type SubscriptionInputBinding,
} from "@yielded/agent/subscription-input";
import { SubscriptionIntake, Subscriptions } from "@yielded/agent/subscriptions";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

const subscriptionRuntimeFromSourcesLayer = <E, R>(options: {
  readonly runtime: NodeDurableAgentRuntimeOptions;
  readonly partition: SourcePartition;
  readonly sources: Layer.Layer<EventSources | SubscriptionInputBindings, E, R>;
  readonly authorizer: SubscriptionAuthorizer["Service"];
}) => {
  const dependencies = Layer.mergeAll(
    subscriptionStoreLayer(options.partition),
    options.sources,
    Layer.succeed(SubscriptionAuthorizer)(options.authorizer),
  ).pipe(
    // All live storage operations share the host's serialized SQLite connection.
    Layer.provideMerge(NodeDurableHost.layerStack(options.runtime)),
  );

  return NodeSubscriptions.layer().pipe(Layer.provideMerge(dependencies));
};

/**
 * Assemble one source partition in the same process and SQLite database owner as the durable
 * host. The caller binds the permitted source catalog and explicit authorization policy.
 */
export const subscriptionRuntimeLayer = (options: {
  readonly runtime: NodeDurableAgentRuntimeOptions;
  readonly partition: SourcePartition;
  readonly sources: ReadonlyArray<EventSource>;
  readonly bindings: ReadonlyArray<SubscriptionInputBinding>;
  readonly authorizer: SubscriptionAuthorizer["Service"];
}) =>
  subscriptionRuntimeFromSourcesLayer({
    ...options,
    sources: Layer.merge(
      Layer.succeed(EventSources)({ sources: options.sources }),
      Layer.succeed(SubscriptionInputBindings)({ bindings: options.bindings }),
    ),
  });

/** Registration returns immediately; the scoped driver later admits matching input. */
export const registerAndAccept = Effect.fn("Example.registerAndAccept")(function* (
  scope: Parameters<Subscriptions["Service"]["subscribe"]>[0],
  options: Parameters<Subscriptions["Service"]["subscribe"]>[1],
  event: {
    readonly principal: Parameters<SubscriptionIntake["Service"]["accept"]>[0];
    readonly payload: unknown;
  },
) {
  const subscriptions = yield* Subscriptions;
  const intake = yield* SubscriptionIntake;
  const registered = yield* subscriptions.subscribe(scope, options);
  const acknowledgement = yield* intake.accept(event.principal, options.source, event.payload);

  return { registered, acknowledgement };
});
