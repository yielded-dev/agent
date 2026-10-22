/**
 * Host-owned routing and authorization for peer messages, bound to the current invocation’s trusted
 * identity.
 *
 * @since 0.1.0
 */

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

import type { AnyDefinition } from "../core/Agent.ts";
import type { ThreadId } from "../core/Identifiers.ts";
import {
  type InboxPage,
  type MessageAddress,
  type MessageRef,
  type MessageStatus,
  MessagingError,
} from "../core/Messaging.ts";
import type { IdempotencyKey, Principal } from "../core/Receipt.ts";
import type { WorkerSource } from "../core/Worker.ts";

/**
 * A named peer declaration paired with the destination Agent definition.
 *
 * @category models
 * @since 0.1.0
 */
export interface PeerTarget {
  readonly name: string;
  readonly target: AnyDefinition;
}

/**
 * A fixed declaration chooses the route. Caller input never selects a destination Thread.
 *
 * @category models
 * @since 0.1.0
 */
export interface SendPeerMessage extends PeerTarget {
  readonly encodedInput: unknown;
  readonly idempotencyKey: IdempotencyKey;
  /** Correlation on sends; replies additionally resolve and authorize the recorded return address. */
  readonly inReplyTo?: MessageRef;
}

/**
 * Trusted per-invocation facet. It carries caller authority without exposing it in Tool schemas.
 *
 * @category services
 * @since 0.1.0
 */
export class MessagingHost extends Context.Service<
  MessagingHost,
  {
    readonly context: Effect.Effect<WorkerSource, MessagingError>;
    readonly send: (request: SendPeerMessage) => Effect.Effect<MessageStatus, MessagingError>;
    readonly reply: (
      request: SendPeerMessage & { readonly inReplyTo: MessageRef },
    ) => Effect.Effect<MessageStatus, MessagingError>;
    readonly inbox: (
      request: PeerTarget & { readonly after?: number; readonly limit: number },
    ) => Effect.Effect<InboxPage, MessagingError>;
    readonly inspect: (
      request: PeerTarget & { readonly message: MessageRef },
    ) => Effect.Effect<MessageStatus, MessagingError>;
    readonly retry: (
      request: PeerTarget & { readonly message: MessageRef },
    ) => Effect.Effect<MessageStatus, MessagingError>;
  }
>()("@effect-agent/engine/MessagingHost") {
  static readonly unavailable: MessagingHost["Service"] = {
    context: Effect.suspend(() =>
      MessagingError.make({ operation: "context", reason: "unavailable" }),
    ),
    send: () => MessagingError.make({ operation: "send", reason: "unavailable" }),
    reply: () => MessagingError.make({ operation: "reply", reason: "unavailable" }),
    inbox: () => MessagingError.make({ operation: "inbox", reason: "unavailable" }),
    inspect: () => MessagingError.make({ operation: "inspect", reason: "unavailable" }),
    retry: () => MessagingError.make({ operation: "retry", reason: "unavailable" }),
  };

  /** Runtime-owned per-call binding. Unconfigured Runs deny peer operations. */
  static readonly forTool = Context.Reference<
    (source: Extract<WorkerSource, { readonly _tag: "tool" }>) => MessagingHost["Service"]
  >("@effect-agent/engine/MessagingHost/forTool", {
    defaultValue: () => () => MessagingHost.unavailable,
  });
}

/**
 * Separate context, history, send and management grants; incoming messages grant none of them.
 *
 * @category models
 * @since 0.1.0
 */
export interface PeerAuthorizationRequest {
  readonly source: MessageAddress;
  readonly principal: Principal;
  readonly operation: MessagingError["operation"];
  readonly access: "context" | "read" | "send" | "control";
  readonly peerName?: string;
  readonly destination?: MessageAddress;
}

/**
 * Host authority for peer operations, denying every request by default. Authorized sends return the
 * principal retained with their frozen envelope.
 *
 * @category services
 * @since 0.1.0
 */
export const PeerAuthorizer = Context.Reference<{
  /** Return the stable delivery principal for sends. It is retained with the frozen envelope. */
  readonly authorize: (
    request: PeerAuthorizationRequest,
  ) => Effect.Effect<Principal, MessagingError>;
}>("@effect-agent/thread/PeerAuthorizer", {
  defaultValue: () => ({
    authorize: (request) => MessagingError.make({ operation: request.operation, reason: "denied" }),
  }),
});

/**
 * Application-owned routing; the target Agent is resolved from the existing exact registrations.
 *
 * @category services
 * @since 0.1.0
 */
export const PeerRoutes = Context.Reference<{
  readonly resolve: (request: {
    readonly source: MessageAddress;
    readonly principal: Principal;
    readonly peerName: string;
    readonly targetAgentId: MessageAddress["agentId"];
  }) => Effect.Effect<ThreadId, MessagingError>;
}>("@effect-agent/thread/PeerRoutes", {
  defaultValue: () => ({
    resolve: () => MessagingError.make({ operation: "send", reason: "route-unavailable" }),
  }),
});

/**
 * Finite delivery and retained-send window in milliseconds (default one day, maximum seven days).
 * Exhaustion parks outstanding work; terminal sends stop consuming capacity at this deadline.
 *
 * @category services
 * @since 0.1.0
 */
export const PeerDeliveryLifetime = Context.Reference<number>(
  "@effect-agent/thread/PeerDeliveryLifetime",
  { defaultValue: () => 86_400_000 },
);

/**
 * Bound outstanding sends plus terminal sends still inside their delivery window, across all peers
 * and principals in one source Thread (default 256, maximum 1,000). Canonical history and exact
 * idempotent receipts remain retained after expiry without consuming this capacity.
 *
 * @category services
 * @since 0.1.0
 */
export const PeerMessageCapacity = Context.Reference<number>(
  "@effect-agent/thread/PeerMessageCapacity",
  { defaultValue: () => 256 },
);
