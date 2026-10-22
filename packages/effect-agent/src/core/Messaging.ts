/**
 * Schemas for peer addresses, authenticated message provenance, bounded inbox pages, and expected
 * messaging failures.
 *
 * @since 0.1.0
 */

import * as Schema from "effect/Schema";

import { AgentId, ThreadId } from "./Identifiers.ts";
import { MessageRef } from "./internal/message-status.ts";
import { FrameworkMessage } from "./Worker.ts";

export {
  /**
   * A retained outbound delivery identity, not a destination Receipt or access grant.
   *
   * @category schemas
   * @since 0.1.0
   */
  MessageRef,
  /**
   * Source-owned delivery evidence. Pending confirms retention only; accepted supplies the
   * destination Receipt, and processed supplies its canonical settlement. Neither retention nor
   * acceptance proves execution. Refused is a definite rejection; parked stops automatic retry
   * while retaining the same identity and any acceptance evidence. Inspect instead of resending.
   *
   * @category schemas
   * @since 0.1.0
   */
  MessageStatus,
} from "./internal/message-status.ts";

/**
 * Application-chosen fixed route name; it never authorizes a destination by itself.
 *
 * @category schemas
 * @since 0.1.0
 */
export const PeerName = Schema.NonEmptyString.check(
  Schema.isMaxLength(64),
  Schema.isPattern(/^[a-zA-Z0-9_-]+$/u),
);

/**
 * Decoded value of {@link PeerName}.
 *
 * @category models
 * @since 0.1.0
 */
export type PeerName = typeof PeerName.Type;

/**
 * Schema identifying an Agent and its destination Thread. Possession grants no routing or send
 * authority.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MessageAddress = Schema.Struct({ threadId: ThreadId, agentId: AgentId });
/**
 * Decoded value of {@link MessageAddress}.
 *
 * @category models
 * @since 0.1.0
 */
export type MessageAddress = typeof MessageAddress.Type;

/**
 * Host-authenticated provenance stored separately from destination-owned application input.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MessageAdmission = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  message: MessageRef,
  peerName: PeerName,
  sender: MessageAddress,
  returnAddress: MessageAddress,
  inReplyTo: Schema.optionalKey(MessageRef),
}).check(
  Schema.makeFilter(
    (value) =>
      value.message.ownerThreadId === value.sender.threadId &&
      value.sender.threadId === value.returnAddress.threadId &&
      value.sender.agentId === value.returnAddress.agentId,
  ),
);

/**
 * Decoded value of {@link MessageAdmission}.
 *
 * @category models
 * @since 0.1.0
 */
export type MessageAdmission = typeof MessageAdmission.Type;

/**
 * Schema for one authenticated message admission at a positive inbox sequence.
 *
 * @category schemas
 * @since 0.1.0
 */
export const InboxEntry = Schema.Struct({
  sequence: Schema.Natural.check(Schema.isGreaterThan(0)),
  admission: MessageAdmission,
});

/**
 * Decoded value of {@link InboxEntry}.
 *
 * @category models
 * @since 0.1.0
 */
export type InboxEntry = typeof InboxEntry.Type;

/**
 * Schema for up to 100 inbox entries and the continuation sequence, or null when no further page is
 * available.
 *
 * @category schemas
 * @since 0.1.0
 */
export const InboxPage = Schema.Struct({
  items: Schema.Array(InboxEntry).check(Schema.isMaxLength(100)),
  next: Schema.NullOr(Schema.Natural),
});

/**
 * Decoded value of {@link InboxPage}.
 *
 * @category models
 * @since 0.1.0
 */
export type InboxPage = typeof InboxPage.Type;

/**
 * A peer operation failed authorization, routing, input validation, capacity, or retained-state
 * checks.
 *
 * @category errors
 * @since 0.1.0
 */
export class MessagingError extends Schema.TaggedError<MessagingError>()("MessagingError", {
  operation: Schema.Literals(["context", "send", "reply", "inbox", "inspect", "retry"]),
  reason: Schema.Literals([
    "denied",
    "unavailable",
    "route-unavailable",
    "binding-mismatch",
    "invalid-input",
    "invalid-reference",
    "conflict",
    "capacity",
    "not-found",
    "storage",
    "corrupt",
  ]),
}) {}

/**
 * Canonical input provenance: peer input or a framework-owned completion message.
 *
 * @category schemas
 * @since 0.1.0
 */
export const InputMessage = Schema.Union([MessageAdmission, FrameworkMessage]);
/**
 * Decoded value of {@link InputMessage}.
 *
 * @category models
 * @since 0.1.0
 */
export type InputMessage = typeof InputMessage.Type;
