/**
 * Typed peer messages through fixed host-authorized routes, with retained delivery status, inbox
 * provenance, and explicit retry controls.
 *
 * @since 0.1.0
 */

import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { AnyDefinition } from "../core/Agent.ts";
import { utf8Bytes } from "../core/internal/utf8.ts";
import {
  InboxPage,
  MessageRef,
  MessageStatus,
  MessagingError,
  PeerName,
} from "../core/Messaging.ts";
import { IdempotencyKey } from "../core/Receipt.ts";
import { WorkerOperationTool } from "../core/SubagentContract.ts";
import { MessagingHost } from "../engine/MessagingHost.ts";

export * from "../core/Messaging.ts";

/**
 * A fixed host route and its destination-owned input Schema. Construction acquires no resources.
 *
 * @category models
 * @since 0.1.0
 */
export interface Peer<Name extends string, Input extends Schema.Top> {
  readonly name: Name;
  readonly target: AnyDefinition & { readonly input: Input };
}

/**
 * Declare a named peer route and its destination Agent input Schema without acquiring resources or
 * granting send authority.
 *
 * @category operations
 * @since 0.1.0
 */
export const peer = <const Name extends string, Input extends Schema.Top>(
  name: Name,
  options: { readonly target: AnyDefinition & { readonly input: Input } },
): Peer<Name, Input> => {
  Schema.decodeSync(PeerName)(name);

  return Object.freeze({ name, target: options.target });
};

const host: Effect.Effect<MessagingHost["Service"], MessagingError, MessagingHost> =
  Effect.serviceOption(MessagingHost).pipe(
    Effect.flatMap((value) =>
      Option.isSome(value)
        ? Effect.succeed(value.value)
        : MessagingError.make({ operation: "context", reason: "unavailable" }),
    ),
  );

const dispatch = Effect.fnUntraced(function* <Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
  input: Input["Type"],
  options: { readonly idempotencyKey: IdempotencyKey; readonly inReplyTo?: MessageRef },
  operation: "send" | "reply",
) {
  const service = yield* host;
  const invalid = () => MessagingError.make({ operation, reason: "invalid-input" });

  const idempotencyKey = yield* Schema.decodeEffect(IdempotencyKey)(options.idempotencyKey).pipe(
    Effect.mapError(invalid),
  );

  const encodedInput = yield* Schema.encodeEffect(declaration.target.input)(input).pipe(
    Effect.mapError(invalid),
  );

  const inReplyTo =
    options.inReplyTo === undefined
      ? undefined
      : yield* Schema.decodeEffect(MessageRef)(options.inReplyTo).pipe(Effect.mapError(invalid));

  const request = {
    ...declaration,
    encodedInput,
    idempotencyKey,
    ...(inReplyTo === undefined ? {} : { inReplyTo }),
  };

  const result =
    operation === "reply"
      ? inReplyTo === undefined
        ? yield* invalid()
        : yield* service.reply({ ...request, inReplyTo })
      : yield* service.send(request);

  return yield* Schema.decodeEffect(MessageStatus)(result).pipe(
    Effect.mapError(() => MessagingError.make({ operation, reason: "corrupt" })),
  );
});

/**
 * Retain a durable message. Its status distinguishes outbound retention from destination acceptance.
 *
 * @category operations
 * @since 0.1.0
 */
export const send = <Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
  input: Input["Type"],
  options: { readonly idempotencyKey: IdempotencyKey; readonly inReplyTo?: MessageRef },
) => dispatch(declaration, input, options, "send");

/**
 * Reply to an authenticated inbound message; return routing still requires independent send authority.
 *
 * @category operations
 * @since 0.1.0
 */
export const reply = <Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
  inReplyTo: MessageRef,
  input: Input["Type"],
  options: { readonly idempotencyKey: IdempotencyKey },
) => dispatch(declaration, input, { ...options, inReplyTo }, "reply");

/**
 * Return bounded provenance for messages from this peer, without granting permission to reply.
 *
 * @category operations
 * @since 0.1.0
 */
export const inbox = Effect.fnUntraced(function* <Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
  options: { readonly after?: number; readonly limit?: number } = {},
) {
  const service = yield* host;

  return yield* service.inbox({ ...declaration, ...options, limit: options.limit ?? 20 });
});

/**
 * Inspect the retained delivery status of one message sent through the declared peer route.
 *
 * @category operations
 * @since 0.1.0
 */
export const inspect = Effect.fnUntraced(function* <Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
  message: MessageRef,
) {
  const service = yield* host;

  return yield* service.inspect({ ...declaration, message });
});

/**
 * Explicitly renew a parked delivery's bounded retry budget, retaining its complete frozen envelope.
 *
 * @category operations
 * @since 0.1.0
 */
export const retry = Effect.fnUntraced(function* <Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
  message: MessageRef,
) {
  const service = yield* host;

  return yield* service.retry({ ...declaration, message });
});

const modelKey = Effect.fnUntraced(function* () {
  const source = yield* (yield* host).context;

  if (source._tag !== "tool")
    return yield* MessagingError.make({ operation: "send", reason: "denied" });
  const crypto = yield* Crypto.Crypto;

  const digest = yield* crypto
    .digest("SHA-256", utf8Bytes(JSON.stringify(source)))
    .pipe(Effect.mapError(() => MessagingError.make({ operation: "send", reason: "unavailable" })));

  return Schema.decodeSync(IdempotencyKey)(`peer-tool:${Hex.encode(digest)}`);
});

const native = <
  const Name extends string,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Dependencies extends Array<typeof MessagingHost | typeof Crypto.Crypto>,
  R,
>(
  name: Name,
  parameters: Parameters,
  success: Success,
  description: string,
  dependencies: Dependencies,
  handler: (parameters: Parameters["Type"]) => Effect.Effect<Success["Type"], MessagingError, R>,
) => {
  const tool = Tool.make(name, {
    parameters,
    success,
    failure: MessagingError,
    description,
    dependencies,
  }).annotate(WorkerOperationTool, true);

  const toolkit = Toolkit.make(tool);

  const build = Effect.gen(function* () {
    const captured = (yield* Effect.context<
      Exclude<R, Context.Service.Identifier<Dependencies[number]>>
    >()).pipe(Context.omit(...dependencies));

    // Request dependencies must come from the live invocation, never the layer's captured context.
    const invoke = (value: Parameters["Type"]) => handler(value).pipe(Effect.provide(captured));

    // The computed single key is precisely the native Tool's literal name.
    return { [name]: invoke } as unknown as Toolkit.HandlersFrom<typeof toolkit.tools>;
  });

  return { tool, toolkit, layer: toolkit.toLayer(build) };
};

/**
 * Install only the native operations chosen by the host; schemas come from the Peer declaration.
 *
 * @category operations
 * @since 0.1.0
 */
export const sendTool = <const Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
) =>
  native(
    `${declaration.name}_send` as const,
    declaration.target.input,
    MessageStatus,
    `Send input to ${declaration.name} through its authorized durable route.`,
    [MessagingHost, Crypto.Crypto],
    (input) =>
      Effect.gen(function* () {
        return yield* send(declaration, input, { idempotencyKey: yield* modelKey() });
      }),
  );

/**
 * Build a Tool that replies to a recorded inbound message through an independently authorized peer
 * route.
 *
 * @category operations
 * @since 0.1.0
 */
export const replyTool = <const Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
) => {
  const inputSchema: Schema.Codec<
    Input["Type"],
    Input["Encoded"],
    Input["DecodingServices"],
    Input["EncodingServices"]
  > = declaration.target.input;

  return native(
    `${declaration.name}_reply` as const,
    Schema.Struct({ inReplyTo: MessageRef, input: inputSchema }),
    MessageStatus,
    `Reply to a recorded inbound message from ${declaration.name}.`,
    [MessagingHost, Crypto.Crypto],
    ({ inReplyTo, input }) =>
      Effect.gen(function* () {
        return yield* reply(declaration, inReplyTo, input, { idempotencyKey: yield* modelKey() });
      }),
  );
};

/**
 * Build a Tool that reads bounded pages of authenticated message provenance from the declared peer.
 *
 * @category operations
 * @since 0.1.0
 */
export const inboxTool = <const Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
) =>
  native(
    `${declaration.name}_inbox` as const,
    Schema.Struct({
      after: Schema.optionalKey(Schema.Natural),
      limit: Schema.optionalKey(
        Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)),
      ),
    }),
    InboxPage,
    `Read a bounded page of message provenance from ${declaration.name}.`,
    [MessagingHost],
    (options) => inbox(declaration, options),
  );

/**
 * Build a Tool that inspects delivery status for one message sent through the declared peer.
 *
 * @category operations
 * @since 0.1.0
 */
export const inspectTool = <const Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
) =>
  native(
    `${declaration.name}_message_status` as const,
    Schema.Struct({ message: MessageRef }),
    MessageStatus,
    `Inspect delivery status of a message sent to ${declaration.name}.`,
    [MessagingHost],
    ({ message }) => inspect(declaration, message),
  );

/**
 * Build a Tool that renews the bounded retry allowance of a parked peer message while retaining its
 * frozen envelope.
 *
 * @category operations
 * @since 0.1.0
 */
export const retryTool = <const Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
) =>
  native(
    `${declaration.name}_retry_message` as const,
    Schema.Struct({ message: MessageRef }),
    MessageStatus,
    `Renew bounded automatic retries for a parked message to ${declaration.name}.`,
    [MessagingHost],
    ({ message }) => retry(declaration, message),
  );
