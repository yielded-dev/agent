/**
 * Schema-defined intermediate Agent findings emitted through an engine-owned port and reserved
 * native Tool.
 *
 * @since 0.1.0
 */

import type * as Tool from "effect/ai/Tool";
import type * as Toolkit from "effect/ai/Toolkit";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { AgentId, ThreadId, RunId } from "./Identifiers.ts";
import { IdempotencyKey } from "./Receipt.ts";

/**
 * Canonical schema-encoded intermediate output, scoped to one logical Run.
 *
 * @category models
 * @since 0.1.0
 */
export class Update extends Schema.Class<Update>("@effect-agent/core/AgentUpdates/Update")({
  schemaVersion: Schema.Literal(1),
  agentId: AgentId,
  threadId: ThreadId,
  runId: RunId,
  updateId: IdempotencyKey,
  sequence: Schema.Int.check(Schema.isGreaterThan(0)),
  value: Schema.Json,
}) {}

/**
 * Fail-closed update acceptance and decoding errors; payloads are never included.
 *
 * @category errors
 * @since 0.1.0
 */
export class UpdateError extends Schema.TaggedError<UpdateError>()("AgentUpdateError", {
  reason: Schema.Literals([
    "validation",
    "unavailable",
    "conflict",
    "capacity",
    "storage",
    "identity",
    "denied",
  ]),
}) {}

/**
 * Engine-owned invocation port. The Agent ID and exact update Schema must match the executing definition.
 *
 * @category services
 * @since 0.1.0
 */
export class Emitter extends Context.Service<
  Emitter,
  {
    readonly emit: (request: {
      readonly target: { readonly id: AgentId; readonly updates?: Schema.Top | undefined };
      readonly updateId: IdempotencyKey;
      readonly value: Schema.Json;
    }) => Effect.Effect<Update, UpdateError>;
  }
>()("@effect-agent/core/AgentUpdates/Emitter") {}

/**
 * Native Tool added to definitions that declare an update Schema.
 *
 * @category utility types
 * @since 0.1.0
 */
export type UpdateTool<S extends Schema.Top> = Tool.Tool<
  "emit_update",
  {
    readonly parameters: Schema.Struct<{ readonly value: S }>;
    readonly success: Schema.Struct<{ readonly emitted: Schema.Literal<true> }>;
    readonly failure: typeof UpdateError;
    readonly failureMode: "return";
  },
  Emitter
>;

/**
 * Preserve the author's Toolkit while adding the reserved update Tool when configured.
 *
 * @category utility types
 * @since 0.1.0
 */
export type UpdateToolkit<
  T extends Toolkit.Any,
  S extends Schema.Top | undefined,
> = S extends Schema.Top
  ? Toolkit.Toolkit<T["tools"] & { readonly emit_update: UpdateTool<S> }>
  : T;

/** Structural schema boundary accepted by definitions and their model bindings. */
type Source<S extends Schema.Top> = {
  readonly id: AgentId;
  readonly updates?: S | undefined;
};

type BoundSource<S extends Schema.Top> = Source<S> | { readonly definition: Source<S> };

/**
 * Emit a decoded update under an explicit stable key. Retries cannot replace accepted values.
 *
 * @category operations
 * @since 0.1.0
 */
export const emit = Effect.fnUntraced(function* <S extends Schema.Top>(
  agent: BoundSource<S>,
  value: NoInfer<S["Type"]>,
  options: { readonly idempotencyKey: IdempotencyKey },
): Effect.fn.Return<Update, UpdateError, Emitter | S["EncodingServices"]> {
  const target = "definition" in agent ? agent.definition : agent;

  if (target.updates === undefined) return yield* UpdateError.make({ reason: "unavailable" });

  const updateId = yield* Schema.decodeEffect(IdempotencyKey)(options.idempotencyKey).pipe(
    Effect.mapError(() => UpdateError.make({ reason: "validation" })),
  );

  const encoded = yield* Schema.encodeEffect(target.updates)(value).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)),
    Effect.mapError(() => UpdateError.make({ reason: "validation" })),
  );

  const emitter = yield* Effect.serviceOption(Emitter);

  if (Option.isNone(emitter)) return yield* UpdateError.make({ reason: "unavailable" });

  return yield* emitter.value.emit({ target, updateId, value: encoded });
});

/**
 * Decode canonical update data through its declaring Agent Schema.
 *
 * @category encoding
 * @since 0.1.0
 */
export const decode = Effect.fnUntraced(function* <S extends Schema.Top>(
  agent: BoundSource<S>,
  update: Update,
): Effect.fn.Return<S["Type"], UpdateError, S["DecodingServices"]> {
  const definition = "definition" in agent ? agent.definition : agent;

  if (definition.updates === undefined) return yield* UpdateError.make({ reason: "unavailable" });

  const decoded = yield* Schema.decodeEffect(Update)(update).pipe(
    Effect.mapError(() => UpdateError.make({ reason: "validation" })),
  );

  if (decoded.agentId !== definition.id) return yield* UpdateError.make({ reason: "identity" });

  return yield* Schema.decodeEffect(definition.updates)(decoded.value).pipe(
    Effect.mapError(() => UpdateError.make({ reason: "validation" })),
  );
});

/**
 * Decode an encoded update stream without hiding its failures or requirements.
 *
 * @category operations
 * @since 0.1.0
 */
export const observe = <S extends Schema.Top, E, R>(
  agent: BoundSource<S>,
  updates: Stream.Stream<Update, E, R>,
) => updates.pipe(Stream.mapEffect((update) => decode(agent, update)));
