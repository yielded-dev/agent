/**
 * Versioned host adapters that validate and normalize event input, derive matching keys, and
 * reconcile source state. Callback dependencies are captured at assembly and per-operation
 * resources are scoped.
 *
 * @since 0.1.0
 */

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

import { PersistedJson } from "./Records.ts";
import {
  type AcceptedEvent,
  type EventSourceVersion,
  type SubscriptionRecord,
  SubscriptionSourceError,
} from "./Subscription.ts";

/**
 * Schema-encoded event payload with a stable source identity, matching key, and optional
 * epoch-millisecond occurrence time.
 *
 * @category models
 * @since 0.1.0
 */
export interface NormalizedEvent {
  readonly eventId: string;
  readonly occurredAtMillis?: number;
  readonly matchingKey: string;
  readonly payload: PersistedJson;
}

/**
 * Runtime-only versioned behavior. Never persist this object or silently replace its version.
 *
 * @category models
 * @since 0.1.0
 */
export interface EventSource {
  readonly source: EventSourceVersion;
  readonly continuity: string;
  readonly parameters: (
    value: unknown,
  ) => Effect.Effect<
    { readonly parameters: PersistedJson; readonly matchingKey: string },
    SubscriptionSourceError
  >;
  readonly normalize: (value: unknown) => Effect.Effect<NormalizedEvent, SubscriptionSourceError>;
  readonly matches: (
    event: AcceptedEvent,
    subscription: SubscriptionRecord,
  ) => Effect.Effect<boolean, SubscriptionSourceError>;
  readonly reconcile?: (
    subscription: SubscriptionRecord,
  ) => Effect.Effect<NormalizedEvent | null, SubscriptionSourceError>;
}

/**
 * Host-assembled registry of versioned event normalization, matching, and reconciliation behavior.
 *
 * @category services
 * @since 0.1.0
 */
export class EventSources extends Context.Service<
  EventSources,
  {
    readonly sources: ReadonlyArray<EventSource>;
  }
>()("@effect-agent/thread/EventSources") {}

const invalid = () => SubscriptionSourceError.make({ code: "source-schema", retryable: false });

/**
 * Capture the source's explicit Effect dependencies at host assembly. Matching and key functions
 * must be pure and bounded under this exact semantic version.
 * The returned source accepts Schema-encoded values at intake and management boundaries.
 * Each operation owns a fresh Scope for its callbacks and codecs; captured host services
 * retain their host lifetime. Operation resources finalize on success, failure, or interruption.
 *
 * @category constructors
 * @since 0.1.0
 */
export const makeEventSource = Effect.fnUntraced(function* <
  Event extends Schema.Top,
  Parameters extends Schema.Top,
  R = never,
>(options: {
  readonly source: EventSourceVersion;
  readonly continuity: string;
  readonly event: Event;
  readonly parameters: Parameters;
  readonly identity: (event: Event["Type"]) => string;
  /** Authenticated source time, stable across delivery retries; required for bounded retention. */
  readonly occurredAtMillis?: (event: Event["Type"]) => number | undefined;
  readonly eventKey: (event: Event["Type"]) => string;
  readonly parameterKey: (parameters: Parameters["Type"]) => string;
  readonly matches: (event: Event["Type"], parameters: Parameters["Type"]) => boolean;
  readonly reconcile?: (
    parameters: Parameters["Type"],
  ) => Effect.Effect<Event["Type"] | null, SubscriptionSourceError, R>;
}): Effect.fn.Return<
  EventSource,
  never,
  Exclude<
    | R
    | Event["DecodingServices"]
    | Event["EncodingServices"]
    | Parameters["DecodingServices"]
    | Parameters["EncodingServices"],
    Scope.Scope
  >
> {
  const services =
    yield* Effect.context<
      Exclude<
        | R
        | Event["DecodingServices"]
        | Event["EncodingServices"]
        | Parameters["DecodingServices"]
        | Parameters["EncodingServices"],
        Scope.Scope
      >
    >();

  const encode = <S extends Schema.Top>(schema: S, value: S["Type"]) =>
    Schema.encodeEffect(schema)(value).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(PersistedJson)),
      Effect.mapError(invalid),
    );

  const normalized = Effect.fnUntraced(function* (event: Event["Type"]) {
    const occurredAtMillis = options.occurredAtMillis?.(event);

    return {
      eventId: options.identity(event),
      ...(occurredAtMillis === undefined ? {} : { occurredAtMillis }),
      matchingKey: options.eventKey(event),
      payload: yield* encode(options.event, event),
    };
  });

  const normalize: EventSource["normalize"] = (value) =>
    Schema.decodeUnknownEffect(PersistedJson)(value).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(options.event)),
      Effect.mapError(invalid),
      Effect.flatMap(normalized),
      Effect.scoped,
      Effect.provideContext(services),
    );

  const parameters: EventSource["parameters"] = Effect.fnUntraced(
    function* (value) {
      yield* Schema.decodeUnknownEffect(PersistedJson)(value).pipe(Effect.mapError(invalid));

      const decoded = yield* Schema.decodeUnknownEffect(options.parameters)(value).pipe(
        Effect.mapError(invalid),
      );

      return {
        parameters: yield* encode(options.parameters, decoded),
        matchingKey: options.parameterKey(decoded),
      };
    },
    (effect) => effect.pipe(Effect.scoped, Effect.provideContext(services)),
  );

  const matches: EventSource["matches"] = Effect.fnUntraced(
    function* (event, subscription) {
      const e = yield* Schema.decodeEffect(options.event)(event.payload).pipe(
        Effect.mapError(invalid),
      );

      const p = yield* Schema.decodeEffect(options.parameters)(
        subscription.configuration.parameters,
      ).pipe(Effect.mapError(invalid));

      return options.matches(e, p);
    },
    (effect) => effect.pipe(Effect.scoped, Effect.provideContext(services)),
  );

  const reconcile = options.reconcile;

  return {
    source: options.source,
    continuity: options.continuity,
    parameters,
    normalize,
    matches,
    ...(reconcile === undefined
      ? {}
      : {
          reconcile: Effect.fnUntraced(
            function* (subscription: SubscriptionRecord) {
              const p = yield* Schema.decodeEffect(options.parameters)(
                subscription.configuration.parameters,
              ).pipe(Effect.mapError(invalid));

              const event = yield* reconcile(p);

              return event === null ? null : yield* normalized(event);
            },
            (effect) => effect.pipe(Effect.scoped, Effect.provideContext(services)),
          ),
        }),
  };
});
