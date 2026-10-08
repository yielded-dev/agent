import { Effect, Schema } from "effect";

import { Identity } from "./contracts.ts";
import { requireBench } from "./deployment.ts";

const Construction = Schema.Struct({
  ...Identity.fields,
  benchConstructor: Schema.Literal(true),
  kind: Schema.Literals(["thread", "actor"]),
});

const optionalString = Schema.optionalKey(Schema.NullOr(Schema.String));

const Logs = Schema.Struct({
  success: Schema.Literal(true),
  result: Schema.Struct({
    events: Schema.Struct({
      count: Schema.Natural,
      events: Schema.Array(Schema.Record(Schema.String, Schema.Unknown)),
    }),
  }),
});

const LogIdentity = Schema.Struct({
  timestamp: Schema.optionalKey(Schema.Union([Schema.String, Schema.Number])),
  $metadata: Schema.Struct({ id: optionalString, type: optionalString, traceId: optionalString }),
  $workers: Schema.Struct({
    executionModel: optionalString,
    durableObjectId: optionalString,
    eventType: optionalString,
    scriptName: optionalString,
    scriptVersion: Schema.optionalKey(Schema.Struct({ id: Schema.String })),
  }),
});

/** Keep only the wrapper's known identity log; never serialize request/response data. */
export const constructors = Effect.fnUntraced(function* (input: unknown) {
  const { result } = yield* Schema.decodeUnknownEffect(Logs)(input);

  yield* requireBench(
    result.events.count === result.events.events.length && result.events.count < 2000,
    "Constructor log export truncated",
  );
  const rows = [];
  const shapes = new Set<string>();

  for (const event of result.events.events) {
    const identity = Schema.decodeUnknownOption(LogIdentity)(event);

    const source = Schema.decodeUnknownOption(Schema.Record(Schema.String, Schema.Unknown))(
      event.source,
    );

    shapes.add(
      JSON.stringify({
        type: identity._tag === "Some" ? identity.value.$metadata.type : "unparsed",
        keys: Object.keys(event).sort(),
        sourceKeys: source._tag === "Some" ? Object.keys(source.value).sort() : [],
      }),
    );
    for (const candidate of [event, event.source, event.message]) {
      const parsed =
        typeof candidate === "string"
          ? Schema.decodeUnknownOption(Schema.fromJsonString(Construction))(candidate)
          : Schema.decodeUnknownOption(Construction)(candidate);

      if (parsed._tag === "None") continue;
      const identity = yield* Schema.decodeUnknownEffect(LogIdentity)(event);

      rows.push({
        timestamp: identity.timestamp,
        id: identity.$metadata.id,
        metadataType: identity.$metadata.type,
        traceId: identity.$metadata.traceId,
        executionModel: identity.$workers.executionModel,
        durableObjectId: identity.$workers.durableObjectId,
        eventType: identity.$workers.eventType,
        scriptName: identity.$workers.scriptName,
        scriptVersion: identity.$workers.scriptVersion,
        construction: parsed.value,
      });
      break;
    }
  }

  return { totalLogs: result.events.count, rows, shapes: [...shapes].sort() };
});
