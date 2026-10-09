import { Effect, Option, Schema } from "effect";

import { Cloudflare } from "./cloudflare.ts";
import { type Invocation } from "./model.ts";
import { BenchError } from "./platform.ts";
import { Target } from "./worker/protocol.ts";

// Decode only the fields used for attribution; never persist raw telemetry or provider IDs.
const Event = Schema.Struct({
  $metadata: Schema.Struct({
    id: Schema.String,
    requestId: Schema.optionalKey(Schema.String),
    type: Schema.String,
  }),
  $workers: Schema.Struct({
    outcome: Schema.optionalKey(Schema.String),
    eventType: Schema.optionalKey(Schema.String),
    cpuTimeMs: Schema.optionalKey(Schema.Number),
    wallTimeMs: Schema.optionalKey(Schema.Number),
  }),
  source: Schema.Union([Schema.String, Schema.Record(Schema.String, Schema.Unknown)]),
});

const Marker = Schema.Struct({
  kind: Schema.NonEmptyString,
  target: Schema.optionalKey(Target),
  object: Schema.optionalKey(Schema.String),
  sample: Schema.optionalKey(Schema.String),
});

const Page = Schema.Struct({
  events: Schema.Struct({ count: Schema.Natural, events: Schema.Array(Event) }),
});

export const cpu = Effect.fnUntraced(function* (name: string, from: number, to: number) {
  const cloud = yield* Cloudflare;
  const pending = [{ from, to }];
  const events = new Map<string, typeof Event.Type>();

  while (pending.length) {
    const window = pending.pop();

    if (!window) break;

    const result = yield* cloud.api("workers/observability/telemetry/query", Page, {
      queryId: name,
      dry: true,
      view: "events",
      limit: 2000,
      timeframe: window,
      parameters: {
        filterCombination: "and",
        filters: [
          {
            key: "$workers.scriptName",
            operation: "eq",
            type: "string",
            value: name,
          },
        ],
      },
    });

    if (result.events.count >= 2000 || result.events.count > result.events.events.length) {
      if (window.to - window.from <= 1000)
        return yield* new BenchError({
          message: "CPU telemetry exceeded the query limit; refusing partial pagination.",
        });
      const middle = Math.floor((window.from + window.to) / 2);

      pending.push({ from: window.from, to: middle }, { from: middle, to: window.to });
    } else for (const event of result.events.events) events.set(event.$metadata.id, event);
  }
  const markers = new Map<string, (typeof Marker.Type)[]>();

  for (const event of events.values()) {
    const id = event.$metadata.requestId;

    if (!id || event.$metadata.type === "cf-worker-event") continue;
    const marker = Schema.decodeUnknownOption(Marker)(event.source);

    if (Option.isSome(marker)) markers.set(id, [...(markers.get(id) ?? []), marker.value]);
  }
  const invocations: Invocation[] = [];

  for (const event of events.values()) {
    if (event.$metadata.type !== "cf-worker-event") continue;
    const id = event.$metadata.requestId;
    const matching = id === undefined ? [] : (markers.get(id) ?? []);
    const marker = matching.length === 1 ? matching[0] : undefined;

    invocations.push({
      kind:
        marker?.kind ??
        (matching.length > 1 ? "ambiguous" : (event.$workers.eventType ?? "unattributed")),
      outcome: event.$workers.outcome ?? "unknown",
      cpuMs: event.$workers.cpuTimeMs ?? null,
      wallMs: event.$workers.wallTimeMs ?? null,
      ...(marker?.target ? { target: marker.target } : {}),
      ...(marker?.object ? { object: marker.object } : {}),
      ...(marker?.sample ? { sample: marker.sample } : {}),
    });
    if (id) markers.delete(id);
  }

  return {
    invocations,
    unmatchedMarkers: [...markers.values()].reduce((n, rows) => n + rows.length, 0),
  };
});
