import { Console, Effect, Option, Schema } from "effect";

import { Cloudflare } from "../cloudflare.ts";
import { BenchError } from "../platform.ts";
import { Meta } from "./model.ts";

const Event = Schema.Struct({
  $metadata: Schema.Struct({
    id: Schema.String,
    requestId: Schema.optionalKey(Schema.String),
    type: Schema.String,
  }),
  $workers: Schema.Struct({
    outcome: Schema.optionalKey(Schema.String),
    eventType: Schema.optionalKey(Schema.String),
    executionModel: Schema.optionalKey(Schema.String),
    entrypoint: Schema.optionalKey(Schema.String),
    cpuTimeMs: Schema.optionalKey(Schema.Finite),
    wallTimeMs: Schema.optionalKey(Schema.Finite),
    event: Schema.optionalKey(
      Schema.Struct({
        rpcMethods: Schema.optionalKey(Schema.Array(Schema.String)),
        rpcCallCount: Schema.optionalKey(Schema.Natural),
      }),
    ),
  }),
  source: Schema.Union([Schema.String, Schema.Record(Schema.String, Schema.Unknown)]),
});

const Page = Schema.Struct({
  events: Schema.Struct({ count: Schema.Natural, events: Schema.Array(Event) }),
});

const Marker = Schema.Struct({
  ...Meta.fields,
  kind: Schema.Literal("rpc-overhead"),
  role: Schema.Literals(["driver", "object"]),
  calls: Schema.Natural,
  warmup: Schema.Natural,
});

/** Same request-marker join as the deployed bench; retain missing rows and strip all provider identifiers. */
export const cpu = Effect.fnUntraced(function* (name: string, from: number, to: number) {
  const cloud = yield* Cloudflare;
  const windows = [{ from, to }];
  const events = new Map<string, typeof Event.Type>();
  let queries = 0;

  while (windows.length) {
    const window = windows.pop();

    if (!window) break;

    const page = yield* cloud.api("workers/observability/telemetry/query", Page, {
      queryId: "rpc-overhead",
      dry: true,
      view: "events",
      limit: 2000,
      timeframe: window,
      parameters: {
        filterCombination: "and",
        filters: [{ key: "$workers.scriptName", operation: "eq", type: "string", value: name }],
      },
    });

    if (++queries % 16 === 0)
      yield* Console.error(
        `CPU export: ${queries} windows queried, ${events.size} distinct events retained.`,
      );
    if (page.events.count >= 2000 || page.events.count > page.events.events.length) {
      if (window.to - window.from <= 1)
        return yield* new BenchError({
          message: "CPU telemetry truncated at the minimum time window",
        });
      const middle = Math.floor((window.from + window.to) / 2);

      windows.push({ from: window.from, to: middle }, { from: middle, to: window.to });
    } else for (const event of page.events.events) events.set(event.$metadata.id, event);
  }
  const markers = new Map<string, (typeof Marker.Type)[]>();

  for (const event of events.values()) {
    const id = event.$metadata.requestId;

    if (!id || event.$metadata.type === "cf-worker-event") continue;
    const marker = Schema.decodeUnknownOption(Marker)(event.source);

    if (Option.isSome(marker)) markers.set(id, [...(markers.get(id) ?? []), marker.value]);
  }

  const rows: {
    marker: typeof Marker.Type | null;
    cpuMs: number | null;
    wallMs: number | null;
    outcome: string;
    eventType: string;
    executionModel: string;
    entrypoint: string;
    rpcMethods: readonly string[];
    rpcCallCount: number | null;
  }[] = [];

  for (const event of events.values()) {
    if (event.$metadata.type !== "cf-worker-event") continue;
    const id = event.$metadata.requestId;
    const matching = id ? (markers.get(id) ?? []) : [];

    rows.push({
      marker: matching.length === 1 ? matching[0]! : null,
      cpuMs: event.$workers.cpuTimeMs ?? null,
      wallMs: event.$workers.wallTimeMs ?? null,
      outcome: event.$workers.outcome ?? "unknown",
      eventType: event.$workers.eventType ?? "unknown",
      executionModel: event.$workers.executionModel ?? "unknown",
      entrypoint: event.$workers.entrypoint ?? "unknown",
      rpcMethods: (event.$workers.event?.rpcMethods ?? []).map((method) =>
        /^[A-Za-z][A-Za-z0-9_]{0,64}$/.test(method) ? method : "unrecognized",
      ),
      rpcCallCount: event.$workers.event?.rpcCallCount ?? null,
    });
    if (id) markers.delete(id);
  }

  return {
    rows,
    unmatchedMarkers: [...markers.values()].flat().length,
    retrievedEvents: events.size,
  };
});
