import { Option, Schema } from "effect";

import { Invocation } from "./model.ts";
import { Target } from "./worker/protocol.ts";

const Marker = Schema.Struct({
  kind: Schema.NonEmptyString,
  target: Target,
  object: Schema.NonEmptyString,
  sample: Schema.NonEmptyString,
  invocation: Schema.NonEmptyString,
  dispatchSource: Schema.optionalKey(Schema.NonEmptyString),
  completes: Schema.optionalKey(Schema.NonEmptyString),
});

const Stored = Schema.Struct({
  invocation: Schema.NonEmptyString,
  record: Invocation,
});

export const TailPage = Schema.Struct({
  records: Schema.Array(Stored),
  cursor: Schema.optionalKey(Schema.String),
});

interface Env {
  BENCH_TOKEN: string;
  BUILD: string;
  RECORDS: R2Bucket;
}

export default {
  async tail(events: TraceItem[], env: Env): Promise<void> {
    await Promise.all(
      events.map(async (event) => {
        const markers = event.logs.flatMap((log) =>
          log.message.flatMap((value: unknown) => {
            const marker = Schema.decodeUnknownOption(Marker)(value);

            return Option.isSome(marker) ? [marker.value] : [];
          }),
        );

        const first = markers[0];

        if (!first) return;

        const oneSample = markers.every(
          (marker) =>
            marker.target === first.target &&
            marker.object === first.object &&
            marker.sample === first.sample,
        );

        const valid =
          oneSample &&
          !event.truncated &&
          event.durableObjectId !== undefined &&
          markers.every(
            (marker) => marker.kind !== "dispatch" || marker.dispatchSource !== undefined,
          );

        const record = Schema.encodeSync(Stored)({
          invocation: first.invocation,
          record: {
            target: first.target,
            object: first.object,
            sample: first.sample,
            kind: valid ? first.kind : "ambiguous",
            kinds: markers.map((marker) => marker.kind),
            invocationIds: markers.map((marker) => marker.invocation),
            invocationType:
              event.event === null
                ? "unknown"
                : "rpcMethod" in event.event
                  ? "jsrpc"
                  : "request" in event.event
                    ? "fetch"
                    : "scheduledTime" in event.event && !("cron" in event.event)
                      ? "alarm"
                      : "other",
            ...(event.event && "rpcMethod" in event.event
              ? { invocationMethod: event.event.rpcMethod }
              : event.event && "request" in event.event
                ? { invocationMethod: new URL(event.event.request.url).pathname }
                : {}),
            dispatches: markers.flatMap((marker) =>
              marker.kind !== "dispatch" || marker.dispatchSource === undefined
                ? []
                : [
                    {
                      id: marker.completes ?? marker.invocation,
                      source: marker.dispatchSource,
                      completed: marker.completes !== undefined,
                    },
                  ],
            ),
            outcome: event.outcome,
            cpuMs:
              valid && Number.isFinite(event.cpuTime) && event.cpuTime >= 0 ? event.cpuTime : null,
            wallMs: event.wallTime,
          },
        });

        // One object per native invocation makes delivery retries idempotent and avoids
        // shared counter updates. Required writes complete inside the tail invocation.
        await env.RECORDS.put(
          `${first.object}/${first.target}/${first.invocation}`,
          JSON.stringify(record),
        );
      }),
    );
  },
  async fetch(request: Request, env: Env): Promise<Response> {
    if (!env.BENCH_TOKEN || request.headers.get("authorization") !== `Bearer ${env.BENCH_TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);

    if (url.pathname === "/health") return Response.json({ ok: true, build: env.BUILD });
    const prefix = url.searchParams.get("object");
    const target = Schema.decodeUnknownSync(Target)(url.searchParams.get("target"));

    if (!prefix || !/^[a-z0-9-]+$/.test(prefix))
      return new Response("object required", { status: 400 });

    const page = await env.RECORDS.list({
      prefix: prefix + "/" + target + "/",
      limit: 1000,
      ...(url.searchParams.has("cursor")
        ? { cursor: url.searchParams.get("cursor") ?? undefined }
        : {}),
    });

    const records = await Promise.all(
      page.objects.map(async ({ key }) => {
        const value = await env.RECORDS.get(key);

        // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- The native Tail fetch handler rejects an incomplete R2 listing instead of returning partial CPU evidence.
        if (!value) throw new Error("Missing persisted CPU record");

        return Schema.decodeUnknownSync(Stored)(await value.json());
      }),
    );

    return Response.json(
      Schema.encodeSync(TailPage)({ records, ...(page.truncated ? { cursor: page.cursor } : {}) }),
    );
  },
};
