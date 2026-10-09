import { Clock, Deferred, Effect, Schema, Stream } from "effect";
import { Ndjson } from "effect/encoding";

import { MeasureRequest } from "./model.ts";
import { AwaitResult, RunResult, SubmitResult, TextObservation } from "./worker/protocol.ts";

class ObservationError extends Schema.TaggedError<ObservationError>()("ObservationError", {
  message: Schema.String,
}) {}

interface Env {
  BENCH_TOKEN: string;
  BENCH_PREFIX: string;
  WORKERS_SUBDOMAIN: string;
  BUILD: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.headers.get("authorization") !== `Bearer ${env.BENCH_TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    if (new URL(request.url).pathname === "/health")
      return Response.json({ ok: true, build: env.BUILD });
    try {
      const input: unknown = await request.json();

      const { targetUrl } = Schema.decodeUnknownSync(Schema.Struct({ targetUrl: Schema.String }))(
        input,
      );

      const target = new URL(targetUrl);

      if (
        target.protocol !== "https:" ||
        target.username ||
        target.password ||
        !target.hostname.startsWith(env.BENCH_PREFIX + "-") ||
        !target.hostname.endsWith(`.${env.WORKERS_SUBDOMAIN}.workers.dev`)
      )
        return new Response("invalid target", { status: 400 });
      if (new URL(request.url).pathname === "/ready")
        return fetch(new URL("/health?probe=" + Date.now(), target), {
          headers: { authorization: `Bearer ${env.BENCH_TOKEN}`, "cache-control": "no-store" },
        });
      const { query } = Schema.decodeUnknownSync(MeasureRequest)(input);

      const invoke = async (path: string, body?: unknown) => {
        const url = new URL(path, target);

        for (const [key, value] of Object.entries(query)) url.searchParams.set(key, String(value));

        const response = await fetch(url, {
          method: "POST",
          headers: {
            authorization: `Bearer ${env.BENCH_TOKEN}`,
            "content-type": "application/json",
          },
          body: JSON.stringify(body ?? {}),
          signal: AbortSignal.timeout(180_000),
        });

        if (!response.ok)
          throw new Error(
            `Target ${path} returned HTTP ${response.status}; turn outcome may be unknown.`,
          );

        return response;
      };

      return Response.json(
        await Effect.runPromise(
          Effect.gen(function* () {
            const ready = yield* Deferred.make<void, ObservationError>();

            type Visible = { atMs: number; text: string };
            const first = yield* Deferred.make<Visible, ObservationError>();
            const visible = new Map<string, Visible>();
            let input: string | undefined = query.target === "pi" ? query.sample : undefined;
            let observationMs: number | null = null;

            if (query.target !== "tardie") {
              const opening = yield* Clock.currentTimeMillis;
              const response = yield* Effect.tryPromise(() => invoke("/text"));
              const body = response.body;

              if (!body)
                return yield* new ObservationError({ message: "Missing text observation body." });

              yield* Stream.fromReadableStream({
                evaluate: () => body,
                onError: () => new ObservationError({ message: "Text observation disconnected." }),
              }).pipe(
                Stream.pipeThroughChannel(Ndjson.decodeSchema(TextObservation)()),
                Stream.concat(
                  Stream.fail(
                    new ObservationError({
                      message: "Text observation ended before cancellation.",
                    }),
                  ),
                ),
                Stream.runForEach((frame) =>
                  Effect.gen(function* () {
                    if (frame._tag === "Ready") {
                      yield* Deferred.succeed(ready, undefined);

                      return;
                    }
                    if (!frame.text.trim()) return;

                    const seen = visible.get(frame.input) ?? {
                      atMs: yield* Clock.currentTimeMillis,
                      text: frame.text,
                    };

                    visible.set(frame.input, seen);
                    if (frame.input === input) yield* Deferred.succeed(first, seen);
                  }),
                ),
                Effect.catchCause(() =>
                  Effect.gen(function* () {
                    const error = new ObservationError({
                      message: "Public text observation failed; no first-text result is available.",
                    });

                    yield* Deferred.fail(ready, error);
                    yield* Deferred.fail(first, error);
                  }),
                ),
                Effect.forkScoped,
              );
              yield* Deferred.await(ready);
              observationMs = (yield* Clock.currentTimeMillis) - opening;
            }

            const startedMs = yield* Clock.currentTimeMillis;
            let admissionMs: number | undefined;

            if (query.target === "yielded") {
              const admission = yield* Effect.tryPromise(async () =>
                Schema.decodeUnknownSync(SubmitResult)(await (await invoke("/submit")).json()),
              );

              admissionMs = (yield* Clock.currentTimeMillis) - startedMs;
              input = admission.receipt.submissionId;
              const seen = visible.get(input);

              if (seen) yield* Deferred.succeed(first, seen);

              const settled = yield* Effect.tryPromise(async () =>
                Schema.decodeUnknownSync(AwaitResult)(
                  await (await invoke("/await", admission.receipt)).json(),
                ),
              );

              if (settled.settlement.outcome !== "completed")
                return yield* new ObservationError({
                  message: "Yielded did not complete the turn.",
                });
            } else {
              yield* Effect.tryPromise(async () =>
                Schema.decodeUnknownSync(RunResult)(await (await invoke("/run")).json()),
              );
            }

            // Preserve the completion endpoint even if the independent text transport arrives later.
            const observedMs = yield* Clock.currentTimeMillis;
            const text = query.target === "tardie" ? undefined : yield* Deferred.await(first);

            return {
              ok: true,
              driverMs: observedMs - startedMs,
              startedMs,
              observedMs,
              admissionMs,
              firstTextMs: text === undefined ? null : text.atMs - startedMs,
              ...(text === undefined ? {} : { firstText: text.text }),
              observationMs,
              colo: typeof request.cf?.colo === "string" ? request.cf.colo : null,
            };
          }).pipe(Effect.scoped, Effect.timeout("3 minutes")),
        ),
      );
    } catch (cause) {
      return Response.json(
        {
          ok: false,
          error:
            cause instanceof Error
              ? cause.message
              : "Turn failed or response was lost; do not resubmit this input.",
        },
        { status: 502 },
      );
    }
  },
};
