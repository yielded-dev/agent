import { Clock, Deferred, Effect, Schema, Stream } from "effect";
import { Ndjson } from "effect/encoding";

import { BuildEvent, DriverRequest, MeasureRequest, type FirstTextSource } from "./model.ts";
import {
  AwaitResult,
  ColdResult,
  errorText,
  PrimeResult,
  RunResult,
  SeedResult,
  SettledTextResult,
  SubmitResult,
  TextObservation,
} from "./worker/protocol.ts";

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
  async fetch(request: Request, env: Env, context: ExecutionContext): Promise<Response> {
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
      const { query } = Schema.decodeUnknownSync(DriverRequest)(input);
      const cancellation = new AbortController();

      const invoke = async (path: string, body?: unknown, timeoutMs = 180_000) => {
        const url = new URL(path, target);

        for (const [key, value] of Object.entries(query)) url.searchParams.set(key, String(value));

        const response = await fetch(url, {
          method: "POST",
          headers: {
            authorization: `Bearer ${env.BENCH_TOKEN}`,
            "content-type": "application/json",
          },
          body: JSON.stringify(body ?? {}),
          signal: AbortSignal.any([AbortSignal.timeout(timeoutMs), cancellation.signal]),
        });

        if (!response.ok)
          throw new Error(
            `Target ${path} returned HTTP ${response.status}: ${await response.text()}; input will not be retried.`,
          );

        return response;
      };

      if (new URL(request.url).pathname === "/build-history") {
        const startedMs = Date.now();
        const colo = typeof request.cf?.colo === "string" ? request.cf.colo : null;
        const encoder = new TextEncoder();
        const encode = Schema.encodeSync(Schema.fromJsonString(BuildEvent));
        let cancelled = false;

        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            // Flush headers before a long batch; empty lines are not measurement events.
            controller.enqueue(encoder.encode("\n"));

            const write = (event: BuildEvent) => {
              if (!cancelled) controller.enqueue(encoder.encode(encode(event) + "\n"));
            };

            context.waitUntil(
              (async () => {
                let completed = 0;

                try {
                  while (completed < query.history) {
                    const to = Math.min(completed + 50, query.history);

                    const result = Schema.decodeUnknownSync(SeedResult)(
                      await (await invoke("/seed", { from: completed, to }, 600_000)).json(),
                    );

                    if (
                      result.from !== completed ||
                      result.to !== to ||
                      result.identity.build !== query.expectedBuild
                    )
                      throw new Error("History batch acknowledgement or Object build mismatch");
                    completed = to;
                    write({ _tag: "Batch", result, driverMs: Date.now() - startedMs, colo });
                    if (completed < query.history) {
                      // Mario's seeder also disposes its runtime after every 50 turns.
                      const reset = Schema.decodeUnknownSync(ColdResult)(
                        await (await invoke("/cold")).json(),
                      );

                      if (
                        !reset.ok ||
                        !reset.threadAborted ||
                        reset.directoryAborted === false ||
                        reset.before.build !== query.expectedBuild ||
                        (reset.directoryBefore &&
                          reset.directoryBefore.build !== query.expectedBuild)
                      )
                        throw new Error(
                          "History batch restart or Object build was not acknowledged",
                        );
                    }
                  }
                } catch (cause) {
                  write({
                    _tag: "Failed",
                    completed,
                    driverMs: Date.now() - startedMs,
                    colo,
                    error: errorText(cause),
                  });
                } finally {
                  if (!cancelled) controller.close();
                }
              })(),
            );
          },
          cancel(reason) {
            cancelled = true;
            cancellation.abort(reason);
          },
        });

        return new Response(body, {
          headers: { "content-type": "application/x-ndjson", "cache-control": "no-store" },
        });
      }
      const { observeText, cold } = Schema.decodeUnknownSync(MeasureRequest)(input);

      return Response.json(
        await Effect.runPromise(
          Effect.gen(function* () {
            const primeAt = yield* Clock.currentTimeMillis;

            const primed = cold
              ? yield* Effect.tryPromise(async () =>
                  Schema.decodeUnknownSync(PrimeResult)(await (await invoke("/prime")).json()),
                )
              : undefined;

            const primeMs = primed ? (yield* Clock.currentTimeMillis) - primeAt : 0;
            const constructorAndProbeMs = primed?.constructorAndProbeMs ?? 0;

            if (
              primed &&
              (!primed.identity.firstEntry ||
                primed.identity.priorAlarms !== 0 ||
                primed.identity.build !== query.expectedBuild)
            )
              return yield* new ObservationError({
                message: "Cold construction was not fresh or reported another build.",
              });
            const ready = yield* Deferred.make<void, ObservationError>();

            type Visible = { atMs: number; text: string; source: FirstTextSource };
            const first = yield* Deferred.make<Visible, ObservationError>();
            const visible = new Map<string, Visible>();
            let input: string | undefined = query.target === "pi" ? query.sample : undefined;
            let observationMs: number | null = null;

            if (observeText && query.target !== "tardie") {
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

                    const seen: Visible = visible.get(frame.input) ?? {
                      atMs: yield* Clock.currentTimeMillis,
                      text: frame.text,
                      source: query.target === "yielded" ? "watchText" : "watchEvents",
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
            let receipt: SubmitResult["receipt"] | undefined;

            if (query.target === "yielded") {
              const admission = yield* Effect.tryPromise(async () =>
                Schema.decodeUnknownSync(SubmitResult)(await (await invoke("/submit")).json()),
              );

              admissionMs = (yield* Clock.currentTimeMillis) - startedMs;
              receipt = admission.receipt;
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

            // Overflow can replace an instant reply with an empty draft snapshot after settlement.
            if (observeText && receipt !== undefined && !(yield* Deferred.isDone(first))) {
              const result = yield* Effect.tryPromise(async () =>
                Schema.decodeUnknownSync(SettledTextResult)(
                  await (await invoke("/result", receipt)).json(),
                ),
              );

              const fallback: Visible = {
                atMs: yield* Clock.currentTimeMillis,
                text: result.text,
                source: "settlementRecord",
              };

              yield* Deferred.succeed(first, fallback);
            }

            const text =
              !observeText || query.target === "tardie" ? undefined : yield* Deferred.await(first);

            return {
              ok: true,
              driverMs: constructorAndProbeMs + observedMs - startedMs,
              observedMs,
              admissionMs,
              firstTextMs: text === undefined ? null : text.atMs - startedMs,
              ...(text === undefined ? {} : { firstText: text.text, firstTextSource: text.source }),
              observationMs,
              constructorAndProbeMs,
              primeMs,
              ...(primed === undefined ? {} : { primed: primed.identity }),
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
