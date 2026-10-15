import { decodeLiveText } from "@yielded/agent-platform-cloudflare/cloudflare-thread-client";
import { ThreadId } from "@yielded/agent/identifiers";
import { Cause, Clock, Deferred, Effect, Option, Schema, Stream } from "effect";
import { Ndjson } from "effect/encoding";

import {
  BuildEvent,
  BuildRequest,
  DriverRequest,
  Health,
  MeasureRequest,
  DriverRequestId,
  MeasureOutcome,
  StoredMeasurement,
  StoredMeasureResponse,
  type FirstTextSource,
} from "./model.ts";
import {
  AwaitResult,
  ColdResult,
  Failure,
  MetricsResult,
  measurementPaths,
  errorText,
  PrimeResult,
  RunResult,
  SeedResult,
  SeedCheckpoint,
  SettledTextResult,
  SubmitResult,
  TextObservation,
  TransientObjectTransport,
  UnknownOutcome,
} from "./worker/protocol.ts";

class ObservationError extends Schema.TaggedError<ObservationError>()("ObservationError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

const attempt = <A>(body: () => Promise<A>) =>
  Effect.tryPromise({
    try: body,
    catch: (cause) =>
      Schema.is(TransientObjectTransport)(cause)
        ? cause
        : ObservationError.make({ message: errorText(cause), cause }),
  });

interface Env {
  BENCH_TOKEN: string;
  BENCH_PREFIX: string;
  WORKERS_SUBDOMAIN: string;
  BUILD: string;
  MEASUREMENTS: DurableObjectNamespace;
}

const operation = {
  async fetch(
    request: Request,
    env: Env,
    context: Pick<ExecutionContext, "waitUntil">,
  ): Promise<Response> {
    if (request.headers.get("authorization") !== `Bearer ${env.BENCH_TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    if (new URL(request.url).pathname === "/health")
      return Response.json({ ok: true, build: env.BUILD });
    let sample: string | null = null;

    try {
      const address = new URL(request.url);

      const readiness =
        address.pathname === "/measure" &&
        request.method === "GET" &&
        address.searchParams.has("readiness");

      // GET is safe against an older driver: its empty JSON body fails before dispatch.
      const input: unknown = readiness
        ? Schema.decodeUnknownSync(Schema.fromJsonString(DriverRequest))(
            address.searchParams.get("readiness"),
          )
        : await request.json();

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

      sample = query.sample;

      if (readiness) {
        const routes = await Effect.runPromise(
          Effect.forEach(
            measurementPaths(query.target),
            (path) =>
              attempt(async () => {
                const url = new URL(path, target);

                for (const [key, value] of Object.entries(query))
                  url.searchParams.set(key, String(value));
                url.searchParams.set("readiness", "1");

                const response = await fetch(url, {
                  method: "GET",
                  headers: {
                    authorization: `Bearer ${env.BENCH_TOKEN}`,
                    "cache-control": "no-store",
                  },
                  signal: AbortSignal.any([request.signal, AbortSignal.timeout(10_000)]),
                });

                if (
                  !response.ok ||
                  !response.headers.get("content-type")?.includes("application/json")
                ) {
                  await response.body?.cancel();

                  return { path, status: response.status };
                }
                const health = Schema.decodeUnknownSync(Health)(await response.json());

                return { path, status: response.status, build: health.build };
              }).pipe(
                Effect.catchTag("ObservationError", (error) =>
                  Effect.succeed({ path, status: 0, error: error.message }),
                ),
              ),
            { concurrency: 6 },
          ),
        );

        return Response.json({ build: env.BUILD, routes });
      }
      const cancellation = new AbortController();

      const invoke = Effect.fnUntraced(function* (
        path: string,
        body?: unknown,
        timeoutMs = 180_000,
      ) {
        const controller = yield* Effect.acquireRelease(
          Effect.sync(() => new AbortController()),
          (controller) => Effect.sync(() => controller.abort()),
        );

        const url = new URL(path, target);

        for (const [key, value] of Object.entries(query)) url.searchParams.set(key, String(value));

        const response = yield* attempt(() =>
          fetch(url, {
            method: "POST",
            headers: {
              authorization: `Bearer ${env.BENCH_TOKEN}`,
              "content-type": "application/json",
            },
            body: JSON.stringify(body ?? {}),
            signal: AbortSignal.any([
              AbortSignal.timeout(timeoutMs),
              cancellation.signal,
              controller.signal,
            ]),
          }),
        ).pipe(
          Effect.catchTag("ObservationError", (error) =>
            UnknownOutcome.make({ message: `Driver → Target ${path}: ${error.message}` }),
          ),
        );

        if (!response.ok) {
          const text = yield* attempt(() => response.text()).pipe(
            Effect.catchTag("ObservationError", (error) =>
              UnknownOutcome.make({ message: `Driver → Target ${path}: ${error.message}` }),
            ),
          );

          const failure = Schema.decodeOption(Schema.fromJsonString(Failure))(text);

          if (
            Option.isSome(failure) &&
            failure.value.sample === query.sample &&
            failure.value.transport !== undefined
          )
            return yield* failure.value.transport;

          if (Option.isNone(failure) && response.status >= 500)
            return yield* UnknownOutcome.make({
              message: `Target ${path} returned HTTP ${response.status}; input outcome unknown.`,
            });

          return yield* ObservationError.make({
            message: `Target ${path} returned HTTP ${response.status}: ${text}; input will not be retried.`,
          });
        }

        return response;
      });

      const invokeJson = Effect.fnUntraced(function* <
        S extends Schema.Top & { readonly DecodingServices: never },
      >(schema: S, path: string, body?: unknown, timeoutMs?: number) {
        const response = yield* invoke(path, body, timeoutMs);

        return yield* Schema.decodeUnknownEffect(schema)(
          yield* attempt(() => response.json()).pipe(
            Effect.catchTag("ObservationError", (error) =>
              UnknownOutcome.make({ message: `Driver → Target ${path}: ${error.message}` }),
            ),
          ),
        ).pipe(
          Effect.mapError((cause) => ObservationError.make({ message: errorText(cause), cause })),
        );
      });

      if (new URL(request.url).pathname === "/history-checkpoint") {
        const checkpoint = await Effect.runPromise(
          invokeJson(SeedCheckpoint, "/seed-progress").pipe(Effect.scoped),
        );

        return Response.json({
          ...checkpoint,
          driverMs: checkpoint.batch ? Date.now() - checkpoint.batch.startedMs : 0,
        });
      }
      if (new URL(request.url).pathname === "/build-history") {
        const { from, startedMs: previousStart } = Schema.decodeUnknownSync(BuildRequest)(input);
        const startedMs = previousStart ?? Date.now();
        const colo = typeof request.cf?.colo === "string" ? request.cf.colo : null;
        const encoder = new TextEncoder();
        const encode = Schema.encodeSync(Schema.fromJsonString(BuildEvent));
        let cancelled = false;
        let stopHeartbeat = () => {};

        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            // Keep the client socket alive while a batch runs; empty lines are not events.
            const heartbeat = () => {
              if (!cancelled) controller.enqueue(encoder.encode("\n"));
            };

            heartbeat();
            const timer = setInterval(heartbeat, 15_000);

            stopHeartbeat = () => clearInterval(timer);

            const write = (event: BuildEvent) => {
              if (!cancelled) controller.enqueue(encoder.encode(encode(event) + "\n"));
            };

            context.waitUntil(
              (async () => {
                let completed = from;

                const failed = (error: string) =>
                  write({
                    _tag: "Failed",
                    completed,
                    driverMs: Date.now() - startedMs,
                    colo,
                    error,
                  });

                try {
                  const checkpoint = await Effect.runPromise(
                    invokeJson(SeedCheckpoint, "/seed-progress").pipe(Effect.scoped),
                  );

                  if (checkpoint.inFlight || checkpoint.completed !== from || from >= query.history)
                    return failed("History is not at the requested verified batch boundary");
                  if (from > 0) {
                    // Close the preceding runtime before admitting this single batch.
                    const reset = await Effect.runPromise(
                      invokeJson(ColdResult, "/cold").pipe(Effect.scoped),
                    );

                    if (
                      !reset.ok ||
                      !reset.threadAborted ||
                      reset.directoryAborted === false ||
                      reset.before.build !== query.expectedBuild ||
                      (reset.directoryBefore && reset.directoryBefore.build !== query.expectedBuild)
                    )
                      return failed("History batch restart or Object build was not acknowledged");
                  }
                  const to = Math.min(from + 50, query.history);

                  const result = await Effect.runPromise(
                    invokeJson(SeedResult, "/seed", { from, to, startedMs }, 600_000).pipe(
                      Effect.scoped,
                    ),
                  );

                  if (
                    result.from !== from ||
                    result.to !== to ||
                    result.startedMs !== startedMs ||
                    result.identity.build !== query.expectedBuild
                  )
                    return failed("History batch acknowledgement or Object build mismatch");
                  completed = to;
                  write({ _tag: "Batch", result, driverMs: Date.now() - startedMs, colo });
                } catch (cause) {
                  failed(errorText(cause));
                } finally {
                  stopHeartbeat();
                  if (!cancelled) controller.close();
                }
              })(),
            );
          },
          cancel(reason) {
            cancelled = true;
            stopHeartbeat();
            cancellation.abort(reason);
          },
        });

        return new Response(body, {
          headers: { "content-type": "application/x-ndjson", "cache-control": "no-store" },
        });
      }

      const { observeText, cold, expectedDriverBuild, quiescent, resetBeforeMetrics } =
        Schema.decodeUnknownSync(MeasureRequest)(input);

      if (expectedDriverBuild !== env.BUILD)
        return Response.json(
          Schema.encodeSync(Failure)({
            ok: false,
            error: "Driver build mismatch; request was not dispatched",
            sample,
          }),
          { status: 503 },
        );

      return Response.json(
        await Effect.runPromise(
          Effect.gen(function* () {
            const primeAt = yield* Clock.currentTimeMillis;

            const primed = cold ? yield* invokeJson(PrimeResult, "/prime") : undefined;

            const primeMs = primed ? (yield* Clock.currentTimeMillis) - primeAt : 0;
            const constructorAndProbeMs = primed?.constructorAndProbeMs ?? 0;

            if (
              primed &&
              (!primed.identity.firstEntry ||
                primed.identity.priorAlarms !== 0 ||
                primed.identity.build !== query.expectedBuild)
            )
              return yield* ObservationError.make({
                message: "Cold construction was not fresh or reported another build.",
              });
            const ready = yield* Deferred.make<void, ObservationError>();

            type Visible = { atMs: number; text: string; source: FirstTextSource };
            const first = yield* Deferred.make<Visible, ObservationError>();
            const visible = new Map<string, Visible>();
            let input: string | undefined = query.target !== "yielded" ? query.sample : undefined;
            let observationMs: number | null = null;
            let observerOpenedMs: number | undefined;

            if (observeText && query.target !== "tardie") {
              observerOpenedMs = yield* Clock.currentTimeMillis;
              const response = yield* invoke("/text");
              const body = response.body;

              if (!body)
                return yield* ObservationError.make({ message: "Missing text observation body." });

              const bytes = Stream.fromReadableStream({
                evaluate: () => body,
                onError: (cause) =>
                  ObservationError.make({ message: "Text observation disconnected.", cause }),
              });

              const observations =
                query.target === "yielded" &&
                response.headers.get("x-yielded-text-format") === "frame-v1"
                  ? decodeLiveText(ThreadId.make(query.storageObject ?? query.object), bytes).pipe(
                      Stream.flatMap((frame) => {
                        if (frame._tag === "Reset")
                          return Stream.succeed<TextObservation>({ _tag: "Ready" });
                        const event = frame.event;

                        return event._tag === "Text" &&
                          event.part.type === "text-delta" &&
                          event.part.delta
                          ? Stream.succeed<TextObservation>({
                              _tag: "Text",
                              input: event.submissionId,
                              text: event.part.delta,
                            })
                          : Stream.empty;
                      }),
                    )
                  : bytes.pipe(Stream.pipeThroughChannel(Ndjson.decodeSchema(TextObservation)()));

              yield* observations.pipe(
                Stream.concat(
                  Stream.fail(
                    ObservationError.make({
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
                      source:
                        query.target === "yielded"
                          ? "watchText"
                          : query.target === "effect-ai-floor"
                            ? "nativeStream"
                            : "watchEvents",
                    };

                    visible.set(frame.input, seen);
                    if (frame.input === input) yield* Deferred.succeed(first, seen);
                  }),
                ),
                Effect.catchCauseIf(
                  (cause) => !Cause.hasInterrupts(cause),
                  (cause) =>
                    Effect.gen(function* () {
                      const error = ObservationError.make({
                        message: `Public text observation failed: ${Cause.pretty(cause)}`,
                        cause,
                      });

                      yield* Deferred.fail(ready, error);
                      yield* Deferred.fail(first, error);
                    }),
                ),
                Effect.forkScoped,
              );
              yield* Deferred.await(ready);
              observationMs = (yield* Clock.currentTimeMillis) - observerOpenedMs;
            }

            const startedMs = yield* Clock.currentTimeMillis;
            const metricStartMs = startedMs - constructorAndProbeMs;
            let admissionMs: number | undefined;
            let receipt: SubmitResult["receipt"] | undefined;
            let settlementRecordRead = false;

            if (query.target === "yielded") {
              const admission = yield* invokeJson(SubmitResult, "/submit");

              admissionMs = (yield* Clock.currentTimeMillis) - metricStartMs;
              receipt = admission.receipt;
              input = admission.receipt.submissionId;
              const seen = visible.get(input);

              if (seen) yield* Deferred.succeed(first, seen);

              const settled = yield* invokeJson(AwaitResult, "/await", admission.receipt);

              if (settled.settlement.outcome !== "completed")
                return yield* ObservationError.make({
                  message: "Yielded did not complete the turn.",
                });
            } else {
              yield* invokeJson(RunResult, "/run");
            }

            // Preserve the completion endpoint even if the independent text transport arrives later.
            const observedMs = yield* Clock.currentTimeMillis;

            // Overflow can replace an instant reply with an empty draft snapshot after settlement.
            if (observeText && receipt !== undefined && !(yield* Deferred.isDone(first))) {
              // Text may arrive during this read; its source cannot prove the RPC was skipped.
              settlementRecordRead = true;

              const result = yield* invokeJson(SettledTextResult, "/result", receipt);

              const fallback: Visible = {
                atMs: yield* Clock.currentTimeMillis,
                text: result.text,
                source: "settlementRecord",
              };

              yield* Deferred.succeed(first, fallback);
            }

            const text =
              !observeText || query.target === "tardie" ? undefined : yield* Deferred.await(first);

            if (resetBeforeMetrics) {
              const reset = yield* invokeJson(ColdResult, "/cold");

              if (!reset.ok || !reset.threadAborted || reset.before.build !== query.expectedBuild)
                return yield* ObservationError.make({
                  message: "Diagnostic reset was not acknowledged.",
                });
            }

            // Capture volatile evidence before cancelling the observer, after both timer endpoints.
            const metrics = yield* invokeJson(
              MetricsResult,
              quiescent ? "/metrics?quiescent=true" : "/metrics",
            );

            return {
              ok: true,
              metrics,
              driverMs: observedMs - metricStartMs,
              observedMs,
              admissionMs,
              firstTextMs: text === undefined ? null : text.atMs - metricStartMs,
              ...(text === undefined ? {} : { firstText: text.text, firstTextSource: text.source }),
              observationMs,
              settlementRecordRead,
              ...(observerOpenedMs === undefined
                ? {}
                : {
                    observerToCompletionMs: observedMs - observerOpenedMs,
                    ...(text === undefined
                      ? {}
                      : { observerToFirstTextMs: text.atMs - observerOpenedMs }),
                  }),
              constructorAndProbeMs,
              primeMs,
              ...(primed === undefined ? {} : { primed: primed.identity }),
              colo: typeof request.cf?.colo === "string" ? request.cf.colo : null,
            };
          }).pipe(Effect.scoped, Effect.timeout("3 minutes")),
        ),
      );
    } catch (cause) {
      const transport = Schema.is(TransientObjectTransport)(cause) ? cause : undefined;

      const unknownOutcome = Schema.is(UnknownOutcome)(cause)
        ? cause
        : Cause.isTimeoutError(cause)
          ? UnknownOutcome.make({ message: "Driver timed out; input outcome unknown." })
          : undefined;

      return Response.json(
        Schema.encodeSync(Failure)({
          ok: false,
          error: errorText(cause),
          sample,
          ...(transport === undefined ? {} : { transport }),
          ...(unknownOutcome === undefined ? {} : { unknownOutcome }),
        }),
        { status: 502 },
      );
    }
  },
};

const requestKeys = [
  ...Object.keys(MeasureRequest.fields),
  ...Object.keys(DriverRequest.fields.query.fields),
];

/** One acknowledged input per durable identity; an interrupted execution is never restarted. */
let driverIsolate: string | undefined;

export class DriverMeasurement {
  private readonly identity = {
    isolate: (driverIsolate ??= crypto.randomUUID()),
    incarnation: crypto.randomUUID(),
    constructedMs: Date.now(),
  };
  private saved: StoredMeasurement | undefined;
  private running: Promise<void> | undefined;

  constructor(
    private readonly ctx: DurableObjectState,
    private readonly env: Env,
  ) {
    ctx.blockConcurrencyWhile(async () => {
      const stored = await ctx.storage.get<unknown>("measurement");

      if (stored !== undefined) this.saved = Schema.decodeUnknownSync(StoredMeasurement)(stored);
    });
  }

  private async complete(request: Request, input: MeasureRequest): Promise<void> {
    // This request has its own lifetime, independent of the controller connection.
    const response = await operation.fetch(
      new Request(request, {
        method: "POST",
        body: JSON.stringify(input),
        signal: new AbortController().signal,
      }),
      this.env,
      this.ctx,
    );

    const measured = Schema.decodeUnknownSync(StoredMeasureResponse)({
      status: response.status,
      body: await response.json(),
    });

    const outcome: StoredMeasureResponse = {
      ...measured,
      body: measured.body.ok ? { ...measured.body, driverIdentity: this.identity } : measured.body,
    };

    const saved = { request: input, outcome };

    await this.ctx.storage.put("measurement", Schema.encodeSync(StoredMeasurement)(saved));
    this.saved = saved;
  }

  async fetch(request: Request): Promise<Response> {
    const requestId = Schema.decodeUnknownSync(DriverRequestId)(
      new URL(request.url).searchParams.get("requestId"),
    );

    if (request.method === "POST") {
      const input = Schema.decodeUnknownSync(MeasureRequest)(await request.json());

      if (input.requestId !== requestId || input.expectedDriverBuild !== this.env.BUILD)
        return new Response("Driver request identity or build mismatch", { status: 409 });
      if (this.saved !== undefined) {
        if (JSON.stringify(this.saved.request, requestKeys) !== JSON.stringify(input, requestKeys))
          return new Response("Driver request ID already belongs to another input", {
            status: 409,
          });
      } else {
        // Set the in-memory identity before yielding. Storage's input/output gates protect
        // admission; persistence precedes the first outgoing measurement request.
        this.saved = { request: input };
        await this.ctx.storage.put("measurement", Schema.encodeSync(StoredMeasurement)(this.saved));
        this.running = this.complete(request, input);
        // Durable Object waitUntil survives client disconnects. The existing measurement
        // timeout is three minutes; a reset leaves a durable accepted record, never a replay.
        this.ctx.waitUntil(this.running);
      }
      if (this.running !== undefined) await this.running;
    } else if (request.method !== "GET") {
      return new Response("method not allowed", { status: 405 });
    }

    const identity = { requestId, build: this.env.BUILD };

    const outcome: MeasureOutcome =
      this.saved === undefined
        ? { _tag: "NotReceived", ...identity }
        : this.saved.outcome === undefined
          ? { _tag: "Running", ...identity }
          : { _tag: "Complete", ...identity, outcome: this.saved.outcome };

    return Response.json(Schema.encodeSync(MeasureOutcome)(outcome));
  }
}

export default {
  async fetch(request: Request, env: Env, context: ExecutionContext): Promise<Response> {
    if (request.headers.get("authorization") !== `Bearer ${env.BENCH_TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    const address = new URL(request.url);

    if (address.pathname !== "/measure" || address.searchParams.has("readiness"))
      return operation.fetch(request, env, context);
    try {
      const input =
        request.method === "POST"
          ? Schema.decodeUnknownSync(MeasureRequest)(await request.json())
          : undefined;

      const requestId = Schema.decodeUnknownSync(DriverRequestId)(
        input?.requestId ?? address.searchParams.get("requestId"),
      );

      address.searchParams.set("requestId", requestId);
      const stub = env.MEASUREMENTS.getByName(requestId, { locationHint: "wnam" });

      return await stub.fetch(
        new Request(address, {
          method: request.method,
          headers: request.headers,
          ...(input === undefined ? {} : { body: JSON.stringify(input) }),
          cf: request.cf,
        }),
      );
    } catch (cause) {
      return new Response(`Driver request unavailable: ${errorText(cause)}`, { status: 503 });
    }
  },
};
