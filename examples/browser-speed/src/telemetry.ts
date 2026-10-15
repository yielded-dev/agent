import { OpenAiClient } from "@effect/ai-openai";
import {
  Cause,
  Clock,
  Context,
  DateTime,
  Effect,
  Exit,
  Layer,
  Option,
  Schema,
  Stream,
  Tracer,
} from "effect";
import { LanguageModel, Telemetry, type Response } from "effect/ai";
import { HttpClient, HttpClientResponse } from "effect/http";
import { OtlpSerialization, OtlpTracer } from "effect/observability";

import {
  browserCommandTimeoutMillis,
  LabError,
  modelOutputEntryLimit,
  modelOutputLimit,
  type ModelOutput,
  type Phase,
  type Report,
  type RunInput,
  type Span,
} from "./contract.ts";
import type { TraceSink } from "./trace-store.ts";

type Details = Partial<
  Pick<
    Span,
    | "name"
    | "inputTokens"
    | "outputTokens"
    | "reasoningTokens"
    | "reasoningSummary"
    | "serviceTier"
    | "bytes"
    | "model"
  >
>;

export const makeTrace = Effect.fnUntraced(function* (
  input: RunInput,
  model: string,
  sink?: TraceSink,
  secrets: ReadonlyArray<string> = [],
) {
  const clock = yield* Clock.Clock;
  const origin = clock.monotonicTimeNanosUnsafe();
  const startedAt = DateTime.formatIso(yield* DateTime.now);
  const spans: Array<Span> = [];
  const modelOutput: Array<ModelOutput> = [];
  const outputIds = new Map<string, number>();
  let outputLength = 0;
  let outputTruncated = false;
  let turn = 0;
  let sequence = 0;
  const now = () => Number(clock.monotonicTimeNanosUnsafe() - origin) / 1_000_000;

  // Retain website observations and generated source, never provider or viewer credentials.
  const redact = (value: unknown): Schema.Json => {
    const seen = new WeakSet<object>();

    const json = JSON.stringify(value, (key, value: unknown) => {
      if (
        /^(authorization|cookie|set-cookie|password|api[-_]?key|access[-_]?token|refresh[-_]?token|encrypted_content|devtoolsFrontendUrl|webSocketDebuggerUrl)$/i.test(
          key,
        )
      )
        return "[redacted]";
      if (typeof value === "bigint") return String(value);
      if (typeof value === "object" && value !== null) {
        if (seen.has(value)) return "[repeated reference]";
        seen.add(value);
      }
      if (typeof value !== "string") return value;
      let text = value;

      if (text.startsWith("data:image/")) return "[image omitted]";

      for (const secret of secrets)
        if (secret.length >= 8) text = text.replaceAll(secret, "[redacted]");

      return text
        .replace(
          /(?:https?|wss?):\/\/(?:live\.browser\.run|[^\s"<>]+[?&](?:jwt|token|api_key|signature)=)[^\s"<>]*/gi,
          "[redacted capability]",
        )
        .replace(/\bBearer\s+[A-Za-z0-9._~+/-]+/gi, "Bearer [redacted]");
    });

    return Schema.decodeSync(Schema.fromJsonString(Schema.Json))(json ?? "null");
  };

  const event = (kind: string, value: unknown) => {
    if (sink === undefined) return;
    const data = redact(value);
    const json = JSON.stringify(data);
    const bytes = new TextEncoder().encode(json).byteLength;

    sink.append({
      sequence: sequence++,
      at: now(),
      kind,
      data:
        bytes <= 1_000_000
          ? data
          : {
              truncated: true,
              originalBytes: bytes,
              preview: json.slice(0, 200_000),
            },
    });
  };

  let report: Report = {
    version: 3,
    fixture: input.scenario === "wikipedia" ? "wikipedia-race-v1" : "website",
    input,
    model,
    commandTimeoutMillis: browserCommandTimeoutMillis,
    startedAt,
    status: "running",
    message: "Opening browser…",
    spans,
    elapsed: 0,
    readyAt: null,
    finishedAt: null,
    verifiedAt: null,
    firstActionAt: null,
    cleanup: "pending",
  };

  const snapshot = (): Report => ({
    ...report,
    spans: [...spans],
    modelOutput: [...modelOutput],
    outputTruncated,
    elapsed: report.status === "running" ? now() : report.elapsed,
  });

  sink?.checkpoint(snapshot());
  event("run.input", { input, model });

  const begin = (phase: Phase, name: string) => {
    if (phase === "model") turn++;
    const id = spans.length;
    const start = now();
    const spanTurn = turn;

    spans.push({ id, phase, name, start, duration: null, outcome: "running", turn: spanTurn });
    event("step.started", spans[id]);

    const end = (exit: Exit.Exit<unknown, unknown>, details: Details = {}) => {
      const failure = Exit.isFailure(exit)
        ? Option.flatMap(Cause.findErrorOption(exit.cause), Schema.decodeUnknownOption(LabError))
        : Option.none();

      spans[id] = {
        ...spans[id],
        ...details,
        ...(Option.isSome(failure) ? { error: failure.value.message } : {}),
        duration: now() - start,
        outcome: Exit.isSuccess(exit)
          ? "success"
          : Cause.hasInterrupts(exit.cause)
            ? "interrupted"
            : Cause.hasDies(exit.cause)
              ? "defect"
              : "failure",
      };
      if (phase === "action" && Exit.isSuccess(exit) && report.firstActionAt === null)
        report = { ...report, firstActionAt: now() };
      event("step.finished", {
        ...spans[id],
        ...(Exit.isFailure(exit) ? { cause: Cause.pretty(exit.cause) } : {}),
      });
      sink?.checkpoint(snapshot());
    };

    return {
      end,
      annotate: (details: Details) => {
        const span = spans[id];

        if (span !== undefined) spans[id] = { ...span, ...details };
      },
    };
  };

  return {
    now,
    begin,
    event,
    // The demo admits one model call at a time. Native IDs are scoped to that span;
    // repeated IDs in later model turns must never overwrite earlier text.
    modelPart: (
      part:
        | Response.TextDeltaPart
        | Response.ReasoningDeltaPart
        | Response.TextEndPart
        | Response.ReasoningEndPart,
    ) => {
      const span = spans.findLast((span) => span.phase === "model" && span.outcome === "running");

      if (span === undefined) return;
      const kind = part.type.startsWith("text-") ? "text" : "reasoning-summary";
      const key = `${span.id}:${kind}:${part.id}`;
      const index = outputIds.get(key);

      if (part.type === "text-end" || part.type === "reasoning-end") {
        if (index !== undefined) modelOutput[index] = { ...modelOutput[index], complete: true };

        return;
      }
      const delta = part.delta.slice(0, modelOutputLimit - outputLength);

      if (!delta || (index === undefined && modelOutput.length === modelOutputEntryLimit)) {
        if (part.delta.length > 0) outputTruncated = true;

        return;
      }
      if (delta.length < part.delta.length) outputTruncated = true;
      outputLength += delta.length;
      if (index === undefined) {
        outputIds.set(key, modelOutput.length);
        modelOutput.push({
          id: modelOutput.length,
          spanId: span.id,
          kind,
          text: delta,
          at: now(),
          complete: false,
        });
      } else modelOutput[index] = { ...modelOutput[index], text: modelOutput[index].text + delta };
    },
    // Called only after the initial page and observation are ready, before any decision or action.
    // Never reset this boundary: a flow failure must not become a preparation retry.
    ready: () => {
      if (report.readyAt === null) report = { ...report, readyAt: now() };
    },
    // The lab permits one model call at a time; provider evidence belongs to its active span.
    annotateModel: (details: Details) => {
      const index = spans.findLastIndex(
        (span) => span.phase === "model" && span.outcome === "running",
      );

      if (index >= 0) spans[index] = { ...spans[index], ...details };
    },
    snapshot,
    update: (patch: Partial<Report>) => {
      report = { ...report, ...patch };
      sink?.checkpoint(snapshot());
    },
    measure: Effect.fnUntraced(function* <A, E, R>(
      phase: Phase,
      name: string,
      effect: Effect.Effect<A, E, R>,
      details?: (value: A) => Details,
      input?: unknown,
    ) {
      const { end } = begin(phase, name);

      event("operation.input", { name, input });

      return yield* effect.pipe(
        Effect.onExit((exit) =>
          Effect.sync(() => {
            event("operation.output", {
              name,
              ...(Exit.isSuccess(exit)
                ? {
                    output:
                      phase === "capture" || name === "Page.captureScreenshot"
                        ? "[image omitted]"
                        : exit.value,
                  }
                : { cause: Cause.pretty(exit.cause) }),
            });
            end(exit, Exit.isSuccess(exit) ? details?.(exit.value) : undefined);
          }),
        ),
      );
    }),
  };
});

export class Trace extends Context.Service<Trace, Effect.Success<ReturnType<typeof makeTrace>>>()(
  "browser-speed/Trace",
) {}

/** Use Effect's actual OTLP serializer; the transport writes locally instead of making HTTP calls. */
export const traceOtlp = (trace: Trace["Service"], runId: string) =>
  OtlpTracer.layer({
    url: "https://trace.invalid/v1/traces",
    resource: { serviceName: "browser-use", attributes: { "run.id": runId } },
    maxBatchSize: 16,
    exportInterval: "1 second",
  }).pipe(
    Layer.provide(OtlpSerialization.layerJson),
    Layer.provide(
      Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make((request) =>
          Effect.sync(() => {
            if (request.body._tag === "Uint8Array")
              trace.event(
                "otlp",
                Schema.decodeSync(Schema.fromJsonString(Schema.Json))(
                  new TextDecoder().decode(request.body.body),
                ),
              );

            return HttpClientResponse.fromWeb(request, new globalThis.Response("{}"));
          }).pipe(Effect.withTracerEnabled(false)),
        ),
      ),
    ),
  );

/** Observe native parts for every provider; the final finish-tool message remains report.message. */
export const traceModelStream = <Services, E, R>(
  modelLayer: Layer.Layer<LanguageModel.LanguageModel | Services, E, R>,
) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const model = yield* LanguageModel.LanguageModel;
      const trace = yield* Trace;

      // The no-error/no-requirement tap preserves every native overload's tool,
      // error and service types; parameters and response parts pass through unchanged.
      const streamText = ((options: Parameters<typeof model.streamText>[0]) =>
        Stream.suspend(() => {
          trace.event("model.request", { prompt: options.prompt });

          return model.streamText(options).pipe(
            Stream.tap((part) =>
              Effect.sync(() => {
                trace.event("model.part", part);
                if (
                  part.type === "text-delta" ||
                  part.type === "reasoning-delta" ||
                  part.type === "text-end" ||
                  part.type === "reasoning-end"
                )
                  trace.modelPart(part);
              }),
            ),
          );
        })) as typeof model.streamText;

      return LanguageModel.LanguageModel.of({
        ...model,
        streamText,
      });
    }),
  ).pipe(Layer.provideMerge(modelLayer));

const decodeServedTier = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literals(["response.completed", "response.incomplete", "response.failed"]),
    response: Schema.Struct({ service_tier: Schema.String }),
  }),
);

const decodeSearchEvent = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literals(["response.output_item.added", "response.output_item.done"]),
    item: Schema.Struct({
      type: Schema.Literal("web_search_call"),
      id: Schema.String,
      status: Schema.optionalKey(Schema.String),
      action: Schema.optionalKey(
        Schema.Struct({
          type: Schema.String,
          queries: Schema.optionalKey(Schema.Array(Schema.String)),
          query: Schema.optionalKey(Schema.String),
          url: Schema.optionalKey(Schema.String),
        }),
      ),
    }),
  }),
);

/** Preserve raw served tiers and trace hosted searches inside their model calls. */
export const traceOpenAiClient = Effect.gen(function* () {
  const client = yield* OpenAiClient.OpenAiClient;
  const trace = yield* Trace;
  const searches = new Map<string, ReturnType<typeof trace.begin>>();

  const searchEvent = (event: unknown) => {
    const decoded = decodeSearchEvent(event);

    if (Option.isNone(decoded)) return;
    const { type, item } = decoded.value;
    const handle = searches.get(item.id) ?? trace.begin("action", "Web search");

    if (type === "response.output_item.added") {
      searches.set(item.id, handle);
      trace.update({ message: "Searching the web…" });
    } else {
      const query = item.action?.queries?.join(" · ") ?? item.action?.query ?? item.action?.url;

      handle.end(
        item.status === "completed"
          ? Exit.void
          : Exit.fail(LabError.make({ code: "browser", message: "Web search did not complete." })),
        { name: `Web search${query ? ` · ${query.slice(0, 240)}` : ""}` },
      );
      searches.delete(item.id);
      if (searches.size === 0) trace.update({ message: "Agent is working…" });
    }
  };

  return OpenAiClient.OpenAiClient.of({
    ...client,
    createResponse: (input) =>
      Effect.sync(() => trace.event("provider.request", input)).pipe(
        Effect.andThen(client.createResponse(input)),
        Effect.tap(([response]) =>
          Effect.sync(() => {
            if (response.service_tier !== undefined)
              trace.annotateModel({ serviceTier: response.service_tier });
            for (const item of response.output)
              searchEvent({ type: "response.output_item.done", item });
          }),
        ),
      ),
    createResponseStream: (input) =>
      Effect.sync(() => trace.event("provider.request", input)).pipe(
        Effect.andThen(client.createResponseStream(input)),
        Effect.tap(([response]) =>
          Effect.sync(() =>
            trace.event("provider.response", {
              status: response.status,
              requestId: response.headers["x-request-id"],
              processingMillis: response.headers["openai-processing-ms"],
            }),
          ),
        ),
        Effect.map(
          ([response, events]) =>
            [
              response,
              events.pipe(
                Stream.tap((event) =>
                  Effect.sync(() => {
                    searchEvent(event);
                    const evidence = decodeServedTier(event);

                    if (Option.isSome(evidence))
                      trace.annotateModel({ serviceTier: evidence.value.response.service_tier });
                  }),
                ),
                Stream.onExit((exit) =>
                  Effect.sync(() => {
                    for (const handle of searches.values())
                      handle.end(
                        Exit.isFailure(exit)
                          ? exit
                          : Exit.fail(
                              LabError.make({
                                code: "browser",
                                message: "Web search ended without a completion event.",
                              }),
                            ),
                      );
                    searches.clear();
                  }),
                ),
              ),
            ] as const,
        ),
      ),
  });
});

/** Tap native model spans without replacing the model service. */
export const traceModels = Effect.fnUntraced(function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
  const trace = yield* Trace;
  const delegate = yield* Tracer.Tracer;
  const parentTransformer = yield* Effect.serviceOption(Telemetry.CurrentSpanTransformer);
  const handles = new Map<string, ReturnType<typeof trace.begin>>();

  const tracer = Tracer.make({
    span(options) {
      const span = delegate.span(options);

      if (!options.name.startsWith("chat ") && !options.name.startsWith("LanguageModel."))
        return span;
      const finish = trace.begin("model", options.name);

      handles.set(span.spanId, finish);

      return {
        _tag: span._tag,
        name: span.name,
        spanId: span.spanId,
        traceId: span.traceId,
        parent: span.parent,
        annotations: span.annotations,
        attributes: span.attributes,
        links: span.links,
        sampled: span.sampled,
        kind: span.kind,
        attribute: span.attribute.bind(span),
        event: span.event.bind(span),
        addLinks: span.addLinks.bind(span),
        get status() {
          return span.status;
        },
        end(time, exit) {
          finish.end(exit);
          span.end(time, exit);
        },
      };
    },
    ...(delegate.context === undefined ? {} : { context: delegate.context.bind(delegate) }),
  });

  return yield* effect.pipe(
    Effect.provideService(Tracer.Tracer, tracer),
    Effect.provideService(Telemetry.CurrentSpanTransformer, (options) => {
      if (Option.isSome(parentTransformer)) parentTransformer.value(options);
      const { span, response } = options;
      let value: Details = {};

      for (const part of response) {
        if (part.type === "response-metadata" && part.modelId !== undefined)
          value = { ...value, model: part.modelId };
        if (part.type === "reasoning")
          value = {
            ...value,
            reasoningSummary: `${value.reasoningSummary ?? ""}${part.text}`.slice(0, 16_000),
          };
        if (part.type === "reasoning-delta")
          value = {
            ...value,
            reasoningSummary: `${value.reasoningSummary ?? ""}${part.delta}`.slice(0, 16_000),
          };
        if (part.type === "finish")
          value = {
            ...value,
            ...(part.usage.inputTokens.total === undefined
              ? {}
              : { inputTokens: part.usage.inputTokens.total }),
            ...(part.usage.outputTokens.total === undefined
              ? {}
              : { outputTokens: part.usage.outputTokens.total }),
            ...(part.usage.outputTokens.reasoning === undefined
              ? {}
              : { reasoningTokens: part.usage.outputTokens.reasoning }),
          };
      }
      // Streaming response metadata can finalize after the native timing span.
      handles.get(span.spanId)?.annotate(value);
    }),
  );
});
