import { OpenAiClient, OpenAiConfig, OpenAiLanguageModel } from "@effect/ai-openai";
import {
  ContextCompactor,
  NativeCompactionProvider,
  type NativeCompaction,
  type NativeCompactionResult,
} from "@yielded/agent/context-compactor";
import { Cause, Effect, Layer, Option, Schema, Stream } from "effect";
import { AiError, Model, ResponseIdTracker } from "effect/ai";
import {
  HttpBody,
  HttpClient,
  HttpClientError,
  HttpClientRequest,
  type HttpClientResponse,
} from "effect/http";

import {
  byteLength,
  completeItems,
  decodeItems,
  decodeReply,
  decodeUsage,
  errorUsage,
  format,
  Identity,
  invalidInput,
  invalidOutput,
  makeContext,
  MAX_BODY_BYTES,
  MAX_ITEMS,
  provider,
  readOutput,
  readWindow,
  responseUsage,
  TokenCount,
  WireItems,
  type WireItem,
} from "./internal/codec.ts";
import { encodePrompt } from "./internal/prompt.ts";

type Service = NativeCompactionProvider["Service"];
type CompactRequest = Parameters<Service["compact"]>[0];
const decodeModel = Schema.decodeEffect(Identity);

const decodeJsonBody = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);

const decodeJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));
const encodeJsonBody = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

const decodeReplayRequest = Schema.decodeUnknownEffect(
  Schema.Struct({
    model: Identity,
    input: Schema.Array(Schema.Record(Schema.String, Schema.Unknown)),
    store: Schema.Literal(false),
    previous_response_id: Schema.optionalKey(Schema.Null),
    conversation: Schema.optionalKey(Schema.Null),
    background: Schema.optionalKey(Schema.Literal(false)),
    truncation: Schema.optionalKey(Schema.Literal("disabled")),
    context_management: Schema.optionalKey(Schema.Null),
  }),
);

const decodeCompactRequest = Schema.decodeUnknownEffect(
  Schema.Struct({
    model: Identity,
    input: Schema.Unknown,
    instructions: Schema.optionalKey(Schema.NullOr(Schema.String)),
    prompt_cache_key: Schema.optionalKey(Schema.NullOr(Schema.String)),
    prompt_cache_retention: Schema.optionalKey(
      Schema.NullOr(Schema.Literals(["in_memory", "in-memory", "24h"])),
    ),
    service_tier: Schema.optionalKey(
      Schema.NullOr(Schema.Literals(["auto", "default", "flex", "priority"])),
    ),
  }),
  { onExcessProperty: "error" },
);

const decodeCountRequest = Schema.decodeUnknownEffect(
  Schema.Struct({
    model: Identity,
    input: WireItems,
    truncation: Schema.Literal("disabled"),
  }),
  { onExcessProperty: "error" },
);

const decodeCountReply = Schema.decodeUnknownEffect(
  Schema.Struct({ object: Schema.Literal("response.input_tokens"), input_tokens: TokenCount }),
  { onExcessProperty: "error" },
);

const isItemReference = Schema.is(Schema.Struct({ type: Schema.Literal("item_reference") }));
const sameItems = Schema.toEquivalence(WireItems);

const configuration = Effect.fnUntraced(function* (
  method: "compact" | "replay",
  model: string,
): Effect.fn.Return<typeof OpenAiLanguageModel.Config.Service | undefined, AiError.AiError> {
  yield* decodeModel(model).pipe(
    Effect.mapError(() =>
      invalidInput(method, "A bounded nonempty OpenAI request model is required"),
    ),
  );
  if (
    Option.isSome(yield* Effect.serviceOption(ResponseIdTracker.ResponseIdTracker)) ||
    Option.isSome(yield* Effect.serviceOption(OpenAiClient.OpenAiSocket))
  ) {
    return yield* invalidInput(
      method,
      "Native compaction requires HTTP without response tracking or WebSocket mode",
    );
  }
  const selectedProvider = Option.getOrUndefined(yield* Effect.serviceOption(Model.ProviderName));

  if (selectedProvider !== undefined && selectedProvider !== provider) {
    return yield* invalidInput(method, "Native context provider affinity mismatch");
  }
  const config = Option.getOrUndefined(yield* Effect.serviceOption(OpenAiLanguageModel.Config));

  if (config?.model !== undefined && config.model !== model) {
    return yield* invalidInput(method, "Native context request-model affinity mismatch");
  }
  if (
    config?.store === true ||
    config?.previous_response_id !== undefined ||
    config?.conversation !== undefined ||
    config?.useItemReferences === true ||
    config?.background === true ||
    config?.truncation === "auto"
  ) {
    return yield* invalidInput(method, "Native context requires stateless full-input requests");
  }

  return config;
});

const retainedInput = Effect.fnUntraced(function* (
  model: string,
  windows: ReadonlyArray<NativeCompaction>,
): Effect.fn.Return<ReadonlyArray<WireItem>, AiError.AiError> {
  if (windows.length > MAX_ITEMS) return yield* invalidInput("validate", "Too many native windows");
  const output: Array<WireItem> = [];

  for (const window of windows) {
    const data = yield* readWindow(window);

    if (window.affinity.model !== model) {
      return yield* invalidInput("validate", "Native context request-model affinity mismatch");
    }
    if (output.length + data.output.length > MAX_ITEMS) {
      return yield* invalidInput("validate", "Native context exceeds its item bound");
    }
    output.push(...data.output);
  }
  if (!completeItems(output)) {
    return yield* invalidInput("validate", "Native windows cannot be combined losslessly");
  }

  return output;
});

const requestBody = Effect.fnUntraced(function* (
  request: HttpClientRequest.HttpClientRequest,
): Effect.fn.Return<
  { readonly value: Readonly<Record<string, unknown>>; readonly contentType: string },
  AiError.AiError
> {
  if (request.body._tag !== "Uint8Array" || request.body.contentLength > MAX_BODY_BYTES) {
    return yield* invalidInput("request", "Native context requires a bounded JSON request body");
  }
  const text = request.body.text ?? new TextDecoder().decode(request.body.body);

  const value = yield* decodeJsonBody(text).pipe(
    Effect.mapError(() =>
      invalidInput("request", "Native context requires a JSON object request body"),
    ),
  );

  return { value, contentType: request.body.contentType };
});

function requestFailure(
  request: HttpClientRequest.HttpClientRequest,
  error: AiError.AiError,
): HttpClientError.HttpClientError {
  return new HttpClientError.HttpClientError({
    reason: new HttpClientError.EncodeError({
      request,
      description:
        error.reason._tag === "InvalidRequestError"
          ? error.reason.description
          : "Native OpenAI request validation failed",
    }),
  });
}

const replayRequest = Effect.fnUntraced(
  function* (
    request: HttpClientRequest.HttpClientRequest,
    model: string,
    prefix: ReadonlyArray<WireItem>,
  ): Effect.fn.Return<HttpClientRequest.HttpClientRequest, AiError.AiError> {
    if (request.method !== "POST" || !/\/responses(?:[?#].*)?$/.test(request.url)) {
      return yield* invalidInput(
        "replay",
        "Native replay requires the stock Responses request path",
      );
    }
    const { value: body, contentType } = yield* requestBody(request);

    const fields = yield* decodeReplayRequest(body).pipe(
      Effect.mapError(() =>
        invalidInput("replay", "Native replay requires stateless full-input requests"),
      ),
    );

    if (fields.model !== model) {
      return yield* invalidInput("replay", "Native context request-model affinity mismatch");
    }
    if (fields.input.some(isItemReference) || prefix.length + fields.input.length > MAX_ITEMS) {
      return yield* invalidInput(
        "replay",
        "Native replay requires bounded full items without references",
      );
    }
    const payload = { ...body, input: [...prefix, ...fields.input] };

    const text = yield* encodeJsonBody(payload).pipe(
      Effect.mapError(() => invalidInput("replay", "Native replay request could not be encoded")),
    );

    if (byteLength(text) > MAX_BODY_BYTES) {
      return yield* invalidInput("replay", "Native replay exceeds the request byte bound");
    }

    return HttpClientRequest.setBody(request, HttpBody.text(text, contentType));
  },
  (effect, request) => Effect.mapError(effect, (error) => requestFailure(request, error)),
);

const guardCompactRequest = Effect.fnUntraced(
  function* (
    request: HttpClientRequest.HttpClientRequest,
    model: string,
    input: ReadonlyArray<WireItem>,
  ): Effect.fn.Return<HttpClientRequest.HttpClientRequest, AiError.AiError> {
    if (request.method !== "POST" || !/\/responses\/compact(?:[?#].*)?$/.test(request.url)) {
      return yield* invalidInput(
        "compact",
        "Native compaction requires the standalone compact request path",
      );
    }
    const { value: body } = yield* requestBody(request);

    const fields = yield* decodeCompactRequest(body).pipe(
      Effect.mapError(() =>
        invalidInput("compact", "Unsupported native compaction request configuration"),
      ),
    );

    const actual = yield* decodeItems(fields.input).pipe(
      Effect.mapError(() => invalidInput("compact", "Unsupported native compaction input")),
    );

    if (fields.model !== model || !sameItems(actual, input)) {
      return yield* invalidInput(
        "compact",
        "A client transform changed native compaction identity or covered input",
      );
    }

    return request;
  },
  (effect, request) => Effect.mapError(effect, (error) => requestFailure(request, error)),
);

const guardCountRequest = Effect.fnUntraced(
  function* (
    request: HttpClientRequest.HttpClientRequest,
    model: string,
    input: ReadonlyArray<WireItem>,
  ): Effect.fn.Return<HttpClientRequest.HttpClientRequest, AiError.AiError> {
    if (request.method !== "POST" || !/\/responses\/input_tokens(?:[?#].*)?$/.test(request.url)) {
      return yield* invalidInput("count", "Native context requires the Responses input-count path");
    }
    const { value: body } = yield* requestBody(request);

    const fields = yield* decodeCountRequest(body).pipe(
      Effect.mapError(() => invalidInput("count", "Unsupported native input-count configuration")),
    );

    if (fields.model !== model || !sameItems(fields.input, input)) {
      return yield* invalidInput(
        "count",
        "A client transform changed the native window being counted",
      );
    }

    return request;
  },
  (effect, request) => Effect.mapError(effect, (error) => requestFailure(request, error)),
);

const readResponse = Effect.fnUntraced(function* (
  response: HttpClientResponse.HttpClientResponse,
): Effect.fn.Return<unknown, AiError.AiError> {
  const chunks: Array<Uint8Array> = [];
  let size = 0;

  yield* Stream.runForEach(response.stream, (chunk) =>
    Effect.suspend(() => {
      size += chunk.byteLength;
      if (size > MAX_BODY_BYTES) return Effect.fail(invalidOutput(undefined));
      chunks.push(chunk);

      return Effect.void;
    }),
  ).pipe(Effect.mapError(() => invalidOutput(undefined)));
  const bytes = new Uint8Array(size);
  let offset = 0;

  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }

  const text = yield* Effect.try({
    try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
    catch: () => invalidOutput(undefined),
  });

  return yield* decodeJson(text).pipe(Effect.mapError(() => invalidOutput(undefined)));
});

const transportFailure = Effect.fnUntraced(function* (
  error: HttpClientError.HttpClientError,
): Effect.fn.Return<never, AiError.AiError> {
  const reason = error.reason;

  if (reason._tag === "StatusCodeError") {
    const raw = yield* readResponse(reason.response).pipe(Effect.option);
    const usage = Option.isSome(raw) ? errorUsage(raw.value) : undefined;

    if (usage !== undefined) return yield* invalidOutput(usage);

    return yield* AiError.make({
      module: "OpenAiCompaction",
      method: "compact",
      reason: AiError.reasonFromHttpStatus({
        status: reason.response.status,
        description: "The OpenAI compact endpoint rejected the request",
      }),
    });
  }
  if (reason._tag === "EncodeError") {
    return yield* invalidInput(
      "compact",
      "Native compaction request encoding or validation failed",
    );
  }
  if (reason._tag === "DecodeError" || reason._tag === "EmptyBodyError") {
    return yield* invalidOutput(undefined);
  }

  return yield* AiError.make({
    module: "OpenAiCompaction",
    method: "compact",
    reason: AiError.NetworkError.make({
      reason: reason._tag,
      request: { method: "POST", url: "/responses/compact", urlParams: [], headers: {} },
      description: "The OpenAI compact request could not be transported",
    }),
  });
});

function replay<A, E, R>(
  stream: Stream.Stream<A, E, R>,
  request: { readonly model: string; readonly windows: ReadonlyArray<NativeCompaction> },
): Stream.Stream<A, E | AiError.AiError, R> {
  if (request.windows.length === 0) return stream;

  return Stream.unwrap(
    Effect.gen(function* () {
      yield* configuration("replay", request.model);
      const prefix = yield* retainedInput(request.model, request.windows);
      const previous = yield* OpenAiConfig.OpenAiConfig.getOrUndefined;
      let intercepted = false;

      const config: typeof OpenAiConfig.OpenAiConfig.Service = {
        ...previous,
        transformClient(client) {
          const configured = previous?.transformClient?.(client) ?? client;

          return HttpClient.mapRequestEffect(configured, (requestBody) =>
            replayRequest(requestBody, request.model, prefix).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  intercepted = true;
                }),
              ),
            ),
          );
        },
      };

      const check = Effect.suspend(() =>
        intercepted
          ? Effect.void
          : Effect.fail(
              invalidInput("replay", "The model stream bypassed the native OpenAI request hook"),
            ),
      );

      return stream.pipe(
        Stream.provideService(OpenAiConfig.OpenAiConfig, config),
        Stream.tap(() => check),
        Stream.onEnd(check),
      );
    }),
  );
}

const providerLayer: Layer.Layer<NativeCompactionProvider, never, OpenAiClient.OpenAiClient> =
  Layer.effect(
    NativeCompactionProvider,
    Effect.gen(function* () {
      const openai = yield* OpenAiClient.OpenAiClient;

      const compact = Effect.fnUntraced(function* (
        request: CompactRequest,
      ): Effect.fn.Return<NativeCompactionResult, AiError.AiError> {
        const config = yield* configuration("compact", request.model);
        const previous = yield* retainedInput(request.model, request.previous);
        const suffix = yield* encodePrompt(request.prompt);
        const input = [...previous, ...suffix];

        if (input.length > MAX_ITEMS || !completeItems(input)) {
          return yield* invalidInput(
            "compact",
            "Native compaction requires bounded complete input items",
          );
        }
        const scoped = yield* OpenAiConfig.OpenAiConfig.getOrUndefined;
        const configured = scoped?.transformClient?.(openai.client) ?? openai.client;

        const client = HttpClient.mapRequestEffect(configured, (requestBody) =>
          guardCompactRequest(requestBody, request.model, input),
        );

        const response = yield* client
          .post("/responses/compact", {
            body: HttpBody.jsonUnsafe({
              model: request.model,
              input,
              ...(config?.instructions === undefined ? {} : { instructions: config.instructions }),
              ...(config?.prompt_cache_key === undefined
                ? {}
                : { prompt_cache_key: config.prompt_cache_key }),
              ...(config?.service_tier === undefined ? {} : { service_tier: config.service_tier }),
            }),
          })
          .pipe(Effect.catchTag("HttpClientError", transportFailure));

        const raw = yield* readResponse(response);
        const knownUsage = errorUsage(raw);

        const { usage } = yield* decodeUsage(raw).pipe(
          Effect.mapError(() => invalidOutput(knownUsage)),
        );

        const body = yield* decodeReply(raw).pipe(Effect.mapError(() => invalidOutput(knownUsage)));

        const context = yield* Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const output = yield* readOutput(body.output);

            const countClient = HttpClient.mapRequestEffect(configured, (requestBody) =>
              guardCountRequest(requestBody, request.model, output),
            );

            const counted = yield* countClient.post("/responses/input_tokens", {
              body: HttpBody.jsonUnsafe({
                model: request.model,
                input: output,
                truncation: "disabled",
              }),
            });

            const count = yield* decodeCountReply(yield* readResponse(counted));

            return yield* makeContext(output, count.input_tokens);
          }).pipe(
            restore,
            Effect.mapError(() => invalidOutput(knownUsage)),
            // Retain billed usage without turning interruption into a recoverable failure.
            Effect.catchCause((cause) =>
              Effect.failCause(
                Cause.hasInterrupts(cause)
                  ? Cause.combine(cause, Cause.fail(invalidOutput(knownUsage)))
                  : cause,
              ),
            ),
          ),
        );

        return {
          context,
          provider,
          model: request.model,
          usage: responseUsage(usage),
          responseId: body.id,
        };
      });

      return NativeCompactionProvider.of({
        provider,
        format,
        compact,
        validate: (window) => Effect.asVoid(readWindow(window)),
        replay,
      });
    }),
  );

/**
 * Supply native compaction and scoped replay for the runtime-selected stock OpenAI Model.
 * Requires the configured OpenAiClient and Responses input-count endpoint support.
 * Inference must use stateless HTTP with store: false. Compact reads its model identity
 * from the runtime and optional settings from scoped Config.
 */
export const layer: Layer.Layer<ContextCompactor, never, OpenAiClient.OpenAiClient> =
  ContextCompactor.layerNative.pipe(Layer.provide(providerLayer));
