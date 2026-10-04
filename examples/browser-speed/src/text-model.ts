import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import {
  OpenAiClient as CompletionsClient,
  OpenAiLanguageModel as CompletionsModel,
} from "@effect/ai-openai-compat";
import { Effect, Layer, type Redacted, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientResponse } from "effect/http";

export const TextProvider = Schema.Literals(["openai", "openrouter"]);
export const TextReasoning = Schema.Literals(["none", "low"]);

const NullServiceTierCompletion = Schema.StructWithRest(
  Schema.Struct({ service_tier: Schema.Null }),
  [Schema.Record(Schema.String, Schema.Json)],
);

// OpenRouter returns null for absent tier metadata; the compatibility client expects omission.
const openRouterChatClient = (client: HttpClient.HttpClient) =>
  client.pipe(
    HttpClient.transformResponse(
      Effect.flatMap((response) =>
        response.status !== 200
          ? Effect.succeed(response)
          : response.json.pipe(
              Effect.flatMap((body) => {
                if (
                  typeof body !== "object" ||
                  body === null ||
                  !Schema.is(NullServiceTierCompletion)(body)
                )
                  return Effect.succeed(response);

                const completion: Record<string, Schema.Json> = { ...body };

                delete completion.service_tier;
                const headers = new Headers(response.headers);

                headers.delete("content-length");
                headers.delete("content-encoding");

                return Effect.annotateCurrentSpan(
                  "openrouter.null_service_tier_omitted",
                  true,
                ).pipe(
                  Effect.as(
                    HttpClientResponse.fromWeb(
                      response.request,
                      Response.json(completion, { status: response.status, headers }),
                    ),
                  ),
                );
              }),
            ),
      ),
    ),
  );

/** The small model that writes Jev's field values. OpenRouter requests JSON-object output. */
export const textModelLayer = (options: {
  readonly provider: typeof TextProvider.Type;
  readonly model: string;
  readonly reasoning: typeof TextReasoning.Type;
  readonly apiKey: Redacted.Redacted;
}) =>
  (options.provider === "openrouter"
    ? CompletionsModel.model(options.model, {
        max_tokens: 1_024,
        response_format: { type: "json_object" },
        reasoning:
          options.reasoning === "none" ? { enabled: false } : { effort: options.reasoning },
      }).pipe(
        Layer.provide(
          CompletionsClient.layer({
            apiKey: options.apiKey,
            apiUrl: "https://openrouter.ai/api/v1",
            transformClient: openRouterChatClient,
          }),
        ),
      )
    : OpenAiLanguageModel.model(options.model, {
        max_output_tokens: 1_024,
        reasoning: { effort: options.reasoning },
      }).pipe(Layer.provide(OpenAiClient.layer({ apiKey: options.apiKey })))
  ).pipe(Layer.provide(FetchHttpClient.layer));
