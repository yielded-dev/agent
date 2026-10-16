import * as WebSearch from "@yielded/agent/web-search";
import type * as AiError from "effect/ai/AiError";
import type * as LanguageModel from "effect/ai/LanguageModel";
import * as Tool from "effect/ai/Tool";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as Stream from "effect/Stream";
import type { expectTypeOf as ExpectTypeOf } from "vite-plus/test";

const hostedSearch = Tool.providerDefined({
  id: "test.web_search",
  customName: "HostedSearch",
  providerName: "web_search",
  args: Tool.EmptyParams,
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
})({});

const invoke = (options: Partial<Omit<WebSearch.Options, "tool">> = {}) =>
  Effect.gen(function* () {
    const tools = yield* WebSearch.toolkit;

    return yield* (yield* tools.handle("WebSearch", { query: "news" }, "call-1")).pipe(
      Stream.runCollect,
    );
  }).pipe(Effect.provide(WebSearch.layer({ tool: hostedSearch, ...options })));

export const verifySearchProviderAndEncoderRequirements = () => {
  expectTypeOf(WebSearch.layer({ tool: hostedSearch })).toEqualTypeOf<
    Layer.Layer<Tool.Handler<"WebSearch">, never, LanguageModel.LanguageModel>
  >();
  expectTypeOf(invoke()).toEqualTypeOf<
    Effect.Effect<
      Array<Tool.HandlerResult<typeof WebSearch.tool>>,
      AiError.AiError,
      LanguageModel.LanguageModel
    >
  >();

  class SearchEncoding extends Context.Service<SearchEncoding, string>()("test/SearchEncoding") {}

  const encodedSearch = Tool.providerDefined({
    id: "test.web_search",
    customName: "EncodedSearch",
    providerName: "web_search",
    args: Tool.EmptyParams,
    parameters: Schema.Struct({
      query: Schema.String.pipe(
        Schema.decode({
          decode: SchemaGetter.passthrough(),
          encode: SchemaGetter.transformEffect((query) => Effect.as(SearchEncoding, query)),
        }),
      ),
    }),
    success: Schema.Struct({ status: Schema.String }),
  })({});

  expectTypeOf(WebSearch.layer({ tool: encodedSearch })).toEqualTypeOf<
    Layer.Layer<Tool.Handler<"WebSearch">, never, LanguageModel.LanguageModel | SearchEncoding>
  >();
};

declare const expectTypeOf: typeof ExpectTypeOf;
