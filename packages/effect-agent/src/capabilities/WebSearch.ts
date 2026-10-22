import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

const decodeUrl = Schema.decodeUnknownOption(Schema.URLFromString);

const SourceUrl = Schema.String.check(
  Schema.isMaxLength(8_192),
  Schema.makeFilter(
    (value) => {
      const parsed = decodeUrl(value);

      return (
        Option.isSome(parsed) &&
        (parsed.value.protocol === "https:" || parsed.value.protocol === "http:") &&
        parsed.value.username === "" &&
        parsed.value.password === ""
      );
    },
    { title: "An HTTP source URL without credentials" },
  ),
);

/** Citations are untrusted references, not permission to fetch their URLs. */
export const Source = Schema.Struct({
  url: SourceUrl,
  title: Schema.String.check(Schema.isMaxLength(4_096)),
});

/** Place hosted search in the agent's own model call; no handler or search model Layer is needed.
 * Citations stay in native text annotations and sources. Configure the provider tool at the host.
 */
export const native = <T extends Tool.AnyProviderDefined>(options: { readonly tool: T }) => {
  if (
    !Tool.isProviderDefined(options.tool) ||
    options.tool.requiresHandler ||
    !["web_search", "web_search_preview"].includes(options.tool.providerName)
  ) {
    // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- WebSearch.native synchronously requires a provider-executed search Tool.
    throw new Error("WebSearch.native requires a provider-executed web_search tool");
  }

  return Toolkit.make(options.tool);
};
