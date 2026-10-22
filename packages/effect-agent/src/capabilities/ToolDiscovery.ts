/**
 * Discover currently authorized Tools through bounded catalogue search and complete schema
 * documentation. Successful selections update exposure without granting authority.
 *
 * @since 0.1.0
 */

import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { utf8ByteLength } from "../core/internal/utf8.ts";
import { DiscoveryTool, PinnedTool } from "../core/ToolExposure.ts";
import { ToolExecutionClass } from "../engine/DurableStep.ts";
import { CurrentToolCatalog, type CatalogEntry } from "../engine/ToolExposure.ts";

const Name = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const Namespace = Schema.NonEmptyString.check(Schema.isMaxLength(128));
const Description = Schema.String.check(Schema.isMaxLength(2_048));
const NamespaceDescription = Schema.String.check(Schema.isMaxLength(512));
const EntryId = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));

/**
 * Search text is bounded and literal; namespace selects one exact host-declared group.
 *
 * @category schemas
 * @since 0.1.0
 */
export const Parameters = Schema.Struct({
  query: Schema.NonEmptyString.check(Schema.isMaxLength(512), Schema.isPattern(/\S/)),
  namespace: Schema.optionalKey(Namespace),
});

/**
 * Metadata exposed only after host visibility and inherited grants are applied.
 *
 * @category models
 * @since 0.1.0
 */
export class Descriptor extends Schema.Class<Descriptor>(
  "@effect-agent/capabilities/ToolDiscovery/Descriptor",
)({
  id: EntryId,
  kind: Schema.Literals(["native", "code-mode"]),
  name: Name,
  nativeToolName: Name,
  namespace: Schema.optionalKey(Namespace),
  method: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(128))),
  description: Schema.optionalKey(Description),
  namespaceDescription: Schema.optionalKey(NamespaceDescription),
}) {}

/**
 * Selected metadata and canonical encoded Tool Schemas. Provider-specific model schema
 * transformations do not change the wire values documented for application handlers.
 *
 * @category models
 * @since 0.1.0
 */
export class Match extends Schema.Class<Match>("@effect-agent/capabilities/ToolDiscovery/Match")({
  ...Descriptor.fields,
  parameters: Schema.Json,
  success: Schema.Json,
  /** Hosted declaration; parameters/success are null when no application handler is required. */
  provider: Schema.optionalKey(
    Schema.Struct({
      providerName: Schema.NonEmptyString,
      requiresHandler: Schema.Boolean,
    }),
  ),
}) {}

/**
 * Only documented nativeToolName values activate schemas; Code Mode aliases activate their
 * owning Tool. A byte-limited result is successful, including when no matches fit: an empty
 * toolNames array clears the non-pinned selection under the engine's replacement contract.
 *
 * @category schemas
 * @since 0.1.0
 */
export const Result = Schema.Struct({
  toolNames: Schema.Array(Name).check(Schema.isMaxLength(64), Schema.isUnique()),
  matches: Schema.Array(Match).check(Schema.isMaxLength(64)),
  /** Present when the byte budget omits complete matches, with guidance for continuing. */
  notice: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(192))),
});

/**
 * Invalid catalogue metadata and unsupported documentation remain typed failures.
 *
 * @category errors
 * @since 0.1.0
 */
export class ToolDiscoveryError extends Schema.TaggedError<ToolDiscoveryError>()(
  "ToolDiscoveryError",
  {
    reason: Schema.Literals(["invalid-catalogue", "invalid-schema"]),
    message: Schema.String.check(Schema.isMaxLength(1_024)),
  },
) {}

const Bounds = Schema.Struct({
  maxResults: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 64 })),
  maxResultBytes: Schema.Int.check(Schema.isBetween({ minimum: 256, maximum: 256 * 1024 })),
  namespaceDescriptions: Schema.Record(Namespace, NamespaceDescription),
});

const resultByteLength = Effect.fnUntraced(function* (result: typeof Result.Type) {
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Result))(result).pipe(
    Effect.mapError(() =>
      ToolDiscoveryError.make({
        reason: "invalid-schema",
        message: "Discovery documentation could not be encoded",
      }),
    ),
  );

  return utf8ByteLength(encoded);
});

/**
 * Options for literal catalogue search and bounded documentation of currently visible Tools.
 *
 * @category models
 * @since 0.1.0
 */
export interface Options {
  /** Optional generic instructions. Namespace names and hints are never appended here. */
  readonly description?: string | undefined;
  /** Maximum returned catalogue entries, default 8 and at most 64. */
  readonly maxResults?: number | undefined;
  /**
   * Complete JSON-encoded result budget, including any notice: default 32768, minimum 256,
   * maximum 262144 UTF-8 bytes. Within the first maxResults candidates, retain whole matches
   * in rank order when they fit; skip oversized matches and try later candidates. Overflow
   * returns a successful result with an actionable notice, never partial schemas.
   */
  readonly maxResultBytes?: number | undefined;
  /** Short category hints returned only for namespaces present in the visible catalogue. */
  readonly namespaceDescriptions?: Readonly<Record<string, string>> | undefined;
}

const entryId = (entry: CatalogEntry): string =>
  entry.kind === "native"
    ? `native:${entry.tool.name}`
    : `code-mode:${entry.nativeToolName}:${entry.namespace}.${entry.method}`;

const compare = (left: string, right: string): number => (left < right ? -1 : left > right ? 1 : 0);

/**
 * Build one native discover_tools Tool and its singleton Toolkit/handler Layer. Include the
 * Tool beside the application's existing native Tools. The engine owns catalogue authority
 * and applies successful native selections after a complete Tool batch; the handler mutates
 * no exposure state. Search matches all case-insensitive whitespace-separated terms
 * against names, descriptions, methods and namespace hints, ordered by catalogue id.
 *
 * @category constructors
 * @since 0.1.0
 */
export const make = (options: Options = {}) => {
  const bounds = Schema.decodeSync(Bounds)({
    maxResults: options.maxResults ?? 8,
    maxResultBytes: options.maxResultBytes ?? 32 * 1024,
    namespaceDescriptions: options.namespaceDescriptions ?? {},
  });

  const tool = Tool.make("discover_tools", {
    description:
      options.description ??
      "Find available tools using short search terms and an optional exact namespace. All terms must match a name, description or namespace hint, ignoring case. Returned native tools become available next turn. Code Mode matches document namespace methods and select their owning execution tool.",
    parameters: Parameters,
    success: Result,
    failure: ToolDiscoveryError,
    failureMode: "error",
    dependencies: [CurrentToolCatalog],
  })
    .annotate(DiscoveryTool, true)
    .annotate(PinnedTool, true)
    .annotate(Tool.Readonly, true)
    .annotate(ToolExecutionClass, "readonly");

  const toolkit = Toolkit.make(tool);

  const handlers = toolkit.toLayer({
    discover_tools: Effect.fnUntraced(function* (request) {
      const snapshot = yield* CurrentToolCatalog;

      const entries = snapshot.entries
        .filter((entry) => request.namespace === undefined || entry.namespace === request.namespace)
        .toSorted((left, right) => compare(entryId(left), entryId(right)));

      const catalogue: Array<{ readonly descriptor: Descriptor; readonly tool: Tool.Any }> = [];
      const ids = new Set<string>();

      for (const entry of entries) {
        const id = entryId(entry);

        if (ids.has(id)) {
          return yield* ToolDiscoveryError.make({
            reason: "invalid-catalogue",
            message: "The visible catalogue contains duplicate entry identities",
          });
        }
        const description = Tool.getDescription(entry.tool);

        const namespaceDescription =
          entry.namespace === undefined ||
          !Object.hasOwn(bounds.namespaceDescriptions, entry.namespace)
            ? undefined
            : bounds.namespaceDescriptions[entry.namespace];

        const descriptor = yield* Schema.decodeEffect(Descriptor)({
          id,
          kind: entry.kind,
          name: entry.tool.name,
          nativeToolName: entry.nativeToolName,
          ...(entry.namespace === undefined ? {} : { namespace: entry.namespace }),
          ...(entry.kind === "code-mode" ? { method: entry.method } : {}),
          ...(description === undefined
            ? {}
            : {
                description:
                  description.length > 2_048 ? `${description.slice(0, 2_047)}…` : description,
              }),
          ...(namespaceDescription === undefined ? {} : { namespaceDescription }),
        }).pipe(
          Effect.mapError(() =>
            ToolDiscoveryError.make({
              reason: "invalid-catalogue",
              message: "The visible catalogue contains invalid metadata",
            }),
          ),
        );

        catalogue.push({ descriptor, tool: entry.tool });
        ids.add(id);
      }

      const terms = request.query.trim().toLowerCase().split(/\s+/);

      const selected = catalogue.filter(({ descriptor, tool }) => {
        const text = [
          descriptor.name,
          descriptor.namespace ?? "",
          descriptor.method ?? "",
          Tool.getDescription(tool) ?? "",
          descriptor.namespaceDescription ?? "",
        ]
          .join(" ")
          .toLowerCase();

        return terms.every((term) => text.includes(term));
      });

      const matches: Array<Match> = [];

      for (const { descriptor, tool } of selected.slice(0, bounds.maxResults)) {
        const schemas = yield* Effect.try({
          try: () =>
            Tool.isProviderDefined(tool)
              ? {
                  parameters: tool.requiresHandler ? Tool.getJsonSchema(tool) : null,
                  success: tool.requiresHandler
                    ? Tool.getJsonSchemaFromSchema(tool.successSchema)
                    : null,
                  provider: {
                    providerName: tool.providerName,
                    requiresHandler: tool.requiresHandler,
                  },
                }
              : {
                  parameters: Tool.getJsonSchema(tool),
                  success: Tool.getJsonSchemaFromSchema(tool.successSchema),
                },
          catch: () =>
            ToolDiscoveryError.make({
              reason: "invalid-schema",
              message: "A selected Tool cannot be documented as JSON Schema",
            }),
        });

        const match = yield* Schema.decodeUnknownEffect(Match)({
          ...descriptor,
          ...schemas,
        }).pipe(
          Effect.mapError(() =>
            ToolDiscoveryError.make({
              reason: "invalid-schema",
              message: "A selected Tool's documentation is outside the JSON surface",
            }),
          ),
        );

        matches.push(match);
      }

      const result = {
        toolNames: [...new Set(matches.map((match) => match.nativeToolName))],
        matches,
      };

      if ((yield* resultByteLength(result)) <= bounds.maxResultBytes) return result;

      // Validate every selected schema before packing. A byte limit must not mask an
      // invalid catalogue/schema. The empty notice envelope fits the minimum 256 bytes.
      let bounded: typeof Result.Type = {
        toolNames: [],
        matches: [],
        notice:
          "Result byte limit reached. Narrow the search or namespace; if one tool still cannot fit, ask the host to increase maxResultBytes.",
      };

      for (const match of matches) {
        const candidates = [...bounded.matches, match];

        const candidate = {
          ...bounded,
          toolNames: [...new Set(candidates.map((entry) => entry.nativeToolName))],
          matches: candidates,
        };

        if ((yield* resultByteLength(candidate)) <= bounds.maxResultBytes) bounded = candidate;
      }

      return bounded;
    }),
  });

  return Object.freeze({ tool, toolkit, handlers });
};
