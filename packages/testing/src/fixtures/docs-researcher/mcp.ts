import {
  McpConnectionRequest,
  McpConnector,
  McpServerIdentity,
  McpToolkitMismatch,
  McpToolCallFailed,
  McpToolResult,
  type McpConnection,
} from "@yielded/agent/mcp";
import * as McpSchema from "effect/ai/McpSchema";
import * as Tool from "effect/ai/Tool";
import * as Effect from "effect/Effect";
import * as JsonPointer from "effect/JsonPointer";
import type * as JsonSchema from "effect/JsonSchema";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import {
  DocContentToolkit,
  DocumentQuery,
  FetchDocument,
  ResearchDocument,
  researchDocumentLookup,
} from "./definition.ts";

// ---------------------------------------------------------------------------
// Scripted MCP fixture: a deterministic `McpConnector` adapter that serves the
// doc-summarizer's content tool. Discovery entries are DERIVED from the
// authored Tool (`Tool.getJsonSchema`). The assembly checks discovery against
// its authored contract before registering a worker; the mismatch and over-limit
// connectors below exercise those fail-closed paths.
// ---------------------------------------------------------------------------

/** Framework-side hard bounds one docs-researcher assembly requests. */
export const docsMcpRequest = McpConnectionRequest.make({
  serverId: "docs-content-mcp",
  maxToolCount: 4,
  maxToolDescriptionBytes: 256,
  maxDiscoveryBytes: 16_384,
  connectTimeoutMillis: 1_000,
});

export const docsMcpIdentity = McpServerIdentity.make({
  serverId: docsMcpRequest.serverId,
  implementation: McpSchema.Implementation.make({
    name: "docs-researcher-content-fixture",
    version: "1.0.0",
  }),
});

/**
 * `Tool.getJsonSchema` produces a `$ref`/`$defs`-shaped schema for
 * `FetchDocument`'s named, refined parameters type, but `McpSchema.Tool`'s
 * `inputSchema` requires a flat `{ type: "object", ... }` root — the shape a
 * real MCP server advertises on the wire. This inlines the single top-level
 * `$ref` so the derivation described above still holds byte-for-byte.
 */
const JsonSchemaDefinitions = Schema.Record(
  Schema.String,
  Schema.Record(Schema.String, Schema.Unknown),
);

const decodeJsonSchemaDefinitions = Schema.decodeUnknownSync(JsonSchemaDefinitions);
const decodeToolJson = Schema.decodeUnknownSync(McpSchema.ToolJson);

const flattenTopLevelRef = (schema: JsonSchema.JsonSchema): McpSchema.ToolJson => {
  const ref = schema["$ref"];

  if (typeof ref !== "string") {
    return decodeToolJson(schema);
  }

  const defs = decodeJsonSchemaDefinitions(schema["$defs"]);

  const key = ref.startsWith("#/$defs/")
    ? JsonPointer.unescapeToken(ref.slice("#/$defs/".length))
    : undefined;

  const resolved = key !== undefined && Object.hasOwn(defs, key) ? defs[key] : undefined;

  return decodeToolJson(resolved ?? schema);
};

const fetchDocumentOutputSchema = flattenTopLevelRef(
  Tool.getJsonSchemaFromSchema(FetchDocument.successSchema),
);

const discoveredFetchDocument = McpSchema.Tool.make({
  name: FetchDocument.name,
  description: "Fetch one bounded research document by its identifier.",
  inputSchema: flattenTopLevelRef(Tool.getJsonSchema(FetchDocument)),
  ...(fetchDocumentOutputSchema.type === "object"
    ? { outputSchema: fetchDocumentOutputSchema }
    : {}),
});

const scriptedConnector = (tools: ReadonlyArray<McpSchema.Tool>): Layer.Layer<McpConnector> =>
  Layer.succeed(McpConnector)({
    connect: () =>
      Effect.acquireRelease(
        Effect.succeed({
          identity: docsMcpIdentity,
          capabilities: McpSchema.ServerCapabilities.make({}),
          tools,
          callTool: Effect.fnUntraced(function* (name, parameters) {
            const failure = (message: string) =>
              McpToolCallFailed.make({
                serverId: docsMcpIdentity.serverId,
                tool: name,
                reason: "tool-error",
                message,
              });

            if (name !== FetchDocument.name) return yield* failure("Unknown document tool");

            const query = yield* Schema.decodeUnknownEffect(DocumentQuery)(parameters).pipe(
              Effect.mapError((error) => failure(error.message)),
            );

            const document = yield* researchDocumentLookup(query).pipe(
              Effect.mapError((error) => failure(error.message)),
            );

            const structuredContent = yield* Schema.encodeEffect(ResearchDocument)(document).pipe(
              Effect.mapError((error) => failure(error.message)),
            );

            return McpToolResult.make({ content: [], structuredContent });
          }),
        }),
        () => Effect.void,
      ),
  });

/** The well-behaved scripted content server. */
export const docsMcpConnectorLayer: Layer.Layer<McpConnector> = scriptedConnector([
  discoveredFetchDocument,
]);

/** Serves a tool description exceeding `maxToolDescriptionBytes` (SEC-013 bound). */
export const docsMcpOversizedConnectorLayer: Layer.Layer<McpConnector> = scriptedConnector([
  McpSchema.Tool.make({
    name: discoveredFetchDocument.name,
    description: "x".repeat(1_024),
    inputSchema: discoveredFetchDocument.inputSchema,
  }),
]);

/** Serves a discovery schema that disagrees with the authored toolkit (drift fails closed). */
export const docsMcpMismatchedConnectorLayer: Layer.Layer<McpConnector> = scriptedConnector([
  McpSchema.Tool.make({
    name: discoveredFetchDocument.name,
    description: discoveredFetchDocument.description,
    inputSchema: { type: "object", properties: { url: { type: "string" } } },
  }),
]);

const isJsonEqual = (left: unknown, right: unknown): boolean =>
  JSON.stringify(left) === JSON.stringify(right);

/**
 * Bind discovered schemas to the separately authored doc-summarizer contract
 * before worker registration. The connection's dynamic Toolkit is derived from
 * discovery, so checking it against discovery would not establish this pin.
 */
export const assertDiscoveryMatchesAuthoredToolkit = Effect.fn(
  "DocsResearcher.assertDiscoveryMatchesAuthoredToolkit",
)(function* (connection: McpConnection): Effect.fn.Return<void, McpToolkitMismatch> {
  const authored = Object.values(DocContentToolkit.tools)
    .map((tool) => ({
      name: tool.name,
      inputSchema: flattenTopLevelRef(Tool.getJsonSchema(tool)),
      outputSchema: flattenTopLevelRef(Tool.getJsonSchemaFromSchema(tool.successSchema)),
    }))
    .sort((left, right) => (left.name < right.name ? -1 : left.name > right.name ? 1 : 0));

  const discovered = connection.discovery.tools
    .map((tool) => ({
      name: tool.name,
      inputSchema: tool.inputSchema,
      outputSchema: tool.outputSchema,
    }))
    .sort((left, right) => (left.name < right.name ? -1 : left.name > right.name ? 1 : 0));

  const matches =
    authored.length === discovered.length &&
    authored.every(
      (tool, index) =>
        tool.name === discovered[index]?.name &&
        isJsonEqual(tool.inputSchema, discovered[index]?.inputSchema) &&
        isJsonEqual(tool.outputSchema, discovered[index]?.outputSchema),
    );

  if (!matches) {
    return yield* McpToolkitMismatch.make({
      serverId: connection.discovery.identity.serverId,
      message:
        "The MCP-discovered toolkit does not match the doc-summarizer's authored content toolkit",
    });
  }
});

/** Round-trip guard for encoded discovery values persisted as fixture evidence. */
export const DocsMcpDiscoveryEvidence = Schema.Struct({
  serverId: Schema.NonEmptyString,
  toolCount: Schema.Natural,
  encodedBytes: Schema.Natural,
  toolkitSchemaDigest: Schema.String,
});
