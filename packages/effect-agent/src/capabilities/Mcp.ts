/**
 * Contracts for bounded MCP connections, discovered Tool metadata, and explicit validation of
 * remote Tool definitions.
 *
 * @since 0.1.0
 */

import * as McpSchema from "effect/ai/McpSchema";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Array from "effect/Array";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import type * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

import { utf8ByteLength, utf8Bytes } from "../core/internal/utf8.ts";
import { ToolExecutionClass, type ToolExecutionClassValue } from "../engine/DurableStep.ts";

const MAX_MCP_TOOLS = 128;
const MAX_MCP_DISCOVERY_BYTES = 1024 * 1024;
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const Sha256Digest = Schema.String.check(Schema.isPattern(/^sha256:[a-f0-9]{64}$/));

/**
 * Server identity copied directly from Effect AI's native MCP initialization schema.
 *
 * @category models
 * @since 0.1.0
 */
export class McpServerIdentity extends Schema.Class<McpServerIdentity>(
  "@effect-agent/capabilities/McpServerIdentity",
)({
  serverId: Schema.NonEmptyString,
  implementation: McpSchema.Implementation,
}) {}

/**
 * Requested hard limits for an adapter-owned MCP connection and discovery response.
 *
 * `expectedToolkitSchemaDigest` pins the tool contract an Agent was authored
 * against: when a connector derives its Toolkit from live discovery, a server
 * that adds, removes, or reshapes a tool fails closed instead of silently
 * changing what the model can call.
 *
 * @category models
 * @since 0.1.0
 */
export class McpConnectionRequest extends Schema.Class<McpConnectionRequest>(
  "@effect-agent/capabilities/McpConnectionRequest",
)({
  serverId: Schema.NonEmptyString,
  maxToolCount: PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_MCP_TOOLS)),
  maxToolDescriptionBytes: PositiveInt,
  maxDiscoveryBytes: PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_MCP_DISCOVERY_BYTES)),
  connectTimeoutMillis: PositiveInt,
  expectedToolkitSchemaDigest: Schema.optionalKey(Sha256Digest),
}) {}

/**
 * The model-visible result of a remote MCP tool call: the server's content
 * blocks and, when the tool declares an `outputSchema`, its structured result.
 * A result with `isError` never becomes this value; it fails the Tool Call.
 *
 * @category models
 * @since 0.1.0
 */
export class McpToolResult extends Schema.Class<McpToolResult>(
  "@effect-agent/capabilities/McpToolResult",
)({
  content: Schema.Array(McpSchema.ContentBlock),
  structuredContent: Schema.optionalKey(Schema.Json),
}) {}

/**
 * Typed failure of one remote MCP tool call. `tool-error` carries the server's
 * error content; the other reasons describe why no trustworthy result exists.
 * None of them claims the remote effect did not happen.
 *
 * @category errors
 * @since 0.1.0
 */
export class McpToolCallFailed extends Schema.TaggedError<McpToolCallFailed>()(
  "McpToolCallFailed",
  {
    serverId: Schema.NonEmptyString,
    tool: Schema.NonEmptyString,
    reason: Schema.Literals(["tool-error", "invalid-arguments", "protocol-error", "transport"]),
    message: Schema.String,
    content: Schema.optionalKey(Schema.Array(McpSchema.ContentBlock)),
  },
) {}

/**
 * Effect AI Tool derived from one discovered MCP tool. Its parameters are the
 * server's JSON Schema, so handlers receive `unknown` and the server validates
 * arguments; `Tool.getJsonSchema` still advertises the discovered schema. A
 * failed call returns `McpToolCallFailed` to the model as a failed tool result.
 *
 * @category models
 * @since 0.1.0
 */
export type McpTool = Tool.Tool<
  string,
  {
    readonly parameters: typeof Schema.Unknown;
    readonly success: typeof McpToolResult;
    readonly failure: typeof McpToolCallFailed;
    readonly failureMode: "return";
  }
>;

/**
 * Toolkit of discovered MCP tools; every member forwards `tools/call` to its server.
 *
 * @category models
 * @since 0.1.0
 */
export type McpToolkit = Toolkit.Toolkit<Record<string, McpTool>>;

const executionClassFor = (
  annotations: McpSchema.ToolAnnotations | undefined,
): ToolExecutionClassValue => {
  if (annotations === undefined) return "uncertain";
  if (annotations.readOnlyHint) return "readonly";
  if (annotations.idempotentHint && !annotations.destructiveHint) return "idempotent";

  return "uncertain";
};

const makeMcpTool = (tool: McpSchema.Tool, trustToolAnnotations: boolean): McpTool => {
  // MCP servers may advertise valid JSON Schemas outside a model provider's strict subset.
  let dynamic: McpTool = Tool.dynamic(tool.name, {
    ...(tool.description === undefined ? {} : { description: tool.description }),
    parameters: tool.inputSchema,
    success: McpToolResult,
    failure: McpToolCallFailed,
    // MCP reports tool errors inside results so the model can self-correct;
    // `return` hands the typed failure to the model the same way.
    failureMode: "return",
  }).annotate(Tool.Strict, false);

  const title = tool.title ?? tool.annotations?.title;

  if (title !== undefined) dynamic = dynamic.annotate(Tool.Title, title);
  if (tool.annotations !== undefined) {
    dynamic = dynamic
      .annotate(Tool.Readonly, tool.annotations.readOnlyHint)
      .annotate(Tool.Destructive, tool.annotations.destructiveHint)
      .annotate(Tool.Idempotent, tool.annotations.idempotentHint)
      .annotate(Tool.OpenWorld, tool.annotations.openWorldHint);
  }
  if (trustToolAnnotations) {
    dynamic = dynamic.annotate(ToolExecutionClass, executionClassFor(tool.annotations));
  }

  return dynamic;
};

/**
 * Typed remote connection failure; no remote execution is claimed exactly-once.
 *
 * @category errors
 * @since 0.1.0
 */
export class McpConnectionError extends Schema.TaggedError<McpConnectionError>()(
  "McpConnectionError",
  {
    serverId: Schema.NonEmptyString,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/**
 * Discovery exceeded an explicit caller or framework hard bound.
 *
 * @category errors
 * @since 0.1.0
 */
export class McpDiscoveryLimitExceeded extends Schema.TaggedError<McpDiscoveryLimitExceeded>()(
  "McpDiscoveryLimitExceeded",
  {
    serverId: Schema.NonEmptyString,
    limit: Schema.Literals(["tool-count", "tool-description-bytes", "discovery-bytes"]),
    limitValue: Schema.Natural,
    observedValue: Schema.Natural,
  },
) {}

/**
 * Native dynamic Toolkit names did not match the bounded MCP discovery response.
 *
 * @category errors
 * @since 0.1.0
 */
export class McpToolkitMismatch extends Schema.TaggedError<McpToolkitMismatch>()(
  "McpToolkitMismatch",
  {
    serverId: Schema.NonEmptyString,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/**
 * Validated discovery metadata; native Effect AI MCP values retain provider detail.
 *
 * @category models
 * @since 0.1.0
 */
export class McpDiscovery extends Schema.Class<McpDiscovery>(
  "@effect-agent/capabilities/McpDiscovery",
)({
  identity: McpServerIdentity,
  capabilities: McpSchema.ServerCapabilities,
  tools: Schema.Array(McpSchema.Tool).check(Schema.isMaxLength(MAX_MCP_TOOLS)),
  encodedBytes: Schema.Natural.check(Schema.isLessThanOrEqualTo(MAX_MCP_DISCOVERY_BYTES)),
  toolkitSchemaDigest: Sha256Digest,
}) {}

/**
 * Raw discovery and execution from one scoped server. The framework constructs
 * its Toolkit from these descriptors; connectors do not supply another copy.
 *
 * @category models
 * @since 0.1.0
 */
export interface McpConnectedServer {
  readonly identity: McpServerIdentity;
  readonly capabilities: McpSchema.ServerCapabilities;
  readonly tools: ReadonlyArray<McpSchema.Tool>;
  readonly callTool: (
    name: string,
    parameters: unknown,
  ) => Effect.Effect<McpToolResult, McpToolCallFailed>;
  /** Server hints affect execution class only when the host explicitly trusts them. */
  readonly trustToolAnnotations?: boolean | undefined;
}

/**
 * Scoped connection with one Toolkit derived from validated, optionally pinned discovery.
 *
 * @category models
 * @since 0.1.0
 */
export interface McpConnection {
  readonly discovery: McpDiscovery;
  readonly toolkit: McpToolkit;
  readonly handlers: Layer.Layer<Tool.Handler<string>>;
}

/**
 * Adapter port for native Effect AI MCP protocol/schema integration.
 *
 * @category services
 * @since 0.1.0
 */
export class McpConnector extends Context.Service<
  McpConnector,
  {
    readonly connect: (
      request: McpConnectionRequest,
    ) => Effect.Effect<McpConnectedServer, McpConnectionError, Scope.Scope>;
  }
>()("@effect-agent/capabilities/McpConnector") {}

const canonicalJson = (value: Schema.Json): Schema.Json => {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    typeof value === "number"
  ) {
    return value;
  }
  if (Array.isArray<Schema.Json>(value)) return value.map(canonicalJson);
  const output: Record<string, Schema.Json> = {};

  // Keys sort by UTF-16 code units (RFC 8785 style): locale-aware collation varies
  // across hosts and treats canonically equivalent distinct key sequences as equal.
  for (const [key, item] of Object.entries(value).sort(([left], [right]) =>
    left < right ? -1 : left > right ? 1 : 0,
  )) {
    output[key] = canonicalJson(item);
  }

  return output;
};

const digestJson = Effect.fnUntraced(function* (serverId: string, value: unknown) {
  const json = yield* Schema.decodeUnknownEffect(Schema.Json)(value).pipe(
    Effect.mapError((error) =>
      McpToolkitMismatch.make({
        cause: error,
        serverId,
        message: `MCP Tool schema is not canonical JSON: ${error.message}`,
      }),
    ),
  );

  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(
    canonicalJson(json),
  ).pipe(
    Effect.mapError((error) =>
      McpToolkitMismatch.make({
        cause: error,
        serverId,
        message: `Could not encode canonical MCP Tool schema JSON: ${error.message}`,
      }),
    ),
  );

  const crypto = yield* Crypto.Crypto;

  const digest = yield* crypto.digest("SHA-256", utf8Bytes(encoded)).pipe(
    Effect.mapError((cause) =>
      McpToolkitMismatch.make({
        cause,
        serverId,
        message: "Could not hash MCP Tool schemas",
      }),
    ),
  );

  return `sha256:${Hex.encode(digest)}`;
});

/**
 * Validate native discovery data before it enters Agent Tool context.
 *
 * @category operations
 * @since 0.1.0
 */
export const validateMcpDiscovery = Effect.fnUntraced(function* (
  request: McpConnectionRequest,
  server: Pick<McpConnectedServer, "identity" | "capabilities" | "tools">,
) {
  if (server.identity.serverId !== request.serverId) {
    return yield* McpToolkitMismatch.make({
      serverId: request.serverId,
      message: `Connected server identity '${server.identity.serverId}' does not match the requested server`,
    });
  }
  if (server.tools.length > request.maxToolCount) {
    return yield* McpDiscoveryLimitExceeded.make({
      serverId: request.serverId,
      limit: "tool-count",
      limitValue: request.maxToolCount,
      observedValue: server.tools.length,
    });
  }
  for (const tool of server.tools) {
    const descriptionBytes = utf8ByteLength(tool.description ?? "");

    if (descriptionBytes > request.maxToolDescriptionBytes) {
      return yield* McpDiscoveryLimitExceeded.make({
        serverId: request.serverId,
        limit: "tool-description-bytes",
        limitValue: request.maxToolDescriptionBytes,
        observedValue: descriptionBytes,
      });
    }
  }
  if (new Set(server.tools.map((tool) => tool.name)).size !== server.tools.length) {
    return yield* McpToolkitMismatch.make({
      serverId: request.serverId,
      message: "MCP discovery contains duplicate tool names",
    });
  }

  const discoverySchemas = server.tools
    .map((tool) => ({
      name: tool.name,
      inputSchema: tool.inputSchema,
      ...(tool.outputSchema !== undefined ? { outputSchema: tool.outputSchema } : {}),
    }))
    .sort((left, right) => (left.name < right.name ? -1 : left.name > right.name ? 1 : 0));

  const toolkitDigest = yield* digestJson(request.serverId, discoverySchemas);

  if (
    request.expectedToolkitSchemaDigest !== undefined &&
    request.expectedToolkitSchemaDigest !== toolkitDigest
  ) {
    return yield* McpToolkitMismatch.make({
      serverId: request.serverId,
      message: `MCP tool discovery digest ${toolkitDigest} does not match the expected ${request.expectedToolkitSchemaDigest}`,
    });
  }

  const encoded = yield* Schema.encodeEffect(
    Schema.Struct({
      identity: McpServerIdentity,
      capabilities: McpSchema.ServerCapabilities,
      tools: Schema.Array(McpSchema.Tool),
    }),
  )({
    identity: server.identity,
    capabilities: server.capabilities,
    tools: server.tools,
  }).pipe(
    Effect.mapError((error) =>
      McpConnectionError.make({
        cause: error,
        serverId: request.serverId,
        message: `Could not encode MCP discovery response: ${error.message}`,
      }),
    ),
  );

  const discoveryJson = yield* Schema.decodeEffect(Schema.Json)(encoded).pipe(
    Effect.mapError((error) =>
      McpConnectionError.make({
        cause: error,
        serverId: request.serverId,
        message: `Could not normalize MCP discovery response as JSON: ${error.message}`,
      }),
    ),
  );

  const discoveryText = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(
    discoveryJson,
  ).pipe(
    Effect.mapError((error) =>
      McpConnectionError.make({
        cause: error,
        serverId: request.serverId,
        message: `Could not serialize MCP discovery response: ${error.message}`,
      }),
    ),
  );

  const discoveryBytes = utf8ByteLength(discoveryText);

  if (discoveryBytes > request.maxDiscoveryBytes) {
    return yield* McpDiscoveryLimitExceeded.make({
      serverId: request.serverId,
      limit: "discovery-bytes",
      limitValue: request.maxDiscoveryBytes,
      observedValue: discoveryBytes,
    });
  }

  return McpDiscovery.make({
    identity: server.identity,
    capabilities: server.capabilities,
    tools: server.tools,
    encodedBytes: discoveryBytes,
    toolkitSchemaDigest: toolkitDigest,
  });
});

/**
 * Acquire a native connection in Scope, enforce a Clock-controlled timeout,
 * then validate discovery before exposing its Toolkit.
 *
 * @category operations
 * @since 0.1.0
 */
export const connectMcp = Effect.fnUntraced(function* (request: McpConnectionRequest) {
  const connector = yield* McpConnector;

  const server = yield* connector.connect(request).pipe(
    Effect.timeoutOrElse({
      duration: Duration.millis(request.connectTimeoutMillis),
      orElse: () =>
        Effect.fail(
          McpConnectionError.make({
            serverId: request.serverId,
            message: "MCP connection timed out",
          }),
        ),
    }),
  );

  const discovery = yield* validateMcpDiscovery(request, server);

  const toolkit: McpToolkit = yield* Effect.try({
    try: () =>
      Toolkit.make(
        ...server.tools.map((tool) => makeMcpTool(tool, server.trustToolAnnotations ?? false)),
      ),
    catch: (cause) =>
      McpConnectionError.make({
        serverId: request.serverId,
        message: "MCP discovery contains tools that cannot form a Toolkit",
        cause,
      }),
  });

  const handlers = toolkit.toLayer(
    Object.fromEntries(
      server.tools.map((tool) => [
        tool.name,
        (parameters: unknown) => server.callTool(tool.name, parameters),
      ]),
    ),
  );

  return { discovery, toolkit, handlers } satisfies McpConnection;
});
