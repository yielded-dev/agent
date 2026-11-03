import * as NativeMcpClient from "effect/ai/McpClient";
import type * as McpClientError from "effect/ai/McpClientError";
import * as McpProtocol from "effect/ai/McpProtocol";
import * as McpSchema from "effect/ai/McpSchema";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Headers from "effect/http/Headers";
import type * as HttpClient from "effect/http/HttpClient";
import * as Layer from "effect/Layer";
import * as Predicate from "effect/Predicate";
import * as ChildProcess from "effect/process/ChildProcess";
import type * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

import {
  McpConnectionError,
  McpConnector,
  type McpConnectedServer,
  type McpConnectionRequest,
  McpServerIdentity,
  McpToolResult,
  McpToolCallFailed,
} from "./Mcp.ts";

export { McpToolResult, McpToolCallFailed } from "./Mcp.ts";
export type { McpTool, McpToolkit } from "./Mcp.ts";

const DEFAULT_MAX_MESSAGE_BYTES = 4 * 1024 * 1024;
const MAX_TOOL_LIST_PAGES = 64;

/** Identity this client presents during native MCP initialization or discovery. */
export const defaultMcpClientInfo = McpSchema.Implementation.make({
  name: "effect-agent",
  version: "1.0.0",
});

/**
 * One reachable MCP server. `transport` acquires Effect's native MCP transport
 * in the caller's Scope; `R` names its required platform services.
 */
export interface McpServerTransport<R = never> {
  readonly serverId: string;
  /** Server hints may set execution class only when explicitly trusted. */
  readonly trustToolAnnotations: boolean;
  readonly transport: Effect.Effect<
    NativeMcpClient.Transport,
    McpClientError.McpClientError,
    Scope.Scope | R
  >;
}

/** Streamable HTTP server address. Header credentials remain application-owned. */
export interface McpHttpTransportOptions {
  readonly serverId: string;
  readonly url: string;
  readonly headers?: Headers.Input | undefined;
  /** Explicit native protocol adapter. Defaults to 2025-11-25; no version fallback. */
  readonly protocol?: McpProtocol.ProtocolAdapter<NativeMcpClient.ProtocolVersion> | undefined;
  /** Largest accepted response message. Defaults to 4 MiB. */
  readonly maxMessageBytes?: number | undefined;
  readonly trustToolAnnotations?: boolean | undefined;
}

/** A local MCP server process speaking newline-delimited JSON-RPC on stdio. */
export interface McpStdioTransportOptions {
  readonly serverId: string;
  readonly command: string;
  readonly args?: ReadonlyArray<string> | undefined;
  readonly cwd?: string | undefined;
  readonly env?: Readonly<Record<string, string>> | undefined;
  /** Explicit native protocol adapter. Defaults to 2025-11-25; no version fallback. */
  readonly protocol?: McpProtocol.ProtocolAdapter<NativeMcpClient.ProtocolVersion> | undefined;
  /** Largest accepted stdout frame. Defaults to 4 MiB. */
  readonly maxMessageBytes?: number | undefined;
  readonly trustToolAnnotations?: boolean | undefined;
}

/** Native scoped HTTP transport; requires `HttpClient`. */
export const McpHttpTransport = {
  make: (options: McpHttpTransportOptions): McpServerTransport<HttpClient.HttpClient> => ({
    serverId: options.serverId,
    trustToolAnnotations: options.trustToolAnnotations ?? false,
    transport: NativeMcpClient.http({
      url: options.url,
      headers: Headers.fromInput(options.headers),
      protocol: options.protocol ?? McpProtocol.v2025_11_25,
      maxMessageBytes: options.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES,
    }),
  }),
} as const;

/** Native scoped stdio transport; requires `ChildProcessSpawner`. */
export const McpStdioTransport = {
  make: (
    options: McpStdioTransportOptions,
  ): McpServerTransport<ChildProcessSpawner.ChildProcessSpawner> => ({
    serverId: options.serverId,
    trustToolAnnotations: options.trustToolAnnotations ?? false,
    transport: NativeMcpClient.stdio({
      command: ChildProcess.make(options.command, [...(options.args ?? [])], {
        ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
        ...(options.env === undefined ? {} : { env: options.env, extendEnv: true }),
      }),
      protocol: options.protocol ?? McpProtocol.v2025_11_25,
      maxMessageBytes: options.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES,
    }),
  }),
} as const;

const ToolArguments = Schema.Record(Schema.String, Schema.Json);
const decodeToolArguments = Schema.decodeUnknownEffect(ToolArguments);

const errorText = (content: ReadonlyArray<McpSchema.ContentBlock>): string =>
  content
    .flatMap((block) => (block.type === "text" ? [block.text] : []))
    .join("\n")
    .slice(0, 4 * 1024);

const describeDefect = (defect: unknown): string =>
  (Predicate.hasProperty(defect, "message") && Predicate.isString(defect.message)
    ? defect.message
    : String(defect)
  ).slice(0, 4 * 1024);

const connectionError = (serverId: string, message: string) => (cause: unknown) =>
  McpConnectionError.make({ serverId, message, cause });

/**
 * Keeps every non-interrupt outcome of one protocol step typed: expected
 * failures and defects (such as an undecodable server reply) both become the
 * connection error, so a misbehaving server cannot crash the connector.
 */
const failClosed =
  (serverId: string, message: string) =>
  <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, McpConnectionError, R> =>
    self.pipe(
      Effect.mapError(connectionError(serverId, message)),
      Effect.catchDefect((defect) => Effect.fail(connectionError(serverId, message)(defect))),
    );

const connectServer = Effect.fnUntraced(function* <R>(
  server: McpServerTransport<R>,
  request: McpConnectionRequest,
  clientInfo: McpSchema.Implementation,
): Effect.fn.Return<McpConnectedServer, McpConnectionError, Scope.Scope | R> {
  const serverId = server.serverId;

  const transport = yield* server.transport.pipe(
    failClosed(serverId, `Could not acquire MCP transport for ${serverId}`),
  );

  const client = yield* NativeMcpClient.make({
    ...clientInfo,
    timeout: request.connectTimeoutMillis,
    discoveryLimits: { maxPages: MAX_TOOL_LIST_PAGES, maxItems: request.maxToolCount },
  }).pipe(
    Effect.provideService(NativeMcpClient.Transport, transport),
    failClosed(serverId, `MCP initialization with ${serverId} failed`),
  );

  // Modern MCP permits missing serverInfo; this connector requires it for discovery identity.
  if (client.serverInfo === undefined) {
    return yield* McpConnectionError.make({
      serverId,
      message: `MCP server ${serverId} did not provide its implementation identity`,
    });
  }
  yield* Effect.annotateCurrentSpan({ "mcp.protocol_version": client.protocolVersion });

  const tools = yield* client
    .listTools()
    .pipe(failClosed(serverId, `MCP tools/list with ${serverId} failed`));

  const byName = new Map(tools.map((tool) => [tool.name, tool]));

  const callTool = Effect.fnUntraced(function* (name: string, params: unknown) {
    const tool = byName.get(name);

    if (tool === undefined) {
      return yield* McpToolCallFailed.make({
        serverId,
        tool: name,
        reason: "protocol-error",
        message: "MCP tool was not in the discovered snapshot",
      });
    }

    const args = yield* decodeToolArguments(params ?? {}).pipe(
      Effect.mapError((cause) =>
        McpToolCallFailed.make({
          serverId,
          tool: name,
          reason: "invalid-arguments",
          message: `MCP tool arguments must be a JSON object: ${cause.message}`,
        }),
      ),
    );

    const result = yield* client.callTool({ tool, arguments: args }).pipe(
      Effect.mapError((error) =>
        McpToolCallFailed.make({
          serverId,
          tool: name,
          reason: toolFailureReason(error.reason),
          message: error.message.slice(0, 4 * 1024),
        }),
      ),
      Effect.catchDefect((defect) =>
        Effect.fail(
          McpToolCallFailed.make({
            serverId,
            tool: name,
            reason: "protocol-error",
            message: describeDefect(defect),
          }),
        ),
      ),
    );

    if (result.isError === true) {
      return yield* McpToolCallFailed.make({
        serverId,
        tool: name,
        reason: "tool-error",
        message: errorText(result.content),
        content: result.content,
      });
    }

    return McpToolResult.make({
      content: result.content,
      ...(result.structuredContent === undefined
        ? {}
        : { structuredContent: result.structuredContent }),
    });
  });

  return {
    identity: McpServerIdentity.make({ serverId, implementation: client.serverInfo }),
    capabilities: client.capabilities,
    tools,
    callTool,
    trustToolAnnotations: server.trustToolAnnotations,
  } satisfies McpConnectedServer;
});

/** Native validation failures at this boundary describe an invalid server response. */
const toolFailureReason = (
  reason: McpClientError.McpClientErrorReason,
): McpToolCallFailed["reason"] => {
  switch (reason._tag) {
    case "TransportError":
    case "HttpError":
    case "ClosedError":
    case "TimeoutError":
    case "LimitError":
      return "transport";
    case "ProtocolError":
    case "UnsupportedError":
    case "ConfigurationError":
    case "ValidationError":
      return "protocol-error";
    case "ToolError":
      return "tool-error";
  }
};

/** Platform services one transport needs, as declared by `McpServerTransport<R>`. */
export type McpTransportRequirements<Transport> =
  Transport extends McpServerTransport<infer R> ? R : never;

const makeConnectorLayer = <R>(
  transports: ReadonlyArray<McpServerTransport<R>>,
  options: { readonly clientInfo?: McpSchema.Implementation | undefined } | undefined,
): Layer.Layer<McpConnector, never, R> =>
  Layer.effect(McpConnector)(
    Effect.gen(function* () {
      const services = yield* Effect.context<R>();
      const byServerId = new Map(transports.map((transport) => [transport.serverId, transport]));
      const clientInfo = options?.clientInfo ?? defaultMcpClientInfo;

      return McpConnector.of({
        connect: (request) => {
          const transport = byServerId.get(request.serverId);

          if (transport === undefined) {
            return Effect.fail(
              McpConnectionError.make({
                serverId: request.serverId,
                message: `No MCP transport is configured for server '${request.serverId}'`,
              }),
            );
          }

          // The Layer supplies the transport's platform services, but the
          // connection must live and die with the caller's Scope, never the
          // Layer's.
          return connectServer(transport, request, clientInfo).pipe(
            Effect.updateContext((caller: Context.Context<Scope.Scope>) =>
              Context.merge(Context.merge(caller, services), Context.pick(Scope.Scope)(caller)),
            ),
          );
        },
      });
    }),
  );

/**
 * `McpConnector` over real transports. Each `connect` acquires the transport
 * in the caller's Scope, uses the selected native protocol, lists tools within the
 * request bounds, and returns discovery metadata plus a typed call operation.
 * `connectMcp` constructs the Toolkit and handlers. The Layer requires the union of every
 * transport's platform services.
 */
export const layer = <const Transports extends ReadonlyArray<McpServerTransport<unknown>>>(
  transports: Transports,
  options?: { readonly clientInfo?: McpSchema.Implementation | undefined },
): Layer.Layer<McpConnector, never, McpTransportRequirements<Transports[number]>> =>
  makeConnectorLayer(
    // Type-level only: each element declares its own `R`, and the Layer
    // requires their union. Nothing about the values changes.
    transports as ReadonlyArray<McpServerTransport<McpTransportRequirements<Transports[number]>>>,
    options,
  );
