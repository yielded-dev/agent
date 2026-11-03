---
"@yielded/agent": patch
---

Use Effect's native MCP client for HTTP and stdio connections while retaining validated discovery and typed tool outcomes. BEHAVIOR CHANGE: Select protocol `2025-11-25` (default) or `2026-07-28` with the transport's native `protocol` adapter, use Effect's `McpClient.ProtocolVersion` instead of the removed `McpClientProtocolVersion` schema, and supply native `McpClient.Transport` through custom transports' `transport` field instead of an RPC `protocol`.
