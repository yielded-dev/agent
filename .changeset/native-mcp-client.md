---
"@yielded/agent": patch
"@yielded/agent-testing": patch
---

Remove framework-owned MCP clients, connectors, discovery contracts, and unused discovery fixtures. BEHAVIOR CHANGE: Use `effect/ai/McpClient` with ordinary Toolkit handlers instead of `@yielded/agent/mcp` or `@yielded/agent/mcp-client`.
