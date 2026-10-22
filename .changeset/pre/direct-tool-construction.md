---
"@yielded/agent": patch
"@yielded/agent-testing": patch
---

Construct MCP toolkits once from validated discovery, render CodeMode declarations only when requested, and keep task-continuation policy in Agent instructions.
**BEHAVIOR CHANGE:** supply discovery schemas and `callTool` from custom MCP connectors, and use native readonly discovery Tools instead of `ToolDiscovery.make` search and failure callbacks.
