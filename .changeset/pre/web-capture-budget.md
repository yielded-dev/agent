---
"@yielded/agent": patch
"@yielded/agent-platform-cloudflare": patch
---

Raise the default web-capture response budget to 5 MiB and accept large page text and scraped elements within that budget. Preserve oversized tool outputs with a host-provided store and retrieve omitted text through `read_tool_result`, including a private R2 adapter for Cloudflare hosts.
