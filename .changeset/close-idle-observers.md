---
"@yielded/agent-platform-cloudflare": patch
---

Release provisional text observers when their RPC scope ends. BEHAVIOR CHANGE: Deploy the Cloudflare client and Thread Object together; direct `watchTextEncoded` callers must read `stream` and dispose the returned `lifetime`.
