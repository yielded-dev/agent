---
"@yielded/agent-platform-cloudflare": patch
---

Defer a Durable Object implementation behind a native `lazyObject` facade and reuse the typed client through `makeClient(ThreadObject.localClientTransport)` inside its existing runtime. Keep the implementation in separately uploaded modules to avoid evaluating its dependencies in a native routing Worker.
