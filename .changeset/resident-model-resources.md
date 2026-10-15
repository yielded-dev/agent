---
"@yielded/agent": patch
---

Allow registered agents to reuse a stable model graph with `modelLifetime: "runtime"`. BEHAVIOR CHANGE: opting in keeps user-provided model Layers and their resources alive until the registration Scope closes, across individual Runs.
