---
"@yielded/agent": patch
"@yielded/agent-ai-decision": patch
---

Resolve AutoModel choices explicitly before passing the returned native model to `Agent.withModel` or a durable registration.
**BEHAVIOR CHANGE:** authorize and validate selection input in the host, and apply a Scope and timeout around selection plus execution; `AutoModel.make` no longer returns a model Layer.
