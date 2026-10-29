---
"@yielded/agent": patch
"@yielded/agent-ai-decision": patch
"@yielded/agent-platform-cloudflare": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-pr-review": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-postgres": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-testing": patch
---

Upgrade to Effect 4.1 and effect-cf 0.59. BEHAVIOR CHANGE: Replace custom Crypto layers with the required `Digest` and `SecureRandom` services from `effect/crypto`; `NodeCrypto.layer` and `BrowserCrypto.layer` supply both.
