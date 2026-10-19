---
"@yielded/agent": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-platform-cloudflare": patch
---

Use `PreparedInputAdmission` for schedule, subscription, and worker-message delivery. Replace the scheduling-specific admission service and import `PreparedInput`, authorization decisions, retry reasons, and admission errors from `@yielded/agent/prepared-input-admission` using their `PreparedInput` names.
