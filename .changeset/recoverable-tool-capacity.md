---
"@yielded/agent": patch
---

Return explicit not-dispatched tool results when a fresh durable batch exceeds result capacity, so the agent can continue with smaller batches. Preserve atomic refusal and existing recovery guarantees for already admitted operations.
