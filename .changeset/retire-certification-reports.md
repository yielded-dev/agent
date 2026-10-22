---
"@yielded/agent": patch
"@yielded/agent-testing": patch
---

BEHAVIOR CHANGE: Replace `@yielded/agent-testing/certification` and `@yielded/agent/testing/certification` with the aggregate `durableCoordinatorConformance` Effect from `@yielded/agent-testing/durable-coordinator-conformance`. Run storage-port conformance and actual restart checks separately; the coordinator helper checks recovery after injected faults without producing certification reports.
