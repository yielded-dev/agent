---
"@yielded/agent": patch
---

Recover accepted native updates after interrupted tool batches without duplicating their findings or delivery.

BEHAVIOR CHANGE: Update exhaustive handlers of `WorkerSummary.state` to handle `unknown`, now returned for unresolved workers previously reported as `active`.
