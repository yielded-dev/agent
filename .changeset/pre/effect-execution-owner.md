---
"@yielded/agent": minor
---

Run agents through one scoped Effect owner and adapt public streams with bounded backpressure. Commit validated Turn facts directly, retaining completion-tool results before input draining and declared failure values for siblings retained during child suspension.

BEHAVIOR CHANGE: Provide services for the whole execution; per-pull Context changes no longer reconfigure streams. Terminal Tool events follow call-local telemetry and failure observation, and `maxRunEvents` bounds observed progress rather than headless execution. Replace `bufferLimits.maxSubagentEventsPerBatch` with `maxBufferedEvents` to bound the public stream queue.

Custom durability hooks must implement `initialize`, `commitTurn`, and `checkpoint`, retaining exposure and parameter-rejection evidence from `RunTurnResponse` and propagating retained infrastructure failures. Return `"committed"` from `commitTurn`; return `"deferred"` only when the response explicitly permits readonly deferral, and persist that response before any call-scoped durable capability. Keep completion projections pure because recovery may reevaluate persisted results before `RunCompleted` fixes the output. Decode retained sibling failures with the Tool's failure Schema instead of expecting `{ errorTag, message }` diagnostics.
