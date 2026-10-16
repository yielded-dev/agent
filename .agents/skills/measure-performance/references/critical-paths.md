# Observing the critical path

Use end-to-end traces and a repeatable benchmark. Add missing instrumentation in the owning app or library so its commands and CI can reuse it. For APIs, account for request latency and affected queue, service, database, or external work. For agent loops, separate first useful output from completion; record model/tool calls, sequential rounds, time in each phase, and tokens where available.

Check timing endpoints and clock domains. Acceptance is not readiness, elapsed time is not Worker CPU, and overlapping spans are not additive. Use CPU or allocation profiles for attribution when needed, separately from timing runs. For instrumentation and capture tools, use [observability](../../o11y/SKILL.md). Include normal tracing overhead in comparisons and bound its payload/export work. Do not add model turns merely to observe or explain product execution.
