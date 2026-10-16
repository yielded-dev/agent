---
name: o11y
description: Instrument and inspect application traces, logs, and metrics, including local Maple tracing, Cloudflare observability, and Effect OTLP export.
---

# Observability

Make critical user paths explainable through the application's normal telemetry. Start with the operation and question: where time went, which work repeated, or why an operation failed. Reuse existing instrumentation and collectors before adding another path.

Keep span boundaries, structured events, and useful counters in the owning library or application so developers, agents, and CI share them. Use stable operation names, service identity, trace correlation, and bounded attributes. Cover meaningful API and agent-loop phases; avoid tracing every helper or recording raw prompts, credentials, and payloads by default.

Read only the relevant reference:

- Local development and trace/log queries: [Maple CLI](references/local-maple.md).
- Deployed Workers and Durable Objects, native traces, and CPU profiles: [Cloudflare](references/cloudflare.md).
- Effect instrumentation and exporter composition: [Effect OTLP](references/effect-otlp.md).

Keep telemetry delivery outside user-facing critical paths where the runtime permits it. Preserve application failures and resource lifetimes, bound buffering and export work, and avoid duplicate exporters or root spans. Include normal instrumentation overhead in performance comparisons.

For a wiring change, verify one representative operation reaches the intended sink with the expected parent/child spans and correlated logs. Reuse that evidence while the wiring is unchanged. A visible trace proves delivery; use [measure-performance](../measure-performance/SKILL.md) for regression claims and [feedback-loops](../feedback-loops/SKILL.md) for slow or duplicated measurement tooling.
