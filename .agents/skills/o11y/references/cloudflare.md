# Cloudflare observability

Identify the account, Worker, deployed version, and relevant Durable Object when applicable. Use the application's existing telemetry path and deployed settings. Inspect a representative invocation before changing global sampling or adding a second exporter.

## Traces and logs

Use native [Workers traces](https://developers.cloudflare.com/workers/observability/traces/) and the project's trace bridge for request, RPC, and application spans. Preserve parent context across boundaries and correlate errors with the same invocation. In an Effect application, inspect its Worker/DO adapter before adding a separate root tracer.

Keep required behavior observable without exporting each event synchronously on the response path. Worker requests, streaming responses, and Durable Objects have different lifetimes: use the runtime adapter's supported delivery and flush mechanism, preserving stream completion and object ownership. A periodic exporter designed for a long-running process is not by itself a reliable Worker integration.

For local Worker development, [Maple](local-maple.md) can receive OTLP from the existing adapter. Keep sink selection in configuration and share application spans between environments. A deployed Worker cannot reach the laptop's `127.0.0.1`; use a configured reachable sink for deployed export. Consult [Effect OTLP](effect-otlp.md) for the composition principles, and the [Effect/Worker guide](../../effect/references/api/cloudflare-workers.md) for the runtime boundary.

## CPU or allocation attribution

Use the installed `cloudflare-cpu-profiling` skill for capture and interpretation. It owns the `cf` profiling command, running-version selection, representative traffic, source maps, and Worker/DO targeting. If unavailable, use the [platform profiling guide](https://developers.cloudflare.com/workers/observability/profiling-in-production/). For general command discovery, use [Cloudflare CLI](../../cloudflare/references/cli.md).

Keep profile capture separate from comparable timing runs. Profiles explain CPU or allocation work; traces show the operation's critical path. Use [measure-performance](../../measure-performance/SKILL.md) for before/after acceptance, and [Alchemy temporary stacks](../../alchemy/references/temporary-stacks.md) when the experiment needs isolated deployed resources.
