# Local tracing with Maple

Use Maple for local OTLP collection and trace inspection. Check the installed `maple --help` and relevant command help when options differ. These commands were checked against Maple 0.0.22.

## Collector and application

Use `maple whoami --local` to inspect the local target. Reuse a working collector. If none is running, start one through the project's dev workflow or in an owned terminal:

```sh
MAPLE_TELEMETRY=off maple start --host 127.0.0.1 --port 4318 --offline
```

`--offline` serves the bundled UI. `MAPLE_TELEMETRY=off` disables Maple's own telemetry; it does not disable application ingest. Keep the loopback bind for local development. A separate store or port is useful only when the task needs isolation; preserve the existing store and its history. See [Maple Local](https://maple.dev/docs/local-mode/).

Configure the application process, using its existing environment mechanism:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:4318"
export OTEL_SERVICE_NAME="my-api"
export OTEL_TRACES_EXPORTER="otlp"
export OTEL_LOGS_EXPORTER="otlp"
```

This is an OTLP/HTTP base URL. Signal-specific URLs end in `/v1/traces` or `/v1/logs`; inspect existing overrides rather than appending a path twice. The [Effect reference](effect-otlp.md) wires these variables to actual exporters. Environment variables alone do not instrument an app.

## Query a focused operation

```sh
maple services --local --since 10m
maple traces --local --service my-api --since 10m --limit 10
maple trace --local "$TRACE_ID"
maple logs --local --trace-id "$TRACE_ID" --since 10m --limit 20
```

Set `TRACE_ID` from the returned trace you want to inspect. Keep `--local` explicit so a stored hosted login cannot select another backend. Results are JSON by default. Narrow by service, time, and trace before expanding the search; use the [CLI reference](https://maple.dev/docs/reference/cli/) for additional filters and commands.

Exercise the real application operation, then inspect its trace tree and relevant logs. For missing data, check the selected endpoint, service, exporter enablement, and flush lifecycle before repeating the workload. Stop only a collector this task owns, gracefully through its terminal or `maple stop` with the matching data directory. A local trace helps explain work; local timings do not establish deployed Cloudflare performance.
