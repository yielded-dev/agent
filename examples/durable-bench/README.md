# Durable thread benchmark

Same local workload as [clavia-labs/durable-bench](https://github.com/clavia-labs/durable-bench).
Seed a SQLite Durable Object with a scripted conversation, restart the runtime, then measure
cold open, warm turns, database size, and process RSS.

Targets:

- `yielded` — this repository's Durable Object runtime
- `pi` — published `@earendil-works/pi-durable` 1.0.4, compaction disabled. Later history fixes that are not in this release are not included.
- `tardie` — published `tardie` 0.44.0

Historical turns repeat a one-tool, one-tool, zero-tool cycle. Each measured sample then runs ten
turns of eight `lookup` calls. Tool results are 256 bytes, except every 97th result, which is 8 KiB.
The model is scripted. Nothing calls a provider.

Cold is the median of open time plus the first turn, excluding Miniflare startup. Warm is the median
of the other nine turns. Yielded Agent's open repairs the canonical log. Each of its turns admits
the input and runs it inline through the same worker an alarm would run, so the alarm timer is not
part of the number. Compaction is off: the agent has no `contextTokenLimit`.

These are local Miniflare numbers. They are not Cloudflare CPU billing.

Yielded Agent bounds the model context it saves for replay at 4,096 records or 32 MiB. Compaction stays off here, so a history past that bound is rejected. pi-durable and tardie use a 1e9-token window with compaction disabled, and still accept the 3,500-turn size.

## Setup

From the repository root:

```sh
vp install
vp run -F @yielded/agent-example-durable-bench vendor
```

## Run

Omit sizes to use 50, 250, 1,000, and 3,500 turns. Three samples is the default.

```sh
vp run -F @yielded/agent-example-durable-bench seed -- yielded
vp run -F @yielded/agent-example-durable-bench seed -- pi
vp run -F @yielded/agent-example-durable-bench seed -- tardie
vp run -F @yielded/agent-example-durable-bench bench -- yielded
vp run -F @yielded/agent-example-durable-bench bench -- pi
vp run -F @yielded/agent-example-durable-bench bench -- tardie
vp run -F @yielded/agent-example-durable-bench report
vp run -F @yielded/agent-example-durable-bench chart
```

A smaller run. `vp run` does not forward `SAMPLES` or `CPU_MAX`, so pass flags. `--cpu-max 1` skips the idle wait. The default gate is 8% CPU.

```sh
vp run -F @yielded/agent-example-durable-bench seed -- yielded 50
vp run -F @yielded/agent-example-durable-bench bench -- yielded 50 --samples 1 --cpu-max 1
```

`report` prints cold, warm, storage, and RSS. `chart` writes `results/chart.svg` for cold and warm
latency. Raw samples stay in `results/results.jsonl`.
