# Durable thread benchmark

Same local workload as [clavia-labs/durable-bench](https://github.com/clavia-labs/durable-bench).
Seed a SQLite Durable Object with a scripted conversation, restart the runtime, then measure
cold open, warm turns, database size, and process RSS.

Targets:

- `yielded` — public RPC admission, native alarm execution, and `awaitSettlement`
- `yielded-inline` — the original direct submit/process baseline
- `pi` — published `@earendil-works/pi-durable` 1.0.4, compaction disabled. Later history fixes that are not in this release are not included.
- `tardie` — published `tardie` 0.44.0

Historical turns repeat a one-tool, one-tool, zero-tool cycle. Each measured sample then runs ten
turns of eight `lookup` calls. Tool results are 256 bytes, except every 97th result, which is 8 KiB.
The model is scripted. Nothing calls a provider.

Cold is the median of open time plus the first turn, excluding Miniflare startup. Warm is the median
of the other nine turns. For `yielded`, the first submission constructs the Object; the separate
open step does no work. The outer Worker calls `CloudflareThreadClient.submit` and `awaitSettlement`
over RPC for each turn. Its elapsed time includes client setup and definition hashing, admission,
alarm dispatch, execution, and settlement observation. The waiter uses wake hints with the
production 500 ms polling fallback; its return
does not imply the alarm has finished maintenance and cleanup.

`yielded-inline` explicitly repairs the canonical log on open, then admits and executes each turn
inline through `processThreadResolved`. Both targets seed their own Object in place through the
same inline seeder and retain the model-visible transcript fingerprint. Compaction is off: the
agent has no `contextTokenLimit`.

These are local Miniflare numbers. They are not Cloudflare CPU billing.

Yielded Agent saves a canonical history range and a Prompt digest, without a per-record manifest
or a lifetime saved-context record limit. Cold recovery rebuilds that original range and verifies
the digest. Compaction stays off here, so reading and projecting the live Prompt still grows with
conversation length. pi-durable and tardie use a 1e9-token window with compaction disabled.

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
vp run -F @yielded/agent-example-durable-bench seed -- yielded-inline
vp run -F @yielded/agent-example-durable-bench seed -- pi
vp run -F @yielded/agent-example-durable-bench seed -- tardie
vp run -F @yielded/agent-example-durable-bench bench -- yielded
vp run -F @yielded/agent-example-durable-bench bench -- yielded-inline
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

New rows include `executionPath`: `rpc-alarm` for `yielded`, and `direct-turn` for the other
targets. Report and chart use this marker to distinguish Yielded paths: unmarked legacy
`yielded` rows and marked `direct-turn` rows are grouped as `yielded-inline`; only `rpc-alarm`
rows are grouped as `yielded`. Unknown markers are rejected. Unmarked production samples cannot
be distinguished from legacy rows and must be regenerated or labeled from their provenance.
