<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset=".github/assets/lockup-agent-paper.svg" />
    <img src=".github/assets/lockup-agent-ink.svg" alt="Yielded Agent" height="48" />
  </picture>
</h1>

<p align="center">
  <a href="https://www.npmjs.com/package/@yielded/agent"><img alt="npm" src="https://img.shields.io/npm/v/@yielded/agent/beta?label=npm&labelColor=121310&color=b9a4ff" /></a>
  <a href="https://github.com/yielded-dev/agent/actions/workflows/ci.yml"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/yielded-dev/agent/ci.yml?branch=main&label=ci&labelColor=121310" /></a>
  <a href="LICENSE"><img alt="MIT license" src="https://img.shields.io/badge/license-MIT-f3f1e8?labelColor=121310" /></a>
</p>

<p align="center">
  <a href="https://yielded.dev/agent/"><b>Documentation</b></a>
  ·
  <a href="https://yielded.dev/agent/guide/getting-started/">Getting started</a>
  ·
  <a href="https://yielded.dev">yielded.dev</a>
</p>

Yielded Agent builds **durable agent Threads** in TypeScript with Effect and Effect AI.
Define schemas, instructions, and tools. The runtime runs the model/tool loop, validates
output, and can retain accepted work across process restarts.

A Thread is the conversation; a Run is one logical execution; an Attempt is one period advancing it.
Additional input can join an active Run.
Model context, history, and recovery state have separate lifetimes, so a long conversation
can be compacted without discarding its committed history.

## What you own

Your application supplies authorization, tool handlers, model services, and storage.
Yielded Agent supplies typed execution, bounded work, approvals, and recovery under a
single owner per Thread. Native Effect AI tools and provider Layers work directly.

A durable receipt means the runtime owes a terminal settlement. Recorded results are reused;
uncertain ordinary mutations require reconciliation. Durable Steps need external idempotency.
There is no exactly-once external execution guarantee.
[Read the durability contract](https://yielded.dev/agent/concepts/durability/).

## Try it

```sh
bun add @yielded/agent@beta effect
```

```ts
import { Agent, AgentRuntime } from "@yielded/agent";
import { Schema } from "effect";
import { Toolkit } from "effect/ai";

const triage = Agent.make("triage", {
  input: Schema.String,
  output: Schema.Struct({
    severity: Schema.Literals(["low", "medium", "high", "critical"]),
    explanation: Schema.String,
  }),
  instructions: "Classify the bug report and explain the severity in one sentence.",
  toolkit: Toolkit.empty,
  policy: { maxTurns: 2, maxToolCalls: 1, maxDuration: "30 seconds" },
});

const result = AgentRuntime.run(triage, "Users cannot sign in.");
```

Supply a model and history Layer to execute the Effect. The
[getting-started guide](https://yielded.dev/agent/guide/getting-started/) has complete provider
setup, installation, and an executable entry point.

## Choose a host

| Need                        | Choice                                            |
| --------------------------- | ------------------------------------------------- |
| Process-local conversations | `InMemory.layer`                                  |
| Retained successful history | SQLite or PostgreSQL adapter                      |
| Durable Node service        | Supplied SQLite host, or a custom PostgreSQL host |
| Durable Cloudflare service  | SQLite-backed Thread Objects                      |

[How it works](https://yielded.dev/agent/concepts/runtime-model/) ·
[Platforms](https://yielded.dev/agent/platforms/) ·
[Packages](https://yielded.dev/agent/reference/packages/)

See the [travel planner](examples/travel-planner/), [runnable snippets](docs/snippets/travel-planner/),
[durable benchmark](examples/durable-bench/), and [cold-start reproduction](examples/cold-start/)
for complete applications and comparisons.

## Development

Framework packages live in `packages/*`; runnable applications live in `examples/*`.
Use Vite+ for repository commands and Bun as the package manager.

```sh
vp install
vp run docs:dev
vp run ready
```

Read [AGENTS.md](AGENTS.md), the [glossary](GLOSSARY.md), and the
[toolchain guide](docs/TOOLCHAIN.md) before contributing. The full gate includes static checks,
tests, package builds, and the documentation build with link validation.
