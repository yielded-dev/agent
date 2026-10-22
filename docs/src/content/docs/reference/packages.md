---
title: Package map
description: Find the package, public import, and ownership boundary for each capability.
---

<a id="package-map"></a>

`@yielded/agent` owns agent definitions, execution, Thread contracts, and durability.
Hosts and adapters supply concrete execution environments and persistence. Install
only the capabilities your application needs, keeping Yielded Agent packages on
the same release. Package manifests declare peer compatibility.

## Public imports

Root exports are module namespaces. Direct module paths use kebab-case and can
select individual declarations:

```ts twoslash
import { Agent, AgentRuntime } from "@yielded/agent";
import { IdGenerator } from "@yielded/agent/id-generator";

Agent.make;
AgentRuntime.run;
IdGenerator;
```

The direct equivalents are `@yielded/agent/agent` and
`@yielded/agent/agent-runtime`. Use direct paths for lazy-loading boundaries and
runtime-specific helpers. A static root import can load modules you intended to
defer; a root namespace is not a lazy loader. Both import forms address the same API.

Cloudflare's root includes Workers-specific modules. Use its dedicated
`/cloudflare-ai-gateway` or `/browser-rest-capture` subpaths
from Node. Unlisted source files are private implementation details.

### Model requirements

Provide a native Effect Model around a Run or subagent handler Layer.
`Agent.withModel` optionally pairs a definition and Model into a Binding.
Durable registrations carry `{ agent, model, definitions }`; hosts acquire their
services once. Providers come from upstream Effect AI, including OpenAI, Anthropic,
and TypeSafe. Yielded Agent does not wrap their tool or prompt types.

### In-memory defaults

`InMemory.layer` provides scoped conversation history and attached-subagent
reservations. Model clients, credentials, and tool handlers remain explicit.
For completed history across restarts, use `PersistentHistory.layer` with a store;
for recovery, use a durable host. See [storage](/storage/).

<a id="capability-inventory"></a>

## Find a capability

| Need                             | Starting point                                                 |
| -------------------------------- | -------------------------------------------------------------- |
| Define, run, or stream           | [Agents](/guide/agents/), [execution](/guide/run-agents/)      |
| Tools, MCP, approvals, discovery | [Tools](/guide/tools/)                                         |
| Delegate or exchange work        | [Subagents](/guide/subagents/), [messaging](/guide/messaging/) |
| Compaction and memory recall     | [Context management](/guide/context-management/)               |
| Browser or generated code        | [Browser](/guide/browser/), [Code Mode](/guide/code-mode/)     |
| Scheduled or event-driven input  | [Operations](/guide/operations/)                               |

## Packages

| Package                                                                           | Owns                                                                                        |
| --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| <a id="effect-agent-umbrella"></a>`@yielded/agent`                                | Platform-neutral definitions, runtime, capabilities, schemas, storage ports, and recovery   |
| <a id="decision-models"></a>`@yielded/agent-ai-decision`                          | [AutoModel and language-model decisions](/reference/decision-models/)                       |
| <a id="effect-agent-platform-node"></a>`@yielded/agent-platform-node`             | [SQLite host, workers, and Node scheduling](/platforms/node/)                               |
| <a id="effect-agent-platform-cloudflare"></a>`@yielded/agent-platform-cloudflare` | [Thread Objects, RPC, alarms, and Cloudflare adapters](/platforms/cloudflare/)              |
| <a id="effect-agent-storage-memory"></a>`@yielded/agent-storage-memory`           | [Non-durable canonical stores](/storage/memory/) for tests and custom assemblies            |
| <a id="effect-agent-storage-sql"></a>`@yielded/agent-storage-sql`                 | Shared SQL implementations; concrete adapters supply connection and transaction integration |
| <a id="effect-agent-storage-sqlite"></a>`@yielded/agent-storage-sqlite`           | [Node SQLite persistence](/storage/sqlite/)                                                 |
| <a id="effect-agent-storage-postgres"></a>`@yielded/agent-storage-postgres`       | [PostgreSQL persistence](/storage/postgres/) through your SQL client                        |
| <a id="effect-agent-storage-cloudflare"></a>`@yielded/agent-storage-cloudflare`   | [Object-local SQLite persistence](/storage/cloudflare/)                                     |
| <a id="effect-agent-testing"></a>`@yielded/agent-testing`                         | [Scripted models and conformance helpers](/guide/testing/)                                  |
| <a id="effect-agent-pr-review"></a>`@yielded/agent-pr-review`                     | Provider-neutral review over supplied patches and immutable source                          |

<a id="typesafe-ai"></a>

Upstream `@effect/ai-typesafe` provides Jev decisions. See its
[configuration](/reference/decision-models/#typesafe-client).

<a id="compaction-and-unsupported-capabilities"></a>

### Limits and unsupported features

Storage supplies persistence, never distributed Thread ownership. Execution adapters
declare their own [isolation boundaries](/guide/sandbox/). Memory extraction, sharing policy, and
application domain state remain host responsibilities. Read each guide's constraints
before choosing an adapter.

## GitHub Action

The [review Action](https://github.com/yielded-dev/agent/blob/main/action/README.md)
adds GitHub integration and report publication around the review library.

<a id="leaf-examples"></a>

## Examples

Start with the [runnable planner](https://github.com/yielded-dev/agent/tree/main/docs/snippets/travel-planner)
or the [Cloudflare application](https://github.com/yielded-dev/agent/tree/main/examples/travel-planner).
These show complete service composition through public imports.
