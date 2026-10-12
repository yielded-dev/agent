---
title: Threads
description: Choose what a conversation retains and whether active work survives process loss.
---

A Thread is an identified conversation shared across Runs. Its lifetime belongs to the
application's history or durable host. Reusing a Thread ID continues that conversation;
it does not by itself make execution durable or authorize access.

## Reuse a conversation

```ts twoslash
import { AgentRuntime, InMemory } from "@yielded/agent";
import { Effect } from "effect";

import { planner, ModelLive, OpenAiLive } from "./node-agent.ts";
// ---cut---
const conversation = Effect.gen(function* () {
  const first = yield* AgentRuntime.run(planner, "Plan a weekend in Lisbon.");
  return yield* AgentRuntime.run(planner, "Make it cheaper.", {
    threadId: first.threadId,
  });
}).pipe(Effect.provide(ModelLive), Effect.provide(OpenAiLive), Effect.provide(InMemory.layer));
```

One Layer surrounds both Runs. In a server, keep the application Layer in one
`ManagedRuntime`. The [runnable Node snippets](https://github.com/yielded-dev/agent/tree/main/docs/snippets/travel-planner)
show the imported definition and services.

## Choose the lifetime

| Assembly                               | What it retains                                                                  | Who ends it                                 |
| -------------------------------------- | -------------------------------------------------------------------------------- | ------------------------------------------- |
| `InMemory.layer`                       | Incremental complete messages and tool batches, including before a later failure | Application Scope or process loss           |
| `InMemory.scoped` inside that Layer    | Conversations created by one owned workflow, including attached children         | Workflow success, failure, or interruption  |
| `PersistentHistory.layer` with storage | Each successful Run as one atomic history commit                                 | Application retention policy                |
| Durable host                           | Committed conversation plus accepted-work and recovery state                     | Explicit lifecycle and retention operations |

<a id="in-memory-conversations"></a>

In-memory storage has explicit capacity: 256 Threads, 1,024 messages and 4 MiB per Thread,
and 64 MiB overall. Exceeding a bound fails; it does not silently evict conversations.
Finishing a Run does not release its history. Use `InMemory.scoped` for disposable workflows
and consume streams or handles before that scope closes.

## Persistent history versus durable work

Persistent history retains successful Runs but cannot resume interrupted execution. A storage
error can occur after commit; inspect the retained history before retrying. Serialize callers
when repeating external work would be unsafe: a conflicting history append can reject a stale
writer after its tools have already run.

The durable host admits a submission, returns a receipt, and owns its execution history and
settlement. Keep ordinary persistent-history Threads and durable-admission Threads on separate
IDs. Use its API for durable work instead of layering ordinary history hooks around it.
See [durability](/concepts/durability/) and [storage choices](/storage/).

## Three views of one Thread

The model reads a selected context window. Recovery reads active Run state and necessary
evidence. A history UI reads ordered ranges. Compaction can shorten model context without
erasing conversation history; archival can relocate old records without resolving open work.
The durable log has no lifetime record cap, while payloads and Runs have [limits](/concepts/budgets/).

`Thread.Store.snapshot` inspects an in-memory conversation; `Thread.toPrompt` turns it into
an Effect AI Prompt. Durable `ThreadStore` and `ThreadReader` expose journal reads instead.
Authorize tenant and Thread access at the host boundary for both.
