---
title: In-memory
description: Share conversations within an application Scope without persistent storage.
---

<a id="in-memory"></a>

`InMemory.layer` keeps conversations and attached-subagent reservations in one
application Scope. It is useful for trying an agent, local sessions, and work that
can end with the process. No storage package is required.

```ts twoslash
// ---cut---
import { AgentRuntime, InMemory } from "@yielded/agent";
import { Effect } from "effect";

import { planner } from "./node-agent.ts";

const conversation = Effect.gen(function* () {
  const first = yield* AgentRuntime.run(planner, "Plan a trip to Lisbon");
  return yield* AgentRuntime.run(planner, "Make it cheaper", {
    threadId: first.threadId,
  });
}).pipe(Effect.provide(InMemory.layer));
```

Supply the agent's model and tool services around this program. The
[runnable planner](https://github.com/yielded-dev/agent/tree/main/docs/snippets/travel-planner)
contains the complete setup.

## Share the application Scope

Provide the Layer once around the conversation, or acquire it in one
`ManagedRuntime` shared by your request handlers. Rebuilding it creates separate
state. Save the returned Thread ID for follow-ups; omitting it starts a new Thread.

Complete history updates remain available if a later step fails or is interrupted.
Closing the Scope or losing the process discards everything. No interrupted Run
can recover after process loss.

| Built-in conversation limit      | Maximum |
| -------------------------------- | ------- |
| Threads per store                | 256     |
| Encoded content per message      | 4 MiB   |
| Encoded content across the store | 64 MiB  |

These bounds protect individual messages and host memory; a Thread has no quota on message count
or total content. Exceeding one fails with `ThreadLimitExceeded`.

## Canonical stores for custom assemblies

`@yielded/agent-storage-memory` implements canonical Thread, Submission, and settlement
ports for tests and custom runtimes. Its ledger is explicitly non-durable. These
ports are separate from the conversation store supplied by `InMemory.layer` and
must share one storage owner when composed.

Choose [persistent history](/storage/sqlite/) to retain completed Runs after restart,
or a [durable platform](/platforms/) to recover accepted work as well.
