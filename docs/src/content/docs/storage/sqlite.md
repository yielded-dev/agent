---
title: SQLite
description: Retain completed conversations in a local SQLite file, or use the Node host for recovery.
---

<a id="sqlite"></a>

SQLite is the local persistence option for Node applications. `SqliteThreadStore`
owns a scoped database connection; `PersistentHistory` connects it to ordinary agent
Runs. Use the [Node host](/platforms/node/) when unfinished work must recover too.

```ts twoslash
// ---cut---
import { AgentRuntime, PersistentHistory } from "@yielded/agent";
import { SqliteThreadStore } from "@yielded/agent-storage-sqlite";
import { Effect, Layer } from "effect";

import { planner } from "./node-agent.ts";

const History = PersistentHistory.layer.pipe(
  Layer.provide(SqliteThreadStore.layer({ filename: "./history.sqlite" })),
);

const program = AgentRuntime.run(planner, "Plan a trip to Lisbon").pipe(Effect.provide(History));
```

Install `@yielded/agent-storage-sqlite@beta` alongside your application packages.
Supply the agent's model and tool Layers; the store supplies its SQL client and
cryptographic services. The [runnable planner](https://github.com/yielded-dev/agent/tree/main/docs/snippets/travel-planner)
contains the agent and provider setup used here.

Save the returned `threadId` and pass it to later Runs, including after restart.
Acquire `History` once around the application so calls share its store. Keep the
file on persistent local storage with working SQLite locks. The convenience Layer
holds an exclusive connection lock; independent readers must wait until it closes.

## What is retained

Each successful Run commits as one atomic batch. Failure or interruption before
commit leaves that Run out of history. A storage error after commit can leave the
whole Run recorded: inspect history before retrying external work.

| Option         | Default   | Meaning                        |
| -------------- | --------- | ------------------------------ |
| `filename`     | Required  | Persistent database path       |
| `synchronous`  | `"FULL"`  | WAL synchronization            |
| `busyTimeout`  | `5000` ms | Write-lock retry window        |
| `verifyOnOpen` | `false`   | Full stored-data audit on open |

`"NORMAL"` synchronization can lose acknowledged commits after power loss or an OS
crash. Existing databases must use the [supported layout and format](/storage/).

## Recover unfinished work

Storage-backed history does not resume interrupted execution. The Node host adds
admission, registrations, recovery, and workers. One process owns a Thread at a
time; separately opened runtimes do not coordinate through distributed leases.
