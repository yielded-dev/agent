---
title: SQLite
description: Retain completed agent runs in a Node.js SQLite database.
---

<a id="sqlite"></a>

Provide `PersistentHistory.layer` with a SQLite store to retain completed runs across Node.js restarts:

```ts twoslash
import { AgentRuntime, PersistentHistory } from "@yielded/agent";
// ---cut---
import { SqliteThreadStore } from "@yielded/agent-storage-sqlite";
import { Effect, Layer } from "effect";

import { planner } from "./node-agent.ts";

const History = PersistentHistory.layer.pipe(
  Layer.provide(SqliteThreadStore.layer({ filename: "./history.sqlite" })),
);

const conversation = Effect.gen(function* () {
  const first = yield* AgentRuntime.run(planner, "Plan a trip to Lisbon");
  return yield* AgentRuntime.run(planner, "Make it cheaper", {
    threadId: first.threadId,
  });
}).pipe(Effect.provide(History));
```

Add the adapter to your existing Yielded Agent application:

```sh
bun add @yielded/agent-storage-sqlite@beta effect
```

Here, `planner` is an [agent definition](/guide/agents/). Supply its model and tool
services around the program. The storage Layer opens a scoped Node SQLite client
and supplies Crypto. Keep framework packages at one release and use compatible
[Effect packages](/guide/getting-started/#installation-and-compatibility).

Keep the database file on persistent storage. Save the returned `threadId` and pass
it to later runs to continue the conversation, including after a process restart.
Provide the history Layer around the complete program or application runtime.

`SqliteThreadStore.layer({ filename, synchronous: "NORMAL" })` opts this store's connection
into WAL `NORMAL` synchronization; the default is `FULL`. Choose the mode when constructing
the store, and use the same choice for storage Layers sharing one SQL client.
WAL `NORMAL` survives process crashes, but power loss or an OS crash can lose acknowledged
commits and cause external effects to repeat during durable recovery.

## What is retained

`PersistentHistory.layer` commits each successful run's input and native messages
as one atomic batch before publishing `RunCompleted`. A failure or interruption
before commit leaves none of that run in history. A storage error after commit can
leave the whole run recorded, so inspect history before retrying.

The database retains conversation history across restarts. An interrupted run has
no automatic recovery. See [retained history](/guide/threads/#retain-completed-runs)
for the commit policy, concurrency, and history inspection.

## Recover unfinished work

Use [`@yielded/agent-platform-node`](/platforms/node/) when accepted work must
survive a restart. It assembles SQLite storage, agent registrations, admission,
recovery, and a worker pool. Run one live host per SQLite file.

For custom durable assemblies, `SqliteSubmissionLedger.ledgerLayer(options)`
provides the separate accepted-work ledger. Point it at the same database file as
`SqliteThreadStore.layer(options)` so ownership claims fence the same thread log.

SQLite tracks layout separately from record meaning. This unreleased protocol opens only fresh
layout-23 stores with `effect-agent/thread@5` records. Predecessor or ambiguous stores are rejected
before mutation. Same-format export/import installs into an empty destination Thread; no older
layout upgrade or archive converter is provided. See the
[operator procedure](/guide/operations/#adopting-these-contracts).
