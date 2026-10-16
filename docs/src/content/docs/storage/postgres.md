---
title: PostgreSQL
description: Use an application-owned SQL pool while retaining one live process owner per Thread.
---

<a id="postgresql"></a>

PostgreSQL stores history and accepted work using your application's Effect SQL
client. Your host must route each Thread to one live process. The adapter does not
provide distributed ownership, lease-based takeover, or a worker service.

```ts twoslash
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as PgClient from "@effect/sql-pg/PgClient";
import { PersistentHistory } from "@yielded/agent";
import { PostgresStorage } from "@yielded/agent-storage-postgres";
import * as Config from "effect/Config";
import * as Layer from "effect/Layer";

const Database = PgClient.layerConfig({
  url: Config.Redacted("DATABASE_URL"),
});

const Persistence = PostgresStorage.layer.pipe(
  Layer.provideMerge(Database),
  Layer.provide(NodeCrypto.layer),
);

const History = PersistentHistory.layer.pipe(Layer.provideMerge(Persistence));
```

Install `@yielded/agent-storage-postgres@beta` and `@effect/sql-pg` alongside your
Effect and platform packages. Create the database and set `DATABASE_URL` in the
host environment. Provide `History` around `AgentRuntime.run`, plus model and tool
services, as in the [planner example](https://github.com/yielded-dev/agent/tree/main/docs/snippets/travel-planner).
Reuse the returned Thread ID for follow-ups.

## What is retained

This history assembly retains each successful Run atomically across restarts; it
does not recover interrupted execution. `PostgresStorage.layer` also supplies the
Submission ledger and settlement publisher for a
[custom durable runtime](/guide/run-agents/#assemble-a-custom-durable-runtime).
Your application must drive recovery and pending work. The supplied Node host uses SQLite.

## Database ownership

Share one pool and storage Layer within the owning process. Stop that owner before
routing its Threads to a replacement. Database persistence does not transfer live
execution authority.

Use `PostgresStorage.layerWith(options)` to configure storage:

| Option                    | Default    |
| ------------------------- | ---------- |
| `schema`                  | `"public"` |
| `observationPollInterval` | `25` ms    |
| `lockTimeout`             | `5000` ms  |
| `verifyOnOpen`            | `false`    |

The Layer initializes tables on acquisition. Pre-provisioning the schema avoids
requiring database-wide schema creation permission. Storage leaves the client's
search path and application query transforms unchanged.

Before upgrading a storage layout, stop every owner sharing that schema. Open it
once with the new release to apply the atomic upgrade, then restart those owners
on the same release. Older releases cannot reopen the upgraded layout.

Call storage writes and snapshot reads outside `sql.withTransaction`; the adapter
owns its transactions. It rejects unsupported [layouts and formats](/storage/).
Keep original stores intact when an open fails.
