---
title: Cloudflare
description: Persist each Thread owner's history and unfinished work in Durable Object SQLite.
---

<a id="cloudflare"></a>

Cloudflare storage belongs to a SQLite Durable Object. Its records survive eviction;
its live resources and cached views belong to the current Object incarnation.
Different Objects have separate databases. Your routing determines which Object
owns a Thread.

## Run durable agents

Use [`ThreadObject.make`](/platforms/cloudflare/#create-the-thread-object) for the
complete host. It assembles the stores, registered agents, admission, and alarm-driven
recovery. The [runnable travel planner](https://github.com/yielded-dev/agent/tree/main/examples/travel-planner)
shows the Worker and Object together. No additional storage Layer is needed there.

For example, the planner's Thread Object retains accepted input while the user's
browser disconnects. A later alarm can continue the work. Reconnecting reads
canonical history; Object-local live text is only a provisional preview.

## Use an existing Object

For an application-owned Object, use
[`ThreadObject.layerInHost`](/platforms/cloudflare/#share-an-application-object).
Share one Effect SQL client and mutation gate with its related application state.
The [shared-owner example](https://github.com/yielded-dev/agent/blob/main/packages/platform-cloudflare/examples/shared-owner.ts)
shows the composition.

`@yielded/agent-storage-cloudflare` also exposes `DoThreadStore` and
`DoSubmissionLedger` for custom hosts. Their convenience Layers accept the same
`ctx.storage` and supply SQL and cryptographic services. Acquiring storage alone starts no agent
execution or recovery driver.

| Concern                              | Responsibility         |
| ------------------------------------ | ---------------------- |
| Retained history and unfinished work | Object SQLite          |
| Current execution ownership          | One live Object owner  |
| Recovery scheduling                  | Host alarm integration |
| Thread access and placement          | Your application       |

Stores initialize a fresh database or open the [supported layout](/storage/).
Unsupported layouts fail before initialization. Caches and prompt checkpoints are
derived state; they do not replace canonical history or unfinished Run checkpoints.

Use the adapter for normal writes. Direct SQL maintenance must quiesce port
operations, use the host mutation gate, and call `DoThreadStore.invalidate(ctx.storage)`
before reads resume. Canonical records remain append-only.

## Optional shared memory

Documents shared across Threads have separate ownership from conversation history.
See [Cloudflare shared memory](/platforms/cloudflare/#shared-memory) for namespace
selection, authorization, and a complete example.
