---
title: Node.js
description: Operate a durable agent host with a scoped worker pool and SQLite.
---

<a id="node-js"></a>

The Node host owns a SQLite file and a bounded worker pool for its application
Scope. It recovers retained work before opening admission, then advances registered
agents as input arrives. Your process owns the server, credentials, and shutdown.

```sh
bun add @yielded/agent-platform-node@beta effect
```

## Create an agent

Define the agent, choose its native Effect model, and supply provider and tool
Layers. A durable registration also carries explicit agent, model, and tool version
declarations. Update those declarations when behavior changes.

The [runnable Node example](https://github.com/yielded-dev/agent/tree/main/docs/snippets/travel-planner)
includes `node-agent.ts`, provider configuration, and the process entry point.
Its provider reads `OPENAI_API_KEY` from the environment.

## Start the host

This example imports that agent setup and acquires one shared host:

```ts twoslash title="node-host.ts" src="snippets/travel-planner/node-host.ts"

```

Give each process start a distinct `producerId`. Run `NodeDurableHost.run` with
`HostLive` provided; the example's `node-main.ts` does this through
`NodeRuntime.runMain`. `run` observes the existing pool and propagates worker
failure. When also serving HTTP, supervise the server and `run` together with
`Effect.raceFirst` so either failure closes the shared Scope.

The host holds an exclusive SQLite connection lock, including against independent
readers. Keep the file on persistent local storage with working SQLite locks.
Stop the host before opening it with the standalone admin CLI or another connection.
Existing files must already use WAL mode; fresh files are initialized for you.

## Configure runtime services

| Option              | Default        | Purpose                               |
| ------------------- | -------------- | ------------------------------------- |
| `workerConcurrency` | `1`            | Concurrent workers; accepts 1–64      |
| `wakeScanInterval`  | `1000` ms      | Fallback scan for missed wake hints   |
| `synchronous`       | `"FULL"`       | SQLite durability setting             |
| `runContext`        | Pass-through   | Application prompt preparation        |
| `toolAuthorization` | Allow all      | Action-time tool policy               |
| `toolReconciler`    | Keep uncertain | Resolve unconfirmed external outcomes |

`"NORMAL"` synchronization can lose acknowledged commits after power loss or an OS
crash, potentially repeating external effects during recovery. Choose it only with
that tradeoff understood.

Services are captured when the host Layer is acquired. Provide context, tool
policy, or compactor Layers at that boundary and keep their resources alive for
its Scope. Authentication and authorization of incoming requests remain yours.

## Submit and follow work

Call `host.submit(agent, input, options)` from an authenticated handler, supplying
Thread identity, principal, definition digests, and an idempotency key. Return the
receipt after admission. Retry the same request with the same key; changed input
under that key conflicts.

Use `host.awaitSettlement(receipt)` for the outcome or `host.observe(receipt)` for
canonical records. An explicit Unknown outcome stays parked for
[authorized resolution](/guide/operations/); a restart does not authorize replay.

## Shutdown and recovery

Closing the Scope stops admission, joins workers, releases live ownership, and
closes storage. A replacement host opens the file and recovers retained work with
fresh local owner generations. There are no distributed leases or takeover protocol.
The host Layer also supplies `DurableAgentRuntime`; use its `explain`, `verify`, and
`scanObligations` methods for inspection and its recovery methods for operator actions.

<a id="workflow"></a>

## Custom runtime composition

`NodeDurableAgentRuntime.layerRegistered` acquires a registered runtime without
starting workers. Use it when your application supplies dispatch, including the
[Effect Workflow assembly](/guide/workflows/#node). For PostgreSQL, compose a
[custom durable host](/storage/postgres/) around its storage ports.
