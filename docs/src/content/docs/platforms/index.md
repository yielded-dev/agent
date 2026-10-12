---
title: Platforms
description: Choose who owns your durable Threads and drives their work.
---

<a id="platforms"></a>

A durable host owns the live execution of a Thread. It admits input, runs registered
agents, and returns to unfinished work after a restart. Storage retains the facts;
the host supplies the execution lifetime and recovery driver.

| Choose                                                              | Execution owner       | Storage                                        | How work advances                         |
| ------------------------------------------------------------------- | --------------------- | ---------------------------------------------- | ----------------------------------------- |
| [Node.js](/platforms/node/)                                         | A process you operate | Local SQLite file                              | Scoped worker pool and wake notifications |
| [Cloudflare](/platforms/cloudflare/)                                | A Durable Object      | Object-local SQLite                            | Bounded maintenance passes and alarms     |
| [Custom host](/guide/run-agents/#assemble-a-custom-durable-runtime) | Your application      | Compatible storage ports, including PostgreSQL | Your recovery and dispatch driver         |

For example, a support assistant that must finish accepted work after its web
request ends needs a durable host. Choose Node when you operate the process and
persistent disk. Choose Cloudflare when Durable Objects fit your deployment and
Thread placement. PostgreSQL is a storage choice; the supplied Node host uses SQLite.

## Choose what you need

Use `AgentRuntime.run` with `InMemory.layer` for work whose lifetime follows the
application Scope. Follow-ups can reuse a Thread, but process loss ends execution
and discards its history.

Use `PersistentHistory.layer` with a [database adapter](/storage/) when completed
conversations must survive restarts. This retains successful Runs without resuming
interrupted ones. Durable hosts additionally retain accepted work and drive it
toward settlement. Read the [durability contract](/concepts/durability/) before choosing.

## What your application supplies

You own authentication, Thread access, model clients, tool handlers, and external
side-effect policies. Each Thread has one live owner, including on PostgreSQL.
An effect recorded as Unknown requires reconciliation or authorized resolution;
changing hosts does not make it safe to replay.

Start with the [Node example](/platforms/node/#start-the-host) or the
[runnable Cloudflare app](https://github.com/yielded-dev/agent/tree/main/examples/travel-planner).
For execution driven by an Effect Workflow engine, see [Workflows](/guide/workflows/).
