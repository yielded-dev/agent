---
title: Operations
description: Authorize durable work, explain blocked submissions, and preserve external-effect evidence through recovery.
---

<a id="operations"></a>

A durable host owes every accepted submission a terminal outcome. Your application must make
pending work visible, authorize operator actions, and keep recovery running. Route each Thread
to one owner process; storage does not coordinate multiple workers competing for that Thread.

## Authorization and isolation

Put the host behind authenticated ingress. Receipts, Thread IDs, principal strings, and audit
authors identify work; they grant no access. Yielded Agent does not supply tenant authentication
or automatic row-level tenant isolation.

Authorize admission before `submit`. Install `OperationAuthorizer` when constructing the runtime
for observation, settlement waits, abort, resolution, and administrative operations. Its default
trusts service holders. Direct store reads bypass that boundary, so keep raw stores and RPCs
inside the trusted host. Use tenant-qualified addresses or separate storage domains consistently.

[`RunToolAuthorization`](/guide/tools/#authorize-tool-calls) separately protects application tool
execution. Approval and subagent grants do not replace current resource authorization.

<a id="administrative-operations"></a>

## Inspect and retry work

Start with a read-only explanation:

```ts twoslash
import { DurableAgentRuntime } from "@yielded/agent/durable-agent-runtime";
import { ThreadId } from "@yielded/agent/identifiers";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const inspect = Effect.gen(function* () {
  const runtime = yield* DurableAgentRuntime;
  return yield* runtime.explainThread(Schema.decodeSync(ThreadId)("support-42"));
});
```

Run this through the application's existing host Layer. `explain(submissionId)` narrows the view
to one submission. An explanation identifies the recovery decision and what an operator can do.

| Operation         | Use                                                     |
| ----------------- | ------------------------------------------------------- |
| `verify`          | Check retained history and active execution checkpoints |
| `scanObligations` | Find aging or blocked accepted work                     |
| `retry`           | Reapply recovery after fixing a recoverable problem     |
| `wake`            | Send a droppable hint that work may be ready            |

Verification checks schema, identity, order, and original context consistency. It is not tamper
evidence or proof of discarded checkpoint revisions. A Run's current continuation is mutable
operational state and is deleted at settlement; unresolved operations retain their own evidence.

<a id="obligation-monitoring"></a>

## Monitor unfinished work

Schedule `scanObligations` from your monitoring service or host alarm. Watch Unknown outcomes,
approval waits, waiting children, and aging ready/running work. The library does not send alerts
for you. Generic retry refuses work awaiting approval or Unknown resolution; use their specific
authorized operations.

## Abort unknown work

An ordinary mutating call with an uncertain outcome is not blindly replayed. Inspect the external
system and use `resolveUnknown` to record the appropriate evidence and decision. Explicit Unknown
remains unresolved until that resolution is processed; readonly or idempotent recovery rules do
not erase it.

`abort` ends the submission's execution obligation without proving whether an external effect
happened. Preserve unresolved evidence after abort. A safe-to-retry decision is permission to
proceed, not proof that a previous effect never occurred.

## Observe an outcome

`observe(receipt)` follows canonical records and can resume from an observation offset;
`awaitSettlement(receipt)` waits for the terminal outcome. Disconnecting either observer does not
cancel accepted work. Retain the receipt and reconnect. Use a persisted abort command when
cancellation is intended.

## Back up and restore on Node.js

Take a file-consistent SQLite backup through the owning host connection or while the host is
stopped. Stop the original owner before serving a restored copy. Keep exports idle and the source
quiescent until every page is captured; import into an empty destination Thread.

After restore, reconcile effects and receipts created after the backup. Restoring data cannot undo
external writes. Run verification before reopening admission. Apply the same caution to Cloudflare
point-in-time restoration and cross-Object work.

## Scheduled input

Schedules and event subscriptions retain delivery obligations separately from Agent execution.
A retained event or message is not a submission receipt, and a receipt does not prove processing.
Inspect parked deliveries and preserve their original idempotency keys when retrying.

Schedule and subscription limits count active or pending work, including paused registrations,
parked deliveries and settlement observation. Settled history does not consume these slots.

Use the [Node scheduling example](https://github.com/yielded-dev/agent/blob/main/packages/platform-node/test/fixtures/scheduling-example.ts)
for runnable setup, and [messaging](/guide/messaging/) for retained-send semantics.
