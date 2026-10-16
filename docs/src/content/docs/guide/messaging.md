---
title: Agent messaging
description: Send retained input between independent Threads through fixed, application-authorized routes.
---

<a id="agent-messaging"></a>

Peer messaging sends typed input to an independent Agent Thread. The destination owns its input
Schema; your host chooses the route and authorizes the sender. Use [background subagents](/guide/subagents/)
when the sender also owns the recipient's creation and lifetime.

## Send messages through fixed peer routes

```ts twoslash
import { Agent, Messaging } from "@yielded/agent";
import * as Toolkit from "effect/ai/Toolkit";
import * as Schema from "effect/Schema";

const advisor = Agent.make("advisor", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Suggest one next action for the request.",
  toolkit: Toolkit.empty,
});

const Advisor = Messaging.peer("advisor", { target: advisor });
const send = Messaging.sendTool(Advisor);
```

Give the sending Agent `send.toolkit` and provide `send.layer`. Register the destination, then
configure `PeerRoutes` and `PeerAuthorizer` on the durable host. Both deny by default. A model
chooses the input, never an arbitrary Thread address or principal.

For application code, acquire `runtime.messagingHost({ sourceThreadId, principal })` and provide
it to `Messaging.send`. Supply a stable `idempotencyKey`; retries must retain the same input.

## Read delivery status

| Status      | What it proves                                               |
| ----------- | ------------------------------------------------------------ |
| `pending`   | The source retained outbound work                            |
| `accepted`  | The destination admitted it and returned a receipt           |
| `processed` | The destination recorded its settlement                      |
| `parked`    | Automatic retries stopped; authorized intervention is needed |
| `refused`   | Delivery was conclusively refused                            |

A retained message reference is not a submission receipt, and acceptance is not execution.
Use `Messaging.inspect` on the same reference. `Messaging.retry` renews a parked delivery's
bounded retry allowance without changing its frozen destination, principal, or input. It cannot
rewind processed or conclusively refused work.

The default delivery policy permits eight attempts with a 30-second attempt timeout and a
24-hour peer deadline. `PeerMessageCapacity` defaults to 256 per source Thread (maximum 1,000),
counting outstanding sends, including parked work, and settled sends until their last delivery
deadline. Settled sends stop counting after that deadline; exact receipts and append-only history
remain available.

## Reply with current authority

`Messaging.inbox` returns authenticated inbound references. `Messaging.reply` uses one of those
references and rechecks send authority. Receiving a message does not grant permission to reply;
`inReplyTo` on a send is correlation only. Peer routes cannot bypass worker admission rules.

Node's scoped delivery pump and Cloudflare maintenance recover retained deliveries after restart.
They need a functioning host and available capacity. Child completion, report delivery, and the
parent's response remain separate outcomes.

See the [Cloudflare application](https://github.com/yielded-dev/agent/tree/main/examples/travel-planner)
for a runnable host, and [operations](/guide/operations/) for observing and controlling accepted work.
