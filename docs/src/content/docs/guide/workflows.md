---
title: Effect Workflows
description: Compose agent work inside an Effect Workflow while keeping agent recovery and host ownership explicit.
---

<a id="effect-workflows"></a>

Use `@yielded/agent-workflow` when an agent is one step in an Effect Workflow. The Workflow owns
application orchestration; Yielded Agent owns the accepted submission, tool execution, and
canonical outcome. Your host supplies the Workflow engine, storage, and repair scheduling.

## Compose agents in a Workflow

```ts twoslash
import { Agent } from "@yielded/agent";
import { AgentWorkflow } from "@yielded/agent-workflow";
import { Schema } from "effect";
import { Toolkit } from "effect/ai";
import { Workflow } from "effect/workflow";

const triage = Agent.make("triage", {
  input: Schema.String,
  output: Schema.Struct({ severity: Schema.Literals(["low", "high", "critical"]) }),
  instructions: "Classify the severity of the bug report.",
  toolkit: Toolkit.empty,
});

const Review = Workflow.make("Review", {
  payload: { issueId: Schema.String, report: Schema.String },
  success: triage.output,
  error: AgentWorkflow.Error,
  idempotencyKey: ({ issueId }) => issueId,
});

const ReviewLive = Review.toLayer(({ report }) =>
  AgentWorkflow.execute(triage, report, { name: "triage" }),
);
```

Register the same `triage` Definition instance and its model in the durable runtime. Provide
`WorkflowAgentHost.layer` and share one `WorkflowEngine` between the host and parent Workflow.
`AgentWorkflow.execute` returns decoded Agent output; while the submission is pending, it
suspends through a durable completion notification.

The step `name` must be stable across replay and unique within a parent execution. Repeated
calls should use stable item IDs. Reusing a step with changed input or registration conflicts
instead of silently starting different work.

## Configure the host

The host requires durable dispatch storage and a `WorkflowRepairTrigger`. The trigger must run
at startup and keep repairing lost wake hints while its Scope is open. `host.repair` also
supports explicit bounded repair. Keep the host and runtime `deploymentId` aligned.

The application supplies the execution principal and model/tool services. A Workflow input
cannot replace that authority. Result reads recheck authorization on replay.

## Follow and control submissions

Parent interruption, timeout, or shutdown detaches the Workflow; it does not abort accepted Agent
work. Use the host's authorized `abort`, `resolveApproval`, or `resolveUnknown` operations.
Compensation does not undo external tool effects, and uncertain ordinary tools retain the
[Unknown outcome rules](/concepts/durability/).

<a id="node"></a>

## Node.js with SQLite

The Node package provides `NodeWorkflowRepairTrigger` and `SqlWorkflowDispatchStore`. Use them
with an Effect Workflow engine and the [Node durable runtime](/platforms/node/), under one
owner process per Thread. See the
[workflow integration](https://github.com/yielded-dev/agent/tree/main/packages/workflow) for
public APIs and executable integration examples. Agent execution must remain outside database
transactions.
