---
title: Code Mode
description: Let a model compose a bounded JavaScript program over explicitly allowed tools.
---

Code Mode lets the model write one program that calls several application tools, transforms their
results, and returns compact JSON. It is useful when intermediate data needs computation but does
not need another model turn. Your host fixes the callable methods and their authority.

## Build an analyst

```ts twoslash
import { CodeMode } from "@yielded/agent";
import { Schema } from "effect";
import { Tool } from "effect/ai";

const Sales = Tool.make("read_sales", {
  parameters: Schema.Struct({ region: Schema.String }),
  success: Schema.Array(Schema.Number),
});

const Analyze = CodeMode.make("analyze", {
  description: "Read regional sales and calculate a summary.",
  tools: { sales: { read: Sales } },
});
```

Give the Agent `Analyze.tool` and provide `Analyze.layer`, the selected tool handlers, and a
`CodeExecutor`. The generated program can call `sales.read`; it cannot discover arbitrary host
services. Handlers still authorize the specific account or dataset being accessed.

## Program results and limits

The program is one async function expression returning JSON. Namespace methods return Promises;
use `await` for dependencies and bounded executor concurrency for independent calls. Logs and the
result share a model-visible byte budget. Source, time, host-call count, concurrency, and payloads
have separate limits. Inner calls also count toward the Agent's tool-call budget.

Arguments are Schema-validated before handlers start. Calls requiring additional approval fail
closed inside the program; obtain approval through an appropriate outer workflow.

## Partial outcomes

Writes are not transactional. A failed program can leave successful writes behind. Failure reports
classify calls as succeeded, failed, uncertain, or not started; omitted evidence does not mean a
call never ran. Reconcile uncertain effects before retrying.

The outer Code Mode tool is always `uncertain`, even if its allowlisted tools are readonly.
A durable host cannot checkpoint or replay an unfinished JavaScript program automatically.
Host `onPassExit` reports are ephemeral and do not survive process loss.

## Run generated code on Cloudflare

`CloudflareCodeMode.layer` supplies an executor through a Worker Loader binding. Each pass runs
in a fresh Dynamic Worker with ambient network disabled and no application bindings, secrets,
filesystem, or environment. Its authority is the scoped callback for allowlisted tools.

The [Cloudflare application](https://github.com/yielded-dev/agent/tree/main/examples/travel-planner)
provides runnable deployment setup; Code Mode additionally needs a Worker Loader binding.
Use `ToolDiscovery` with `includeDeclarations: false` when method documentation is too large to
send on every request. Discovery still respects the host's visibility policy.
