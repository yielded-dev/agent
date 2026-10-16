---
title: Deterministic testing
description: Exercise the real agent runtime with scripted model responses and certify custom storage adapters.
---

<a id="deterministic-testing"></a>

`@yielded/agent-testing` replaces the model provider with scripted responses while retaining the
real interpreter, tools, and output validation. Use it to verify integration behavior without
credentials, network calls, or model variance.

## Provide a scripted model layer

```ts twoslash
import { ScriptedModel } from "@yielded/agent-testing/scripted-model";
import { Model } from "effect/ai";

const TestModel = Model.make(
  "scripted",
  "test-model",
  ScriptedModel.layer([
    {
      _tag: "Stream",
      parts: [
        { type: "text-start", id: "answer" },
        { type: "text-delta", id: "answer", delta: "ready" },
        { type: "text-end", id: "answer" },
        {
          type: "finish",
          reason: "stop",
          usage: { inputTokens: { total: 1 }, outputTokens: { total: 1 } },
        },
      ],
      termination: { _tag: "Complete" },
    },
  ]),
);
```

Provide `TestModel` in place of the provider Layer for a text-output Agent. Keep the application's
runtime and handler composition. Each scripted Turn is consumed once; inspection exposes requests,
remaining turns, and `assertExhausted`. Hooks can inspect the normalized prompt or observe stream
finalization. Scripts can also emit tool calls, fail, or wait for interruption.

<a id="what-to-assert"></a>

## Assert behavior beyond output

Choose assertions for your integration's actual risks: authorized tools, expected output, resource
cleanup, or the outcome after restart. A scripted model proves how the runtime handles the supplied
response; it does not prove that a real model will choose that response.

The [testing package](https://github.com/yielded-dev/agent/tree/main/packages/testing) includes
executable travel-planner and researcher fixtures. Use live evaluation separately when the question
is model quality.

## Certify storage adapters

Run `certifyDurableAdapters` from `@yielded/agent-testing/certification` against your combined
`SubmissionLedger` and `ThreadStore` Layers in a TestClock-backed environment with Crypto.
Use disposable storage. If both ports share a connection root, pass the same combined Layer instance.

| Evidence | What it exercises                             |
| -------- | --------------------------------------------- |
| Tier 1   | Shared storage-port contracts                 |
| Tier 2   | Coordinator recovery across injected failures |
| Tier 3   | Real process loss or eviction                 |

Inspect the full report. Tier 3 distinguishes execution in this run, cited evidence, and
`not-exercised`; a passing in-process suite does not establish crash durability. Custom subscription
stores have separate conformance cases. See the
[certification entry point](https://github.com/yielded-dev/agent/blob/main/packages/testing/src/Certification.ts)
for adapter requirements and the report contract.
