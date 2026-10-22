---
title: Deterministic testing
description: Exercise agent integrations with scripted models and check custom storage and executor behavior.
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

## Check storage and coordinator behavior

Run the shared storage-port cases against disposable storage to check each adapter's contract.
The `@yielded/agent/testing/*-store-conformance` modules and
`@yielded/agent/testing/submission-ledger-conformance` expose those cases independently.

For coordinator recovery, run `durableCoordinatorConformance` from
`@yielded/agent-testing/durable-coordinator-conformance` in a TestClock-backed environment such
as `it.effect`. Provide one combined Layer containing `SubmissionLedger`, `ThreadStore`,
`SettlementPublisher`, `StorageOwner`, and `Crypto`. Use pristine disposable storage with native
Thread verification. The helper discovers six scenario paths, injects faults, and checks bounded
recovery through settled history; violations fail with `DurableCoordinatorConformanceViolation`.
See the [coordinator conformance module](https://github.com/yielded-dev/agent/blob/main/packages/testing/src/DurableCoordinatorConformance.ts)
for its contract.

Injected faults do not establish recovery after process loss or eviction. Exercise actual restarts
separately against the host and storage you deploy. Likewise, the reusable
`@yielded/agent-testing/code-executor-conformance` checks executor behavior and declared metadata;
verify isolation against the actual executor.
