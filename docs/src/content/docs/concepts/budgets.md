---
title: Budgets and limits
description: Public policy defaults, exhaustion behavior, and the scope of execution limits.
---

The Agent Definition bounds work; the application chooses limits appropriate to its model
and tools. A durable Run keeps cumulative accounting across replacement Attempts.
Model-context capacity and Thread history are separate from those execution budgets.

## Policy defaults

Pass a partial `policy` to `Agent.make`. Omitted fields use standalone defaults; delegated
children can inherit the parent's policy within their delegation ceilings.

| Option                        | Default                                                                 | Meaning                                                                |
| ----------------------------- | ----------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `maxTurns`                    | `12`                                                                    | Model turns per Run                                                    |
| `maxToolCalls`                | `24`                                                                    | Tool calls, including programmatic calls                               |
| `maxDuration`                 | `"5 minutes"`                                                           | Ordinary Run deadline; active Attempt deadline for durable execution   |
| `toolConcurrency`             | `4`                                                                     | Parallel application handlers per batch                                |
| `repeatedFailureLimit`        | `3`                                                                     | Consecutive terminal tool failures                                     |
| `onExhaustion`                | `"final-answer"`                                                        | Turn, tool-call, and token exhaustion behavior                         |
| `tokenBudget`                 | Unset                                                                   | Cumulative input and output tokens                                     |
| `costBudgetMicrousd`          | Unset                                                                   | Estimated cost in millionths of a US dollar                            |
| `completionReserveTokens`     | 20% of `tokenBudget`, capped at `4,096`; `4,096` without a token budget | Capacity reserved for final delivery                                   |
| `contextTokenLimit`           | Unset                                                                   | Estimated live context that triggers compaction                        |
| `toolResultBounds.maxBytes`   | `51,200`                                                                | Encoded bytes per application tool result                              |
| `runStatus`                   | `"off"`                                                                 | Set `"appended"` for a derived status message in model requests        |
| `restartOnJoinedInput`        | Off                                                                     | Replace a disposable model call on joined input, at most twice per Run |
| `modelRetries`                | `0`                                                                     | Retry a qualifying model failure before content arrives                |
| `compaction.keepRecentTokens` | `20,000`                                                                | Recent context preserved during compaction                             |
| `compaction.mode`             | `"prune-then-summarize"`                                                | Compaction strategy                                                    |

```ts twoslash
import { Agent } from "@yielded/agent";
import { Schema } from "effect";
import { Toolkit } from "effect/ai";
// ---cut---
const researcher = Agent.make("researcher", {
  input: Schema.String,
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Answer from the available evidence.",
  toolkit: Toolkit.empty,
  policy: {
    maxTurns: 8,
    maxToolCalls: 16,
    maxDuration: "2 minutes",
    tokenBudget: 80_000,
    contextTokenLimit: 32_000,
    completionReserveTokens: 4_000,
  },
});
```

[Context management](/guide/context-management/) explains compaction and oversized tool results.
The [travel planner](https://github.com/yielded-dev/agent/tree/main/examples/travel-planner)
shows policies in a complete application.

## At the limit

`onExhaustion: "final-answer"` allows at most one constrained finalization opportunity for
turn, tool-call, or token exhaustion. A [designated completion tool](/guide/agents/) remains required.
A completed result carries
`finishReason: "budget-exhausted"` and the exhausted dimension. Use `"fail"` when a partial
answer is not acceptable. Duration, estimated cost, and repeated failure limits always fail.

Usage is measured at response boundaries and can cross a threshold. These budgets are not
prepaid provider spending caps. A strict monetary ceiling requires host admission that reserves
possible charges before calling the provider. Cost budgets require the host's
`estimateCostMicrousd`; a missing estimator or unknown pricing fails with a typed cost-policy error.

The estimator receives one request with `usage`, `provider`, and `model`, plus optional
`response`, `finishMetadata`, `purpose`, and `webSearchCalls`. It returns an Effect containing
`{ costMicrousd, pricingStatus?, serviceTier?, pricingVersion? }`.

Durable `maxDuration` uses the original allowance for each active Attempt; eviction,
suspension, and missing-binding waits do not consume it. An actual duration exhaustion remains
terminal across recovery. Other cumulative counters do not reset.

`modelRetries` applies only before content, for retryable failures. Hosted tools must all be
marked readonly. Backoff counts toward the active deadline; streamed content prevents retry.

## Caller and delegation limits

`turnAllowance` and `toolCallAllowance` can tighten one Run's policy, never raise it.
Set `scheduling: { runOverride: 1 }` on a Run to execute its application tools sequentially.
`runOverride` accepts a positive integer and only tightens `policy.toolConcurrency`.
Tools that require sequential execution still run alone within their batch.

Subagents reserve from a shared pool with their own count, concurrency, duration, and result
bounds. Unknown usage stays charged conservatively. See the [subagent reference](/reference/subagents/).

Optional usage-budget hooks share accounting across application scopes such as tenant and
Thread; the application owns their accounting period. Code Mode separately bounds inner host
calls; its default concurrency is four.
See [Code Mode](/guide/code-mode/) for its caller-visible limits.

## Durable storage bounds

| Boundary                                           | Limit                   |
| -------------------------------------------------- | ----------------------- |
| Individual persisted JSON payload                  | 1 MiB                   |
| Whole canonical record wire                        | 4 MiB                   |
| Logical committed record JSON per Turn             | 4 MiB                   |
| Run execution dispatch                             | 16,384 facts and 32 MiB |
| Distinct application tool-call IDs per durable Run | 4,096                   |

Thread history has no lifetime quota; these bounds apply per payload, Turn or Run.
Reservations can refuse a dispatch before its byte ceiling to retain room for outcomes and
failure settlement. These limits do not replace model context sizing or the application's
physical storage capacity.

Persisted sequence numbers and counters use safe integers with a representational ceiling of 2⁵³−1.
