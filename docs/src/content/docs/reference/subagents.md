---
title: Subagent policies and recovery
description: Defaults, authority, funding, and caller limits for attached and background subagents.
---

Use the [subagent guide](/guide/subagents/) to choose a lifetime. This reference describes
`Subagent.make`, `Subagent.layer`, and `Subagent.background`.

## Input and result mappings

| Option          | Default                       | Caller contract                                            |
| --------------- | ----------------------------- | ---------------------------------------------------------- |
| `target`        | Required Definition           | Child owns instructions, input/output schemas, and toolkit |
| `parameters`    | Child input Schema            | Model-facing delegation input                              |
| `prepareInput`  | Validate input as child input | Receives bounded parent metadata, never its transcript     |
| `success`       | `{ output, budgetExhausted }` | Schema for the parent's result                             |
| `projectResult` | Wrap child output             | Validate and bound what crosses back to the parent         |
| `failure`       | `Schema.Never`                | Declared application failure data                          |
| `failureMode`   | `"error"`                     | `"return"` exposes expected delegation failures as data    |
| `needsApproval` | Absent                        | Approves establishment only                                |

`Subagent.layer` captures the child model and services at construction. An explicit model or
Binding can override the surrounding model. Custom projections must retain exhaustion information
when partial output matters to the parent.

<a id="handle-failures"></a>

## Let the parent handle a failed child

Return mode contains expected failures; defects and interruption retain their Effect meaning.
Durable waiting remains suspension. Durable child failures use bounded `SubagentExecutionFailure`
data; `mapChildFailure` maps expected failures on the ephemeral path only. Decode contained
failures with the declaration's `containedFailure` Schema.

## Bound child work

| Setting                           | Default or behavior                                                                       |
| --------------------------------- | ----------------------------------------------------------------------------------------- |
| Child `policy` fields             | Omitted fields inherit the parent's resolved policy; declaration ceilings then clamp them |
| Delegation `policy`               | Omission derives a shared child pool from parent limits                                   |
| Explicit `SubagentPolicy`         | Sets per-child ceilings; aggregate caps multiply by `maxChildren`                         |
| `parentCaps` on the handler Layer | Optional shared aggregate pool; declarations in one parent Run must agree                 |
| `toolCallAllowance`               | Child's resolved allowance; an override can only tighten it                               |
| `descendantInvocations`           | Zero when omitted                                                                         |
| Token and cost caps               | Optional; missing usage never creates budget credit                                       |

Each parent Run has its own delegation pool, separate from its execution counters. Durable
accounting conservatively charges reservations when usage is unavailable. Increasing an allowance
requires a fresh delegation, not a top-up of an active child. See [budgets](/concepts/budgets/).

<a id="limit-authority"></a>

## Limit the child's authority

| Grant field        | Default                      | Effect                                             |
| ------------------ | ---------------------------- | -------------------------------------------------- |
| `allowedToolNames` | Target's declared tool names | Intersection narrows visible and executable tools  |
| `maxDepth`         | `1`                          | Root-relative ceiling; a direct child is depth one |
| `childLifetimes`   | Both when omitted            | Restricts attached/background descendants          |

Parent tools do not become child tools. Resource access still needs handler authorization.

## Bound nested delegation

Nested work needs permitted tool names, depth, lifetime, invocation slots, and remaining allocation.
Descendants share the reserved subtree allowance after the child's own execution ceiling is
accounted for. More depth alone does not fund a grandchild. Handoff is unsupported.

## Independently fund background Runs

`source-subtree` funding uses the selected source Run's allowance. Root programmatic starts
without selected input default to `worker-run` funding, which requires `WorkerBudgetAuthorizer`;
its default denies. Models cannot select funding scope. Joined input shares the active Run's
budget. A host concurrency ceiling limits active workers separately from per-Run tool concurrency.

## Background delivery and recovery

| Operation or option          | Caller limit                                                                     |
| ---------------------------- | -------------------------------------------------------------------------------- |
| `background` operation flags | Opt in individually; no waiting tool                                             |
| `start` / `followUp`         | Retained delivery status, then destination receipt; reuse keys after uncertainty |
| `inspect`                    | Supply exactly one message reference or receipt                                  |
| `await`                      | Programmatic only; interruption detaches the waiter                              |
| `cancel`                     | One exact receipt                                                                |
| `stop`                       | Permanently seals the worker; does not undo external actions                     |
| Assignment lifecycle         | Typed completion seals only after latest accepted input is applied               |

Worker summaries report `unknown` when unresolved operations park a Run. This state is not
active progress; reconcile the original operations before retrying external effects.

## Completion report guarantees

`reportToParent: true` sends standard completion messages. Optional pure `reportCompletion` and
`reportUpdate` predicates filter notifications. Child settlement, delivery acceptance, and parent
processing are distinct. Programmatic reporting requires the exact `sourceSubmissionId` supplying
parent context. Reports cannot target an attached child's nonexistent continuing inbox.

## Update delivery guarantees

An Agent's `updates` Schema defines `emit_update` findings independently of final output.
`AgentUpdates.emit` uses a stable Run-scoped key; changed payloads conflict. `AgentUpdates.observe`
decodes accepted updates. Acceptance retains the finding; it does not prove parent delivery.
Native update recovery reuses that acceptance under the original call identity. A recovery
sweep can also close an accepted update previously marked unknown without replaying it.
