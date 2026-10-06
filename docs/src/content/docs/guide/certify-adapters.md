---
title: Certify storage adapters
description: Certify a SubmissionLedger and ThreadStore pair at three failure levels.
---

<a id="certify-storage-adapters"></a>

`certifyDurableAdapters` tests a candidate `SubmissionLedger` and `ThreadStore` pair and
returns one schema-encoded report.

| Level                      | Evidence                                                                           |
| -------------------------- | ---------------------------------------------------------------------------------- |
| 1. Port contract           | Runs all shared ledger and thread store conformance cases                          |
| 2. Coordinator convergence | Discovers six coordinator paths, injects reached failpoints and drives recovery    |
| 3. Runtime loss            | Exercises process termination or eviction, or records the committed suites that do |

<a id="store-contract"></a>

## Implement the store contract

A `ThreadStore` materializes a thread, appends fenced batches, reads or observes
records, exports history, and inspects the tail. Appends must be atomic, digest-bound, idempotent by
batch ID, checked against the expected tail, and fenced by producer epoch. Reads decode stored
values through schemas.

`ThreadStore.checkpoints` is optional storage for application projections. An adapter offering it
must run the generic checkpoint conformance suite. Retained-history execution does not use it.

A new `ThreadCheckpoint` needs only `schemaVersion`, `threadId`,
`throughSequence`, `tailDigest`, `state`, and `createdAt`. Adapters bind the sequence and digest
to a canonical batch tail; consumers decode `state` and decide whether its projection version
supports suffix replay. `ThreadProjection` uses version 3 and rejects older snapshots so
consumers can rebuild from canonical records.

Canonical execution progress is separate from application checkpoints. Implement native
`RunContinuation` reads for the newest owner record at or before a captured tail, bounded sorted
`RunEvidence` pages, and retained `WorkHandoffs` pages. Publish their indexes with the canonical
facts under the same epoch, expected-tail, and idempotency checks. Exact evidence references use
logical record identities and content digests; physical storage layout cannot change their meaning.

An append containing progress must reject conflicting revisions, changed original references,
regressed accounting, or a frontier outside the same batch before writing. Preparations stay
indexed after Run settlement; destination acknowledgement owns closure. An index grants no
execution authority, and missing required evidence is a typed fault, not an empty completed Run.
See [Run continuations](/concepts/durability/#run-continuations) for recovery bounds.

This unreleased format accepts only fresh layout-21 storage and `effect-agent/thread@3` archives.
Refuse predecessor, newer, or ambiguous layouts before any DDL or record mutation. Preserve the
rejected store; there is no layout upgrade or historical record decoder.

Thread age has no canonical record ceiling. Export and verification stream bounded pages. Keep each
`ThreadRead` page at or below 1,024 records and each `CanonicalBatch` at or below 256. An export
must preserve one captured tail and independent fact revision across its bounded pages, including
all immutable admissions, accepted commands, deliveries, and cross-range dependencies. Implement
exact native locators and streaming verification; never silently treat one page as complete history.

Implement `SubmissionLedger.readAbortIntent` as a strongly consistent read of one submission's
abort intent. The runtime polls this method during execution, so its work must stay independent
of other submissions, approvals, and attached children. Unknown submissions fail with `LedgerError`.
The read grants no ownership, and `canonicalRecordId` must come from canonical history.

```ts
import type { SubmissionId } from "@yielded/agent/identifiers";
import { AbortIntentRequest, SubmissionLedger } from "@yielded/agent/submission-ledger";
import { Effect } from "effect";

const readAbort = Effect.fn(function* (submissionId: SubmissionId) {
  const ledger = yield* SubmissionLedger;
  return yield* ledger.readAbortIntent(AbortIntentRequest.make({ submissionId }));
});
```

Cloudflare keeps this read local to the submission's owning Durable Object. The abort command
still becomes canonical under the append gate before the runtime interrupts execution.

Joined input belongs to its host Submission. Validate a supplied `RevertJoiningRequest.guard`
atomically with rollback: leave a different host link untouched, and require the current host's
ownership token unless that exact host has settled. The shared ledger conformance suite checks
these recovery boundaries.

## Run the certification

```ts
import { Effect } from "effect";
import { certifyDurableAdapters } from "@yielded/agent-testing/certification";

const certificate = Effect.gen(function* () {
  return yield* certifyDurableAdapters({
    adapter: { name: "@your-org/storage-yours" },
    submissionLedger: yourLedgerLayer,
    threadStore: yourStoreLayer,
    tierThreeEvidence: ["path/to/your/real-loss.test.ts"],
  });
});
```

Provide `Crypto.Crypto` with `NodeCrypto.layer` on Node or `BrowserCrypto.layer` in workerd. Run
with a `TestClock` through `@effect/vitest` or a manual `TestClock.layer()` root. Tier 1 advances
leases, and tier 2 advances past the ownership lease during recovery. Pass
`ownershipLeaseDuration` when the ledger uses a different lease.

If both ports share a connection, pass the same combined layer instance to both fields. Layer
memoization will acquire it once.

The returned `CertificationReport` includes adapter identity, the ledger durability claim, each
tier 1 result, each tier 2 cell, and tier 3 evidence. `ok` is true only when every executed check
passes. Statuses such as `not-triggered`, `recorded-evidence`, `not-exercised`, and
`not-applicable` describe scope. They count as neither a pass nor a failure.

Use `fullyCertified` when a gate requires complete durable-adapter certification in this run.
It requires `ok: true`, a durable adapter, and at least one passing real-loss case from `crashLever`.
All lever cases must belong to the `real-loss` suite. An empty lever reports `not-exercised`.

| Tier 3 status                              | `ok` when executed checks pass | `fullyCertified` |
| ------------------------------------------ | ------------------------------ | ---------------- |
| `exercised` with passing real-loss cases   | `true`                         | `true`           |
| `recorded-evidence`                        | `true`                         | `false`          |
| `not-exercised`                            | `true`                         | `false`          |
| `not-applicable` for a non-durable adapter | `true`                         | `false`          |

Any failed executed check makes both fields `false`. Recorded citations are external evidence;
the runner does not execute or verify those suites. Non-durable adapters can pass conformance
without earning durable certification. Reports keep the `effect-agent/certification@2` format.

<a id="what-tier-2-asserts-exactly"></a>

## Interpret tier 2 results

Tier 2 first drives and verifies each of six scenarios without a fault, recording reached
coordinator locations. It then injects each reached failpoint into fresh thread state and
drives recovery through public operations. Locations observed during recovery also enter the
sweep. Each scenario/location pair is armed at most once. The runner resolves unknown outcomes
as `SafeToRetry` and approvals as `approved` only when `explainThread` authorizes that action.

Each cell reports:

- `converged` when the failpoint fired and recovery settled with verified invariants;
- `not-triggered` when the location did not fire, including locations that share the scenario's
  verified clean run because they were never reached;
- `failed` for any other result, with bounded diagnostic detail.

`not-triggered` records the tested scope. It makes no fault-survival claim. Every scenario/location
pair remains in the report; shared clean results are identified in their detail. A location absent
from both discovery and the documented never-fired set is armed in every scenario, so new
failpoints cannot silently disappear from the sweep. Repository tests pin the fired paths per
scenario and the exact never-fired set. Dedicated suites and crash matrices cover those operator, abort, compaction,
background-worker, and Agent-update paths.
Those dedicated suites run separately; the certificate runner does not execute them.

The final invariant check recomputes the digest chain from `EMPTY_TAIL_DIGEST` and uses the same
checker as the administrative `verify` operation.

<a id="tier-3-evidence"></a>

## Provide tier 3 evidence

- `@yielded/agent-storage-memory` reports `not-applicable` because it declares non-durable state.
- `@yielded/agent-storage-sqlite` records the platform Node process-kill suites.
- `@yielded/agent-storage-postgres` reports `not-exercised` because no committed process-kill
  suite drives it yet.
- `@yielded/agent-storage-cloudflare` records Durable Object eviction, cross-object subagent, and
  Miniflare restart suites.
- A third-party adapter may pass `crashLever` to kill or evict its runtime and reopen storage for
  selected rows. Successful rows report `exercised`. Without that lever or committed evidence, the
  report says `not-exercised`.

<a id="shipped-adapter-tests"></a>

Import `CertificationReport` and `certifyPorts` from
`@yielded/agent/testing/certification`. The shared conformance cases live in
`@yielded/agent/testing/thread-store-conformance` and
`@yielded/agent/testing/submission-ledger-conformance`. Production schemas, ports,
replay, verification, and runtime APIs have their own public thread modules.

<a id="subscription-stores"></a>

## Certify subscription stores

An adapter that implements `SubscriptionStore` must also run
`subscriptionStoreConformanceCases` from `@yielded/agent/testing/subscription-store-conformance`. Give each case a fresh
partition. The cases cover intake cutoffs, deduplication, once selection, capacity, cancellation,
prepared recovery, catch-up, scan cursors, and replay after limits tighten.

The thread and submission certificate does not include these cases. Add restart or eviction
tests around intake, partial fanout, selection, preparation, admission, and receipt persistence.
