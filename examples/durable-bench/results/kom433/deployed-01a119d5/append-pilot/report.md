# Append pilot: stopped at the feasibility gate

**No candidate or deployed append experiment was run.** Inspection of the clean baseline found that a complete conversion exceeds the permitted small adapter/hooks scope: it requires a parallel native implementation of shared continuation reads, archive updates and work-index publication, integrated with the existing owner and maintenance transaction. I stopped rather than presenting a partial journal rewrite as complete.

This is a scope blocker, **not evidence against append flattening**. The earlier 14–15% optimistic whole-RPC evaluation-removal ceiling is not CPU. Both usage experiments remain inconclusive; no earlier gate or result changed.

## Exact stage boundary

The target starts at public `DoThreadStore.append` invocation and ends after result validation and `append:after`. It includes capture, preflight, the real digest wait, the writer transaction and all associated writes. The inspected closure is recorded with file hashes in [source-inspection.json](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/results/kom433/deployed-01a119d5/append-pilot/source-inspection.json).

| Baseline boundary                                               | Work that a complete candidate must preserve                                                                                                                                                                                         |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `DoThreadStore.ts:379–425`; `canonical-append.ts:16–59`         | Private capture, observed Thread read, transfer bound, scalar metadata, actual Crypto batch digest, typed error mapping, failpoints and `AppendResult` validation.                                                                   |
| `do-journal.ts:646–1009`                                        | Identifier/value bounds; producer recheck inside the writer; replay-before-tail-conflict ordering; duplicate identities; canonical sequence bounds; canonical batch/record and Run-membership writes; existing cache updates.        |
| `SqlThreadNativeReads.ts:233–342`; `RunContinuation.ts:384–471` | Latest-continuation SQL, bounded initial `RunEvidence` selection when needed, persisted decoding, frontier/revision/accounting checks. These are not merely codecs.                                                                  |
| `SqlThreadArchiveRange.ts:236–302`                              | Conditional range advance and descriptor validation; otherwise frontier checks, sealing and insertion. Failures after an UPDATE must still roll back.                                                                                |
| `SqlThreadWork.ts:326–479`; `ThreadWork.ts:491–721`             | Settlement/tool/refusal pointers, work-header completeness checks, evidence references, entry reads/encoding/upserts/deletes and header publication. Evidence hashing can await Crypto on a cache miss.                              |
| `layers.ts:637–708`; `Alarm.ts:965–999`; `due-queue.ts:189–251` | Actual `progress.committed("canonical")`, native lane enrollment, maintenance KV get/put, alarm get/set, notification and due-queue flush before source commit. The mapped host does **not** use the default no-op progress service. |

The resource boundary is `withWriteTransaction` → `SqlStorageOwner.transaction` → native maintenance transaction → installed DO SQL client. `OwnedState` gates cached readers and invalidates speculative views on failure. The installed client uses **asynchronous `storage.transaction`**, holding its connection permit through settlement and rolling back failures/interruption; individual SQL statements use synchronous `storage.sql.exec`.

A valid conversion could retain that asynchronous owner and flatten several synchronous regions between real waits. A bare `transactionSync` replacement cannot contain native progress's KV/alarm awaits. Retaining the owner alone also does not flatten the Effect-only continuation/archive/work helpers: direct equivalents need access to their private state and rollback contracts. That cross-module native storage path is the concrete boundary beyond a small hook. No new framework API, synchronous Effect interpreter or `runSync` wrapper was introduced.

Capture/record serialization already has synchronous Schema kernels and private immutable snapshots. A codec-only change would therefore miss the requested work. I also inspected start-prefix lifecycle retention (`do-journal.ts:1339–1398`, `SqlLifecyclePublication.ts:353–476`). It is optional and disabled in the mapped fixture, so it is **not** used to inflate this fixture's blocker.

## Retained representative batches

Offline extraction of the earlier deployed large Object's `warmFreshFirst` found the five public appends below. These are **old observations**, not new samples. Record bytes exclude batch headers.

| Canonical sequences | Contents, each including continuation       | Records | Record-wire bytes |
| ------------------- | ------------------------------------------- | ------: | ----------------: |
| 1097–1098           | Admitted input                              |       2 |             1,966 |
| 1099–1101           | Run start and context                       |       3 |             3,030 |
| 1102–1103           | Model response declaring two ordinary tools |       2 |             3,803 |
| 1104–1106           | Both tool settlements                       |       3 |             2,873 |
| 1107–1109           | Final response and completion               |       3 |             3,604 |

All five frontier hashes matched their retained continuation references. The subsequent settlement uses the ledger-owned `appendPrepared` path, not a sixth public append. [Retained wire, identities and counts](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/results/kom433/deployed-01a119d5/append-pilot/retained-observations.json) preserve the exact inputs to this inspection. The old full RPC had 13,264 evaluations and 91 SQL statements; its public-append inclusive count was 2,466. None is a new count delta or CPU result.

## Scoped failure inventory, before implementation

- Capture/encoding drift or caller mutation across a wait could change canonical bytes, digests or scalar/index identities.
- Wrong refusal order could accept a stale producer, lose exact replay identity, or miss a tail/record conflict. Replays must not publish new progress.
- Partial conversion could skip continuation integrity/bounds, archive validation, tool/work writes or maintenance enrollment while still returning append success.
- A late SQL, decoder, work or progress failure could leave committed rows, KV/alarm state or speculative cached views instead of rolling back together.

The corresponding small deployed proof would require matching success/audit observations, explicit replay/fencing/tail refusals and a post-write failure followed by a rollback audit. **None was executed; no tests or replacement harness were added.**

## Identity, protocol and retirement

Framework baseline: **`8c05714d`**. Protected worktree: **`58359ea2`**, unchanged and clean. Original usage fixture: **`67e2ae74`**; retained crossover/read-only verifier: **`1e9bac87`**. Exact source/build identities are retained above.

The [stop decision](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/results/kom433/deployed-01a119d5/append-pilot/decision.json) was frozen before implementation/upload. **Source patch: none. Flattened work: none. New count delta/CPU: unavailable.** No measured protocol, repetitions/order, SQL-work equality gate, CPU resolution/control gate or crossover was frozen or exercised. Those remain blocked, not passed. Actual workload was **0 RPCs, 0 Objects, 0 inner iterations, 0 application SQL writes and 0 retained application bytes**; dispatch never started.

No new stage or private Alchemy state existed to destroy. The unchanged GET-only verifier, run through the required original-worktree `direnv` context, independently rechecked all **nine historical Workers and exact namespace IDs**. Every Worker returned 404 and every namespace was absent from the complete paged listing; the last check was **19:17:43 UTC** and the approved account digest matched. [Cleanup receipts](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/results/kom433/deployed-01a119d5/append-pilot/cleanup.json) and [commands](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/results/kom433/deployed-01a119d5/append-pilot/commands.txt) are retained. No workload retry, sample replacement, provider/model call, PR, push or merge occurred. No local runtime test/benchmark or `ready` suite ran. [Final state](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/results/kom433/deployed-01a119d5/append-pilot/final-state.json) confirms all 475 earlier evidence files and all inspected sources are unchanged, the checkouts are clean, and no experiment process remains. Report formatting passed. This inspection grants no rollout approval.
