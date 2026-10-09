# Admission gates and settlement retirement

This follows the user's additional [combined re-benchmark](https://github.com/yielded-dev/agent/blob/4cbcad5fd1e6544edfbb70e47728d7e0264386f0d3/examples/durable-bench/results/rebench/report.md).
Its provider-dependent warm admission is a lead, not evidence of a previous-alarm
queue. In that run **60/60 accepted Yielded turns had no active alarm at admission
entry or metrics collection**. Our initial map has the same observation in 32/32.
Both controllers read metrics before advancing to the next input. An alarm may
outlive client completion, but these entries do not show the next submit queuing
behind that alarm. Neither count measures time inside a native input gate.

## Which gates can delay admission

| Gate | Scope relevant to this task |
| --- | --- |
| Owned-state reader permit | Cached reads wait for the complete source transaction, including cache updates and rollback handling. Admission performs lookup and nonterminal scanning before its mutation prearm. |
| SQL connection permit | The DO SQL driver holds it through `storage.transaction()` resolution. Raw alarm operations reserve it in a small operation scope, not for the whole event. |
| Mutation generation permit | Prearm and release hold it; the mutation body does not. Maintenance snapshots/checkpoints/retirement can hold it across their SQL/native transaction waits. |
| Native input gate | Storage transaction critical sections exclude incoming event delivery. Synchronous view rebuilding also occupies the Object's JavaScript execution; the SQL driver disables scheduler yielding in a transaction. |
| Maintenance pass permit | Covers native dispatch, scope cleanup, optional host work and alarm disposition. Admission does not acquire it. Wake deferral is a counter, not this mutex. |
| Native output gate | Outgoing receipts and settlement responses wait for confirmation of preceding writes. A transaction callback ending, its promise resolving and client delivery are different boundaries. |

Owners: `internal/owned-state.ts` (`OwnedState.read` / `transaction`),
`internal/layers.ts` (source transaction decoration), the installed
`@effect/sql-sqlite-do/src/SqliteClient.ts`, and `Alarm.ts` (`withSnapshot`, native
checkpoint and final disposition). No construction, first-pass opening or alarm
reconciliation code was changed.

## Remaining work after settlement becomes observable

The waiter replays finalization against canonical authority and reads the terminal
envelope for usage/disposition. Both are keyed record reads. The producer repeats
notification handling, checks joined submissions, materializes the settlement and
closes the Run scope. Ownership cleanup already detects absent ownership before
opening another write transaction. The ordinary root path contains no full-history
scan in this interval. Child/worker/message notification and scoped service cleanup
remain required; detaching them would change the durability/resource contract.

Maintenance joins native/auxiliary work, optionally checkpoints, closes scopes,
runs due after-native lanes and clears or rearms its alarm. The event scope then
closes; the native RPC-target finalizer only clears WeakMaps. The benchmark tracer
owns no storage-holding finalizer. A late `reportParked` read follows pass-permit
release. SQL/transaction counts through metrics therefore include some work that
may follow client completion.

## Repeated work selected for a separate experiment

`ownedRows.apply` formerly rebuilt every retained view on every successful
`RETURNING`, including empty results and views of unrelated submissions. Each
rebuild copied rows, serialized them to measure UTF-8 bytes, deleted/reinserted the
view and rescanned cache byte totals. Each cache is bounded at 128 views / 4 MiB;
these are per-cache bounds, not one aggregate per Object. Normal commits retain
these views across turns. Rollback and explicit invalidation clear them.

Finalization updates the full submission cache, its control-metadata cache and
the ownership cache. For one changed submission and ownership row, the old loop
can call retention `Vs + Vm + Vo + 3` times (up to 387). This is a source-derived
upper bound, not an observed count. The deployed `owned.rows.*` counters measure
actual copies, retained rows and serialized bytes separately for admission,
setup, model processing, waiter and tail observations.

The candidate skips a view only if no returned changed key was in its prior rows
and no returned row now matches its predicate. It also skips all maintenance for
empty `RETURNING`. It preserves updates entering/leaving predicates, unique-key
seeding, memory bounds, the reader gate and rollback invalidation. It introduces
no new cache or mode. Unaffected views no longer move in eviction order, so cache
miss counts can differ and are retained in the evidence.

This is a candidate explanation for history-dependent **write work**. It does not
establish why the re-benchmark's instant and 400 ms admissions differ. The ledger's
128-row lane-view retention limit is another independent difference between these
history sizes. The deployed A/B comparison and invocation CPU, rather than source
complexity or local clocks, determine whether the change is worth keeping.

## Earlier alarm, later receipt

On `warm-floor-candidates-h250-d0-o0`, the eight identical baseline observations
have a 57.5 ms median admission (49–81 ms) and 762.5 ms total turn. Four interleaved
immediate-prearm turns have 233.5 ms median admission and the same 762.5 ms total.
Baseline receipt-to-first-provider brackets are 149–296 ms across those samples;
immediate-prearm brackets are 3–20 ms. The total-turn baseline range is 266 ms.

These are observations on one Object, not a universal cost estimate. They show
why subtracting the configured 50 ms from receipt-to-model latency would be
misleading. Moving the deadline can move the receipt boundary without advancing
model dispatch relative to submission. Native output gating and concurrent work
from the current alarm are a plausible explanation; these observations do not
establish that the previous turn blocked admission. Exact per-sample brackets,
driver timestamps and repeat comparisons are retained in `candidates/turns.jsonl`
and `candidates/comparisons.json`.

The original cache behavior dates to [#706](https://github.com/yielded-dev/agent/pull/706).
Durability uses normal Cloudflare confirmation throughout.
[Cloudflare storage options and gates](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#supported-options-1).
