# Admission map before product changes

Captured 8 October 2026, against `d52ee32a51c7bf93a226272578f13cebcc66f2e1`
(`origin/main`). Framework admission, storage, and runtime files were byte-identical
to that revision. The esbuild observer adds trace events without changing their
branches. Uploaded bundle SHA-256:
`709dcdf0964edbef9a4cf16438e48f10ec352fa3a1303ea6ad3b8d444aca63cb`.

All 48 deployed turns completed, including eight verified fresh Object incarnations.
All 432 provider requests matched independently computed transcripts. Seed
fingerprints were `b017b487524e44a4` (50) and `dcea9f30b0917245` (250).
One Object per condition is enough to map calls, not to establish an optimization.

| History / provider delay | Warm admission median (repeat range) | Cold admission median (repeat range) | Warm submit CPU | Cold submit CPU |
| --- | ---: | ---: | ---: | ---: |
| 50 / 0 ms | 218 (54) ms | 683 (52) ms | 13.5 ms | 91.5 ms |
| 50 / 400 ms | 212.5 (105) ms | 505 (94) ms | 8 ms | 50 ms |
| 250 / 0 ms | 317 (195) ms | 736.5 (113) ms | 16 ms | 74.5 ms |
| 250 / 400 ms | 521 (286) ms | 1010.5 (111) ms | incomplete join | 109.5 ms |

Warm has eight repeats; cold has two. CPU is Cloudflare invocation attribution,
not a sum of semantic phases. The 250/400 Object is slower throughout its turn;
same-Object interleaving is essential for candidate comparisons.

## Ordered observations

The client encodes the request, awaits native RPC, decodes the response and returns
the receipt. Cold construction precedes endpoint execution: three layout acquisitions,
runtime/layer initialization, native-lane registration and alarm reconciliation
produce five extra asynchronous transactions, twelve SQL statements and one KV get.
The trace labels the last two transactions `register native maintenance lanes` and
`ensure maintenance alarm`. Due-queue initialization itself uses synchronous SQL
through an async wrapper, without another native transaction.

Within the endpoint, every seeded fresh admission follows this order:

1. Decode request; placement check; `gateAdmissionLimits` (idempotency lookup,
   input size, nonterminal queue bound and database size).
2. `withMutation`: acquire generation permit, pre-arm transaction **T1**.
   Read maintenance KV, advance dirty generation / due queue, write KV, get alarm,
   set alarm to the earlier existing deadline or `now + 50 ms`.
3. Runtime input encode/decode/digest and admission-request validation.
4. `ledger.admit` transaction **T2**: authoritative duplicate/authority checks,
   queue sequence allocation, admission `INSERT … RETURNING`, progress enrollment.
   Progress reads/writes maintenance KV and checks the alarm in this transaction.
5. Materialize at epoch zero; inspect canonical tail. Seeded Threads already have
   `ThreadCreated`, so neither step appends a canonical batch in this fixture.
6. `markReady` transaction **T3**: update admission state with `RETURNING`, retain
   applicable publication intent, enroll progress and check the alarm again.
7. Runtime `wake.notify`: volatile notifications, then `scheduleNow` / `armNow`
   transaction **T4**: get alarm and move it earlier, from `now + 50` to `now`.
8. `publishCommitted`: empty for the default publication layer in this fixture.
   Finish mutation bookkeeping; construct and encode response; return through RPC.
9. Cloudflare holds the outgoing response until outstanding confirmed writes satisfy
   the output gate. The driver receives, validates and returns the receipt.

Observed per admission: **4 async transaction calls; 0 transactionSync; 3 KV gets;
3 KV puts; 4 getAlarm; 2 setAlarm**. Warm SQL count is 5 at history 50 and 11 at 250;
cold admission itself is 10 and 14 respectively, in addition to construction.
`ensureScheduledBy` is another async alarm transaction API, but is not called by
this measured submit path. The immediate wake is inside runtime submit, rather
than inside default `publishCommitted`.

## Durability and clock limit

Four sequential transaction promise completions are **not four measured durability
round trips**. SQLite transaction commit releases the local savepoint and attaches
replication/alarm scheduling to the output gate; it need not wait for replication
before the JavaScript transaction promise resolves. See Cloudflare's
[SQLite storage contract](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
and [workerd commit implementation](https://github.com/cloudflare/workerd/blob/main/src/workerd/io/actor-sqlite.c%2B%2B).

All constructor and admission trace points in each sampled invocation share one
I/O-clock timestamp. This is **unresolved elapsed time**, not zero execution time.
For example, 50/0 warm m4 has driver start `1791500603292`, endpoint trace timestamp
`1791500602450`, and driver receipt `1791500603499`: subtracting the stale Object
timestamp would invent a negative entry interval. The timestamps have millisecond
representation but no useful within-invocation resolution without real I/O.

There are zero explicit `storage.sync` confirmations on the production admission
path and one externally enforced response durability boundary. The number of
replication batches behind that boundary cannot be inferred from these traces.
A separate entry-echo / final-sync diagnostic follows, before product fixes,
to expose the wait without changing production durability.

## Evidence and non-ok outcomes

See [raw requests](requests.jsonl), [plan](map-plan.json),
[computed tables](map/tables.md), and [complete outcome inventory](map/failed-outcomes.json).
The first telemetry collection includes 15 intentional cold-reset fetch aborts,
70 canceled alarm deliveries, and two provider-readiness 404s during initial DNS /
deployment propagation. No measured submit or settlement failed. Canceled alarms
remain evidence; they are not silently recategorized as successful invocations.
CPU joins with missing or conflicting telemetry stay missing. This document freezes
the initial map; later runs and final cleanup have separate evidence.
