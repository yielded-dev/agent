# Baseline map, recorded before prototypes

Baseline: `d52ee32a51c7bf93a226272578f13cebcc66f2e1` (`origin/main`).
Recorded 2026-10-08, personal Cloudflare account **Danieljmerwe@gmail.com's Account**.
The deployed observer adds counters and I/O-clock markers; no framework behavior was changed.
See `map-plan.json`, `requests.jsonl.gz` and `telemetry-*.json.gz`, `map/counts.json.gz`, `map/turns.jsonl.gz`, and the archived bundle/source identity.

This mapping run has one physical Object per history/delay, two forced-cold turns,
two settling turns, and four warm turns. It is attribution evidence, not a
candidate comparison or a reproduction of the old report's numerical medians.
All 32 turns completed with identical model-visible transcripts, nine provider
requests each, verified cold incarnations, and alarm-owned production execution.
Seed fingerprints: 50 `b017b487524e44a4`; 250 `dcea9f30b0917245`.

| History | Provider delay | Warm driver median | Warm repeat range | Provider gap median | Narrow gap transactions (writes) | SQL statements | Waiter status reads / fallback wakes / settlement hints |
| --: | --: | --: | --: | --: | --: | --: | --: |
| 50 | 0 ms | 1,387.5 ms | 368 ms | 111 ms | 2 (1) | 15 | 3.5 / 1.5 / 1 |
| 50 | 400 ms | 5,407.5 ms | 308 ms | 108 ms | 2 (1) | 15 | 12 / 10 / 1 |
| 250 | 0 ms | 1,580 ms | 281 ms | 133 ms | 2 (1) | 17 | 4 / 2 / 1 |
| 250 | 400 ms | 5,531 ms | 245 ms | 113 ms | 2 (1) | 17 | 12 / 10 / 1 |

Gap medians pool the eight provider gaps per warm turn. Driver range is max minus
min across the four warm repeats. This small map cannot establish an improvement.

## One gap in order

Representative: `prod-turn-map-h50-d0-o0`, `m6`, first lookup round.
The provider finished at `1791500516781` and accepted the next request at
`1791500516901`: **120 ms** on the provider's clock.

| Event indices | Work before the next provider fetch | Awaited storage / counters |
| -- | -- | -- |
| 188 | Previous provider stream ends; Effect decodes the streamed response | Stream reader I/O |
| 189–192 | Prepare response continuation; readonly response commit is deferred | No write; first continuation preparation |
| 193–194 | Execute the readonly lookup tool through the scoped tool batch | No external I/O for this mock tool |
| 195–202 | Prepare settled continuation, validate/encode/hash canonical batch | Second continuation preparation |
| 203–221 | Append `ModelResponseRecorded`, `ToolCallSettled`, `RunContinuation` together | One awaited native write transaction; 15 SQL statements (10 mutations), one KV put, one getAlarm and one setAlarm |
| 222–225 | Return append and publish a progress hint | No separate alarm write from the deferred wake scheduler |
| 226–229 | Attempt to claim joined inputs; none are available | One awaited native transaction, zero writes |
| 230–231 | Materialize next prompt, close step/checkpoint scopes, prepare provider request | 280 materialized messages, 281 provider messages |
| 232 | Start next provider fetch | Cloudflare output gate applies to outstanding durable writes |

The 15 SQL statements include append replay/duplicate/continuation checks,
journal range allocation, batch/record/run inserts, thread tail update, tool
declaration, work index reads and writes (including an entry inserted then
deleted in this same batch), and the maintenance due-row update. History 250
adds two append/index statements in the mapped path.

All Object timestamps in this particular narrow interval are
`1791500515651`. That is a **frozen I/O clock**, not zero elapsed work. Native
transaction return is not a separately observable physical replication barrier.
There is one canonical write transaction awaited before the next `fetch`, no
explicit `storage.sync`, and one empty joining transaction; the trace cannot
count internal Cloudflare replication rounds or assign separate milliseconds to
synchronous decoding, continuation preparation, SQL, or prompt construction.
External receipt timestamps and clock-offset bounds must be used for elapsed
time. Async beacons on alternating map turns are diagnostic and may perturb
flush ordering; they are not a basis for a candidate latency claim.

## Work overlapping a provider call

In that turn, after the first provider fetch starts, a maintenance claim updates
the due queue, scans runnable threads, and opens empty selection/caught-up
transactions. The captured Object clock advances at callbacks to +68 ms and
+78 ms; the waiter begins at +109 ms; stream headers/end arrive at +116 ms.
Canonical progress increments the Native due-row revision and invalidates the
maintenance discovery cache while the same Attempt remains active. Source
enrollment also writes maintenance KV and ensures the alarm inside the source
transaction. This is distinct from the narrow step commit.

Warm whole-turn native transaction medians are 101/273 (history 50, delays
0/400) and 113/279 (history 250). Longer provider waits permit more maintenance
and polling to overlap the model wait. These counts explain why adding phase
durations is not a valid additive waterfall; causal variants are still needed.

## What the waiter does

The endpoint authorizes and checks receipt placement once. The runtime then
authorizes, subscribes to **settlement** hints before every authoritative read,
and races that subscription against the 500 ms fallback while pending. Local
progress hints already skip the settlement subscription hub. In this run the
usual final settlement contributes one hint; the other wakes are fallbacks.

Each pending runtime read looks up one submission and validates receipt identity.
Cloudflare's owned-state row cache and row-identity decode cache serve unchanged
pending reads without SQL or repeated admission decoding. The endpoint adds one
lookup. A terminal read calls finalization replay and then reads the canonical
record: two canonical SELECTs/JSON decodes, with identity/outcome validation.
The producer separately reads canonical settlement during publication/finalization.
Large invocation CPU attributed to this RPC is not yet evidence of waiter cost:
the alarm and waiter execute concurrently on the Object. A hints-only diagnostic
will test attribution while preserving production's fallback.

## Last model response to settlement

The observed order is final response decoding; a canonical
`ModelResponseRecorded` + `RunCompleted` + continuation transaction; a second
canonical `SubmissionSettled` + continuation transaction; a third transaction
to finalize the ledger and release ownership; a settlement hint; the waiter's
terminal authority reads; then RPC delivery through Cloudflare's output gate.
Joined settlement processing and maintenance can run after the hint and before
the client receives the result. The same frozen-clock limitation prevents an
exact millisecond split between these phases. Compatible beacon/driver bounds
place warm response-to-client delivery at 103.5–112.5 ms (50/0) and
134.5–143.5 ms (250/400); other map conditions have incompatible clock bounds
and are not assigned a fabricated duration.

## Local deterministic cross-check

The #821 production target at histories 50 and 250 adds exactly 12 canonical
batches, 34 records, 22 record-run rows, eight tool declarations and one
submission/attempt per turn, including admission. `local-counts.json` contains
the counts, fingerprint checks and command. No local elapsed time is evidence.

Candidate boundaries: remove no-op maintenance/joining transactions; test
settlement-only waiting without fallback as a diagnostic; and co-finalize an
ordinary settlement in its publication transaction where the existing runtime
contract allows it. Admission, mutation gate and pre-arming remain outside
this task. Every timing claim requires a same-Object interleaved comparison.
