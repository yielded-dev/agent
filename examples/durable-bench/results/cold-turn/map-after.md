# Experimental cold path after the three deletions

The layout and reconciliation experiments each remove **two cold-only native transactions**. Layout inspection also removes six SQL reads; reconciliation removes one no-op CREATE. Neither experiment changes the **11 post-construction transactions before the first model request**, or the **34 post-construction transactions through the observed alarm finish**. The runtime experiment removes repeated historical declaration work without changing storage I/O. None established a driver-latency saving; all product changes were restored. See [report.md](report.md).

This extends the pre-edit [map](map-before.md) and [ordered await inventory](await-inventory.md). The exact post-change event order, SQL, native await edges, provider receipts and driver timestamps remain in local `map-after/ordered-awaits.json.gz`. Local `map-after/branch-witness.json` records the selected projection body and counts for all 32 diagnostic turns. These are the same four persisted map Objects, extended by the original eight map turns and three profile turns, then measured at `m11`–`m18`. History was neither rewound nor reseeded.

Each Object ran baseline cold/warm, layout cold/warm, reconciliation cold/warm and runtime cold/warm. Every cold turn followed confirmed quiescence, a persisted variant selection, `storage.sync()` and `ctx.abort()`. The following warm turn had to retain that incarnation. These fixed-order, beacon-instrumented observations establish execution and counts, **not candidate latency estimates**. Randomized comparisons use separate evidence in `compare1/` and `compare2/`.

## Ordered differences from the baseline

| Phase, in execution order | Baseline | Layout experiment | Reconciliation experiment | Runtime experiment | Warm pays it? |
| --- | --- | --- | --- | --- | --- |
| Acquire mutation gate, SQL owner and due queue | CREATE, PRAGMA, queue SELECT | Same | PRAGMA, queue SELECT; CREATE only when absent | Same | No |
| Open three storage ports | 3 layout transactions, 9 SELECTs | 1 shared acquisition, 3 SELECTs; exact SQL-client identity required | Same as baseline | Same as baseline | No |
| Compile registration/digests and acquire runtime | Existing Effect/Schema work and crypto promises | Same | Same | Same | No |
| Register native lanes | Empty native transaction on consistent stored state | Same | Inspect under the reservation; no transaction when already registered | Same | No |
| Constructor alarm reconciliation | KV read/decode, recovery-event hydration and due-queue read inside a transaction | Same | Retain the reads, decode, hydration and mutation gate; skip the transaction only for consistent clean idle state | Same | No |
| Submit pre-arm, durable receipt, mark ready | 3 transactions | Same | Same | Same | Yes |
| First alarm: begin, discovery checkpoint, lane selection, claim, input append, input-applied ledger | 6 transactions | Same | Same | Same | Yes |
| Historical journal fold before first model | Validate declarations/accounting, construct historical tool-result slots and ordering maps | Same | Same | Reuse declarations and validated IDs from accounting; build ordering map only for owning Run | Yes |
| Initial context append and initial join drain | 2 transactions | Same | Same | Same | Yes |
| Eight model/tool cycles | 16 transactions, 8 writes | Same | Same | Same | Yes |
| Final join, final response, settlement publication and ledger finalization | 4 transactions, 3 writes | Same | Same | Same | Yes |
| Native source checkpoint and alarm finish | 3 transactions, 2 writes; can outlive client return | Same | Same | Same | Yes |

The first model request follows the initial join drain. AwaitSettlement's concurrent subscribe/read/hint-or-timeout/read sequence is unchanged. The complete baseline await order still applies except for the removed transaction promises. In particular, reconciliation still awaits the existing platform operation and KV read; removing its transaction does not remove those checks. Schema parser construction is synchronous Effect work, not a separately measurable storage await. The fixed harness KV read selecting a variant precedes framework construction and is excluded from every variant's framework counts.

| Native counts per turn | Baseline | Layout | Reconciliation | Runtime | Warm, all variants |
| --- | ---: | ---: | ---: | ---: | ---: |
| Constructor transactions | 5 | 3 | 3 | 5 | 0 |
| Constructor SQL statements | 12 | 6 | 11 | 12 | 0 |
| Constructor KV reads / writes | 1 / 0 | 1 / 0 | 1 / 0 | 1 / 0 | 0 / 0 |
| Constructor write transactions / alarm calls | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Transactions before first model, including construction | 16 | 14 | 14 | 16 | 11 |
| Transactions through observed finish, including construction | 39 | 37 | 37 | 39 | 34 |
| Write transactions through observed finish | 22 | 22 | 22 | 22 | 22 |

There are still 12 canonical batches, 34 canonical records and nine model requests per turn. At the extended map history, post-construction SQL is 227–229 cold / 222–223 warm for the 50-turn seed, and 264 cold / 259 warm for the 250-turn seed. Pre-model SQL is 71–73 / 65–67 and 90 / 86 respectively. The history grows during the schedule and crosses read-page boundaries; these differences are not candidate read regressions. First-use canonical validation and cache-fill reads remain.

The diagnostic bundle is `7957a2d222704610a945f69d79e063631c31c6a0e8fa72e0501e96fe7b76de2f`. Each runtime turn records exactly one `journal.fold.runtime`, before its first provider dispatch. Every other turn records exactly one `journal.fold.baseline`. This counter was added after the timing comparisons; the selector and projection bodies are the same. It is absent from both comparison bundles. Source-derived removed traversal/ID-decode counts remain in local `candidates/runtime-source-counts.json`; these are not measured milliseconds or a count of all Schema checks.

## Deployed timestamps

Example observations for the 50-turn instant-provider Object, Unix milliseconds:

| Variant / sample | Driver submit | Driver receipt | First provider arrival | Driver settlement | Constructor native clock, start = end |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline / m11 | 1791517713442 | 1791517714324 | 1791517714345 | 1791517715249 | 1791517713857 |
| Layout / m13 | 1791517718169 | 1791517718612 | 1791517718658 | 1791517719375 | 1791517718407 |
| Reconciliation / m15 | 1791517721450 | 1791517721998 | 1791517722042 | 1791517722728 | 1791517721820 |
| Runtime / m17 | 1791517724056 | 1791517724769 | 1791517724803 | 1791517725480 | 1791517724578 |

For layout `m13`, provider-side construction beacons arrived at `1791517718440` (layer start), `1791517718451` (runtime services ready), `1791517718450` (layer end) and `1791517718452` (gate ready). Their arrival order is not guaranteed. All constructor native transaction edges retained a single I/O timestamp in each sample. Representation is integer milliseconds; clock advance requires I/O. These observations cannot time parser compilation or individual replication waits, and cross-clock subtraction is not a component duration. CPU and repeat-controlled latency are reported separately.

The fail-closed layout/version checks, durable receipts, mutation gate, pre-arm ordering, dispatch prerequisites, hash chain, fencing, claims, leases, Unknown behavior, historyDigest recovery and canonical accounting remain in the reviewed patches. Independent layout opens still validate; inconsistent reconciliation still takes the repair path. Local `candidates/review.md` records the checks and their limits. `Alarm.ts` from `beginPass` onward and the settlement implementation were never edited; no warm-floor change is requested.
