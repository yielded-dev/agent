# History-dependent turn cost

Work in progress. Latency claims require a completed deployed same-Object comparison; local counts below are diagnostic. The primary metric is submission to the mock provider's first-request arrival, with whole-turn latency retained. Warm/cold, instant/400 ms and 50/250/1,000-turn coverage are tracked separately. A 3,500-turn deployed import exceeded the Object isolate's memory limit; its failure and verified target cleanup are retained.

The product baseline is `07f0272e7ba49a494064b6b74c6318b55514ae19` (main after #827). The benchmark is the separate #828 worktree; its latest measured head and local adaptations are recorded per run. Product candidates contain no benchmark or results files.

## Prior work and candidates

#810 already introduced a fixed canonical context range, retained exceptions, narrow prompt records and `historyDigest`. #815/#816 removed several duplicate captures, decodes, byte counts and continuation checks. #821 made the deployed benchmark use production `ThreadObject`; #823/#825/#826 removed redundant wake, pre-arm and maintenance work; #827 skips empty or unchanged row views. This investigation preserves those changes and does not retry the rejected startup, minification, transaction or pre-arm experiments.

| Candidate | Commit | Change | Status |
|---|---|---|---|
| Single message-schema traversal | `40485f33` | Remove the outer encoded-message validation while retaining the native decoder | No demonstrated warm gain; no PR |
| Input filtering | `87158447` | Omit historical admissions from prompt reads; fetch exact compaction/late-owner admissions when needed | Deployed measurement in progress |
| Terminal filtering | `4417a095` | Omit a duplicate settlement when an earlier terminal record is in the logical read range | Counts complete; timing pending |
| Native constructor restoration | `0451aacb` | Keep upstream encoded validation and restore native Prompt values with upstream constructors | Counts and full gate passed; deployed timing pending |

Canonical append/import/recovery codecs, hash-chain verification, continuation verification, fencing, claims, leases, Unknown handling and confirmed durability remain in place. No warm in-memory context cache is introduced.

## Deterministic work

One inline Run uses eight ordinary Tool calls and nine scripted model calls. Import, seeding, fixture verification, public RPC transport and polling are outside these counters. All counted historical reads, metadata visits and journal folds complete before the first scripted model callback. That callback is a local diagnostic boundary, not the deployed provider arrival.

| Seeded turns | Canonical records stored | Prompt records read | Prompt JSON bytes decoded | `readPrompt` Effect evaluations | First-model Effect evaluations |
|---:|---:|---:|---:|---:|---:|
| 50 | 603 | 268 | 204,113 | 7,667 | 15,349 |
| 250 | 3,002 | 1,334 | 1,024,025 | 37,839 | 56,123 |
| 1,000 | 12,002 | 5,334 | 4,112,455 | 151,084 | 208,859 |
| 3,500 | 42,003 | 18,668 | 14,430,077 | 528,733 | 718,221 |

Each selected record is JSON-decoded once. Metadata and journal projection each visit it once. At baseline, encoded and native message validation each traverse 184/917/3,667/12,834 messages. The decoder uses about 35.5 interpreted Schema-node calls per selected record; the full read, including planning/row schemas, uses about 46.5. ReadPrompt costs about 28.3 Effect evaluations per selected record. Counts represent executed interpreter work, not CPU time.

pi directly parses entry JSON without Effect Schema validation in its history read. In this nine-request workload it derives context twice per request: 18 scans and 3,044/15,086/60,266/210,854 historical entry parses. The different record models make per-record elapsed-time comparisons approximate; pi's stated roughly 3.3 µs per entry is a prior result, not a new timing measurement in this report.

| Seeded turns | Yielded prompt SQL cursor reads | Yielded whole-Run SQL reads | pi SQL reads | pi historical JSON bytes decoded |
|---:|---:|---:|---:|---:|
| 50 | 539 | 1,220 | 4,500 | 1,080,912 |
| 250 | 2,675 | 5,652 | 16,596 | 5,515,302 |
| 1,000 | 10,690 | 15,275 | 61,956 | 22,409,010 |
| 3,500 | 37,410 | 47,355 | 213,120 | 79,377,432 |

SQL numbers are observed workerd cursor rows, including index reads, not returned records, physical disk I/O or billing. Yielded row writes remain 544 per measured Run.

The input filter removes one prompt record per seeded turn. At 1,000 it reduces prompt records 5,334 → 4,334, readPrompt Effects 151,084 → 141,000, and first-model Effects 208,859 → 192,609. SQL scans barely change. The terminal filter similarly reduces record/Effect counts and cuts decoded bytes by about 24%, but adds correlated SQL probes: at 1,000, whole-Run SQL reads rise 15,275 → 19,292. Neither count result alone establishes lower latency.

At 250 and 1,000, the post-first-model work is exactly 22,046 Effects across baseline and both SQL candidates. Outside readPrompt, unscoped Schema calls grow 35,733 → 119,733. Context collection, journal folding and full-history digest encoding are source-identified work; the initial instrumentation does not assign independent Effect totals to each stage.

Native constructor restoration removes the second message-schema decode while retaining the upstream encoded schema and native constructors. At 1,000 turns it reduces readPrompt Effects 151,084 → 55,010 (63.6%) and first-model Effects 208,859 → 112,783 (46.0%). At 250 the corresponding first-model count is 56,123 → 32,098; at 3,500 it is 718,221 → 381,982. JSON bytes, records, SQL rows, metadata visits, fold visits and fingerprints are unchanged. Decoder Schema nodes fall from about 35.5 to 24.5 per selected record. These are operation-count reductions, not measured latency gains.

A separate identical digest-scope hook on both builds attributes 27,703 → 110,703 Schema nodes and 2,753 → 10,861 Effects to history-digest preparation from 250 → 1,000 turns. That explains 83,000 of the 84,000 extra Schema nodes outside readPrompt. Constructor restoration does not change digest preparation. Removing the extra scope counters exactly reproduces the previous count maps and model snapshots.

Reproduction, exact counters, module/capture hashes and limitations: [baseline/codec/pi](counts/codec-and-pi/REPORT.md), [input filter](counts/prompt-reads/REPORT.md), [terminal filter](counts/terminal-reads/REPORT.md), [first-model breakdown](counts/first-model/REPORT.md). Each bundle has an explicit publication allowlist and SHA-256 manifest. Large archives, databases and raw capture logs remain outside this branch.

## Deployed experiments

Rigorous runs use the same Objects across balanced ABBA/BAAB builds. Each pass verifies cold eviction, discards one warmup, and collects warm repeats. Seeds and all nine provider request fingerprints are checked. Histories grow across the balanced sequence. Ratios use medians of Object medians; paired changes are compared with baseline epoch drift and repeat ranges. No result smaller than the observed control spread is claimed as a gain.

The first completed experiment removed only the outer encoded-message traversal. Ten Objects per cell, six warm repeats per pass, 1,280 successful turns and 11,520 matching model requests showed no demonstrated warm improvement:

| History | Provider | State | Yielded ÷ pi before | After | Paired candidate ÷ baseline |
|---:|---:|---|---:|---:|---:|
| 50 | instant | warm | 1.33× | 1.28× | 0.963× |
| 50 | instant | cold | 1.62× | 1.32× | 0.978× |
| 250 | instant | warm | 1.48× | 1.43× | 1.002× |
| 250 | instant | cold | 1.71× | 1.63× | 0.891× |

These are complete-turn measurements, before provider-arrival retention was added. Warm baseline epoch drift medians were 10.5% and 5.3%; apparent gains were within the control/repeat spread. The change is not proposed as a PR. [Compact samples and outcomes](runs/codec.json), [full small table](runs/codec.md).

First-arrival measurements use the provider's receipt timestamp and the driver's submission start. Three echo probes before and after each turn bound the driver/provider offset, outside the timer and without contacting the measured Object. Corrected values use the interval midpoint and require consistent same-colo bounds across the turn. They assume stable provider-host clock offsets within that colo; raw differences and probe evidence are retained. Invalid clock checks are excluded from this metric and explicitly counted.

### Setup outcomes retained

- [3,500-turn import](runs/import-3500-memory-limit.json): isolate memory reset before any timed samples. Target cleanup verified.
- [Required-size matrix import](runs/import-250-storage-timeout.json): storage timeout/reset during a 250-turn seed, before timing. Target cleanup verified.
- [1,000-turn capability probe](runs/import-1000-smoke.json): canonical import and all 24 measured/warmup turns succeeded. One Object per target and one warm repeat; diagnostic only, not an improvement claim.

Every controller/sample failure and every observed non-ok invocation outcome is kept in compact run evidence. Telemetry can be delayed or incomplete; missing CPU rows are not treated as zero. Imported histories omit old Attempt rows through the real importer, so normalized inventories are compared; claims or ownership rows are never copied to bypass that normalization. No failed or uncertain measured input is replayed.

## Validation and cleanup

[Gate evidence](ready.json) binds passing `vp run ready` runs to exact product revisions, with no vendored `third-party/node_modules`. Existing context-continuity, compaction, adapter, crash and recovery proofs are reused; no new committed test suite is added.

Shared benchmark infrastructure is still in use. Final Alchemy teardown and prefix-wide Cloudflare API verification are pending; this report does not yet claim full cleanup.
