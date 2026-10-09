# History-dependent turn cost

Work in progress. Latency claims require a completed deployed same-Object comparison; local counts below are diagnostic. The primary metric is submission to the mock provider's first-request arrival, with whole-turn latency retained. Warm/cold, instant/400 ms and 50/250/1,000-turn coverage are tracked separately. Both the original 3,500-turn import and the repaired snapshot seed exceeded the deployed Object memory limit before timing; their failures and verified target cleanup are retained. Deterministic counts still cover 3,500 turns.

The product baseline is `07f0272e7ba49a494064b6b74c6318b55514ae19` (main after #827). The benchmark is the separate #828 worktree; its latest measured head and local adaptations are recorded per run. Product candidates contain no benchmark or results files.

## Prior work and candidates

#810 already introduced a fixed canonical context range, retained exceptions, narrow prompt records and `historyDigest`. #815/#816 removed several duplicate captures, decodes, byte counts and continuation checks. #821 made the deployed benchmark use production `ThreadObject`; #823/#825/#826 removed redundant wake, pre-arm and maintenance work; #827 skips empty or unchanged row views. This investigation preserves those changes and does not retry the rejected startup, minification, transaction or pre-arm experiments.

| Candidate | Commit | Change | Status |
|---|---|---|---|
| Single message-schema traversal | `40485f33` | Remove the outer encoded-message validation while retaining the native decoder | No demonstrated warm gain; no PR |
| Input filtering | `87158447` | Omit historical admissions from prompt reads; fetch exact compaction/late-owner admissions when needed | Counts and full gate passed; deployed setup failures retained |
| Terminal filtering | `4417a095` | Omit a duplicate settlement when an earlier terminal record is in the logical read range | Counts complete; timing pending |
| Native constructor restoration | `0451aacb` | Keep upstream encoded validation and restore native Prompt values with upstream constructors | Warm/instant first-arrival gain at 1,000 repeated in both build orders |
| Direct history digest encoding | `442c988c` | Encode upstream messages once, avoiding the Prompt wrapper's redundant pass | Exact digest equivalence, counts and full gate passed; deployed matrix running |

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

Direct history digest encoding removes the Prompt wrapper’s second traversal while retaining upstream message encoding and canonical JSON. At 50/250/1,000/3,500 turns, digest Schema visits fall 5,601 → 3,507 / 27,703 → 17,355 / 110,703 → 69,355 / 387,401 → 242,707 (about 37%). First-model Schema visits fall by 9.9–11.4%; Effect evaluations change by only 6–8. SQL, bytes, record/projection counts and fingerprints remain identical. This isolates a second candidate; it is not a latency claim.

Reproduction, exact counters, module/capture hashes and limitations: [baseline/codec/pi](counts/codec-and-pi/REPORT.md), [input filter](counts/prompt-reads/REPORT.md), [terminal filter](counts/terminal-reads/REPORT.md), [first-model breakdown](counts/first-model/REPORT.md), [native constructor restoration and digest scope](counts/prompt-hydration/REPORT.md), [direct digest encoding](counts/digest-encoding/REPORT.md). Each bundle has an explicit publication allowlist and SHA-256 manifest. Large archives, databases and raw capture logs remain outside this branch.

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

First-arrival measurements use the provider's receipt timestamp and the driver's submission start. Three echo probes before and after each turn travel through the measured Object to bound the driver/provider offset. Pre-probes finish before cold eviction and the timer; post-probes follow the completion timestamp. No Object call occurs between the cold abort and the timed submission. Corrected values use the interval midpoint and require consistent same-colo bounds across the turn. They assume stable provider-host clock offsets within that colo; raw differences and probe evidence are retained. Invalid clock checks are excluded from this metric and explicitly counted. Ratios require complete epoch/repeat coverage for both builds on each Object; incomplete pairs are excluded and counted. The earlier direct driver-to-provider probes reached different colos from model requests, invalidating 24 of 32 non-warmup timestamps in the [one-Object smoke](runs/seed-gate-clock-smoke.json). That smoke makes no latency claim.

### Native constructor restoration: required-size matrix

The first full matrix completed with ten Objects per target/size/provider cell, six warm repeats per pass, BAAB order, 3,840 successful turns and 34,560 matching model requests. The measured revisions are `07f0272e` → `0451aacb`; target cleanup was verified. The primary result is submission to the first provider-request arrival:

| Seed turns | Provider ms | State | Yielded ÷ pi before → after | Paired candidate ÷ baseline | Paired change |
|---:|---:|---|---:|---:|---:|
| 50 | 0 | cold | 2.561× → 2.952× | 1.134× | +89.8 ms |
| 50 | 0 | warm | 3.355× → 3.187× | 0.966× | -7.5 ms |
| 50 | 400 | cold | 3.949× → 3.514× | 1.006× | +1.9 ms |
| 50 | 400 | warm | 4.374× → 4.313× | 0.947× | -12.4 ms |
| 250 | 0 | cold | 4.416× → 4.255× | 1.118× | +96.1 ms |
| 250 | 0 | warm | 6.143× → 4.766× | 0.885× | -46.2 ms |
| 250 | 400 | cold | 3.571× → 4.266× | 1.082× | +84.0 ms |
| 250 | 400 | warm | 4.754× → 4.949× | 0.952× | -15.1 ms |
| 1,000 | 0 | cold | 3.509× → 3.128× | 0.987× | -19.4 ms |
| 1,000 | 0 | warm | 4.994× → 4.381× | 0.871× | -86.0 ms |
| 1,000 | 400 | cold | 3.636× → 3.431× | 1.022× | +27.8 ms |
| 1,000 | 400 | warm | 7.301× → 6.338× | 0.902× | -61.1 ms |

At 1,000 turns with an instant provider, the paired warm median improved 12.9% (86.0 ms). The six cohort repeat estimates were 0.830–0.923 candidate/baseline (9.3 percentage points of spread); median baseline epoch drift was 4.2%. This clears those observed variations. At 250 warm/instant the apparent 11.5% gain is approximately the 11.4-point repeat spread, so it is not a resolved claim. The 50-turn and 400 ms provider estimates also fall within their observed repeat/control spread. Cold estimates are mixed, including slower 50/250-turn instant medians. The reversed-order confirmation below retains a separate instant-provider cohort. None of those cold cells is claimed as a gain.

Whole-turn warm/instant Yielded ÷ pi was 1.188 → 1.135 / 1.662 → 1.512 / 0.921 → 0.895 at 50/250/1,000 seeded turns. Those ratios alone do not establish a gain. [All first-arrival and complete-turn conditions, medians and control drift](runs/prompt-hydration.md), [compact evidence](runs/prompt-hydration.json), [repeat estimates](runs/prompt-hydration.repeats.json), [numeric samples](runs/prompt-hydration.samples.csv).

Successful marked warm/instant invocation CPU medians are below. Each cell has 113–120 observed invocations per build; missing telemetry is not treated as zero. These are whole-invocation CPU costs, not isolated history-decoding microseconds per record.

| Seed turns | Yielded alarm CPU before → after | pi Run CPU before → after |
|---:|---:|---:|
| 50 | 112 → 107 ms | 309 → 297 ms |
| 250 | 270 → 222 ms | 305 → 306 ms |
| 1,000 | 477.5 → 426 ms | 949 → 930 ms |

The native constructor change is [PR #830](https://github.com/yielded-dev/agent/pull/830). Source inspection found no deferred persistent history-index or row-view backfill: row views are memory-only, import populates canonical selectors and Run indexes, and pre-probes initialize native services. Imported snapshots do omit maintenance KV state; constructor setup schedules bootstrap maintenance but does not await it. Possible overlap with the first cold sample remains an uncertainty, not an established explanation for its admission latency.

The clock check accepted 3,359 of 3,360 non-warmup turns. One pi 1,000-turn warm/400 ms sample had inconsistent bounds, so that metric uses nine complete pi Objects and ten Yielded Objects; all other first-arrival cells have ten each. Median clock-interval width was 31 ms (17–87 ms). Unattributed non-ok telemetry comprised 1,934 canceled alarms, 209 canceled fetches and 476 aborted fetches; the result retains them all, along with 61 unmatched telemetry markers. There were no controller or timed-workflow failures.

### Native constructor restoration: reversed-order confirmation

The ABBA confirmation used 20 Objects per target/size cell, instant providers, six warm repeats, and the same product revisions. All 3,840 turns, 34,560 model fingerprints and 3,360 non-warmup clock checks passed. Every timed Object's actual build binding was verified. Target cleanup was verified; there were no controller or workflow failures.

| Seed turns | State | Yielded ÷ pi before → after | Paired candidate ÷ baseline | Paired change | Baseline epoch drift |
|---:|---|---:|---:|---:|---:|
| 50 | cold | 3.602× → 3.061× | 0.899× | -89.2 ms | 15.2% |
| 50 | warm | 3.864× → 3.699× | 0.935× | -18.2 ms | 34.6% |
| 250 | cold | 4.261× → 3.802× | 0.868× | -143.9 ms | 24.3% |
| 250 | warm | 4.558× → 4.335× | 0.933× | -20.2 ms | 16.4% |
| 1,000 | cold | 4.754× → 4.336× | 0.843× | -192.8 ms | 9.8% |
| 1,000 | warm | 6.574× → 6.073× | 0.878× | -83.0 ms | 11.1% |

Warm/instant first arrival at 1,000 turns improved by a paired 12.2% (83 ms). All six cohort repeat estimates improved: 0.869–0.897 candidate/baseline, a 2.8-percentage-point range; their delta estimates spanned 27.5 ms. Median baseline epoch drift was 11.1%. Together with the earlier 12.9% / 86 ms result in BAAB order, this supports the narrow warm/instant 1,000-turn claim. First-request latency remains above pi: the confirmation's ratio is 6.073× after the change.

At 50 and 250, apparent warm gains of 6.5% and 6.7% remain inside repeat/control variation. Cold estimates differ across runs, so a cold gain is not established. Complete-turn estimates also remain inside repeat/control spread; no complete-turn improvement is claimed. The confirmation uses instant-only cohorts, while the earlier matrix mixed instant and 400 ms provider cells. Different placements, cohort workloads and protocol versions are retained rather than pooled.

Clock-interval width had a 32 ms median (15–88 ms). Observed non-ok telemetry comprised 1,931 canceled alarms, 473 aborted fetches and 182 canceled fetches, all unattributed; 78 telemetry markers lacked matching invocation rows. Missing CPU is not inferred as zero. [Full condition tables](runs/prompt-hydration-abba.md), [compact evidence](runs/prompt-hydration-abba.json), [repeat estimates](runs/prompt-hydration-abba.repeats.json), [numeric samples](runs/prompt-hydration-abba.samples.csv).

### Setup outcomes retained

- [Fresh-canary deployed check](runs/object-health-smoke.json): all 24 turns and 216 model requests matched, with target cleanup verified. It checks deployment transitions; one Object per target cannot support a performance claim. Later comparisons additionally require each timed Object’s build binding to match its declared framework build. The first full matrix predates that additional guard.

- [Canary readiness timeout](runs/abba-object-health-timeout.json): all 120 seeds were verified, but production readiness failed before timing (524.6 s total). Cleanup verified. The canary probes reused names across builds; later probes use fresh names per build and probe and retain the last health error. Local handler checks pass, without establishing deployed latency or the cause of this timeout.

- [Reverse-order setup](runs/abba-seed-missing-fetch.json): the first 250-turn seed reached an Object without a fetch handler after router health had passed. Zero timed samples; target cleanup verified. Subsequent runs add separate read-only canary Object health/build checks before seeding and timing. The uncertain seed was not retried.

- [3,500-turn import](runs/import-3500-memory-limit.json): isolate memory reset before any timed samples. Target cleanup verified. The later [snapshot capability probe](runs/snapshot-3500-memory-limit.json) also exceeded the deployed Object memory limit (125.6 s total, zero timed samples), despite a successful local round trip. Its target cleanup was also verified; deployed 3,500-turn timing remains unsupported.
- [Required-size matrix import](runs/import-250-storage-timeout.json): storage timeout/reset during a 250-turn seed, before timing. Target cleanup verified.
- [First-arrival matrix import](runs/first-arrival-input-seed-timeout.json): another storage timeout/reset during a 250-turn seed, before timing. Target cleanup verified. Its serial 250/1,000 setup ran for about 16 minutes; observed import fetch CPU was under one second while separate alarm invocations consumed tens of seconds. This suggested alarm/storage interleaving, but does not prove partial canonical rows were visible.
- [Native-client import](runs/native-import-seed-timeout.json): a 1,000-turn seed timed out despite using the production SQL client and mutation gate. Observed fetch CPU was 139 ms and a concurrent alarm used 44,490 ms; no timed turn ran. Target cleanup verified.
- [1,000-turn capability probe](runs/import-1000-smoke.json): canonical import and all 24 measured/warmup turns succeeded. One Object per target and one warm repeat; diagnostic only, not an improvement claim.

Every controller/sample failure and every observed non-ok invocation outcome is kept in compact run evidence. Telemetry can be delayed or incomplete; missing CPU rows are not treated as zero. Imported histories omit old Attempt rows through the real importer, so normalized inventories are compared; claims or ownership rows are never copied to bypass that normalization. No failed or uncertain measured input is replayed.

The seed gate and native-client changes did not eliminate setup timeouts. A subsequent [offline canonical import](runs/offline-import-seed-timeout.json) also timed out at 1,000 turns with no alarm observed: fetch CPU was 70,552 ms over 85,588 ms elapsed. This refutes alarms as a sufficient explanation of the timeout. No timed sample ran; target cleanup was verified. The current harness canonically imports every destination fixture locally, then deploys an explicit offline seed bundle to restore its exact SQLite snapshot into a fresh Object. It verifies the source and restored SHA-256, complete normalized table counts, and transcript fingerprints, including all indexes and triggers. Old Attempts and ownership were normalized by the real importer; no canonical payload is rewritten. The seed class cannot execute native submission RPCs. Production `ThreadObject` then replaces it on the same Worker, namespace and Object names before timing. Native gates, alarms, confirmed writes and recovery remain unchanged. [Adaptation patch, method and snapshot proof](harness/README.md).

Pre-probes instantiate the production host before the timed cold eviction. “Cold” therefore means a fresh production instance over the prepared store. Native initialization, gates, alarms, submission, settlement and recovery stay active during all measured builds. The seed bundle hash and framework revision are retained alongside the two measured build hashes.

## Remaining history read outside this ownership area

The Cloudflare submission ledger's ownership query reads retained submissions even when ownership is empty. At 250/1,000/3,500 turns, the existing local capture observes two executions totaling 504/2,004/7,004 SQL cursor reads. Source control flow places one claim before model work and another after settlement; the capture does not split those totals, so they are not all attributed to first-provider arrival. The 50-turn fixture uses a bounded lane path and does not execute this query.

Host SQLite query plans on copied fixtures show that an ownership-first CROSS JOIN would scan ownership and probe the existing submission primary key. The generic SQL ledger already has an empty-ownership EXISTS guard; the Cloudflare ledger does not. This is a handoff finding, with no product change, workerd candidate measurement, latency claim or lease-contention proof. [Queries, catalogs and observed host plans](counts/handoff/ownership-query-plans.json).

## Validation and cleanup

[Gate evidence](ready.json) binds passing `vp run ready` runs to exact product revisions, with no vendored `third-party/node_modules`. Existing context-continuity, compaction, adapter, crash and recovery proofs are reused; no new committed test suite is added.

Shared benchmark infrastructure is still in use. Final Alchemy teardown and prefix-wide Cloudflare API verification are pending; this report does not yet claim full cleanup.
