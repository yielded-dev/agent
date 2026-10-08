# Wake deferral measurement

partial; no complete-matrix claim. 200/224 measured turns admitted across 25/28 complete active Objects.

Worker latency in milliseconds. Each cell summarizes the independent Object pairs admitted from the active plan; positive savings mean baseline minus candidate. Retired Objects and unmatched attempts remain in the evidence and error accounting.

| Seed history | TTFT | State | Objects | Baseline | Candidate | Paired saving | Object saving range | Baseline repeat spread | Candidate repeat spread |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 50 | 0 | warm | 7 | 2308.0 | 1664.0 | 910.5 | 62.0–2595.0 | 637.0 | 205.0 |
| 50 | 0 | cold | 7 | 3170.5 | 1898.0 | 939.0 | 47.5–2177.0 | 118.0 | 250.0 |
| 50 | 400 | warm | 7 | 6289.5 | 5617.5 | 605.0 | -111.0–1164.0 | 61.0 | 88.0 |
| 50 | 400 | cold | 7 | 6802.0 | 5887.5 | 810.5 | 411.5–1199.0 | 124.0 | 117.0 |
| 250 | 0 | warm | 4 | 1826.0 | 1478.5 | 40.3 | -75.5–679.5 | 467.5 | 71.0 |
| 250 | 0 | cold | 4 | 2664.3 | 2076.0 | 629.0 | 82.5–987.0 | 378.0 | 201.0 |
| 250 | 400 | warm | 7 | 6431.5 | 5768.5 | 593.5 | 338.0–985.0 | 143.0 | 42.0 |
| 250 | 400 | cold | 7 | 6750.0 | 6079.5 | 498.5 | 350.0–975.0 | 90.0 | 70.0 |

The last two columns are median absolute differences between each arm's two planned repeats. These descriptive spreads and the per-Object effect range do not establish whether an effect exceeds noise. Missing CPU joins remain unknown. Full distributions, individual model gaps, mechanism reports, errors, and join evidence are retained in [analysis.json.gz](analysis.json.gz); offline reduction also writes the plain analysis.json.

A/B baseline bypasses only withProcessing in the candidate build. Both arms use acquireUseRelease in maintenance and the same observer code; observation cost varies with work and has not been subtracted. Driver timing includes the measurement receipt. The original baseline and local-counts provenance are separate evidence.

Driver/laptop durations use their own monotonic clocks. Model arrival, provider-based inter-model gaps, and provider tail compare Worker wall clocks and may include skew. DO Date.now advances at I/O and is not a CPU stopwatch.

objectModelGapsIoMs[i - 1] = calls[i].fetchStartedMs - calls[i - 1].endMs, i=1..8, using the same Object's Date.now I/O clock. Each gap starts at stream wrapper finalization (EOF/disposal), not exact DONE parsing, and ends at the next fetch dispatch. The array, objectModelGapMedianIoMs, and objectModelGapTotalIoMs require a valid transcript and all eight finite gaps; otherwise they are null. These durations do not measure CPU.

Returned active-window edges and boundary snapshots establish overlap and starts inside the turn; mutable query labels do not. alarmCpuMs is complete observed-handler CPU for the turn, not all native scheduling deliveries. It requires both logged edges and one alarm ID to one invocation/trace in both directions. All alarm CPU is whole native invocation cost, never prorated. alarmOverlapFullCpuMs is a non-additive diagnostic; shared invocations are flagged across all returned turns and nulled in affected paired CPU metrics. alarmAccounting.nativeTotals/nativeCategories also retain unjoined native CPU by outcome and handler-log presence. attributedCpuMs includes fetch plus contained alarms only. A 250 ms drain does not prove alarm completion.

Native alarm records across all captured phases and outcomes; CPU is the known, unweighted sum in each category. Handler-log absence is not a claim about whether or why execution was canceled.

| Outcome | Turn join | Handler log | Native records | CPU known/missing | Nonzero CPU records | Known CPU ms |
| --- | --- | --- | --- | --- | --- | --- |
| aborted | unjoined | not seen | 7 | 7/0 | 6 | 521.0 |
| canceled | unjoined | not seen | 2988 | 2988/0 | 1890 | 55568.0 |
| ok | unjoined | not seen | 861 | 861/0 | 853 | 11146.0 |
| ok | unjoined | seen | 1941 | 1941/0 | 1941 | 173676.0 |
| ok | joined | seen | 2558 | 2558/0 | 2558 | 273392.0 |

Native counts and CPU sums describe captured records without sampling weights. sampleInterval > 1 reflects ingestion/platform sampling independently of statistics.abr_level; abr_level=1 cannot certify complete capture. See the [Cloudflare telemetry API](https://developers.cloudflare.com/api/resources/workers/subresources/observability/subresources/telemetry/methods/query/).

Captured error/outcome records: 7 requests; 0 unanswered attempts; 0 controller errors; 3253 invocation records with non-ok or missing outcomes (including explicit cold aborts); 0 exceededCpu; 0 exceededMemory. Review outcome groups and query sampling before interpreting counts as workload failures; no sampling weights are applied. See capture coverage before interpreting zero counts.

Cleanup verified: true. Credential scan passed: true.
