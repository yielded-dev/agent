# Wake deferral measurement

partial; no complete-matrix claim. 32/216 measured turns admitted across 4/27 complete active Objects.

Worker latency in milliseconds. Each cell summarizes the independent Object pairs admitted from the active plan; positive savings mean baseline minus candidate. Retired Objects and unmatched attempts remain in the evidence and error accounting.

| Seed history | TTFT | State | Objects | Baseline | Candidate | Paired saving | Object saving range | Baseline repeat spread | Candidate repeat spread |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 50 | 0 | warm | 1 | 1316.0 | 1044.0 | 272.0 | 272.0–272.0 | 206.0 | 228.0 |
| 50 | 0 | cold | 1 | 1472.0 | 1322.5 | 149.5 | 149.5–149.5 | 410.0 | 67.0 |
| 50 | 400 | warm | 1 | 6742.0 | 5968.0 | 774.0 | 774.0–774.0 | 26.0 | 10.0 |
| 50 | 400 | cold | 1 | 7312.0 | 6542.0 | 770.0 | 770.0–770.0 | 128.0 | 98.0 |
| 250 | 0 | warm | 1 | 3060.5 | 1845.0 | 1215.5 | 1215.5–1215.5 | 177.0 | 38.0 |
| 250 | 0 | cold | 1 | 3239.0 | 1925.0 | 1314.0 | 1314.0–1314.0 | 14.0 | 66.0 |
| 250 | 400 | warm | 1 | 7080.0 | 6242.5 | 837.5 | 837.5–837.5 | 412.0 | 251.0 |
| 250 | 400 | cold | 1 | 7767.5 | 6842.0 | 925.5 | 925.5–925.5 | 291.0 | 100.0 |

The last two columns are median absolute differences between each arm's two planned repeats. These descriptive spreads and the per-Object effect range do not establish whether an effect exceeds noise. Missing CPU joins remain unknown. Full distributions, individual model gaps, mechanism reports, errors, and join evidence are retained in [analysis.json.gz](analysis.json.gz); offline reduction also writes the plain analysis.json.

A/B baseline bypasses only withProcessing in the candidate build. Both arms use acquireUseRelease in maintenance and the same observer code; observation cost varies with work and has not been subtracted. Driver timing includes the measurement receipt. The original baseline and local-counts provenance are separate evidence.

Driver/laptop durations use their own monotonic clocks. Model arrival, provider-based inter-model gaps, and provider tail compare Worker wall clocks and may include skew. DO Date.now advances at I/O and is not a CPU stopwatch.

objectModelGapsIoMs[i - 1] = calls[i].fetchStartedMs - calls[i - 1].endMs, i=1..8, using the same Object's Date.now I/O clock. Each gap starts at stream wrapper finalization (EOF/disposal), not exact DONE parsing, and ends at the next fetch dispatch. The array, objectModelGapMedianIoMs, and objectModelGapTotalIoMs require a valid transcript and all eight finite gaps; otherwise they are null. These durations do not measure CPU.

Returned active-window edges and boundary snapshots establish overlap and starts inside the turn; mutable query labels do not. alarmCpuMs is complete observed-handler CPU for the turn, not all native scheduling deliveries. It requires both logged edges and one alarm ID to one invocation/trace in both directions. All alarm CPU is whole native invocation cost, never prorated. alarmOverlapFullCpuMs is a non-additive diagnostic; shared invocations are flagged across all returned turns and nulled in affected paired CPU metrics. alarmAccounting.nativeTotals/nativeCategories also retain unjoined native CPU by outcome and handler-log presence. attributedCpuMs includes fetch plus contained alarms only. A 250 ms drain does not prove alarm completion.

Native alarm records across all captured phases and outcomes; CPU is the known, unweighted sum in each category. Handler-log absence is not a claim about whether or why execution was canceled.

| Outcome | Turn join | Handler log | Native records | CPU known/missing | Nonzero CPU records | Known CPU ms |
| --- | --- | --- | --- | --- | --- | --- |

Native counts and CPU sums describe captured records without sampling weights. sampleInterval > 1 reflects ingestion/platform sampling independently of statistics.abr_level; abr_level=1 cannot certify complete capture. See the [Cloudflare telemetry API](https://developers.cloudflare.com/api/resources/workers/subresources/observability/subresources/telemetry/methods/query/).

Captured error/outcome records: 2 requests; 1 unanswered attempts; 5 controller errors; 0 invocation records with non-ok or missing outcomes (including explicit cold aborts); 0 exceededCpu; 0 exceededMemory. Review outcome groups and query sampling before interpreting counts as workload failures; no sampling weights are applied. See capture coverage before interpreting zero counts.

Cleanup verified: false. Credential scan passed: false.
