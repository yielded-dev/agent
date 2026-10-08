# Where durable-turn latency goes on Cloudflare

**Yielded's clearest measured lever is coalescing redundant wake scheduling: 581 [406–1,009] ms saved in this warm, 50-seeded-turn direct-run experiment.** The old roughly 180 ms non-CPU residual is not an established durability floor. A small write adds about 37 ms in the calibration; native RPC invocation accounting adds about 132 ms relative to DO fetch without a corresponding client penalty. Networked turns also incur substantial CPU in alarm invocations, which fetch-only CPU omits.

The zero-programmed-delay provider produced the following results. Every cell is milliseconds, median [Q1–Q3] across **seven Object medians**. Warm is m2–m5; cold is a verified new Object incarnation with existing history. These are measured scopes, **not additive columns**.

| Target | Seed history | Warm client | Cold client | Warm DO fetch wall | Warm observed combined CPU† | Warm per-step gap* |
|---|---:|---:|---:|---:|---:|---:|
| Yielded | 50 | 2,320 [1,941–2,996] | 3,206 [2,689–3,846] | 2,194 [1,588–2,596] | 649 [449–715] | 138 [122–241] |
| pi-durable 1.0.4 | 50 | 901 [864–996] | 1,200 [1,135–1,238] | 569 [541–629] | 168 [153–269] | 57 [56–65] |
| tardie 0.44.0‡ | 50 | 2,923 [2,577–3,203] | 3,493 [3,270–3,858] | 2,309 [2,048–2,594] | 467 [443–491] | 224 [190–262] |
| Yielded | 250 | 2,168 [1,948–3,290] | 3,308 [2,995–3,541] | 2,114 [1,822–3,201] | 1,335 [986–1,540] | 168 [156–290] |
| pi-durable 1.0.4 | 250 | 818 [672–921] | 1,138 [1,032–1,241] | 710 [608–837] | 332 [292–459] | 71 [62–83] |
| tardie 0.44.0‡ | 250 | 2,453 [2,087–3,013] | 3,585 [2,958–3,786] | 2,144 [1,906–2,839] | 637 [575–1,085] | 171 [154–234] |

† Available fetch plus fully contained, uniquely joined alarm/RPC CPU; not complete critical-path CPU. Accepted combined-CPU joins are respectively 17, 27, 28, 21, 26 and 24 of the 28 warm turns in these rows. Platform sampling and ambiguous alarm contexts remain missing. *Gaps use provider endpoint I/O clocks; clock qualifications appear below. ‡Tardie's main path includes two harness Actor observation RPCs. The later on/off calibration did not resolve a stable correction; these are explicitly the instrumented totals. All targets include counters, provider receipts and diagnostic response encoding.

These are descriptive cohort comparisons. The identical-code Yielded control varies enough to prevent a portable framework speed ranking: at 50/zero-delay its paired client differences span −438 to +2,195 ms; at 250 they span −2,316 to +2,492 ms. The apparently faster 250-turn totals also coincide with different HTTP routing. Do not interpret them as a beneficial history-length effect. [All warm/cold/delay rows](network-table.md), [machine-readable results](summary.json.gz).

## Ranked latency levers

The ranking favors a demonstrated user-latency effect, then quantifiable opportunities. Conditional budgets below are **not measured optimization gains and must not be added together**.

| Priority | Lever | User-latency estimate and evidence | Durable-contract boundary |
|---:|---|---|---|
| 1 | Coalesce redundant wake hints while directly processing a turn | **581 [406–1,009] ms saved** at 50; all seven Object estimates positive. At 250, 328 [85–503] ms is inside much larger repeated-baseline variation and is unresolved. The intervention barely changes SQL payload, so this is not evidence for a byte-volume explanation. | Existing `withWakesDeferred` retains pre-arming and durable generation checks. Removing the required alarm, claims or leases is forbidden. |
| 2 | Reduce runtime/adapter CPU and cold recovery work | A **conditional 25%** reduction of the available warm CPU would be about **160 ms at 50 / 330 ms at 250**, only to the extent it lies on the critical path. Yielded's paired cold premium is **747 [637–852] / 962 [408–1,260] ms**; this is an opportunity envelope for the entire cold path, not an achievable saving estimate. | Cache/reuse disposable derived state and compiled work with valid ownership and `historyDigest`; keep verification, original-context recovery and canonical accounting. |
| 3 | Remove redundant sequential durability waits | Calibration prices an additional forced small-write wait at about **37 ms**. Eliminating one genuinely redundant wait in each of eight gaps would be a **conditional ~296 ms/turn**. Native flush counts are not exposed, so that opportunity is unproven. Explicitly adding `sync()` before fetch shows no benefit. | Atomically batch compatible facts without changing append order/hash-chain semantics. Required intent/ownership facts must be durable before dependent external dispatch; `allowUnconfirmed` is not an admissible shortcut. |
| 4 | Minify and reduce global startup work | **No cold-latency saving resolved here.** Minification removes 55% of raw bytes, but its 113 [−11–276] ms client contrast is smaller than the control's 267 ms maximum absolute drift. Upload startup also shows no benefit. Reducing initialization work remains a separate candidate. | Semantics-preserving packaging is allowed; lazy work must still complete before its dependent operation. |
| 5 | Improve Object/provider/client placement | **Unmeasured placement opportunity.** Saving 10 ms on each of nine serial model round trips would save a conditional **90 ms/turn**, plus any entry/return improvement. This public-Worker mock already has a nearby no-write echo interval of 7 [6–9] ms. | Placement can change without weakening durability; this run fixes `wnam` and does not establish the best location for a real provider. |
| 6 | Write fewer bytes and encode smaller responses | Payload-only calibration is flat from 1–16 KiB and about 4 ms higher at 128 KiB. A 128→16 KiB reduction before each of nine comparable barriers would be a **conditional ~36 ms/turn**; actual typical bound payloads are much smaller. No native byte-only or Yielded response-encoding saving is established. | Compact/version storage representation and disposable projections; do not delete canonical facts or rewrite existing history in place. |

Changing RPC to fetch is a **measurement correction, not a demonstrated latency optimization**. Its roughly 132 ms invocation-wall reduction was absent in client latency. Effect #8914 is outside this experiment; the completed sibling found no resolvable latency gain from it.

## Native networked evidence

All **112 main cohorts / 672 turns** completed without a failed measured request, and all nine model-visible reference-projection fingerprints per turn match the shared reference. The 112 cold entries are verified. The separate Yielded intervention adds 28 settling and 84 comparison turns, also with valid transcripts. Main summary tables exclude m1 settling turns; they retain 112 cold and 448 warm observations.

With 400 ms to the first frame and 10 ms between frames, the provider programs **4,130 ms per turn**. The adjusted total subtracts that programmed delay per turn; it still includes provider processing, timer overshoot, network and framework work.

| Target | Seed history | Warm client ms | Warm client − scripted ms | Cold client ms | Cold client − scripted ms |
|---|---:|---:|---:|---:|---:|
| Yielded | 50 | 6,717 [6,155–6,850] | 2,587 [2,025–2,720] | 7,485 [6,981–7,544] | 3,355 [2,851–3,414] |
| pi | 50 | 4,859 [4,769–4,966] | 729 [639–836] | 5,174 [5,023–5,384] | 1,044 [893–1,254] |
| tardie‡ | 50 | 6,226 [6,065–6,500] | 2,096 [1,935–2,370] | 7,028 [6,772–7,337] | 2,898 [2,642–3,207] |
| Yielded | 250 | 6,495 [6,232–7,013] | 2,365 [2,102–2,883] | 7,236 [6,996–7,526] | 3,106 [2,866–3,396] |
| pi | 250 | 5,034 [4,822–5,337] | 904 [692–1,207] | 5,539 [5,332–5,908] | 1,409 [1,202–1,778] |
| tardie‡ | 250 | 6,299 [6,231–6,972] | 2,169 [2,101–2,842] | 6,815 [6,796–7,317] | 2,685 [2,666–3,187] |

Time to first model request and gaps, zero programmed provider delay:

| Target | Seed history | First request, warm ms* | First request, cold ms* | Gap, cold ms* | Eight-gap sum, warm ms* |
|---|---:|---:|---:|---:|---:|
| Yielded | 50 | 439 [425–519] | 1,075 [857–1,165] | 226 [186–298] | 1,551 [1,086–1,991] |
| pi | 50 | 233 [212–243] | 459 [406–489] | 59 [56–63] | 477 [456–547] |
| tardie‡ | 50 | 580 [570–699] | 1,103 [1,022–1,304] | 241 [203–270] | 1,843 [1,582–2,149] |
| Yielded | 250 | 530 [441–578] | 1,330 [1,109–1,451] | 177 [159–257] | 1,479 [1,343–2,209] |
| pi | 250 | 138 [124–146] | 365 [328–392] | 73 [70–83] | 591 [500–682] |
| tardie‡ | 250 | 404 [336–678] | 1,431 [1,363–1,614] | 198 [152–239] | 1,629 [1,358–1,999] |

*First-request observations cross controller/provider clocks; gaps cross provider I/O clocks. These endpoint timestamp differences do not establish an exact additive client-latency partition. No negative first-request or gap differences occurred in the main matrix. CPU/DO wall for every cold and 400 ms cell are retained in [network-table.md](network-table.md) and `summary.json.gz`.

Additional zero-delay cold and invocation-scope measurements, milliseconds:

| Target | History | Cold DO fetch wall | Cold observed combined CPU† | Same-Object cold − warm client | Warm client − DO fetch wall |
|---|---:|---:|---:|---:|---:|
| Yielded | 50 | 3,074 [2,359–3,492] | 1,066 [769–1,441] | 747 [637–852] | 333 [309–397] |
| pi | 50 | 837 [826–981] | 263 [229–348] | 308 [211–406] | 350 [315–368] |
| tardie‡ | 50 | 2,927 [2,565–3,242] | 754 [540–824] | 680 [497–796] | 574 [444–626] |
| Yielded | 250 | 3,148 [2,925–3,430] | 2,133 [1,559–2,270] | 962 [408–1,260] | 95 [59–124] |
| pi | 250 | 1,096 [934–1,122] | 433 [395–649] | 363 [325–407] | 79 [62–98] |
| tardie‡ | 250 | 2,976 [2,406–3,365] | 914 [884–1,008] | 770 [508–1,135] | 178 [150–181] |

Cold Tardie/250 DO wall and CPU have six available Object observations; the other cold cells have seven. Client − DO wall is calculated per observation before reduction. It includes scopes outside the measured DO fetch, including the Tardie observation RPCs, and must not be labeled pure client–Object network time. The 50/250 difference reinforces the need to retain routing and invocation scope when comparing the earlier ~60 and ~185 ms residuals.

### Writes, alarms and per-step attribution

**All 112 primary warm Yielded turns and all 112 primary warm Tardie turns overlap recorded native alarm activity; pi has none.** Fetch-only CPU is incomplete turn accounting. At 50/zero-delay, Yielded has 106 [53–136] ms fetch CPU and 515 [389–601] ms available alarm CPU; Tardie has 12 [10–18] and 457 [427–476] ms respectively. These marginal statistics use different available subsets and do not sum to the combined-CPU column. Alarm IDs with ambiguous invocation mappings are excluded from combined CPU, while their valid client/transcript data remain included. Native Yielded maintenance can process thread work; receipts do not identify which individual model calls ran inside an alarm.

Typical counters during the gap **after the previous native stream completes and before the next fetch begins**:

| Target | History | Observed gap ms | SQL bound bytes | KV JSON bytes | Native transactions / mutation-bearing calls | Mutation SQL statements |
|---|---:|---:|---:|---:|---:|---:|
| Yielded | 50 | 138 [122–241] | 6,829 | 311 | 3 / 1.5 | 10 |
| Yielded | 250 | 168 [156–290] | 6,894 | 315 | 2 / 1 | 10 |
| pi | 50 | 57 [56–65] | 4,951 | 0 | 9 / 9 | 43 |
| pi | 250 | 71 [62–83] | 5,014 | 0 | 9 / 9 | 43 |
| tardie | 50 | 224 [190–262] | 2,606 | 1,902 | 6 / 6 | 8 |
| tardie | 250 | 171 [154–234] | 2,618 | 1,920 | 6 / 6 | 8 |

Counts use the same nested median reduction as gaps; fractional counts are medians. All 1,344 gaps have an end-of-stream counter snapshot. Whole-call counters are different: Yielded at 50 has 11.25 transaction calls per whole model-call interval versus 3 after the previous stream. Some work happens during the stream. Before the first request, Yielded submits about 9.7 KB of SQL bindings, pi 1.4 KB and Tardie 0.9 KB. Tardie's per-step snapshots cover the Thread; all separately recorded Actor counters in this warm zero-delay subset are zero, although observation RPC overhead is still present.

Whole-turn warm counters at zero programmed delay, again medians across seven Object medians:

| Target | History | SQL binding B | KV JSON B | Native transactions / mutation-bearing calls | Mutation SQL statements | Explicit native sync calls |
|---|---:|---:|---:|---:|---:|---:|
| Yielded | 50 | 76,232 | 12,132.5 | 109.5 / 54 | 141.5 | 0 |
| Yielded | 250 | 77,018 | 10,015 | 89.5 / 44.5 | 138 | 0 |
| pi | 50 | 42,257 | 0 | 77 / 77 | 372 | 0 |
| pi | 250 | 42,787 | 0 | 77 / 77 | 372 | 0 |
| tardie | 50 | 25,295.5 | 22,382 | 73 / 72 | 80 | 72 |
| tardie | 250 | 25,403 | 23,334 | 79 / 78 | 80 | 78 |

With 400 ms TTFT, native transaction / mutation-bearing medians are 163 / 81 and 154 / 76.5 for Yielded at 50/250, 92 / 92 for Tardie at both histories, and 77 / 77 for pi. Background activity makes these counts depend on the timing setting. API counts cannot establish that pi has more durability waits than Yielded. All main turns have the required counters; recorded transaction-overlap, rollback and window-crossing diagnostics are zero. Tardie's native sync-interval sums overlap and must not be treated as a 2.6 s additive durability budget.

**An exact durability/CPU/rest split per step is not identified.** With one comparable outstanding small-write barrier, the calibration suggests a conditional ~37 ms contribution. The remaining gap cannot be split into CPU and other waits: invocation CPU is not per-step, native sync intervals use stale I/O-clock boundaries and can overlap, and API transaction counts are not physical flush counts. Subtracting 37 from each gap, dividing turn CPU by nine, or summing sync intervals would manufacture precision. Input-gate queueing, response encoding and routing are not separately priced by these observations. Per-turn bytes, transaction counts, overlaps, rollback/window diagnostics and all nine step records are retained in `turns.jsonl.gz` and `summary.json.gz`.

The sibling's approximately 90 KB Yielded / 16.8 KB pi / 42 KB Tardie figures are **net database growth**, not these submitted-byte counters or physical replication bytes. Its common ~180 ms residual fits the scale of RPC-lifetime accounting plus a small-write barrier, but this is not a numerical decomposition of that older cohort. Its additional Yielded 130–150 ms cannot be assigned to bytes from these data. Multiple barriers, alarms and work charged to other invocations are plausible; the wake intervention supplies direct evidence of a larger avoidable path. [Completed sibling report](https://github.com/danieljvdm/effect-agent/blob/f58a534b5e0a944d512544aa3e5ff483b5a56736/examples/durable-bench/results/cf-bench-8914/report.md)

### Same-Object wake and explicit-sync experiments

Two randomized repeats per condition in each of seven warm Objects, zero-delay provider, baseline minus intervention:

| Seed history | Intervention | Client ms saved | Eight-gap sum ms saved | Interpretation |
|---:|---|---:|---:|---|
| 50 | Defer wake hints | 581 [406–1,009] | 380 [198–768] | Clearest observed benefit; all seven client estimates positive |
| 250 | Defer wake hints | 328 [85–503] | 71 [38–263] | Inside repeated-baseline spread; unresolved |
| 50 | Explicit sync before each fetch | −209 [−355–−137] | −145 [−351–−98] | No latency benefit established |
| 250 | Explicit sync before each fetch | −32 [−83–247] | 74 [−28–290] | No latency benefit established |

Absolute differences between each Object's two baseline client observations are 50–515 ms at 50 (median 303), and 343–1,392 ms at 250 (median 679). At 50, 7/7 gains are positive and 5/7 exceed that Object's own baseline-repeat difference; at 250 those counts are 5/7 and 1/7. These are descriptive checks, not significance tests. The cross-Worker control is not substituted for this same-Object repeat check. The 50-turn median gain exceeds the full observed baseline-repeat spread, but this small experiment is not a universal guarantee. Its 380 ms eight-gap reduction is equivalent to 47.5 ms per gap on average, not a uniform per-step saving.

Wake deferral changes submitted SQL bindings by only 161 B/turn at the 50-turn median, KV arguments by 3,202 B and mutation-bearing transaction calls by 18.5. Available paired CPU changes cross zero. The experiment identifies a combined scheduling/storage/CPU-path effect; it does not isolate queueing from durability or prove that CPU is unchanged. Both conditions retain the large lookup #97 in prior history after m7; this later comparison is not the exact m2–m5 history.

The intervention wraps this harness's direct `execute(input)` path. `ThreadMaintenance.pass` already uses `withWakesDeferred`; this is not evidence to wrap that maintenance path again, or a promise of 581 ms for every consumer. The actionable finding is redundant wake activity during direct execution. Confirm the consuming application's entry point before applying it. [Direct-run experiment](network/yielded.ts), [existing maintenance path](../../../../packages/platform-cloudflare/src/Alarm.ts).

### Tardie observation calibration

All 14 same-Object cohorts passed identity, version, stream and transcript checks: two warmups followed by three randomized on/off pairs, at both histories. Off returns the native Thread response directly; on adds Actor `begin`/`end` RPCs, counter collection, response buffering and wrapping. Canonical operations are unchanged.

| Seed history | On client ms | Off client ms | Paired on − off ms | Same-state repeat difference, on / off ms |
|---:|---:|---:|---:|---:|
| 50 | 3,092 [2,284–3,204] | 2,386 [2,195–3,034] | 173 [−5–187] | 193 / 197 |
| 250 | 2,292 [2,021–2,988] | 2,440 [2,213–3,060] | −91 [−220–33] | 733 / 645 |

The paired effect is a difference of per-Object medians, not a difference of the two marginal table medians. It lies inside repeat variation and changes sign between histories: **no stable correction is established**. Off-path totals are useful additional observations, but subtracting the point estimate from the earlier main table would not recover native production latency. Actor identity is bracketed through observed turns and final release; off turns deliberately do not claim complete Actor CPU/accounting. Raw rows and admission checks are in the `instrumentation` object of [the archived summary](summary.json.gz).

### Minification, upload startup and cold recovery

The follow-up builds use identical source inputs and fixture hashes at `7a5bec91a32e3747151bd5c0457f92defe1eb724`; only the minification flag changes. This is the **combined three-target native module**, not a standalone Yielded bundle.

| Build | Raw bytes | Gzip bytes |
|---|---:|---:|
| Plain | 7,298,737 | 1,403,605 |
| Minified | 3,319,078 | 948,291 |

That is 55% fewer raw bytes and 32% fewer gzip bytes. Primary uploads follow plain/minified/minified/plain/minified/plain/plain/minified; the control uploads identical plain bytes in every round. Each round cold-resets the same seven Yielded Objects per Worker and submits one zero-delay turn. After m6/m7 preparation, comparison inputs m8–m15 have 58–65 completed turns of history before them. The ABBA BAAB order balances linear progression, not nonlinear drift. Canonical history is never rewound.

All **112/112 cold turns, 14/14 Object sequences and 16/16 upload identities** passed admission, including all nine reference-projection hashes per turn. For each Object, the comparison is the mean of its four plain positions minus the mean of its four minified positions, followed by median [Q1–Q3] across seven Objects. The control uses those same positions with plain code throughout. Positive values mean a lower time in the positions assigned to minification.

| Comparison | Client contrast ms | Full Object range ms | First-model contrast ms* | Eight-gap contrast ms* |
|---|---:|---:|---:|---:|
| Primary plain − minified | 113 [−11–276] | −501 to +358 | 96 [42–146] | −6 [−63–76] |
| Unchanged-code control positions | 20 [−145–87] | −267 to +96 | −46 [−54–−14] | 34 [−86–131] |
| Primary contrast − paired control contrast | 118 [57–215] | −235 to +338 | 147 [112–167] | −53 [−92–4] |

**No user-latency gain is resolved.** The 113/118 ms client point estimates are smaller than the control's maximum absolute drift of 267 ms and full range width of 363 ms; the adjusted Object range crosses zero. A positive adjusted IQR is insufficient evidence for claiming a small improvement. The first-model differences have the additional cross-clock qualifications described above; they are not a standalone startup saving.

| Upload group | Startup ms [Q1–Q3] | Full upload range ms | Unique uploads |
|---|---:|---:|---:|
| Primary plain | 165.5 [160.25–172.75] | 152–187 | 4 |
| Primary minified | 184 [165.25–249] | 151–402 | 4 |
| Control plain | 189 [175–280.5] | 152–329 | 8 |

Startup is not lower in the minified sample, and the control spread is broad. Each measurement was rechecked against the exact version's startup and `UPLOAD_REVISION` binding together: [version audit](minification-metadata.json). The controller stopped twice **before** timed inputs: after 30 turns an existing Object still reported the prior version, and after 70 an active-deployment API read lagged an upload. It resumed only after auditing acknowledged inputs/releases and reconciling the recorded version. No canonical input or completed upload was repeated. Both interruptions and the additional preparation are retained in [the plan](minification.json); their idle gaps can introduce nonlinear drift. The main matrix and wake experiment preceded these interruptions.

Cloudflare's `startup_time_ms` measures parsing and global execution during upload validation. It is not a measured addition to each cold Object turn. The statistical unit for startup is the unique upload: four per primary flavor and eight controls, rather than the seven Objects sharing each upload. [Worker startup time](https://developers.cloudflare.com/workers/platform/limits/#worker-startup-time)

For scale, the completed sibling measured **standalone scripted** bundles as follows, with three separate uploads per cell. These are context from another run, not pooled with the combined-module experiment:

| Standalone target | Original module raw / gzip B | Startup at 50, ms | Startup at 250, ms |
|---|---:|---:|---:|
| Yielded / pinned Effect 4.0.0 | 3,432,935 / 655,834 | 90 [88.5–113.5] | 92 [91–93.5] |
| pi-durable 1.0.4 | 879,497 / 158,081 | 7 [6.5–8] | 11 [8.5–12] |
| tardie 0.44.0 | 1,689,120 / 342,158 | 60 [59.5–79] | 71 [68–108] |

The standalone size/startup differences make packaging and initialization reasonable cold-path candidates. They do not establish that removing bytes would remove the whole Yielded cold premium, which also includes recovery and execution. [Sibling bundle and startup evidence](https://github.com/danieljvdm/effect-agent/blob/f58a534b5e0a944d512544aa3e5ff483b5a56736/examples/durable-bench/results/cf-bench-8914/report.md#storage-upload-startup-and-bundles)

### Identical-code deployment control

The two Workers ran byte-identical main bundles. Values below are primary minus control, pairing corresponding Object names and subtracting their warm client medians. They are different physical Objects, so these are deployment-control spreads, not same-Object interventions.

| History | Programmed TTFT ms | Client difference ms [Q1–Q3] | Full Object range ms |
|---:|---:|---:|---:|
| 50 | 0 | 542 [−69–643] | −438 to +2,195 |
| 50 | 400 | 562 [−293–1,126] | −996 to +1,522 |
| 250 | 0 | −211 [−360–901] | −2,316 to +2,492 |
| 250 | 400 | 200 [−404–497] | −1,195 to +976 |

The sign changes and broad ranges prevent assigning smaller cross-Worker differences to framework code. Same-Object interventions use their own repeated-baseline comparison; minification additionally has an unchanged-code temporal control.

### Client and routing

**The physical client was in Palo Alto**, as confirmed by the user; its configured timezone was America/Los_Angeles. These are the observed response `CF-Ray` colos and provider/echo receipt colos. HTTP ingress is neither the physical client location nor verified DO placement. The network/egress path responsible for the changing ingress was not isolated.

| Run | HTTP ingress colos | Provider/echo colos |
|---|---|---|
| Durability calibration | MAD, MRS | DFW, LAX, SEA, SJC |
| Transport / alarm2 | MRS | No outbound provider |
| Primary 50 / TTFT 0 | CDG, CPH, MIA, MRS, SEA | DEN, DFW, LAX, SEA, SJC |
| Primary 50 / TTFT 400 | ATL, BOS, DFW, MIA, SEA, SJC | DEN, DFW, LAX, SEA, SJC |
| Primary 250 / TTFT 0 | ATL, PDX, SJC | DFW, LAX, SEA, SJC |
| Primary 250 / TTFT 400 | PDX, SJC | DEN, DFW, LAX, SEA, SJC |
| Control 50 / TTFT 0 | KIX, MAD, MRS, SEA | DEN, DFW, LAX, SEA, SJC |
| Control 50 / TTFT 400 | MRS, SEA, SJC | DFW, LAX, SJC |
| Control 250 / TTFT 0 | DFW, PDX, SEA, SJC | DEN, DFW, SEA, SJC |
| Control 250 / TTFT 400 | PDX, SJC | DEN, DFW, LAX, SEA, SJC |
| Wake/sync 50 | CDG, CPH, MRS, SEA | DFW, SEA, SJC |
| Wake/sync 250 | ATL, PDX, SJC | DFW, LAX, SEA, SJC |
| Tardie observation 50 | SJC | DEN, DFW, SEA |
| Tardie observation 250 | SJC | DFW, LAX, SEA, SJC |
| Minification comparison | PDX, SJC | DFW, LAX, SEA, SJC |

Main rows include m0–m5; intervention rows contain comparison turns. All 42 Tardie on/off pairs share HTTP colo SJC, and provider-colo distributions match between arms. The five main-run start/end clock probes reached ATL and PDX respectively; their RTTs were 72–547 and 27–247 ms. They are diagnostic samples, not a universal clock correction or a stable network floor. Every request's `cfRay` and every provider receipt's `colo` remain in the raw evidence.

### Failures and limits reached

The final capture contains **73,697 distinct Worker-scoped telemetry events** and **45,050 invocation records**, including the post-deletion tail. Event IDs are qualified by Worker name: 26 ID collisions across Workers must not collapse distinct events. Invocation outcomes are:

| Observed outcome | Invocations |
|---|---:|
| ok | 39,096 |
| canceled | 5,278 |
| aborted | 637 |
| exception | 35 |
| exceededMemory | 4 |
| exceededCpu | 0 observed |

The four memory-limit records describe **two Tardie/250 seed incidents**, each with a fetch and alarm record. Both fixtures were replaced; neither is a failed final measured turn. All 672 main, 112 Yielded variant/settling, 112 Tardie observation/warmup and 140 minification/prelude turns returned successfully. Every final comparison's nine provider reference-projection hashes matched. The 17 fingerprint failures in the global analysis belong to the excluded second pilot, before the SSE framing correction.

The **54 failed controller requests** are confined to readiness, setup/pilots and three seed attempts. Of the 637 aborts, 503 are explicitly `/cold` fetches; the remaining aborts are not all individually established as intentional resets. Cancellation/abort invocation counts are not failed-turn counts. Every observed non-ok invocation and failed controller request is retained in `failed-outcomes.json.gz`; all join exclusions and missing CPU/wall observations remain in the analysis. A transient telemetry-query HTTP 504 was retried as a read-only collection operation. The two minification preparation stops are also retained, without canonical-input retries. Platform sampling means this inventory is exhaustive for captured evidence, not a claim that Cloudflare retained every invocation.

## Durability calibration

Calibration values are milliseconds, median [Q1–Q3] across eight Object medians; the brackets describe the interquartile range, not a confidence interval. Each Object ran every case five times in deterministic shuffled order: 2,880 successful requests. Positive payload bytes are the total per request, divided across 1, 4 or 12 `transactionSync` calls. Zero bytes performs no SQL mutation or transaction. The small echo Worker was reached over its public HTTPS URL. Source-log/request-ID joins recover 2,782 of 2,880 DO invocations; 98 remain missing. All controller and application-clock observations are present, while CPU/wall summaries use only available invocation telemetry.

| Total payload | Observed explicit-sync interval, one transaction | Extra outbound-fetch interval after writing | Extra client latency, write then return | Extra DO RPC wall, write then return |
|---|---:|---:|---:|---:|
| 0 KiB | 0.0 [0.0–0.0] | baseline | baseline | baseline |
| 1 KiB | 37.0 [34.5–40.8] | 36.2 [33.0–41.5] | 31.1 [25.8–40.8] | 35.0 [34.0–40.5] |
| 16 KiB | 37.0 [34.8–40.0] | 36.5 [33.5–40.5] | 35.7 [33.9–42.3] | 37.8 [33.8–46.5] |
| 64 KiB | 37.5 [34.8–42.0] | 39.0 [38.2–42.2] | 37.1 [33.8–48.0] | 42.0 [35.4–45.9] |
| 128 KiB | 41.0 [38.2–44.0] | 40.0 [36.0–44.2] | 40.4 [36.4–48.1] | 39.5 [37.0–49.8] |
| 512 KiB | 52.5 [51.2–58.8] | 55.5 [52.8–59.5] | 56.7 [50.9–68.2] | 57.0 [52.8–64.4] |

Deltas pair each positive-byte request with its zero-write control by Object, repeat, mode and transaction count, requiring matching ingress/echo colos. Explicit-sync and outbound-fetch intervals use the Object’s I/O clock; they are not exact CPU-independent wall durations. The no-write fetch interval is 7 [6–9.25] ms. Added fetch intervals closely track explicit `storage.sync()` intervals, and controller latency differences have the same order of magnitude. This is consistent with write-associated output-gate blocking, but does not isolate a physical durability round trip. Cloudflare documents that output gates hold outgoing messages until writes are durable and that `sync()` waits for outstanding durability work. [SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)

| Transactions, fixed 16 KiB total | Observed interval: one sync at end | Observed summed intervals: sync after each |
|---:|---:|---:|
| 1 | 37.0 [34.8–40.0] | 36.5 [34.8–41.0] |
| 4 | 37.5 [35.2–42.2] | 146.5 [137.8–160.0] |
| 12 | 38.5 [36.2–41.8] | 434.0 [399.8–478.2] |

In this fixed-total-byte probe, observed intervals grow approximately with **forced serial waits**, while increasing synchronous SQL transaction count before one final sync has little effect. Twelve write/sync pairs have roughly twelve times the observed sync interval of one pair; this does not identify twelve physical flushes. Payload-size growth is much weaker: 1–16 KiB have the same median, and 512 KiB is about 1.4× the 1 KiB interval despite 512× the payload. The native adapters use asynchronous transaction APIs too; their API-call counts are not observed physical flush counts.

## Why the previous non-CPU residual was not all durability

A separate transport check ran the same minimal operation through native DO RPC and DO `fetch()` in the same eight stable-incarnation Objects, with five paired repeats at zero and 128 KiB. It had 160 successful requests and 152 joined DO invocations; eight invocation records remain missing. Each size has 40 controller pairs and 36 telemetry pairs across all eight Objects. Values are median [Q1–Q3] across Object medians, RPC minus fetch:

| Payload | Client latency difference | DO invocation-wall difference |
|---|---:|---:|
| Zero | −4.9 [−13.7–−1.6] | +132.2 [125.8–155.2] |
| 128 KiB | +5.7 [4.4–8.6] | +141.5 [131.8–155.6] |

At zero writes, RPC invocation wall is 132 [125.75–155.25] ms and DO fetch invocation wall is 0 [0–0.25] ms. Their paired CPU difference is 0 [0–0] ms at reported millisecond precision, and the client difference is small beside the invocation-wall difference. The large wall discrepancy reflects different measurement lifetimes; it is not a corresponding amount of recoverable user latency. The earlier in-process-model harness used RPC, so its previously reported roughly 180 ms `wall − CPU` residual cannot be labeled durability wait. That historical scalar is not a new cohort estimate, and this experiment supplies no exact numerical correction for it. The native comparison uses DO fetch for all three targets; Tardie also performs its native Actor RPCs.

The Object’s `Date.now()` advances at I/O boundaries and can stay fixed during substantial execution. The observed post-alarm timer timestamps do not recover all elapsed CPU time. Small probe CPU does not establish exact application-clock accuracy. Only Cloudflare invocation telemetry supplies CPU and invocation wall here; controller elapsed time is measured separately. Export timestamps are not assumed to mark invocation start or end. Invocation lifetimes and alarm CPU must not be added into a purported exact partition of client latency. [Workers timers](https://developers.cloudflare.com/workers/runtime-apis/performance/), [invocation lifetime](https://developers.cloudflare.com/logs/logpush/logpush-job/datasets/account/workers_trace_events/#walltimems)

## Alarm interference calibration

Eight Objects each ran four shuffled repeats of three alarm CPU loads (0, 20 million and 80 million integer-loop iterations) and three configured controller delays after the arm response (0, 110 and 230 ms): 288 successful arm/probe pairs. Each alarm was scheduled at DO `Date.now() + 200 ms`; this does not establish its actual start relative to the probe. Deltas pair the loaded alarm with the zero-iteration alarm by Object, repeat and controller delay, requiring matching ingress colos. The zero-iteration alarm still performs its timer and scheduling work. These are scheduled-CPU interference measurements, not proof of exact overlap from frozen clocks.

All six comparisons below use median [Q1–Q3] ms across eight Object medians. Client and outer-wall columns are paired differences; alarm CPU is the absolute loaded-alarm statistic, not a paired difference or a critical-path allocation.

| Loop iterations | Controller delay | Added client latency | Added outer Worker wall | Alarm CPU |
|---|---:|---:|---:|---:|
| 20M | 0 | 4.721 [−1.787–132.470] | 6 [0.625–133.5] | 42.5 [34.875–354.875] |
| 20M | 110 | 0.582 [−3.476–57.270] | 3.5 [0.5–62.25] | 39.75 [35–362.625] |
| 20M | 230 | 3.659 [0.095–38.372] | 2.5 [0.375–29.75] | 39.25 [34–361.625] |
| 80M | 0 | 71.122 [−3.681–1346.917] | 3 [−0.875–1335] | 141.25 [124.25–1503] |
| 80M | 110 | 9.692 [−2.287–1240.266] | 7 [1.125–1220] | 149.25 [133.375–1484.375] |
| 80M | 230 | 10.167 [3.578–1151.196] | 2.75 [1–1112.25] | 147.25 [131–1522.25] |

The 80M loop’s Object-level CPU medians range from 117 to 2,569 ms across the delay cells, so iteration count is not a portable CPU-duration specification. Three stable-incarnation Objects show large effects at delay zero. Here the brackets summarize repetitions within the named Object, rather than medians across Objects:

| Object suffix | Added client latency | Added outer Worker wall | Alarm CPU | Added probe RPC wall |
|---|---:|---:|---:|---:|
| 3 | 1930.195 [1913.581–2033.441] | 1911 [1903.5–2111] | 1983 [1953.75–2077.25] | 27.5 [25.5–30.5] |
| 5 | 1152.491 [1134.932–1280.589] | 1143 [1129–1272.75] | 1343 [1313.75–1459] | 32 [27–37.5] |
| 6 | 2520.386 [2377.996–2657.846] | 2518 [2375–2661.25] | 2569 [2498–2602.5] | 29.5 [28.5–30] |

Each individual cell has four observations except Object 3 outer wall and Object 6 alarm CPU, which have three. Across the full alarm experiment, telemetry joins cover 275/288 probe RPCs, 277/288 outer invocations and 268/288 alarms; all joined alarm outcomes are `ok`. Each comparison has 32 controller pairs, but missing telemetry changes the contributing repetitions by metric. Four other Objects changed incarnation during the matrix; the three highlighted Objects did not. The earlier partial `alarm-*` pilot is excluded.

Client and outer-wall effects greatly exceed the reported probe RPC-wall effects for these three Objects. This is consistent with interference outside the reported probe lifetime, without identifying exact queueing or overlap intervals. Full distributions and coverage are in `summary.json.gz` under `probe.alarms`. The experiment does not price a native framework alarm or establish native optimization savings.

## Method and durability boundaries

The initial framework source baseline is `8c05714de84d68961b14e5ab7a3b7d809599563f`; the native primary/control deployment build is `b942186159f7e5ead9cf1f35f67dc55c2de10338` (see provenance below). The harness uses native provider adapters: Yielded’s upstream `@effect/ai-openai-compat` Layer, Pi’s `@earendil-works/pi-ai` OpenAI-compatible provider, and Tardie’s `@tardie/ai-openai-compat` through `liveModelServices`. No framework-owned provider replaces them. The stateless mock emits OpenAI-compatible SSE, including fragmented tool arguments and usage.

The fixed workload is `src/plan.ts`: 50 or 250 historical turns seeded in place, compaction off, then nine model requests and eight readonly `lookup` results per measured turn. Results are 256 bytes except every 97th lookup, which is 8 KiB. The native adapters retain their original storage and recovery paths. Provider receipts verify every call against the reference transcript, with seed fingerprints `b017b487524e44a4` and `dcea9f30b0917245`. The projection is the durable-bench role/text/tool-call transcript; it removes system/developer entries and decodes native Effect JSON-string tool-result encoding once. All targets use the shared system text. Raw HTTP fingerprints are retained separately because native wire encodings differ.

Seven Objects per role/history/delay form the planned comparison: Yielded, Pi and Tardie in one Worker plus an identical-byte Yielded Worker as the deployment control. Every Object uses the `wnam` location hint. This is a placement hint, not verified physical co-location. Each cohort runs m0 cold, m1 settling and m2–m5 warm. Summaries first take each Object’s median, then median [Q1–Q3] across Objects. The eight gaps within a turn are not eight independent Objects.

Cold means a **fresh Object runtime incarnation with its existing durable history**, forced by a successful storage sync and an explicitly acknowledged `ctx.abort()`, immediately before m0. Tardie’s Thread and Actor are both reset. Before/after Object IDs, incarnation IDs, first-entry receipts and lifetime alarm counters verify this; available constructor telemetry can veto contradictory cold evidence. It is not guaranteed to be a fresh isolate, virgin JIT, or uncached bundle. Fully completed runtimes are released the same way outside every measured interval; their canonical histories remain intact.

The zero-delay setting has no programmed delays. The realistic setting configures 400 ms to the first frame and 10 ms between frames: 9×400 + 53×10 = 4,130 ms of scripted provider delay per turn. The primary adjusted metric is **client latency minus scripted provider time**: controller elapsed time minus 0 or 4,130 ms, calculated per turn before Object-level reduction. This is not isolated framework overhead; it retains network, scheduling, runtime, persistence, provider processing and timer-overshoot effects.

Observed provider I/O intervals are a separate diagnostic: the sum of each receipt’s `endMs − arrivalMs`. Provider clocks also advance only at I/O boundaries, so these intervals can omit CPU spent handling bodies, encoding frames or hashing receipts. Subtracting them does not remove all provider processing and is not the primary adjusted metric. First-byte/end timestamps mark server stream enqueue boundaries, not network delivery. Provider timing and fingerprint receipts travel in an SSE comment immediately before `[DONE]`; all three unmodified native SSE parsers ignore comments. Receipt hashing happens after the first enqueue and before the terminal frame, but its CPU duration is not guaranteed to appear in the I/O-clock interval.

Warm Yielded zero-delay cohorts additionally run two settling turns, then two randomized repeats each of baseline, existing `withWakesDeferred`, and an explicit `storage.sync()` immediately before each model fetch. The settling turns keep the intentionally large lookup #97 outside the 50-turn variant comparison. These are throwaway experiments. Wake deferral retains required pre-arming, producer fencing, claims/leases and durable generation checks; it suppresses only existing droppable promptness hints. [Alarm contract](../../../../packages/platform-cloudflare/src/Alarm.ts)

The probes and counters do not remove canonical facts, batch hash chains, accounting, `historyDigest`, original-context recovery, or Unknown outcomes. Allowed work includes batching semantically compatible records atomically and reducing disposable projection work. Sending a dependent model/tool request before required facts are durable, `allowUnconfirmed` dispatch, removing fencing or lease checks, overwriting canonical history, and automatically replaying an unresolved ordinary tool call are not admissible latency optimizations. [Runtime model](../../../../docs/src/content/docs/concepts/runtime-model.md)

Byte counters measure submitted SQL binding bytes and JSON-sized KV arguments, including predicates and repeated submissions. They are not physical WAL or replication bytes. Cold request totals include the separately retained constructor snapshots once; per-step counters exclude that initialization bucket. Transaction counters distinguish native API calls from fulfilled calls classified as mutation-bearing; no-op calls may still be classified as mutations, explicit rollbacks are excluded, and overlap/window-crossing diagnostics qualify attribution. They do not count distinct replication flushes. Native sync intervals can include stale clock boundaries and overlap; their sums are not an additive latency partition. The explicit pre-fetch sync is also included in native sync counters and must never be added a second time.

## Evidence limits and operations

All latency evidence comes from deployed Cloudflare resources created through the included Alchemy stack, with `bundle: false`, exact upload verification, CPU limits of 300,000 ms, invocation logs and head sampling 1. Local tools only build, reduce captured data and run repository correctness checks. The combined network bundle contains all three targets; its minification experiment does not price savings for a standalone Yielded application. Counters, SSE receipts and JSON response encoding are included in client latency; their cost was not independently removed. Tardie's on/off observation comparison did not resolve a stable correction. `clientReceiptBytes` records the returned diagnostic payload size.

Timed requests are serial, but native background alarms remain enabled. Completed runtimes are explicitly released; an alarm can wake them again. Failed seed fixtures also remain in the disposable namespaces until cleanup. The identical-code control has Yielded cohorts, while the primary Worker hosts all three targets. The control therefore exposes deployment and workload-environment variation rather than isolating a hardware-only multiplier.

Cloudflare still sampled some logs at ingestion. Its telemetry API describes `sampleInterval` as including both configured head sampling and platform sampling, independently of `statistics.abr_level`. Re-querying cannot reconstruct discarded events. Returned provider/alarm receipts provide reliable identities; missing platform CPU/wall joins stay missing. Observed CPU sums may omit sampled descendant RPC events, especially for Tardie. They are not complete per-turn CPU totals or exact critical-path CPU. [Telemetry API](https://developers.cloudflare.com/api/resources/workers/subresources/observability/subresources/telemetry/methods/query/)

Client-to-first-provider differences cross controller and provider clocks and are not established latency components. Echo requests before and after measurement provide diagnostic offset observations; interpreting them as bounds requires the sampled Worker clock to track wall time during that request. They do not bound other Worker/DO invocation clocks or justify a universal offset correction. Provider-to-provider step gaps retain their colos and clock limitations: equal colo does not prove a shared clock. Observed routing differs across this run and the earlier reports; that does not establish a change in the client's physical location. Absolute client totals from those runs should not be pooled.

Setup failures and excluded pilots are retained, including the initial native-state wrapper/transport/framing errors, one connection-reset seed attempt and two memory-limit failed seed requests. Three Tardie cohort identities were retired: `cf-latency-net-h250-d0-o0`, `cf-latency-net-h250-d400-o4`, and its first replacement `cf-latency-net-h250-d400-o4-fresh1`. The connection reset left an unknown seed outcome; the two memory-limit failures occurred during 250-turn seeding. Failed or uncertain seed/turn requests are not replayed. Fresh replacement Object identities, reasons and prior plans are retained in `network-plan.json` and its archived predecessors. Intentional cold/release aborts are identified separately from failed workload turns.

The account was **Danieljmerwe@gmail.com's Account**. Every Worker/namespace/stack was prefixed `cf-latency`. Credentials came from this checkout’s direnv; Alchemy state and auth stayed in a private mode-700 temporary directory outside the repository. **Cleanup was verified at `2026-10-08T19:44:25.619Z`:** all five Worker lookups returned 404, the same account's complete listings contained no `cf-latency` Workers or Durable Object namespaces, and the private state directory was removed. [Cleanup receipt](cleanup.json). The final credential scan covered raw and decompressed artifacts plus all deployment/destroy logs; it found none of the account ID, API token or private benchmark token. [Scan receipt](secret-scan.json).

## Provenance and reproduction

[Deployment records](resources.json) retain deployed build identities and superseded probe builds; [the main-run snapshot](resources-main.json) preserves the pre-follow-up deployment metadata. Bundle bytes below are uncompressed; every listed gzip archive was decompressed and checked against its SHA-256 and byte count.

| Worker / phase | Deployment build commit | Bytes | Bundle SHA-256 |
|---|---|---:|---|
| Echo | `8c05714de84d68961b14e5ab7a3b7d809599563f` | 528 | `dad2beee9ff1ccc675fde94a3b6567821f0ef1ebecb227f6dab7491401fe3dfb` |
| Probe: calibration | `8c05714de84d68961b14e5ab7a3b7d809599563f` | 5,809 | `fdc6ede068179c6e4caa76b0396db4e815c3f9f1b3587ca21ef6d149bceae7f3` |
| Probe: alarm2 | `8c05714de84d68961b14e5ab7a3b7d809599563f` | 5,966 | `d6ce7851bc13d72cf13771b9eafafc2af1e2597ce6037726e576932be5a8b584` |
| Probe: transport | `8c05714de84d68961b14e5ab7a3b7d809599563f` | 6,624 | `fc8f7b6b94124d2364ef8901c9dd01f4796c7f268f3c5ac848ca27ead02d8f03` |
| Provider | `8c05714de84d68961b14e5ab7a3b7d809599563f` | 558,857 | `4215666ec7434e47135c4755d19880e3853e64210371552e7c614725f20c0597` |
| Primary | `b942186159f7e5ead9cf1f35f67dc55c2de10338` | 7,298,444 | `ab54bf5d40afdc1c69d896dd2d2e7e7fd82a8ed5c516160829c4fe51838e76db` |
| Control | `b942186159f7e5ead9cf1f35f67dc55c2de10338` | 7,298,444 | `ab54bf5d40afdc1c69d896dd2d2e7e7fd82a8ed5c516160829c4fe51838e76db` |
| Primary/control: observation and plain minification rounds | `7a5bec91a32e3747151bd5c0457f92defe1eb724` | 7,298,737 | `4dc513fb84b6830b5476ecb5de36970cd311f6e645950c79932b51526a438050` |
| Primary: minified rounds | `7a5bec91a32e3747151bd5c0457f92defe1eb724` | 3,319,078 | `922ff360d0cc12df976e8aa277c27cb6f94870f4acb6c4e341ef78df72f5931b` |

All builds record fixture SHA-256 `e740ba932fdcd3253d84f1bcfd1f20693a3bf0116a0b29f7d53e66b86eed442f`: the hash of the ordered path/hash manifest for `src/plan.ts`, `src/yielded.ts`, `third-party/src/pi.ts` and `third-party/src/tardie.ts`. It identifies those baseline fixtures, not the whole network harness. Each bundle also has a compiled-input hash manifest in `build-identities/<name>-<sha>-inputs.json.gz` and exact module bytes in `<name>-<sha>.mjs.gz` (main/plain primary and control use `network`; minified uses `network-min`). Large result files are gzip archives with byte-identical decompression checked in [artifact-archives.json](artifact-archives.json).

[build.mjs](build.mjs) records HEAD without dirty status. Early pilot commit labels may therefore describe modified trees; exact archived bytes are stronger provenance than HEAD alone. Input manifests retain hashes, not source snapshots. [all.json](build-identities/all.json) is the latest-build index, not deployment history: later echo/probe rebuilds at `8f036fcc003a8791d6c4ebc6e43d7c83bd8d6bae` and provider at `b942186159f7e5ead9cf1f35f67dc55c2de10338` produced the same respective hashes above.

Recorded builds use esbuild 0.28.1, root Effect 4.0.0 and ES2024; only the named minified rounds enable minification. Tardie retains its separate pinned Effect 4.0.0-rc.115 and compat provider 4.0.0-rc.113-clavia.7. [stack.ts](stack.ts) uploads prebuilt modules unchanged, with compatibility date `2026-08-18` and `nodejs_compat`. Reproduction needs the repository Vite+/Node/Bun toolchain, root dependencies, the separate third-party installation, direnv and authorized Cloudflare account/token environment. Third-party direct pins are Chord/Pi 1.0.4 and Tardie 0.44.0; retain both lockfiles.

For a fresh experiment, use the deployed runtime commit in an unused worktree and restore the final controller/reducer from this branch. Configure its authorized environment independently. Do not copy archived resources, plans, requests, completion files or private state into it. `init` refuses an existing local run and any account Worker/namespace with the `cf-latency` prefix; wait for verified cleanup before reusing the account.

```sh
git worktree add --detach ../effect-agent-cf-latency-repro b942186159f7e5ead9cf1f35f67dc55c2de10338
cd ../effect-agent-cf-latency-repro
cf_result=examples/durable-bench/results/cf-latency
git restore --source=dan/cf-latency-breakdown -- "$cf_result/run.mjs" "$cf_result/analyze.mjs" "$cf_result/analyze-probe.mjs" "$cf_result/analyze-instrumentation.mjs" "$cf_result/analyze-minification.mjs" "$cf_result/validation-env.cjs"
vp install --frozen-lockfile
vp run -F @yielded/agent-example-durable-bench vendor
cf() { vp run -F @yielded/agent-example-durable-bench cf-latency "$@"; }
cf init
vp run -F @yielded/agent-example-durable-bench cf-latency:build
cf deploy-probe
cf calibrate
cf alarms2
cf transport
cf deploy-network
cf seed
cf activate
cf measure
cf telemetry
# The follow-up uses its separately recorded source and build flags.
git restore --source=7a5bec91a32e3747151bd5c0457f92defe1eb724 -- "$cf_result/build.mjs" "$cf_result/stack.ts" "$cf_result/network"
vp run -F @yielded/agent-example-durable-bench cf-latency:build network network-min
cf refresh-network-measure
cf instrumentation
cf minification
cf audit-minification-metadata
cf telemetry
vp run -F @yielded/agent-example-durable-bench cf-latency:analyze
# After retaining the evidence, clean up this new run only:
cf secret-scan
cf cleanup
```

This sequence calibrates with the final probe. Reproducing the historical Part1 deployments requires the archived calibration → alarm2 → transport modules and matching build identities, with a probe refresh between phases; rebuilding current source is not equivalent. The runner stops on failed or uncertain seeding, and never replays a measured turn. A failed fixture requires a fresh named Object and retained replacement lineage, as in this run. `minification-resume` admits only completed inputs with unique acknowledged receipts/releases and entirely unattempted remaining inputs; it cannot replay an ambiguous turn. Always execute cleanup even when measurement fails. [analyze.mjs](analyze.mjs) reduces captured evidence offline and accepts `.gz` fallbacks for its JSON/JSONL inputs. Run it in a separate evidence copy when preserving an existing `summary.json.gz`.

[validation.json](validation.json) records the final full repository correctness gate passing with exit code 0 at `2026-10-08T19:44:07.120Z`: `NODE_OPTIONS=--require=<repository>/examples/durable-bench/results/cf-latency/validation-env.cjs vp run ready` (51/71 task cache hits). The preload sets `VITEST_MAX_WORKERS=1` only in `packages/testing`; Cloudflare pool settings, tests, assertions and timeouts are unchanged. This covers static checks, existing tests and build, not local latency evidence. Generated result files are outside the repository static-check scope; their proof is the exact deployed bytes, returned identity/transcript receipts, syntax checks and captured-data reduction. Earlier failed/interrupted attempts, the previous full pass and the focused passing check remain recorded. [Artifact integrity](artifact-integrity.json) verifies all 22 archived modules and the recorded deployment/input/fixture identities; cleanup has its separate operational receipt.
