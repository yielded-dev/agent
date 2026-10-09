# Receipt, pass and settlement timing

**No universal 50 ms post-receipt wait was observed.** Immediate pre-arming did not establish a driver saving in the primary comparison. The calibration below supplies conditional before/after bounds for both ends; it is a separate attribution pass, not additional evidence of a performance win.

The original 128-turn diagnostic pass had 96 warm turns. None had valid provider/driver bounds: 95 had a provider/probe colo mismatch and one had inconsistent echo intervals. Those observations remain in `diagnostic/`; their unresolved intervals are not treated as zero.

The follow-up continued on the same four physical Objects at m32–m46, without reseeding or replaying a turn. Each Object had one verified cold turn, two settling turns, then two randomized rounds of all six labels. Baseline pools production and repeat. Warm/settling clock echoes went through the already-open Object to the provider; the cold turn retained direct echoes so it did not pre-open the Object. All 60 turns completed with matching transcripts and alarm-only model dispatch. All 48 warm turns have matching-colo, consistent echo bounds.

Each interval below is the median lower bound … median upper bound, in milliseconds, over one Object (four baseline or two candidate samples). The pass column bounds **beacon arrival**, an upper bound on pass entry after output gating and network transport. It cannot give an exact start time. First-model means provider request arrival; tail starts at final provider stream completion. The echoes bracket a stable provider/driver clock offset, conditional on that offset remaining stable within the turn. They are not proof of synchronized clocks. Integer timestamps have 1 ms resolution; fractional medians do not add precision. Negative values are retained.

| Seed / provider | Variant | Admission median | Receipt → pass beacon | Receipt → first model request | Last model output → client |
| --- | --- | ---: | ---: | ---: | ---: |
| 50 / instant | baseline | 369 | −18.5…10.5 | 0…32 | 100…132 |
| 50 / instant | prearm | 347.5 | −11.5…8.5 | 13…33 | 87.5…107.5 |
| 50 / instant | settlement | 383.5 | −19.5…6 | −1…24.5 | 96.5…122 |
| 50 / instant | combined | 408.5 | −18…4.5 | 4…26.5 | 495…517.5 |
| 50 / instant | views | 365.5 | −16.5…9.5 | 9…35 | 110.5…136.5 |
| 50 / 400 ms | baseline | 264 | −49.5…5 | −15.5…39 | 71…124 |
| 50 / 400 ms | prearm | 227.5 | −51.5…1.5 | −18.5…34.5 | 67.5…120.5 |
| 50 / 400 ms | settlement | 362 | −54…1 | −24…31 | 72.5…127.5 |
| 50 / 400 ms | combined | 225 | −50.5…4.5 | −18.5…36.5 | 70.5…125.5 |
| 50 / 400 ms | views | 250.5 | −45.5…8.5 | −14.5…39.5 | 68.5…122.5 |
| 250 / instant | baseline | 286 | −16.5…4.5 | 19…38 | 79…99 |
| 250 / instant | prearm | 307 | −10…7 | 20…37 | 74.5…91.5 |
| 250 / instant | settlement | 357.5 | −18…3.5 | 11.5…33 | 79.5…101 |
| 250 / instant | combined | 284 | 2.5…22 | 20…39.5 | 81…100.5 |
| 250 / instant | views | 299 | −14.5…6.5 | 17.5…38.5 | 73.5…94.5 |
| 250 / 400 ms | baseline | 308 | −46…6.5 | −21.5…31 | 60…112.5 |
| 250 / 400 ms | prearm | 309 | −50…5.5 | −31…24.5 | 60…115.5 |
| 250 / 400 ms | settlement | 311 | −46…5.5 | −25…26.5 | 91.5…143 |
| 250 / 400 ms | combined | 297 | −47.5…5.5 | −27…26 | 62.5…115.5 |
| 250 / 400 ms | views | 204 | −51…5.5 | 63…119.5 | 59…115.5 |

These additional beacons and Object-routed echoes change the observation workload. Do not subtract these times from the unprobed primary driver results or use them to claim a small gain. The large 50/instant combined tail includes the successful 2,090 ms m37 turn and is retained without trimming.

Three warm alarm-exit beacons have an upper bound before client settlement, and one has a lower bound after client settlement. The latter only locates the beacon arrival: it does not prove the alarm itself outlived settlement. The remaining intervals straddle client delivery. All admission-entry snapshots had no active previous alarm; metrics and probes between inputs mean this does not measure an immediate-next-submit input-gate queue.

Raw timestamps, bounds, markers, SQL/transaction snapshots and clock status are in `timing/turns.jsonl.gz`; schedule and transcript references are in [timing-plan.json](timing-plan.json). The primary no-beacon comparison, including narrower valid instantaneous-provider bounds, remains in `candidates/turns.jsonl.gz` and the [report](report.md). Recompute with `vp exec node --experimental-transform-types examples/durable-bench/results/warm-floor/analyze.mjs --phase timing --out-dir examples/durable-bench/results/warm-floor/timing`.
