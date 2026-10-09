Driver-observed milliseconds; median [Q1–Q3] of Object medians.
Yielded/pi ranges use min(Y)/max(pi)–max(Y)/min(pi) of Object medians; descriptive, unpaired, not confidence intervals.

50 history · 0 ms TTFT · cold · baseline

| Target | Turn ms | Object median range | Repeat range, median / max | Objects |
|---|---:|---:|---:|---:|
| yielded | 1,625 [1,511–1,852] | 1,324–2,491 | 106 / 1,078 | 10 |
| pi | 1,003 [946–1,160] | 863–1,189 | 164 / 463 | 10 |

Yielded ÷ pi: **1.62×**; observed ratio range **1.11–2.89×** (10 Yielded / 10 pi Objects).

50 history · 0 ms TTFT · cold · candidate

| Target | Turn ms | Object median range | Repeat range, median / max | Objects |
|---|---:|---:|---:|---:|
| yielded | 1,589 [1,445–1,770] | 1,351–2,022 | 386 / 1,063 | 10 |
| pi | 1,206 [1,026–1,340] | 903–1,391 | 220 / 841 | 10 |

Yielded ÷ pi: **1.32×**; observed ratio range **0.97–2.24×** (10 Yielded / 10 pi Objects).

50 history · 0 ms TTFT · warm · baseline

| Target | Turn ms | Object median range | Repeat range, median / max | Objects |
|---|---:|---:|---:|---:|
| yielded | 902 [843–945] | 762–1,122 | 372 / 871 | 10 |
| pi | 679 [616–733] | 567–753 | 173 / 931 | 10 |

Yielded ÷ pi: **1.33×**; observed ratio range **1.01–1.98×** (10 Yielded / 10 pi Objects).

50 history · 0 ms TTFT · warm · candidate

| Target | Turn ms | Object median range | Repeat range, median / max | Objects |
|---|---:|---:|---:|---:|
| yielded | 863 [829–941] | 781–1,084 | 406 / 923 | 10 |
| pi | 675 [631–736] | 597–840 | 280 / 871 | 10 |

Yielded ÷ pi: **1.28×**; observed ratio range **0.93–1.82×** (10 Yielded / 10 pi Objects).

250 history · 0 ms TTFT · cold · baseline

| Target | Turn ms | Object median range | Repeat range, median / max | Objects |
|---|---:|---:|---:|---:|
| yielded | 1,969 [1,743–2,204] | 1,483–2,725 | 182 / 983 | 10 |
| pi | 1,152 [1,001–1,446] | 838–1,672 | 124 / 357 | 10 |

Yielded ÷ pi: **1.71×**; observed ratio range **0.89–3.25×** (10 Yielded / 10 pi Objects).

250 history · 0 ms TTFT · cold · candidate

| Target | Turn ms | Object median range | Repeat range, median / max | Objects |
|---|---:|---:|---:|---:|
| yielded | 1,886 [1,626–2,286] | 1,211–2,728 | 286 / 670 | 10 |
| pi | 1,155 [953–1,478] | 782–1,713 | 137 / 1,122 | 10 |

Yielded ÷ pi: **1.63×**; observed ratio range **0.71–3.49×** (10 Yielded / 10 pi Objects).

250 history · 0 ms TTFT · warm · baseline

| Target | Turn ms | Object median range | Repeat range, median / max | Objects |
|---|---:|---:|---:|---:|
| yielded | 1,092 [915–1,165] | 707–1,282 | 238 / 587 | 10 |
| pi | 739 [658–828] | 519–897 | 186 / 500 | 10 |

Yielded ÷ pi: **1.48×**; observed ratio range **0.79–2.47×** (10 Yielded / 10 pi Objects).

250 history · 0 ms TTFT · warm · candidate

| Target | Turn ms | Object median range | Repeat range, median / max | Objects |
|---|---:|---:|---:|---:|
| yielded | 1,067 [924–1,185] | 678–1,317 | 401 / 1,010 | 10 |
| pi | 745 [651–927] | 471–1,042 | 282 / 680 | 10 |

Yielded ÷ pi: **1.43×**; observed ratio range **0.65–2.80×** (10 Yielded / 10 pi Objects).

Same-Object Yielded A/B (candidate ÷ baseline; <1 is faster):

| Cell | Paired ratio, median | Baseline repeat drift, median / max |
|---|---:|---:|
| 50/0/cold | 0.978× | 6.6% / 55.2% |
| 50/0/warm | 0.963× | 10.5% / 23.0% |
| 250/0/cold | 0.891× | 8.8% / 76.2% |
| 250/0/warm | 1.002× | 5.3% / 19.0% |

Do not claim gains smaller than the repeat/control spread. Histories grow across the balanced build sequence.

Observed invocation CPU (telemetry may be sampled or delayed):

| Target / invocation | CPU ms | Observed | Non-ok outcomes |
|---|---:|---:|---:|
| unattributed/fetch | 1 [1–2] | 4998 | 161 |
| unattributed/alarm | 0 [0–0] | 672 | 645 |
| yielded/await | 297 [209–398] | 631 | 0 |
| pi/run | 350 [256–477] | 628 | 0 |
| yielded/submit | 10 [7–16] | 633 | 0 |
| yielded/alarm | 201 [126–297] | 630 | 0 |
| unattributed/jsrpc | 8 [6–15] | 5 | 0 |

Unmatched invocation markers: 11. CPU totals are not inferred from missing rows.

Failures: 0. Target cleanup: verified.
