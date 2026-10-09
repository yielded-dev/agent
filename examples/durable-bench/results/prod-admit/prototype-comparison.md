# First prototype comparison

All 336 planned turns completed; all 3,024 model request fingerprints matched. Each Yielded Object has four baseline and two candidate cold requests, and eight baseline and four candidate warm requests. CPU is submit-invocation attribution.

The noise threshold is the entire pooled baseline repeat range, within the same Object and state. A smaller difference is inconclusive. No baseline outlier is discarded. All five prearm-only gains also exceed the candidate’s own range. The two prearm-now differences above the baseline range do not exceed the candidate range and establish no gain.

## prearm-only

| History / delay / Object | State | Baseline → candidate (ms) | Saved (ms) | Baseline / candidate ranges (ms) | Above baseline range? | CPU baseline → candidate (ms) |
| --- | --- | ---: | ---: | ---: | --- | ---: |
| h50-d0-o0 | cold | 600 → 405 | 195 | 134 / 8 | Yes | 69 → 47.5 |
| h50-d0-o0 | warm | 253.5 → 56.5 | 197 | 114 / 23 | Yes | 13.5 → 10 |
| h50-d0-o1 | cold | 627.5 → 642.5 | -15 | 183 / 163 | No | 27 → 60.5 |
| h50-d0-o1 | warm | 301.5 → 275 | 26.5 | 66 / 54 | No | 6.5 → 7.5 |
| h50-d400-o0 | cold | 601.5 → 415 | 186.5 | 4003 / 64 | No | 30 → 32.5 |
| h50-d400-o0 | warm | 201 → 192.5 | 8.5 | 196 / 33 | No | 7.5 → 6.5 |
| h50-d400-o1 | cold | 593.5 → 401.5 | 192 | 58 / 31 | Yes | 60.5 → 52.5 |
| h50-d400-o1 | warm | 245.5 → 54.5 | 191 | 182 / 29 | Yes | 13 → 11 |
| h250-d0-o0 | cold | 674.5 → 462.5 | 212 | 284 / 143 | No | 50 → 44.5 |
| h250-d0-o0 | warm | 439.5 → 69 | 370.5 | 319 / 22 | Yes | 19 → 19 |
| h250-d0-o1 | cold | 885.5 → 824 | 61.5 | 135 / 142 | No | 23 → 25.5 |
| h250-d0-o1 | warm | 291.5 → 301 | -9.5 | 160 / 75 | No | 8.5 → 8 |
| h250-d400-o0 | cold | 477 → 387.5 | 89.5 | 187 / 179 | No | 35 → 23 |
| h250-d400-o0 | warm | 275 → 116.5 | 158.5 | 260 / 85 | No | 8 → 7 (missing 0/1) |
| h250-d400-o1 | cold | 758 → 787 | -29 | 80 / 104 | No | 50 → 45 |
| h250-d400-o1 | warm | 406 → 351 | 55 | 208 / 112 | No | 16 → 15 |

## prearm-now

| History / delay / Object | State | Baseline → candidate (ms) | Saved (ms) | Baseline / candidate ranges (ms) | Above baseline range? | CPU baseline → candidate (ms) |
| --- | --- | ---: | ---: | ---: | --- | ---: |
| h50-d0-o0 | cold | 600 → 446.5 | 153.5 | 134 / 181 | Yes | 69 → 51 |
| h50-d0-o0 | warm | 253.5 → 221 | 32.5 | 114 / 209 | No | 13.5 → 11 |
| h50-d0-o1 | cold | 627.5 → 513 | 114.5 | 183 / 110 | No | 27 → 47.5 |
| h50-d0-o1 | warm | 301.5 → 263 | 38.5 | 66 / 119 | No | 6.5 → 7 |
| h50-d400-o0 | cold | 601.5 → 410 | 191.5 | 4003 / 46 | No | 30 → 40 |
| h50-d400-o0 | warm | 201 → 174 | 27 | 196 / 63 | No | 7.5 → 7 |
| h50-d400-o1 | cold | 593.5 → 366 | 227.5 | 58 / 280 | Yes | 60.5 → 69.5 |
| h50-d400-o1 | warm | 245.5 → 206 | 39.5 | 182 / 256 | No | 13 → 13.5 |
| h250-d0-o0 | cold | 674.5 → 532 | 142.5 | 284 / 326 | No | 50 → 54 |
| h250-d0-o0 | warm | 439.5 → 333.5 | 106 | 319 / 72 | No | 19 → 15 |
| h250-d0-o1 | cold | 885.5 → 791.5 | 94 | 135 / 15 | No | 23 → 32 |
| h250-d0-o1 | warm | 291.5 → 300 | -8.5 | 160 / 178 | No | 8.5 → 8 |
| h250-d400-o0 | cold | 477 → 495.5 | -18.5 | 187 / 19 | No | 35 → 37 |
| h250-d400-o0 | warm | 275 → 272.5 | 2.5 | 260 / 72 | No | 8 → 7.5 |
| h250-d400-o1 | cold | 758 → 761.5 | -3.5 | 80 / 145 | No | 50 → 75 |
| h250-d400-o1 | warm | 406 → 382 | 24 | 208 / 416 | No | 16 → 16 |

Native endpoint-return snapshots: baseline A/B always 4 transactions and 2 alarm writes; prearm-only always 3/1; prearm-now always 4/1. No normal endpoint snapshot includes an alarm start or provider call. Separate diagnostic samples are excluded from the comparisons above.

See [full reduction](candidates-admission.json), [turn rows](candidates-admission-turns.jsonl), [plan](candidates-plan.json) and [joined verification](candidates/summary.json).
