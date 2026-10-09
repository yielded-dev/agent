# Complete candidate measurements

Milliseconds. Object medians first, then median across the two Objects. Baseline pools the identical production/repeat bodies. Positive difference means the candidate appeared faster; no difference cleared the predeclared repeat-spread threshold. Repeat spans below are medians of the full within-Object ranges; the acceptance rule is evaluated separately in every Object, not on these aggregate spans. [acceptance.json](acceptance.json) retains every Object.

| Cohort / candidate / seed / delay / state | Driver before → after | Before − after | Baseline / candidate repeat span | Admission before → after |
| --- | ---: | ---: | ---: | ---: |
| compare1 / layout / 50 / 0 / cold | 1318.5 → 1449.5 | -131 | 358.5 / 239 | 522.75 → 609.5 |
| compare1 / reconcile / 50 / 0 / cold | 1318.5 → 1375 | -56.5 | 358.5 / 310 | 522.75 → 525 |
| compare1 / layout / 50 / 0 / warm | 1003.75 → 978.75 | 25 | 422 / 168.5 | 210.25 → 210.75 |
| compare1 / reconcile / 50 / 0 / warm | 1003.75 → 1071.25 | -67.5 | 422 / 231.5 | 210.25 → 260 |
| compare1 / layout / 50 / 400 / cold | 5662.5 → 5479 | 183.5 | 438 / 282.5 | 577.75 → 489 |
| compare1 / reconcile / 50 / 400 / cold | 5662.5 → 5494 | 168.5 | 438 / 139 | 577.75 → 499.5 |
| compare1 / layout / 50 / 400 / warm | 5218.75 → 5210 | 8.75 | 450 / 267.5 | 248.25 → 283.75 |
| compare1 / reconcile / 50 / 400 / warm | 5218.75 → 5262.75 | -44 | 450 / 311 | 248.25 → 258.25 |
| compare1 / layout / 250 / 0 / cold | 1561.5 → 1473 | 88.5 | 397 / 576.5 | 584.5 → 569 |
| compare1 / reconcile / 250 / 0 / cold | 1561.5 → 1443.5 | 118 | 397 / 585.5 | 584.5 → 584.5 |
| compare1 / layout / 250 / 0 / warm | 1191.5 → 1172.75 | 18.75 | 719.5 / 187.5 | 333.75 → 343.5 |
| compare1 / reconcile / 250 / 0 / warm | 1191.5 → 1226 | -34.5 | 719.5 / 319.5 | 333.75 → 337 |
| compare1 / layout / 250 / 400 / cold | 5346.5 → 5333 | 13.5 | 528 / 929.5 | 529.5 → 548 |
| compare1 / reconcile / 250 / 400 / cold | 5346.5 → 5384 | -37.5 | 528 / 165 | 529.5 → 552.5 |
| compare1 / layout / 250 / 400 / warm | 5051.5 → 4995.5 | 56 | 336.5 / 197 | 261.75 → 260.5 |
| compare1 / reconcile / 250 / 400 / warm | 5051.5 → 5067.75 | -16.25 | 336.5 / 181 | 261.75 → 271.25 |
| compare2 / runtime / 50 / 0 / cold | 1636.5 → 1650.5 | -14 | 374.5 / 462.5 | 601.75 → 640 |
| compare2 / runtime / 50 / 0 / warm | 1158.5 → 1182.75 | -24.25 | 483 / 331.5 | 258.5 → 276 |
| compare2 / runtime / 50 / 400 / cold | 5515.75 → 5481 | 34.75 | 786.5 / 1187.5 | 517 → 556 |
| compare2 / runtime / 50 / 400 / warm | 5173.75 → 5242.5 | -68.75 | 427 / 475.5 | 216 → 293 |
| compare2 / runtime / 250 / 0 / cold | 1645.75 → 1567 | 78.75 | 881 / 426 | 703.5 → 640 |
| compare2 / runtime / 250 / 0 / warm | 1199.75 → 1172 | 27.75 | 406 / 230 | 322.25 → 363.5 |
| compare2 / runtime / 250 / 400 / cold | 5473.75 → 5581.5 | -107.75 | 548 / 275 | 670.75 → 729.5 |
| compare2 / runtime / 250 / 400 / warm | 5062 → 5083.5 | -21.5 | 251 / 191.5 | 272 → 290 |

## Invocation CPU

Each cell is baseline → candidate (number of complete paired Objects / 2). Both sides use the same subset. A missing invocation makes that Object unavailable for the corresponding repeated metric. CPU is Cloudflare invocation attribution, not exclusive semantic phase cost; await CPU may include concurrent alarm work. Whole invocations can outlive client settlement. Do not add the medians. No CPU difference cleared its full repeat spread.

| Cohort / candidate / seed / delay / state | Submit CPU | Alarm-pass CPU | Await CPU | Joined invocation CPU |
| --- | ---: | ---: | ---: | ---: |
| compare1 / layout / 50 / 0 / cold | 39 → 37.5 (2/2) | 134.25 → 172 (2/2) | 267.25 → 286 (2/2) | 441.25 → 487.5 (2/2) |
| compare1 / reconcile / 50 / 0 / cold | 39 → 43.5 (2/2) | 134.25 → 171 (2/2) | 267.25 → 261 (2/2) | 441.25 → 470 (2/2) |
| compare1 / layout / 50 / 0 / warm | 7 → 7 (1/2) | 86.5 → 91 (1/2) | 216.5 → 188.5 (1/2) | 329 → 278 (1/2) |
| compare1 / reconcile / 50 / 0 / warm | 7 → 5 (1/2) | 86.5 → 95.5 (1/2) | 216.5 → 203 (1/2) | 329 → 313 (1/2) |
| compare1 / layout / 50 / 400 / cold | 36.25 → 29 (2/2) | 158.75 → 115 (2/2) | 327.25 → 322.5 (2/2) | 530.5 → 478.5 (2/2) |
| compare1 / reconcile / 50 / 400 / cold | 36.25 → 39 (2/2) | 114.5 → 84 (1/2) | 327.25 → 328.5 (2/2) | 378 → 300 (1/2) |
| compare1 / layout / 50 / 400 / warm | 6.5 → 6.5 (1/2) | 136.25 → 152.75 (2/2) | 320 → 339.75 (2/2) | 288 → 333 (1/2) |
| compare1 / reconcile / 50 / 400 / warm | 6.5 → 7 (1/2) | 191 → 198 (1/2) | 320 → 357.75 (2/2) | missing → missing (0/2) |
| compare1 / layout / 250 / 0 / cold | 40 → 35.5 (2/2) | 225.25 → 219 (2/2) | 306 → 317.5 (2/2) | 547.25 → 566.5 (2/2) |
| compare1 / reconcile / 250 / 0 / cold | 40 → 42.5 (2/2) | 225.25 → 228.5 (2/2) | 306 → 352.5 (2/2) | 547.25 → 612.5 (2/2) |
| compare1 / layout / 250 / 0 / warm | 13.75 → 13 (2/2) | 221 → 232.25 (2/2) | 307.5 → 301.5 (2/2) | 549 → 553 (2/2) |
| compare1 / reconcile / 250 / 0 / warm | 13.75 → 14.75 (2/2) | 221 → 225.25 (2/2) | 307.5 → 315 (2/2) | 549 → 586.5 (2/2) |
| compare1 / layout / 250 / 400 / cold | 29.75 → 26.5 (2/2) | 139.75 → 152.5 (2/2) | 253 → 260.5 (2/2) | 418.25 → 442.5 (2/2) |
| compare1 / reconcile / 250 / 400 / cold | 29.75 → 27 (2/2) | 139.75 → 193 (2/2) | 253 → 260.5 (2/2) | 418.25 → 499.5 (2/2) |
| compare1 / layout / 250 / 400 / warm | 8.25 → 9.25 (2/2) | missing → missing (0/2) | 210.5 → 225 (1/2) | missing → missing (0/2) |
| compare1 / reconcile / 250 / 400 / warm | 8.25 → 9.25 (2/2) | missing → missing (0/2) | 210.5 → 208 (1/2) | missing → missing (0/2) |
| compare2 / runtime / 50 / 0 / cold | 53 → 57.5 (2/2) | 174.5 → 185.5 (2/2) | 344 → 356.5 (2/2) | 580.75 → 573.5 (2/2) |
| compare2 / runtime / 50 / 0 / warm | 11 → 10.5 (2/2) | 147.5 → 174.5 (2/2) | 328.5 → 326.25 (2/2) | 501 → 503.75 (2/2) |
| compare2 / runtime / 50 / 400 / cold | 53.5 → 57 (2/2) | 187.75 → 164 (2/2) | 391.25 → 353.5 (2/2) | 635.75 → 565 (2/2) |
| compare2 / runtime / 50 / 400 / warm | 9.75 → 11.5 (2/2) | 148.5 → 154 (2/2) | 403 → 395.25 (2/2) | 563.5 → 542 (2/2) |
| compare2 / runtime / 250 / 0 / cold | 49.25 → 39 (2/2) | 282 → 266.5 (2/2) | 355.75 → 334 (2/2) | 665 → 650 (2/2) |
| compare2 / runtime / 250 / 0 / warm | 14 → 14.25 (2/2) | 221 → 245.25 (2/2) | 310 → 321.25 (2/2) | 556.25 → 589.5 (2/2) |
| compare2 / runtime / 250 / 400 / cold | 34.75 → 28.5 (2/2) | 172.5 → 198.5 (2/2) | 249.5 → 257.5 (2/2) | 453 → 488.5 (2/2) |
| compare2 / runtime / 250 / 400 / warm | 10 → 9.5 (1/2) | 155.5 → 166.5 (2/2) | 250.75 → 246.75 (2/2) | 427.5 → 448 (1/2) |

## Pi reference at matched history positions

Pi has a separate namespace. The values below use the same sample positions as each Yielded baseline/candidate in the interleaved plan, then aggregate Object medians. They are descriptive point estimates, not supported improvements. No gap below the repeat spread is claimed resolved; there is no retained optimization and no demonstrated closing of the gap. The per-Object pi spans are in acceptance.json.

| Cohort / candidate / seed / delay | Pi beside baseline / candidate | Yielded − pi beside baseline / candidate |
| --- | ---: | ---: |
| compare1 / layout / 50 / 0 | 1299.25 / 1162 | 19.25 / 287.5 |
| compare1 / reconcile / 50 / 0 | 1299.25 / 1131.5 | 19.25 / 243.5 |
| compare1 / layout / 50 / 400 | 5141.25 / 5100 | 521.25 / 379 |
| compare1 / reconcile / 50 / 400 | 5141.25 / 5179 | 521.25 / 315 |
| compare1 / layout / 250 / 0 | 1147.75 / 1233.5 | 413.75 / 239.5 |
| compare1 / reconcile / 250 / 0 | 1147.75 / 1188 | 413.75 / 255.5 |
| compare1 / layout / 250 / 400 | 5163.25 / 5185 | 183.25 / 148 |
| compare1 / reconcile / 250 / 400 | 5163.25 / 5187.5 | 183.25 / 196.5 |
| compare2 / runtime / 50 / 0 | 1167.5 / 1251 | 469 / 399.5 |
| compare2 / runtime / 50 / 400 | 5185 / 5147.5 | 330.75 / 333.5 |
| compare2 / runtime / 250 / 0 | 1307 / 1171 | 338.75 / 396 |
| compare2 / runtime / 250 / 400 | 5378 / 5316 | 95.75 / 265.5 |

The full reducers also retain provider-clock bounds, per-gap data, SQL/transaction counts, CPU coverage, repeat order, outcomes and join failures in compare1/ and compare2/. See [README.md](README.md) for offline reproduction.
