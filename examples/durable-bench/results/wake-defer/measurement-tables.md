partial; no complete-matrix claim. All times below are milliseconds.

Two repeats per arm reduce to one median per Object and temperature. Each reported metric uses only Objects with both arm medians known. Positive delta is baseline minus candidate. Repeat columns are absolute differences between the two repeats; the conservative positive-effect flag additionally requires every paired effect positive and the median larger than the largest observed within-arm repeat difference. This is descriptive, not a confidence interval.

### Driver latency and repeats

| Seed | TTFT | State | Objects | Driver baseline → candidate | Paired Δ median [min, max] | Baseline repeat spread median [min, max] | Candidate repeat spread median [min, max] |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 50 | 0 | warm | 7 | 2308.0 → 1664.0 | 910.5 [62.0, 2595.0] | 637.0 [30.0, 986.0] | 205.0 [7.0, 492.0] |
| 50 | 0 | cold | 7 | 3170.5 → 1898.0 | 939.0 [47.5, 2177.0] | 118.0 [9.0, 992.0] | 250.0 [4.0, 856.0] |
| 50 | 400 | warm | 7 | 6289.5 → 5617.5 | 605.0 [-111.0, 1164.0] | 61.0 [31.0, 430.0] | 88.0 [33.0, 1741.0] |
| 50 | 400 | cold | 7 | 6802.0 → 5887.5 | 810.5 [411.5, 1199.0] | 124.0 [8.0, 543.0] | 117.0 [24.0, 326.0] |
| 250 | 0 | warm | 4 | 1826.0 → 1478.5 | 40.3 [-75.5, 679.5] | 467.5 [233.0, 557.0] | 71.0 [6.0, 106.0] |
| 250 | 0 | cold | 4 | 2664.3 → 2076.0 | 629.0 [82.5, 987.0] | 378.0 [217.0, 803.0] | 201.0 [7.0, 558.0] |
| 250 | 400 | warm | 7 | 6431.5 → 5768.5 | 593.5 [338.0, 985.0] | 143.0 [13.0, 537.0] | 42.0 [1.0, 680.0] |
| 250 | 400 | cold | 7 | 6750.0 → 6079.5 | 498.5 [350.0, 975.0] | 90.0 [19.0, 265.0] | 70.0 [12.0, 640.0] |

### Model timing and alarm CPU

| Seed | TTFT | State | First dispatch from turn start | Driver → provider arrival* | Provider gap median* | Object I/O-clock gap median | Alarm CPU (paired Objects) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 50 | 0 | warm | 0.0 → 0.0 | 311.0 → 305.0 | 188.5 → 94.5 | 0.0 → 0.0 | 476.0 → 442.0 (n=5) |
| 50 | 0 | cold | 0.0 → 0.0 | 636.5 → 628.0 | 278.3 → 144.0 | 0.0 → 0.0 | 417.0 → 424.5 (n=5) |
| 50 | 400 | warm | 0.0 → 0.0 | 350.5 → 359.5 | 192.3 → 104.5 | 0.0 → 0.0 | 901.8 → 1113.3 (n=4) |
| 50 | 400 | cold | 0.0 → 0.0 | 759.5 → 667.0 | 197.5 → 107.3 | 0.0 → 0.0 | 532.8 → 835.0 (n=6) |
| 250 | 0 | warm | 0.0 → 0.0 | 361.0 → 402.0 | 130.5 → 120.6 | 0.0 → 0.0 | 623.5 → 585.5 (n=3) |
| 250 | 0 | cold | 0.0 → 0.0 | 714.3 → 761.0 | 190.5 → 151.1 | 0.0 → 0.0 | 555.0 → 688.5 (n=3) |
| 250 | 400 | warm | 0.0 → 0.0 | 469.0 → 434.5 | 195.5 → 106.3 | 0.0 → 0.0 | 937.5 → 1098.8 (n=6) |
| 250 | 400 | cold | 0.0 → 0.0 | 801.0 → 820.5 | 196.5 → 111.8 | 0.0 → 0.0 | 949.0 → 1115.0 (n=6) |

*Provider-arrival comparisons can include cross-clock skew. Object timers advance at I/O; zero does not exclude intervening CPU work. Alarm CPU is whole invocation cost for uniquely joined entered handlers, including labeled boundary work; it is not all native scheduling CPU.

### Counts per turn

| Seed | TTFT | State | All scheduleNow | Nondeferred scheduleNow | Overlapping entered handlers | Native setAlarm calls |
| --- | --- | --- | --- | --- | --- | --- |
| 50 | 0 | warm | 20.5 → 25.0 | 12.5 → 2.0 | 6.5 → 5.0 | 32.0 → 21.5 |
| 50 | 0 | cold | 21.0 → 25.0 | 12.5 → 2.0 | 7.0 → 5.0 | 33.0 → 24.5 |
| 50 | 400 | warm | 23.0 → 33.0 | 14.0 → 2.0 | 9.0 → 9.0 | 38.0 → 30.0 |
| 50 | 400 | cold | 23.0 → 33.0 | 14.0 → 2.0 | 9.0 → 9.0 | 38.0 → 30.0 |
| 250 | 0 | warm | 17.0 → 19.3 | 10.3 → 2.0 | 3.0 → 2.3 | 19.5 → 16.3 |
| 250 | 0 | cold | 19.0 → 22.8 | 11.0 → 2.0 | 5.0 → 4.0 | 28.0 → 21.5 |
| 250 | 400 | warm | 23.0 → 33.0 | 14.0 → 2.0 | 9.0 → 9.0 | 38.0 → 29.5 |
| 250 | 400 | cold | 22.5 → 32.0 | 13.5 → 2.0 | 8.5 → 8.5 | 37.0 → 29.5 |

### Secondary laptop latency

| Seed | TTFT | State | Laptop baseline → candidate | Response CF-Ray colos (turns) |
| --- | --- | --- | --- | --- |
| 50 | 0 | warm | 2330.0 → 1685.7 | SJC: 28 |
| 50 | 0 | cold | 3204.4 → 1921.7 | SJC: 28 |
| 50 | 400 | warm | 6315.7 → 5645.0 | SJC: 28 |
| 50 | 400 | cold | 6828.6 → 5913.6 | SJC: 28 |
| 250 | 0 | warm | 1852.5 → 1500.3 | SJC: 16 |
| 250 | 0 | cold | 2683.9 → 2102.2 | SJC: 16 |
| 250 | 400 | warm | 6457.3 → 5794.4 | SJC: 28 |
| 250 | 400 | cold | 6777.8 → 6102.1 | SJC: 28 |

Every response's complete CF-Ray and placement receipt is retained in requests.jsonl.gz. The driver is the primary client clock; laptop elapsed time is secondary.
