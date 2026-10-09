# Combined alarm shortcut comparison

All 264 planned turns qualified, with 72 verified cold incarnations and 2,376 matching provider fingerprints. This prototype is **rejected**: no warm difference clears the full baseline repeat range. Two cold regression estimates and one cold improvement exceed baseline ranges but remain smaller than the candidate’s own ranges; none establishes a change beyond both observed spreads. Baseline and candidate run interleaved in the same Object and bundle.

Warm comparisons use eight baseline and four candidate requests; cold uses four and two. CPU is Cloudflare submit-invocation attribution. No repeat is discarded.

| History / delay / Object | State | Baseline → candidate (ms) | Saved (ms) | Baseline / candidate ranges (ms) | Result | CPU baseline → candidate (ms) |
| --- | --- | ---: | ---: | ---: | --- | ---: |
| h50-d0-o0 | cold | 431 → 413 | 18 | 202 / 30 | Inconclusive | 31 → 51.5 |
| h50-d0-o0 | warm | 196.5 → 187 | 9.5 | 108 / 59 | Inconclusive | 6.5 → 6 |
| h50-d0-o1 | cold | 682.5 → 435 | 247.5 | 188 / 280 | Baseline range only; inconclusive | 45 → 71 |
| h50-d0-o1 | warm | 272.5 → 263 | 9.5 | 67 / 175 | Inconclusive | 13 → 10.5 |
| h50-d400-o0 | cold | 476.5 → 836 | -359.5 | 94 / 812 | Baseline range only; inconclusive | 32 → 24.5 |
| h50-d400-o0 | warm | 244.5 → 234.5 | 10 | 47 / 80 | Inconclusive | 6 → 5 |
| h50-d400-o1 | cold | 629 → 914.5 | -285.5 | 144 / 593 | Baseline range only; inconclusive | 45.5 → 77 |
| h50-d400-o1 | warm | 303.5 → 278.5 | 25 | 79 / 83 | Inconclusive | 10.5 → 10 |
| h250-d0-o0 | cold | 897 → 893 | 4 | 78 / 204 | Inconclusive | 51.5 → 67 |
| h250-d0-o0 | warm | 382.5 → 387 | -4.5 | 79 / 48 | Inconclusive | 13.5 → 16.5 |
| h250-d0-o1 | cold | 919.5 → 780.5 | 139 | 168 / 131 | Inconclusive | 73 → 49 (missing 0/1) |
| h250-d0-o1 | warm | 491.5 → 495.5 | -4 | 123 / 134 | Inconclusive | 16 → 15.5 |
| h250-d400-o0 | cold | 609 → 482 | 127 | 371 / 84 | Inconclusive | 26.5 → 46.5 |
| h250-d400-o0 | warm | 286.5 → 246.5 | 40 | 230 / 77 | Inconclusive | 10 → 9 (missing 0/1) |
| h250-d400-o1 | cold | 896 → 580 | 316 | 387 / 518 | Inconclusive | 66 → 92 |
| h250-d400-o1 | warm | 403.5 → 329 | 74.5 | 148 / 618 | Inconclusive | 19.5 → 17.5 |

Normal endpoint snapshots show 3 transactions, 4 alarm reads and 1 alarm write for the candidate, compared with 4/4/2 for baseline. Diagnostic samples are excluded from those timing comparisons.

See [reduction](fast-admission.json), [turn rows](fast-admission-turns.jsonl), [verification](fast/summary.json) and [exact deployed build](fast-build.json).
