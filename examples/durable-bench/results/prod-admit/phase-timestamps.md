# Admission boundary timestamps

Sixteen diagnostic-only turns: the unchanged path (`clock-sync`) and the kept fix (`prearm-only-probe`), one each per cold/warm state at 50/250 seed turns and 0/400 ms provider delay. Pair order is shuffled within each state. These samples do not support optimization claims.

All boundary columns below are **provider arrival milliseconds after that request’s submit-entry beacon**, not durations of the named step. They include normal output gating and transport; neighboring beacons may arrive out of order. The local Worker clock remains frozen through most admission work. Driver and provider clock epochs are retained separately in [raw normalized timestamps](phase-timestamps.json).

An explicit final `storage.sync()` and the beacons perturb the trace and can permit alarm work before response return. Admission counts use the earlier native body-return snapshot. Cold constructor acquisition precedes these beacons; its wall time remains unresolved.

## 50 seed turns / 0 ms provider delay / cold

Baseline sample `m0`; kept sample `m1`. Provider entry epochs: `1791504538141` / `1791504540680`. Driver admission I/O intervals (diagnostic): 494 / 580 ms. Adjacent entry-control offsets: -2 / 0 ms.

| Emitted at source boundary | Baseline arrival offset (ms) | Kept arrival offset (ms) |
| --- | ---: | ---: |
| `submit-enter` | 0 | 0 |
| `submit-enter-control` | -2 | 0 |
| `limits:end` | 31 | 16 |
| `alarm/advance maintenance generation:end` | 275 | 74 |
| `input-digest:end` | 276 | 73 |
| `ledger-admit:end` | 276 | 74 |
| `materialize:end` | 278 | 74 |
| `thread-created:end` | 277 | 75 |
| `mark-ready:end` | 277 | 76 |
| `wake:end` | 279 | 201 |
| `publish-committed:end` | 280 | 198 |
| `encode-response:end` | 281 | 200 |
| `submit-body-return` | 282 | 198 |
| `submit-sync-return` | 288 | 206 |

## 50 seed turns / 0 ms provider delay / warm

Baseline sample `m2`; kept sample `m3`. Provider entry epochs: `1791504542639` / `1791504544669`. Driver admission I/O intervals (diagnostic): 218 / 206 ms. Adjacent entry-control offsets: 1 / 2 ms.

| Emitted at source boundary | Baseline arrival offset (ms) | Kept arrival offset (ms) |
| --- | ---: | ---: |
| `submit-enter` | 0 | 0 |
| `submit-enter-control` | 1 | 2 |
| `limits:end` | 11 | 12 |
| `alarm/advance maintenance generation:end` | 158 | 133 |
| `input-digest:end` | 163 | 131 |
| `ledger-admit:end` | 157 | 134 |
| `materialize:end` | 162 | 134 |
| `thread-created:end` | 157 | 134 |
| `mark-ready:end` | 159 | 132 |
| `wake:end` | 163 | 135 |
| `publish-committed:end` | 163 | 138 |
| `encode-response:end` | 164 | 138 |
| `submit-body-return` | 165 | 138 |
| `submit-sync-return` | 174 | 147 |

## 50 seed turns / 400 ms provider delay / cold

Baseline sample `m1`; kept sample `m0`. Provider entry epochs: `1791504553760` / `1791504547433`. Driver admission I/O intervals (diagnostic): 562 / 655 ms. Adjacent entry-control offsets: 1 / -1 ms.

| Emitted at source boundary | Baseline arrival offset (ms) | Kept arrival offset (ms) |
| --- | ---: | ---: |
| `submit-enter` | 0 | 0 |
| `submit-enter-control` | 1 | -1 |
| `limits:end` | 16 | 44 |
| `alarm/advance maintenance generation:end` | 244 | 332 |
| `input-digest:end` | 245 | 332 |
| `ledger-admit:end` | 246 | 330 |
| `materialize:end` | 248 | 331 |
| `thread-created:end` | 249 | 333 |
| `mark-ready:end` | 250 | 328 |
| `wake:end` | 252 | 344 |
| `publish-committed:end` | 251 | 346 |
| `encode-response:end` | 252 | 342 |
| `submit-body-return` | 255 | 343 |
| `submit-sync-return` | 267 | 361 |

## 50 seed turns / 400 ms provider delay / warm

Baseline sample `m3`; kept sample `m2`. Provider entry epochs: `1791504565289` / `1791504559575`. Driver admission I/O intervals (diagnostic): 238 / 216 ms. Adjacent entry-control offsets: 0 / 0 ms.

| Emitted at source boundary | Baseline arrival offset (ms) | Kept arrival offset (ms) |
| --- | ---: | ---: |
| `submit-enter` | 0 | 0 |
| `submit-enter-control` | 0 | 0 |
| `limits:end` | 15 | 16 |
| `alarm/advance maintenance generation:end` | 197 | 157 |
| `input-digest:end` | 197 | 155 |
| `ledger-admit:end` | 195 | 158 |
| `materialize:end` | 202 | 160 |
| `thread-created:end` | 199 | 157 |
| `mark-ready:end` | 201 | 160 |
| `wake:end` | 203 | 161 |
| `publish-committed:end` | 203 | 162 |
| `encode-response:end` | 205 | 165 |
| `submit-body-return` | 208 | 166 |
| `submit-sync-return` | 220 | 189 |

## 250 seed turns / 0 ms provider delay / cold

Baseline sample `m1`; kept sample `m0`. Provider entry epochs: `1791504574422` / `1791504571670`. Driver admission I/O intervals (diagnostic): 663 / 648 ms. Adjacent entry-control offsets: 2 / 1 ms.

| Emitted at source boundary | Baseline arrival offset (ms) | Kept arrival offset (ms) |
| --- | ---: | ---: |
| `submit-enter` | 0 | 0 |
| `submit-enter-control` | 2 | 1 |
| `limits:end` | 23 | 37 |
| `alarm/advance maintenance generation:end` | 354 | 457 |
| `input-digest:end` | 357 | 459 |
| `ledger-admit:end` | 355 | 461 |
| `materialize:end` | 357 | 460 |
| `thread-created:end` | 353 | 461 |
| `mark-ready:end` | 356 | 460 |
| `wake:end` | 360 | 463 |
| `publish-committed:end` | 362 | 464 |
| `encode-response:end` | 360 | 468 |
| `submit-body-return` | 363 | 465 |
| `submit-sync-return` | 374 | 488 |

## 250 seed turns / 0 ms provider delay / warm

Baseline sample `m2`; kept sample `m3`. Provider entry epochs: `1791504576914` / `1791504579150`. Driver admission I/O intervals (diagnostic): 307 / 356 ms. Adjacent entry-control offsets: 0 / 1 ms.

| Emitted at source boundary | Baseline arrival offset (ms) | Kept arrival offset (ms) |
| --- | ---: | ---: |
| `submit-enter` | 0 | 0 |
| `submit-enter-control` | 0 | 1 |
| `limits:end` | 19 | 22 |
| `alarm/advance maintenance generation:end` | 280 | 298 |
| `input-digest:end` | 279 | 301 |
| `ledger-admit:end` | 278 | 305 |
| `materialize:end` | 282 | 299 |
| `thread-created:end` | 281 | 302 |
| `mark-ready:end` | 283 | 305 |
| `wake:end` | 286 | 396 |
| `publish-committed:end` | 284 | 400 |
| `encode-response:end` | 285 | 399 |
| `submit-body-return` | 287 | 394 |
| `submit-sync-return` | 295 | 408 |

## 250 seed turns / 400 ms provider delay / cold

Baseline sample `m0`; kept sample `m1`. Provider entry epochs: `1791504582267` / `1791504588989`. Driver admission I/O intervals (diagnostic): 793 / 691 ms. Adjacent entry-control offsets: -2 / 1 ms.

| Emitted at source boundary | Baseline arrival offset (ms) | Kept arrival offset (ms) |
| --- | ---: | ---: |
| `submit-enter` | 0 | 0 |
| `submit-enter-control` | -2 | 1 |
| `limits:end` | 41 | 32 |
| `alarm/advance maintenance generation:end` | 609 | 329 |
| `input-digest:end` | 613 | 327 |
| `ledger-admit:end` | 611 | 332 |
| `materialize:end` | 609 | 332 |
| `thread-created:end` | 612 | 329 |
| `mark-ready:end` | 611 | 331 |
| `wake:end` | 619 | 334 |
| `publish-committed:end` | 619 | 335 |
| `encode-response:end` | 620 | 336 |
| `submit-body-return` | 621 | 340 |
| `submit-sync-return` | 634 | 364 |

## 250 seed turns / 400 ms provider delay / warm

Baseline sample `m3`; kept sample `m2`. Provider entry epochs: `1791504600779` / `1791504594890`. Driver admission I/O intervals (diagnostic): 292 / 310 ms. Adjacent entry-control offsets: -1 / 0 ms.

| Emitted at source boundary | Baseline arrival offset (ms) | Kept arrival offset (ms) |
| --- | ---: | ---: |
| `submit-enter` | 0 | 0 |
| `submit-enter-control` | -1 | 0 |
| `limits:end` | 20 | 17 |
| `alarm/advance maintenance generation:end` | 269 | 251 |
| `input-digest:end` | 268 | 249 |
| `ledger-admit:end` | 275 | 251 |
| `materialize:end` | 270 | 256 |
| `thread-created:end` | 274 | 254 |
| `mark-ready:end` | 273 | 255 |
| `wake:end` | 276 | 259 |
| `publish-committed:end` | 276 | 261 |
| `encode-response:end` | 278 | 261 |
| `submit-body-return` | 280 | 269 |
| `submit-sync-return` | 289 | 288 |
