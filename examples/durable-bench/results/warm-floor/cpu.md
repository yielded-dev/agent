# Invocation CPU

Milliseconds, matched baseline/candidate Objects only. Baselines pool production/repeat. Each Object metric requires every planned repeated invocation to have an unambiguous START join and an ok CPU event. Missing pairs are excluded from that metric, not filled with zero. These are Cloudflare invocation attribution, not additive semantic phase costs. No CPU saving exceeded the full baseline repeat range on any matched Object.

| Experiment / seeded turns / provider | Variant | Invocation | Objects | Baseline | Candidate | Difference (baseline − candidate) | Repeat range |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| candidates / 50 / instant | prearm | submit | 2/2 | 11.75 | 10.5 | 1.25 | 8 |
| candidates / 50 / instant | prearm | awaitSettlement | 1/2 | 375.5 | 330 | 45.5 | 202 |
| candidates / 50 / instant | prearm | alarm | 1/2 | 165 | 171.5 | -6.5 | 66 |
| candidates / 50 / instant | settlement | submit | 2/2 | 11.75 | 13.5 | -1.75 | 8 |
| candidates / 50 / instant | settlement | awaitSettlement | 1/2 | 375.5 | 333 | 42.5 | 202 |
| candidates / 50 / instant | settlement | alarm | 1/2 | 165 | 203 | -38 | 66 |
| candidates / 50 / instant | combined | submit | 2/2 | 11.75 | 12.25 | -0.5 | 8 |
| candidates / 50 / instant | combined | awaitSettlement | 1/2 | 375.5 | 275.5 | 100 | 202 |
| candidates / 50 / instant | combined | alarm | 1/2 | 165 | 172.5 | -7.5 | 66 |
| candidates / 50 / 400 ms | prearm | submit | 2/2 | 6.75 | 7.5 | -0.75 | 5.5 |
| candidates / 50 / 400 ms | prearm | awaitSettlement | 2/2 | 230.75 | 237.25 | -6.5 | 122.5 |
| candidates / 50 / 400 ms | prearm | alarm | 2/2 | 107.25 | 94.25 | 13 | 60 |
| candidates / 50 / 400 ms | settlement | submit | 2/2 | 6.75 | 7.75 | -1 | 5.5 |
| candidates / 50 / 400 ms | settlement | awaitSettlement | 2/2 | 230.75 | 237.5 | -6.75 | 122.5 |
| candidates / 50 / 400 ms | settlement | alarm | 2/2 | 107.25 | 101.5 | 5.75 | 60 |
| candidates / 50 / 400 ms | combined | submit | 2/2 | 6.75 | 7 | -0.25 | 5.5 |
| candidates / 50 / 400 ms | combined | awaitSettlement | 2/2 | 230.75 | 237.25 | -6.5 | 122.5 |
| candidates / 50 / 400 ms | combined | alarm | 2/2 | 107.25 | 102.5 | 4.75 | 60 |
| candidates / 250 / instant | prearm | submit | 2/2 | 14 | 12.5 | 1.5 | 8.5 |
| candidates / 250 / instant | prearm | awaitSettlement | 1/2 | 358 | 371.5 | -13.5 | 590 |
| candidates / 250 / instant | prearm | alarm | 1/2 | 265.5 | 281.5 | -16 | 317 |
| candidates / 250 / instant | settlement | submit | 1/2 | 10 | 12 | -2 | 7 |
| candidates / 250 / instant | settlement | awaitSettlement | 1/2 | 358 | 463.5 | -105.5 | 590 |
| candidates / 250 / instant | settlement | alarm | 1/2 | 265.5 | 218 | 47.5 | 317 |
| candidates / 250 / instant | combined | submit | 2/2 | 14 | 12 | 2 | 8.5 |
| candidates / 250 / instant | combined | awaitSettlement | 1/2 | 358 | 337.5 | 20.5 | 590 |
| candidates / 250 / instant | combined | alarm | 1/2 | 265.5 | 252 | 13.5 | 317 |
| candidates / 250 / 400 ms | prearm | submit | 1/2 | 10.5 | 9.5 | 1 | 4 |
| candidates / 250 / 400 ms | prearm | awaitSettlement | 1/2 | 257 | 283 | -26 | 60 |
| candidates / 250 / 400 ms | prearm | alarm | 1/2 | 197.5 | 186 | 11.5 | 119 |
| candidates / 250 / 400 ms | settlement | submit | 2/2 | 9.75 | 8.75 | 1 | 3.5 |
| candidates / 250 / 400 ms | settlement | awaitSettlement | 1/2 | 257 | 268 | -11 | 60 |
| candidates / 250 / 400 ms | settlement | alarm | 2/2 | 169.25 | 173 | -3.75 | 110.5 |
| candidates / 250 / 400 ms | combined | submit | 2/2 | 9.75 | 11.25 | -1.5 | 3.5 |
| candidates / 250 / 400 ms | combined | awaitSettlement | 1/2 | 257 | 276.5 | -19.5 | 60 |
| candidates / 250 / 400 ms | combined | alarm | 2/2 | 169.25 | 171.5 | -2.25 | 110.5 |
| views / 50 / instant | views | submit | 2/2 | 5.5 | 6.75 | -1.25 | 4 |
| views / 50 / instant | views | awaitSettlement | 2/2 | 158.5 | 170 | -11.5 | 71.5 |
| views / 50 / instant | views | alarm | 2/2 | 81 | 95.75 | -14.75 | 49.5 |
| views / 50 / 400 ms | views | submit | 2/2 | 7.75 | 7 | 0.75 | 8.5 |
| views / 50 / 400 ms | views | awaitSettlement | 2/2 | 235.5 | 230.25 | 5.25 | 78 |
| views / 50 / 400 ms | views | alarm | 2/2 | 86.5 | 89 | -2.5 | 140 |
| views / 250 / instant | views | submit | 2/2 | 15 | 16.75 | -1.75 | 10 |
| views / 250 / instant | views | awaitSettlement | 2/2 | 419.5 | 409.75 | 9.75 | 289 |
| views / 250 / instant | views | alarm | 2/2 | 133.25 | 159.75 | -26.5 | 288 |
| views / 250 / 400 ms | views | submit | 1/2 | 11.5 | 8.5 | 3 | 7 |
| views / 250 / 400 ms | views | awaitSettlement | 2/2 | 264 | 252.25 | 11.75 | 102 |
| views / 250 / 400 ms | views | alarm | 2/2 | 147.25 | 181.75 | -34.5 | 92 |
