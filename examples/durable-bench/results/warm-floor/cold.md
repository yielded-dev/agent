# Cold driver observations

Milliseconds. One cold observation per variant per Object, two Objects per condition; an acknowledged eviction and changed Object incarnation are required. The isolate may already be warm. These sparse observations do not establish equivalence or absence of regression. All candidate implementations were discarded.

| Experiment / seeded turns / provider | Variant | Baseline | Candidate | Difference (baseline − candidate) | Baseline repeat range |
| --- | --- | ---: | ---: | ---: | ---: |
| candidates / 50 / instant | prearm | 1,329 | 1,171 | 158 | 412 |
| candidates / 50 / instant | settlement | 1,329 | 1,291 | 38 | 412 |
| candidates / 50 / instant | combined | 1,329 | 1,441.5 | -112.5 | 412 |
| candidates / 50 / 400 ms | prearm | 5,282 | 5,302 | -20 | 94 |
| candidates / 50 / 400 ms | settlement | 5,282 | 5,457.5 | -175.5 | 94 |
| candidates / 50 / 400 ms | combined | 5,282 | 5,192 | 90 | 94 |
| candidates / 250 / instant | prearm | 1,223.5 | 1,918.5 | -695 | 236 |
| candidates / 250 / instant | settlement | 1,223.5 | 1,390 | -166.5 | 236 |
| candidates / 250 / instant | combined | 1,223.5 | 1,306 | -82.5 | 236 |
| candidates / 250 / 400 ms | prearm | 5,501 | 5,747 | -246 | 287 |
| candidates / 250 / 400 ms | settlement | 5,501 | 5,641.5 | -140.5 | 287 |
| candidates / 250 / 400 ms | combined | 5,501 | 5,312.5 | 188.5 | 287 |
| views / 50 / instant | views | 987.5 | 960 | 27.5 | 262 |
| views / 50 / 400 ms | views | 5,297.25 | 5,353.5 | -56.25 | 137.5 |
| views / 250 / instant | views | 1,605.5 | 1,680 | -74.5 | 364 |
| views / 250 / 400 ms | views | 5,456 | 5,506 | -50 | 233 |
