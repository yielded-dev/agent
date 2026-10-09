# Benchmark adaptations

Use a separate checkout of `78cb4eb7589984c1cd194e0cb01a5a0055a6d3f3` (#828). The current comparison applies these patches in order:

1. `bench.patch`
2. `force-abba.patch`
3. `object-health-fresh.patch`
4. `object-build.patch`

Replace `/path/to/agent` with the credential-enabled checkout and `/tmp/history-cost` with a private mode-700 task directory. Run commands through `vp`, with Cloudflare credentials supplied by `direnv exec .`. Product candidates contain none of these adaptations. [Patch reproduction](patch-reproduction.json) verifies clean application and equality with all 20 changed running files after replacing only local paths.

The first full native-constructor matrix used only `bench.patch`, with BAAB order, `--rigorous --baseline 07f0272e7ba49a494064b6b74c6318b55514ae19 --candidate 0451aacb627ec3dbe76618c043bce9d6b91596ad --targets yielded,pi --sizes 50,250,1000 --ttft 0,400 --objects 10 --repeats 6 --concurrency 10`. Its reversed-order confirmation uses all four patches, ABBA order, `--ttft 0 --objects 20`, and the same other options. The build-order patch changes only the controller.

The deployed canonical importer repeatedly exceeded its storage timeout at 250/1,000 turns. The seed phase therefore canonically imports each addressed fixture locally, then restores an exact SQLite snapshot into a fresh deployed Object. It copies no old ownership or claim rows: the real importer has already normalized them. Restored rows, schema text, columns, indexes and triggers must have the same SHA-256 as that source; complete table counts and transcript fingerprints are checked again. Triggers are installed after rows to avoid duplicating derivative updates. Local preparations have concurrency at most four, deployed uploads are serial, and seeding ends before timing. `snapshot-roundtrip.json` records successful local 50/250/1,000/3,500 restoration proof; deployed 3,500-turn restoration still exceeded memory limits.

The offline seed bundle cannot execute native submission RPCs. Production ThreadObject replaces it on the same Worker, namespace and Object identities before timing. Native gates, prearming, alarms and confirmed durability are unchanged.

Provider arrival is captured before body parsing or artificial TTFT. Three clock probes traverse each measured Object before and after its timed turn. Cold probes finish before abort; no Object invocation follows the abort until the timer starts. Post-probes follow the completion timestamp. Matching provider-colo probes bound the clock offset; intersected bounds assume stable host clock offsets within that colo. The report retains uncertainty and excludes inconsistent probes and incomplete A/B Object coverage.

Fresh canary Objects check both Yielded and pi handlers and their build bindings before seeding or timing. Their names include the build and probe identifiers and are separate from measured Objects. Root and driver readiness each require three matching responses. Every timed Object additionally reports its build binding in its identity; a mismatch rejects the sample. Successful samples retain `objectBuildVerified`. The first full BAAB matrix predates this additional guard.

The failed original canary patch, `object-health.patch`, is retained only to reproduce the readiness-timeout attempt; do not apply it with `object-health-fresh.patch`. The fresh-canary deployed smoke completed all 24 turns and target cleanup. Its one-Object timings are diagnostic only. `object-health-proof.json` records local handler checks.

`compact-results.py` reduces a retained raw result into numeric samples, Object summaries, control drift, clock uncertainty, failures and observed CPU outcomes. `publish-run.py` moves numeric rows into a CSV linked by SHA-256 from the summary. `repeat-spread.py` reports cohort estimates by warm repeat index; these are descriptive variation, not independent trials or confidence intervals. `render-tables.py` renders condition tables. These helpers consume the full compact result before CSV publication. Large raw results and fixture archives are not published.

Run `deployed -- --teardown` through the same benchmark task and private Alchemy state after measurement. Keep state until prefix-wide API checks show no `history-cost` Workers or Durable Object namespaces. The checks list Worker scripts and paginate all Object namespaces, failing closed if pagination exceeds its bound.
