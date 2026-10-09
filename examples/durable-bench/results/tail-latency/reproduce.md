The measurements used the deployed harness from PR #828 at `78cb4eb7589984c1cd194e0cb01a5a0055a6d3f3`. Product baseline was `07f0272e7ba49a494064b6b74c6318b55514ae19`. The harness head was fetched and checked again before A/B1. Use a separate worktree; none of these adaptations belong in a product PR.

The [notification candidate](candidate-notifications.patch) and [maintenance-write candidate](candidate-maintenance-writes.patch) are independent patches against that baseline. Their original local commit hashes identify the measured sources. If a candidate is not published as a product branch, apply its patch to the baseline in another checkout and use the resulting local commit as `--candidate`.

Apply either [distribution instrumentation](distribution-instrumentation.patch) or [A/B instrumentation](ab-instrumentation.patch) to that harness revision. They are complete alternatives, not cumulative patches. Both align the older harness checkout's one differing framework file to the product baseline, preserve all transcript/fixture checks, prefix every deployed resource with `tail-latency`, and remove account information from console output. The A/B version additionally counts alarm methods invoked through transaction callback objects and retains stage snapshots and target Worker route telemetry.

Replace the private temporary path in `deployed/platform.ts` with a fresh mode-700 directory outside the repository. Keep its Alchemy ownership state until teardown. The run used credentials inherited through `direnv exec .` from the product checkout. Do not write credential values, account identifiers, account names, Worker subdomains or email addresses into results. Install the pinned repository dependencies with `vp install --frozen-lockfile`; the harness installs its third-party dependencies in this isolated bench checkout. Product validation used a different checkout without those vendor dependencies.

From the product checkout, replacing `/absolute/bench` with the isolated bench path:

```sh
direnv exec . vp -C /absolute/bench/examples/durable-bench run deployed -- --targets yielded,pi --sizes 50,250 --ttft 0,400 --objects 10 --repeats 24 --concurrency 10 --cpu

direnv exec . vp -C /absolute/bench/examples/durable-bench run deployed -- --rigorous --baseline 07f0272e --candidate 514b83d0 --targets yielded,pi --sizes 50,250 --ttft 0,400 --objects 10 --repeats 12 --concurrency 10 --cpu

direnv exec . vp -C /absolute/bench/examples/durable-bench run deployed -- --rigorous --baseline 07f0272e --candidate 413bb4bc --targets yielded,pi --sizes 50,250 --ttft 0,400 --objects 16 --repeats 8 --concurrency 10 --cpu
```

The rigorous command selects ABBA or BAAB, redeploys the same Worker/namespaces, and preserves each Object's history across passes. It excludes one verified cold turn and one warmup per Object/pass. History therefore grows during the experiment; inspect epoch and paired-Object results as well as aggregate quantiles. Treat pi's changes and repeated-build spread as controls. No sample is silently dropped or replayed after an uncertain outcome.

The second A/B command above is planned and has not completed yet; its status will be updated in the report.

Recompute the published warm distribution without Cloudflare access:

```sh
vp exec node examples/durable-bench/results/tail-latency/recompute.mjs examples/durable-bench/results/tail-latency/distribution-latencies.json
```

The same command accepts each later `*-latencies.json` file. Compact data retains every warm driver's elapsed value, Object index, build and epoch. Summary files retain the input artifact hash, framework/bundle revisions, counts, CPU attribution coverage, all outcome categories and target cleanup result. Raw telemetry and large model/history archives stay outside the evidence branch.

Each run removes its target stack automatically. After the last run, remove shared resources and verify that the entire prefix is empty:

```sh
direnv exec . vp -C /absolute/bench/examples/durable-bench run deployed -- --teardown
```

Retain the resulting `cleanup.json`. Teardown uses Alchemy ownership state and independently lists Workers and Durable Object namespaces through the Cloudflare API. The final evidence must show no remaining `tail-latency` resource. The temporary Postgres service used for local validation is also stopped separately.
