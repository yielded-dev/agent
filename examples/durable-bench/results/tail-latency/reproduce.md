The measurements used the deployed harness from PR #828 at `78cb4eb7589984c1cd194e0cb01a5a0055a6d3f3`. Product baseline was `07f0272e7ba49a494064b6b74c6318b55514ae19`. Both remote heads were fetched and checked again before the guarded maintenance A/B. Use a separate worktree; none of these adaptations belong in a product PR.

The [notification candidate](candidate-notifications.patch) and [maintenance-write candidate](candidate-maintenance-writes.patch) are independent patches against that baseline. Their original local commit hashes identify the measured sources. If a candidate is not published as a product branch, apply its patch to the baseline in another checkout and use the resulting local commit as `--candidate`.

For new experiments, apply [build-verified A/B instrumentation](verified-ab-instrumentation.patch) to that harness revision. The older [distribution instrumentation](distribution-instrumentation.patch) and [unverified A/B instrumentation](ab-instrumentation.patch) reproduce the historical experiments only. All three are complete alternatives, not cumulative patches. They align the older harness checkout's one differing framework file to the product baseline, preserve transcript/fixture checks, prefix every deployed resource with `tail-latency`, and remove account information from console output. The A/B versions additionally count alarm methods invoked through transaction callback objects and retain stage snapshots and target Worker route telemetry.

The build-verified version checks the Object's own `BUILD` before admitting an input and again in metrics, and checks the provider receipt's expected build. Cold readiness waits for the correct Object build, records all observed builds and reset attempts, then leaves the Object cold for its timed input. A successful Worker deployment alone is insufficient: one historical baseline-labeled Object continued executing candidate code after a reset. Do not use the old labeled comparisons for causal claims.

Received HTTP 5xx responses from cold setup are retried within the readiness bound and retained in `readinessFailures`. Uncertain transport outcomes, seed failures and timed input failures still stop the run. This prevents an unrelated setup response from silently disappearing or forcing input replay.

A fresh target Worker waits 45 seconds after creation before its existing direct and driver health checks. Existing targets redeployed between A/B passes still use health checks plus Object-build readiness. This setup interval is outside timing and was added after a seed request received the Cloudflare HTTP 404 placeholder despite earlier successful health checks.

Replace the private temporary path in `deployed/platform.ts` with a fresh mode-700 directory outside the repository. Keep its Alchemy ownership state until teardown. The run used credentials inherited through `direnv exec .` from the product checkout. Do not write credential values, account identifiers, account names, Worker subdomains or email addresses into results. Install the pinned repository dependencies with `vp install --frozen-lockfile`; the harness installs its third-party dependencies in this isolated bench checkout. Product validation used a different checkout without those vendor dependencies.

From the product checkout, replacing `/absolute/bench` with the isolated bench path:

```sh
direnv exec . vp -C /absolute/bench/examples/durable-bench run deployed -- --targets yielded,pi --sizes 50,250 --ttft 0,400 --objects 10 --repeats 24 --concurrency 10 --cpu

direnv exec . vp -C /absolute/bench/examples/durable-bench run deployed -- --rigorous --baseline 07f0272e --candidate 514b83d0 --targets yielded,pi --sizes 50,250 --ttft 0,400 --objects 10 --repeats 12 --concurrency 10 --cpu

direnv exec . vp -C /absolute/bench/examples/durable-bench run deployed -- --rigorous --baseline 07f0272e --candidate 413bb4bc --targets yielded,pi --sizes 50,250 --ttft 0,400 --objects 16 --repeats 8 --concurrency 10 --cpu
```

The rigorous command selects ABBA or BAAB, redeploys the same Worker/namespaces, and preserves each Object's history across passes. It excludes one verified cold turn and one warmup per Object/pass. History therefore grows during the experiment; inspect epoch and paired-Object results as well as aggregate quantiles. Treat pi's changes and repeated-build spread as controls. No sample is silently dropped or replayed after an uncertain outcome.

The notification experiment completed with unverified Object-build labels and is inconclusive. The first maintenance attempt failed during seeding; the next exposed stale Object code and later a lost-network failure. A guarded pilot passed. The first full guarded run stopped on a pi cold-setup error during pass three; all 3,479 completed inputs had verified builds, but the final baseline pass was missing. The next attempt received a route-level HTTP 404 during seeding. Every failed run retained known failures and verified target cleanup. The guarded maintenance comparison with setup retries and initial route settling is running on fresh Objects. See the report for current status.

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
