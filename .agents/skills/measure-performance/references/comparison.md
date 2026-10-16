# Comparable measurements

Record the revision, environment, workload, model/settings, concurrency, and cold or warm state where relevant. Measure before changing the path, reuse a matching baseline, or recover one from the prior revision using the same harness. An unavailable baseline or target environment remains an explicit gap.

Use enough representative samples to distinguish the change from variation. A single successful run is a smoke check. Deterministic counts reveal added work; they do not prove latency. For Cloudflare paths, measure on deployed resources. Do not wait for a quiet laptop or use global CPU locks. Use [temporary Alchemy stacks](../../alchemy/references/temporary-stacks.md) for the pre-authorized cloud experiment lifecycle.

## Compare complete behavior

Run the same selected workload on both revisions with normal instrumentation. Interleave runs when load or cache drift matters. Verify useful work completed correctly; retain failures, retries, timeouts, slow samples, and completed-operation counts. Report typical results and spread, including tails when samples support them.

Judge total user completion and total work. A microbenchmark win cannot compensate for a slower critical path. Check surprising gains against the affected component's share of time and plausible resource limits. Preserve required behavior and observability while optimizing.

For CI cache or concurrency claims, include setup and transfer costs, verify invalidation and nested concurrency, and compare hosted cold and cached runs on fresh runners. Apply these checks to that claim only.

## Evidence and decision

Before calling the change complete or ready to land, provide baseline/candidate results, revisions, workload, sample counts, variation, and evidence links. Passing functional checks alone is insufficient. Early PRs allowed by repository policy must identify outstanding performance verification.

Fix or drop material regressions. Quantify an unavoidable tradeoff and alternatives and obtain the user's explicit decision before accepting it. Insufficient evidence means performance is unverified; keep the gap open while continuing useful work. Revert ineffective optimizations unless they have a separate, stated benefit within scope. Do not relabel reduced work as demonstrated speedup.

Keep reusable measurement code in the repository and compact run evidence in artifacts. For broad audits, account for every requested outcome as measured, unmeasured, or explicitly removed from scope. Reconsider the hypothesis before spending another experiment on it.
