# KOM-433 baseline receipt

Baseline: `ba5813ec33880a9063147be6e6cce94698b11725`; source was clean before prototype work.
Lockfile SHA-256: `574fa0aff46d96f79a01c9f0125154c96266363804f47a0265c5cad564952abf`.
Production build SHA-256: `a3ad44235b4789408d94203bbfd8cff1295ecaa84410ee181a91694d75e678f2`.

The original workflow measurement session suffered a transport serialization error. Its baseline archive and production stage were preserved. The next session prepared instrumentation and CPU helpers but repeatedly failed Miniflare startup. The parent stopped that session, retained its files, and repaired the task-local runner.

## Recovered counting run

Command (exit 0):

```sh
cd /tmp/kom433-spike-01a119d5-baseline.bcZT4R
vp run counts -- /tmp/kom433-spike-01a119d5-baseline.bcZT4R/baseline-checkout /tmp/kom433-spike-01a119d5-baseline.bcZT4R/counts-50-stage-attribution counted
```

`modulesRoot: "/"` is required with this Miniflare 5 configuration and absolute module filenames. The counted bundle is loaded directly. A fresh store uses `/setup` followed by `/seed`; `/wake` belongs after a populated store has been reopened. Calling `/wake` on the empty store returned a typed work-inventory recovery block. These are harness setup constraints; no production source was changed.

The seed fingerprint at 50 turns was `b017b487524e44a4`. Ten following eight-tool turns completed with final model-visible fingerprint `b73859cee894aca6`. The first measured turn has exactly 50 historical turns. Later turns grow that history, so preserve the per-turn series and compare matching positions.

First measured turn: 37431 evaluated primitives, 44026 primitive allocations, 2303 separately counted inline successes, and 430 selected call-site entries. Nine warm-position medians: {"evaluations": 42335, "inlineSuccesses": 3038, "allocations": 50911, "calls": 430}. Every turn has 189 Async evaluations. Exclusive stage evaluations and allocations sum to each turn's whole totals; every recorded site closes. The raw report includes inclusive stages, exclusive stages, selected call sites, nesting, and operation classes. Inline generator success work is separate from the run-loop evaluation counter. The initial run's stage attribution was invalid because the injected banner's `.filter(Boolean)` resolved to a bundled Schema function. The corrected probe uses `.filter(globalThis.Boolean)`; its complete rerun has identical whole-turn totals and valid domain-stage attribution with no `null` bucket. Use only the corrected run for stage comparisons; retain the initial attempt as diagnostic evidence.

Evidence: `baseline/counts-50-corrected-attribution/`, `baseline/production-stage/identity.json`, `baseline/source-ba5813ec.tar.gz`, and `harness-corrected/`. The CPU helpers are `stage.mjs`, `cpu-capture.mjs`, `cpu-worker.mjs`, and `cpu-cohorts.py`. Full temporary setup remains at `/tmp/kom433-spike-01a119d5-baseline.bcZT4R` for the measurement owner. Keep failed attempt reports when assembling final artifacts.

## Remaining measurement work

Build the candidate, adapt the same probe to changed synchronous function shapes, and compare matched positions. Run the prepared Node CPU lane for seven alternating cohorts of baseline, prototype, and identical-code control, with 500 warmups and 1000 checked operations. Inspect its scripts before use: initial helpers hardcode the shell's Node version in places, while the parent Vite+ probe actually ran Node v24.16.0. Match the actual worker runtime across all arms and report it. Controller `process.version` under Bun is not the Node worker version.

Per-stage Node CPU, final candidate counts, all CPU comparisons, durable-bench competitor results, candidate transcript fingerprints, and long-thread diagnostics remain pending. Neither performance target is established by this baseline. The final corrected source has a successful full gate under the worker setting below, with the raw log and exit code retained in `prototype-final/review-dispositions.json` and `prototype-final/checks/ready-one-worker.log`.

## Full-gate concurrency diagnosis

The integrator's two `vp run ready` attempts timed out in the existing Action-publication test at its unchanged 30-second deadline. The parent reproduced that timeout with the entire testing package alone (`parent-checks/testing-package-alone.log`, exit 1), so workspace-task overlap was not required. The installed Vitest runner supports `VITEST_MAX_WORKERS` (`coverage.DM_a_rWm.js` reads it into `resolved.maxWorkers`).

`VITEST_MAX_WORKERS=1 vp run -F @yielded/agent-testing test` then passed all 128 tests in all 20 files (exit 0; `parent-checks/testing-package-one-worker.log`). The publication case completed in 11.156 seconds, and every test and timeout remained unchanged. The single-worker package run took 103.92 seconds. This establishes a usable concurrency setting; the underlying operating-system bottleneck is unproven.

`VITEST_MAX_WORKERS=1 vp run ready` subsequently exited 0 on the corrected, frozen candidate. The publication fixture passed in 20.885 seconds during that full gate. Its source fingerprint is recorded under `prototype-final/source-fingerprint.json`. Reuse this successful gate while source, dependencies and configuration remain unchanged, following the repository's evidence-reuse policy. A required rerun should keep this environment and run outside CPU measurement. Apply no test/config edits or timeout increases.

## Independent review artifacts

The parent retained the full reviewer responses in `reviews/durability.md` and `reviews/effect-boundaries.md`. Resolve corrected unrestricted public aggregation with cooperative 64-input batches; source and frozen production probes verify interruption and accounting. Their read-only tool mode supported source and retained-patch inspection but omitted terminal/Git execution. The parent independently verified all 11 current changed files against the final fingerprint, the final patch hash, the unchanged lockfile, all 284 staged production JavaScript files against current builds, and all 10 shared fixture files against the baseline. `parent-checks/freeze-verification.json` records the passing check for source fingerprint `c1b557af834cfb57e30d74459876956089050fbcd23d8d0f31a52fe3d6eaabbf`. Recheck after any source change.

The parent also independently verified every required count in all 21 CPU worker reports, recomputed the seven paired ratios, checked both counter-series closure and fingerprints, and rechecked the source identity after measurement. `parent-checks/measurement-verification.json` records the passing verification: evaluation ratio 0.9174959, allocation ratio 0.9251307, CPU ratio 1.0074023, identical-control ratio 1.0053407. Exact converted-stage CPU remains unverified; the full measurement report explains the attribution and noise limits.
