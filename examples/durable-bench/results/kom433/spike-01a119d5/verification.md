# KOM-433 verification receipt

**Follow-up:** The parent subsequently completed both long-thread diagnostics using a genuinely clean, byte-matched local snapshot. All four batches and 56 samples passed. See the [completed proof](long-thread-clean/README.md) and [final report](report.md). The original attempt and its blocker remain below as historical evidence.

**Status when this receipt was written: acceptance incomplete.** Fresh pi fingerprint parity, all 18 informational competitor samples, and the final ready gate passed. Both required long-thread diagnostics were blocked before execution by the existing clean-checkout requirement. This verification does not change the [measurement recommendation to stop this performance prototype](measurement-01a11a40/report.md).

Production source was read-only. No tests, timeouts, repository configuration, production formats, or layouts were changed. No task commit, push, publication, deployment, hosted benchmark, paid model call, or external message was made.

## Identity

The local branch remains `dan/KOM-433`, based on `ba5813ec33880a9063147be6e6cce94698b11725`.

| Identity | SHA-256 |
|---|---|
| Complete retained [source patch](prototype-final/prototype-final.patch) | `a25078d625d0d2c20d9eb15a888584224e782e115cc5cf95ac5555ed0db3f2e8` |
| Eleven changed source files | `c1b557af834cfb57e30d74459876956089050fbcd23d8d0f31a52fe3d6eaabbf` |
| Baseline production build | `a3ad44235b4789408d94203bbfd8cff1295ecaa84410ee181a91694d75e678f2` |
| Candidate production build | `718658c2959cd5bc8734882d6d149722543e86d2654cf3db5c4dc1b682507352` |
| Both lockfiles | `574fa0aff46d96f79a01c9f0125154c96266363804f47a0265c5cad564952abf` |

[Preverification](verification-20261008/preverification.json) and [postverification](verification-20261008/postverification.json) confirm unchanged source/Git status and byte equality for all 283 baseline and 284 candidate production modules. Existing builds were reused. `git diff --check` passed.

Environment: macOS 26.6.2 / Darwin 25.6.0, arm64 Apple M4 Max, 16 logical CPUs, 128 GiB RAM; Bun 1.4.2, Effect 4.0.0, esbuild 0.28.1, Miniflare 5.20260811.1-alpha and workerd package 1.20260811.1. The diagnostic controller's actual Node version probe returned `v24.16.0`. Doctor advertised managed resolution 24.21.0 and warned about other version managers; that advertised resolution is not worker-runtime evidence. See the [doctor log](verification-20261008/logs/env-doctor.log) and [host metadata](verification-20261008/logs/environment.txt).

## Exact fingerprint proof

Fresh seeds used the unchanged durable-bench `seed` command: `/setup`, public `/seed` calls in 50-turn batches, awaited Miniflare disposal, then snapshot copies. The candidate consumed frozen production `dist` modules, not framework TypeScript source. The published competitor installation was reused read-only after checking its manifests, lockfile and adapter source against this checkout. Historical fixtures were inspected but their sidecars lacked current source/build provenance, so fresh public-path proof was used.

| Historical turns | pi 1.0.4 | Candidate | Exact equality |
|---:|---|---|---|
| 50 | `b017b487524e44a4` | `b017b487524e44a4` | Pass |
| 250 | `dcea9f30b0917245` | `dcea9f30b0917245` | Pass |
| 1,000 | `ac520308146f2a8f` | `ac520308146f2a8f` | Pass |
| 3,500 | `0a8c8e4b0d9a0794` | `0a8c8e4b0d9a0794` | Pass |

[Fingerprint proof](verification-20261008/fingerprint-proof.json) retains the independent comparisons, raw sidecar identities, worker-bundle hashes and production input paths. The fingerprint is the first eight SHA-256 bytes of normalized `[role,text,calls]` tuples in the **last provider request**, excluding system messages and the subsequently emitted final answer. It is not a full canonical archive digest. All 102 fixture files and all three worker bundles remained byte-identical through the comparisons.

The first candidate seed command exited 1 before any turn: the retained Node-oriented stage omitted package-local `@effect/sql-sqlite-do` and `puppeteer-core` resolution. Doctor exited 0. A task-local copy received links to those already-installed dependencies; every production module and benchmark script remained unchanged. Only the blocked candidate command was retried; passing pi seeds were reused. The [failure log](verification-20261008/logs/seed-yielded.log) and [resolution receipt](verification-20261008/durable-stage-resolution.json) are retained. Nonfatal workerd alarm-mismatch warnings during seeding and comparisons are also retained.

## Informational local wall time

Three serial cohorts ran one fresh copied-store sample per target and size: `yielded/pi/tardie` at 50 then 250; `tardie/pi/yielded` at 250 then 50; `pi/yielded/tardie` at 50 then 250. Each sample ran ten eight-tool turns. All **18 commands / 180 turns** completed successfully. No verification benchmark overlapped another benchmark or the ready gate; this work followed the completed CPU phase.

Cold is open plus the first turn, excluding Miniflare startup; warm covers the nine later turns. Values below are **median [minimum–maximum] milliseconds**, with 3 cold and 27 warm observations per cell.

| Target | 50 cold | 50 warm | 250 cold | 250 warm |
|---|---:|---:|---:|---:|
| pi-durable 1.0.4 | 56.9 [51.0–70.2] | 23.6 [21.7–29.7] | 63.1 [62.4–68.9] | 41.5 [40.0–46.6] |
| tardie 0.44.0 | 89.7 [88.3–123.4] | 33.7 [26.6–98.8] | 137.6 [137.6–143.5] | 37.8 [33.6–55.1] |
| Candidate, local beta.167 | 103.7 [101.4–118.5] | 45.4 [35.6–66.5] | 133.3 [120.2–153.5] | 68.9 [45.6–102.9] |

These are **informational elapsed wall times**, not CPU, Cloudflare billing, inference latency, a statistically established competitive ranking, or a prototype speedup. The soft 8% idle gate was exceeded in 13/18 invocations and measured anyway as designed. Returned pre-run busy fractions ranged 4.95%–10.83%; load ranged 3.02–3.79. These fractions are the quietest pre-run windows, not CPU during a turn. Host conditions changed across cohorts; no slow sample was discarded.

All raw values, including startup, open, every turn, storage, RSS, host observations and timestamps, remain in [durable-results.jsonl](verification-20261008/durable-results.jsonl). [Order](verification-20261008/durable-order.tsv), [descriptive spread](verification-20261008/durable-summary.json), and the [unchanged report output](verification-20261008/logs/durable-report.log) are retained. The harness's per-command sample index is always zero because each invocation used `--samples 1`; the order receipt supplies cohort identity.

## Required diagnostics and final gate

The command `vp run perf:diagnose --base-dir /tmp/kom433-spike-01a119d5-baseline.bcZT4R/baseline-checkout --case long-thread-aging-256-131328 --case long-thread-store-size --out-dir <new-output>` exited **1** with `Diagnostics require clean exact-commit checkouts` at `scripts/runtime-diagnostics.ts:282`.

The [raw diagnostic report](verification-20261008/long-thread/report.json) records both exact frozen build hashes, baseline `dirty: false`, candidate `dirty: true`, both requested cases, and **zero worker batches**. Neither diagnostic passed or reached a recovery operation. Committing the candidate was prohibited; the guard was not bypassed, Git cleanliness was not falsified, and no harness/test/configuration or timeout was changed. An unchanged retry would supply no new evidence.

**`VITEST_MAX_WORKERS=1 vp run ready` exited 0**, running from 07:43:13 to 07:46:02 UTC on 2026-10-08. Vite+ reused valid cached tasks and ran remaining work, including the final docs/link build. This is not a claim that every cached test was freshly executed. The complete [ready log](verification-20261008/logs/ready-final.log), including warnings and normal opt-in skips, has SHA-256 `77b8d884e849ddc04a6424eec9439e54d23813ada35df82094ad5d51147f02c8`.

[commands.json](verification-20261008/commands.json) retains exact working directories, commands, starts, finishes, exits and full-log hashes for all 26 workflow commands: 24 zero exits, the resolved candidate-stage setup failure, and the still-blocked diagnostic attempt.

## Replay and retention

1. Preserve the original baseline archive, final patch/fingerprint and frozen stages already in this evidence bundle. Recheck the identities above before reuse.
2. Extract [durable-bench-inputs.tar.gz](verification-20261008/durable-bench-inputs.tar.gz) into a **new task-owned** `consumer/examples/durable-bench` directory. It contains unchanged scripts, source adapters, manifests/competitor lockfile, all ten closed seed snapshots with complete persistence directories, sidecars, and the three measured bundles. Archive SHA-256: `9496c948833f56ae426ab45fe157ed77d5c6e3a37c0b0645c9378bc9373eb209`.
3. Link `consumer/packages` and the consumer's `node_modules/@yielded` to a task-local copy of the frozen candidate stage. Recreate installed external links using [durable-bench-inputs.json](verification-20261008/durable-bench-inputs.json) and [durable-stage-resolution.json](verification-20261008/durable-stage-resolution.json); keep framework resolution on `dist`. Reuse the matching competitor installation or install its retained lockfile through Vite+ in an isolated directory.
4. From that consumer directory, use `vp run --no-cache bench -- TARGET SIZE --samples 1 --cpu-max 0.08` in the recorded order, then `vp run --no-cache report`. To regenerate fingerprint proof instead of reusing snapshots, use `vp run --no-cache seed -- pi 50 250 1000 3500`, the same command for `yielded`, and `seed -- tardie 50 250`. **Seeding deletes its named work/output fixtures:** run only in recognized task-owned destinations. Compare all four pi/candidate sidecars explicitly; `bench` and `report` do not assert cross-target parity.
5. Replay the diagnostic and ready commands from their recorded workspace. The diagnostic clean-checkout blocker remains unresolved under the no-commit constraint.

The archive was decompressed and its fixture, source and bundle hashes independently checked before cleanup. All owned background commands completed. `lsof` found no open task files. Only `/tmp/kom433-spike-01a119d5-verification.9bhu73` was removed; shared stages, the parent baseline helper, other worktrees and other processes were left intact. See [cleanup.json](verification-20261008/cleanup.json).

## Gap ledger

| Requirement or concrete risk | Status |
|---|---|
| pi fingerprint equality at all four requested sizes | **Passed**, with the precise fingerprint scope above. |
| Informational candidate/pi/tardie 50/250 comparison and raw samples | **Completed**; host contention and limited sample independence preclude stronger timing claims. |
| `long-thread-aging-256-131328` and `long-thread-store-size` passes | **Unverified / blocked** by clean-checkout preflight; zero worker batches. |
| Final ready gate | **Passed**, with unchanged tests/timeouts and normal task-cache reuse. |
| 50% evaluation and 15% converted-stage CPU targets | **Not established** by the retained measurement phase: evaluation reduction is below target; exact converted-stage user+system CPU remains unverified. These wall-time results do not fill that gap. |
| Append caller-mutation/laziness and schema-valid false continuation-accounting negative proof | **Still unverified**; fingerprint parity and the ordinary gate do not replace those targeted negatives. |
| General host fairness and high-cardinality public-summary performance | **Unmeasured**, as retained in the integrator/measurement handoffs. |

Keep the prototype local. No rollout or merge acceptance is claimed.
