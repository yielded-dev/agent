**Effect evaluation cost: evidence and replay**

Start with [report.md](report.md). This is a closed, task-specific experiment, not a product package or a general benchmark framework. All timing evidence was collected on deployed Cloudflare resources through Alchemy. Local execution was restricted to deterministic counts, checksum/finalization proof, offline reduction and the repository's normal handoff gate. The copied historical Node timing harness was not executed.

| Artifact | Purpose |
|---|---|
| [cleanup.json](cleanup.json) | Aggregate API verification for all 13 created Worker names and their DO namespaces, including the failed startup. |
| [verify-cleanup.mjs](verify-cleanup.mjs) | Read-only supplemental check against the captured account hash, all 13 Worker names, all 12 recorded namespace IDs and removed private state directories. |
| [cleanup-supplemental-status.json](cleanup-supplemental-status.json) | A later recheck was blocked by a changed account context; its unguarded predecessor was invalidated. Original account-guarded Alchemy cleanup receipts remain valid. |
| [validation.json](validation.json) | Full repository handoff gate and retained earlier environment/timeout failures. |
| [calibration](calibration/) | Twenty cases including the empty loop, deployed Worker/Alchemy stack/controller, count-only builder, counts, telemetry, receipts, model fits and all-build tables. |
| [calibration-race](calibration-race/) | Focused race supplement chosen after attribution; ordered checksum and interruption/finalization proof, independent deployed controls. |
| [attribution](attribution/README.md) | Repeated deterministic source-mode counts, plain fixture control, exact source selectors and attribution semantics. |
| [real-turn](real-turn/) | Yielded-only fallback Worker/control, seeded/reopened fixture, invocation telemetry, canonical table checks and explicit incomplete cohort. |
| [projection-tables.md](projection-tables.md) | Separate count-model projections for stages, sites and modules, and conditional conversion scenarios. These models fail whole-turn validation. |
| [synthesize.py](synthesize.py), [figures.py](figures.py) | Offline arithmetic and standalone PNG/SVG figure generation from retained measurements. |
| [kom433-source](kom433-source/) | Requested copy of the original report, corrected harness and discovery documents. Original report links outside this copied subset are historical references, not newly collected evidence. |
| [compression.json](compression.json) | Lossless packing receipts with compressed and original byte hashes. |
| [artifact-verification.json](artifact-verification.json) | Frozen bundle identity checks, source-fixture checks and secret scan outcome. |
| [offline-replay.json](offline-replay.json) | Byte-for-byte reproduction of packed numeric reductions with their expected incomplete-telemetry exit statuses. |

Large raw JSON/JSONL files are gzip-compressed without changing their uncompressed bytes. The analyzers and Python reducers accept either form. Telemetry here means the sanitized, per-event field projection used for analysis: request authorization headers, account ID and credentials are not retained. Query inputs, available sampling metadata, repeated polls, failed receipts and missing-join reasons are retained. Immutable frozen uploaded bundles are in each lane's `build-identities/bundle-*.mjs.gz`; their uncompressed hashes match the deployment manifests. Source snapshots, runtime-file inventories and fixture hashes distinguish the measured build from later changes to the controller or reducer.

**Offline replay.** From the repository root, with its Vite+ dependency tree installed:

```sh
task_evidence="$PWD/examples/durable-bench/results/effect-eval-cost"
vp node "$task_evidence/calibration/analyze.mjs"
vp node "$task_evidence/calibration-race/analyze.mjs"
vp node "$task_evidence/real-turn/analyze.mjs"
vp exec python3 "$task_evidence/synthesize.py"
```

The three analyzers intentionally exit **1** after saving their complete reductions because the original capture contains missing telemetry/failures. This preserves incomplete proof rather than making the run look complete. They do not deploy or execute workloads. The expected coverage is 2,817/2,940 main joins, 538/560 race joins, and 123 included real-turn CPUs from 13 completed cohorts. Output tables and numeric JSON should reproduce; newly expanded JSON may coexist with its archived `.gz` until repacked.

`figures.py` needs Matplotlib and NumPy; the versions used are in [figures/environment.json](figures/environment.json). Use a temporary Python environment rather than adding repository dependencies. For this capture:

```sh
MPLCONFIGDIR=/private/tmp/effect-eval-cost-20261008/matplotlib vp exec /private/tmp/effect-eval-cost-20261008/plot-venv/bin/python "$task_evidence/figures.py"
```

That path is machine-specific scratch. The committed PNG and SVG are standalone. A normal temporary environment with the recorded versions can regenerate them elsewhere.

To inspect a compressed raw file without unpacking it:

```sh
vp node --input-type=module -e 'import {readFileSync} from "node:fs"; import {gunzipSync} from "node:zlib"; console.log(gunzipSync(readFileSync(process.argv[1])).toString())' "$task_evidence/calibration/hosted/telemetry-pin.json.gz"
```

The [attribution README](attribution/README.md) gives separate fresh-output commands for two independent count captures and their plain control. It describes why `outside-eight-stages`, unassigned dynamic sites and unknown construction origins are different quantities. Do not use the copied KOM-433 `cpu-*` scripts for this task: they collect local timing.

**Rebuilding and collecting a new deployed run.** The captured product revision is `8c05714de84d68961b14e5ab7a3b7d809599563f`, tree `2281b0377b65cbc5355223a518472e5ce228dfae`. Use an isolated checkout at that revision with the harness copied in, or record a deliberately different source identity. Install with `vp install --frozen-lockfile`; the capture also built the Cloudflare platform dependency graph through Vite+ (`vp run --filter '@yielded/agent-platform-cloudflare...' build`). Both real-turn and attribution bundles resolve workspace package source and patched Effect dist through the repository's exports. Their exact input inventories are retained; do not infer source/dist resolution from whether a package build has run.

Never overwrite this closed capture or recreate its deleted resource names. Copy only each lane's `.ts` and `.mjs` harness files to a **fresh sibling directory at the same repository depth**, such as `examples/durable-bench/results/effect-eval-cost-replay/`, and run them there. The builders will create new count/build metadata; controllers refuse an existing resources/plan file. Also copy `synthesize.py` and the desired offline helpers if reproducing analysis. No credentials belong in that directory.

Set `EVAL_COST_PRIVATE` to a new private temporary directory (mode 700). `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN` come from the repository environment. Do not echo them. No model-provider keys are used. The recorded preparation for upstream variants was:

```sh
task_repo="$PWD"
task_head=01c6222ccf74390848595633ef23410cbfa6983b
task_base=757821fe99b7179f907d6d1a34a4e86de4173112
task_upstream_main=13ace20e6c0501a0d01eb2b028672081c9780e42
git clone --filter=blob:none --no-checkout https://github.com/Effect-TS/effect.git "$EVAL_COST_PRIVATE/upstream"
git -C "$EVAL_COST_PRIVATE/upstream" fetch origin "$task_head" "$task_upstream_main"
git -C "$EVAL_COST_PRIVATE/upstream" checkout --detach "$task_head"
git -C "$EVAL_COST_PRIVATE/upstream" merge-base "$task_head" "$task_upstream_main"
git -C "$EVAL_COST_PRIVATE/upstream" worktree add --detach "$EVAL_COST_PRIVATE/merge-base" "$task_base"
git -C "$EVAL_COST_PRIVATE/upstream" apply --directory=packages/effect --exclude='*/dist/*' "$task_repo/patches/effect@4.0.0.patch"
git -C "$EVAL_COST_PRIVATE/merge-base" apply --directory=packages/effect --exclude='*/dist/*' "$task_repo/patches/effect@4.0.0.patch"
```

Verify that `merge-base` prints the recorded base before building. The source hunk is the same repository patch; generated dist hunks are excluded because these variants are built from source. Main and race builders transpile the upstream source identically and resolve only the selected Effect dist. Pin resolves the repository's patched published 4.0.0. The build step collects deterministic E/A counts locally, not timing.

For a fresh copied harness at `$task_capture`, run sequentially:

```sh
vp node "$task_capture/calibration/build.mjs"
vp node "$task_capture/calibration-race/build.mjs"
vp node "$task_capture/calibration-race/proof.mjs"
vp node "$task_capture/real-turn/build.mjs"
vp node "$task_capture/calibration/run.mjs" dry-run
vp node "$task_capture/calibration-race/run.mjs" dry-run
vp node "$task_capture/real-turn/run.mjs" dry-run
```

Each micro controller supports `deploy`, `readiness`, `pilot`, `measure`, `resume`, `gc`, `telemetry`, `cleanup`, or `all`. `all` uses `finally` to destroy recorded resources. If running commands individually, **always finish `cleanup` even after a failed measurement**. For real turns, use `deploy`, `seed`, `activate`, `measure`, `telemetry`, `cleanup`, or `all`. `activate` changes only an environment generation marker and proves that seeded objects reopen in a new runtime before the separately invoked recovery and measured turns.

The main micro schedule contains 2,940 invocations (including exact-N auxiliary baselines); the race schedule has 560. Whole-turn seed creates 14 independent objects, two Workers × seven cohorts, with a 140-turn plan. The CPU ceiling is 300,000 ms per DO invocation. Worker, namespace and stack names use `effect-eval-cost` prefixes and a fresh random run suffix. Every DO uses `locationHint: "wnam"`; this does not guarantee the same physical CPU.

`resume` does not repeat an attempted invocation with unknown outcome. Microcases skip recorded attempts. Real turns abandon an interrupted cohort and continue only untouched cohorts. Poll telemetry again before cleanup if ingestion is incomplete, retaining every raw poll; absence is never an implied zero or a reason to retry a canonical turn. The final cleanup command performs Alchemy destruction, API Worker 404 checks and a paginated namespace absence check before removing private state/auth. A new run is closed only after those checks pass; do not reuse this capture's aggregate cleanup receipt for it.

**Validation scope.** This task added no product tests or product code. Existing repository checks and direct workflow evidence were used. `vp run ready` passed with hermetic Git configuration and a serial Vitest worker limit after earlier Git-fixture timeouts and an orphaned workerd were diagnosed. Original timeouts were not increased and tests were not weakened. The ordinary repository gate is distinct from the deliberately nonzero offline analysis exits for incomplete hosted telemetry. See [validation.json](validation.json) and its logs for the exact commands and limitations.
