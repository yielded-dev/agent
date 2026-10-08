# Gate sidecar receipt — 2026-10-08

Status: **full ready has not passed**. No gate or heavy test process remains running.

HEAD and the locally recorded `origin/main` are both
`8c05714de84d68961b14e5ab7a3b7d809599563f`. Product code, tests, test timeouts,
dependencies, and task definitions were not edited by this sidecar. No commits
or delegation.

## Environment change

`validation-env.cjs` now sets `VITEST_MAX_WORKERS=1` in Node package children
under `packages`, `tooling`, and `examples`. It leaves the root and both
Cloudflare packages unset; see `gate-env-scope.log`. This bounds per-package
file workers without changing the existing Cloudflare pools or test selection.

The root still schedules four package test jobs (`package.json:25`). An outer
concurrency limit cannot override that explicit nested limit: installed Vite+
documentation says command-line limits take precedence over
`VP_RUN_CONCURRENCY_LIMIT` (`node_modules/vite-plus/docs/guide/run.md:349`).

## Proof and preserved failures

- `ready-initial-failed.log` and `ready.log` remain unchanged. Their checksums
  are in `gate-source-receipt.log`. The prior task receipt is
  `gate-prior-details.log`: sandbox-local failed; three sibling jobs exited 137.
- The original focused failure remains at the beginning of `sandbox-check.log`.
  The appended complete package run passed all nine tests with the existing
  500 ms deadline (`packages/sandbox-local/test/local-sandbox.test.ts:204`).
- `ready-bounded-workers.log` records a full `vp run --no-cache ready` attempt
  from 20:49:20Z to 20:52:01Z, using managed Node 24.21.0 and the worker cap.
  Static/export checks passed (zero errors; 115 existing lint warnings).
  Sandbox-local passed 9/9 within that full gate. Storage-memory,
  cloudflare-memory, and effect-agent also passed.
- That full attempt failed six local-Git cases in `tooling/pr-review-eval`,
  each at its existing 5 s deadline. The task receipt is
  `gate-bounded-details.log`; platform-node, platform-cloudflare, and
  context-continuity-eval were canceled with exit 137. Build was not reached.
  All 42 recorded tasks had caching disabled.
- `gate-pr-review-isolated.log` ran **all** tests in that package, alone,
  with the same runtime and worker cap. It reproduced all six Git timeouts:
  13 passed / 6 failed, exit 1. Package overlap is therefore insufficient to
  explain this remaining failure.

## Diagnosis and bounds

The original sandbox failures were sensitive to process-launch pressure:
bounding Node workers allowed the unchanged suite to pass in the full gate.
The remaining blocker is reproducible host child-process latency, independent
of the benchmark production target. The underlying host mechanism is **not
identified**; no claim is made that an OS service, security product, or Node
regression caused it.

- `gate-git-startup.log`: isolated `git --version` took 255–260 ms.
- `gate-executable-comparison.log`: an absolute `/usr/bin/true` took
  253–254 ms, eliminating Git work and PATH lookup as sufficient explanations.
- `gate-spawn-phases.log`: `spawn()` returned in under 3 ms; child output
  arrived about 257 ms (Git) / 275 ms (Node) after launch.
- `gate-runtime-comparison.log`: already-installed, repo-supported Node
  22.22.1 and 24.16.0 showed the same floor as the default 24.21.0.
- `gate-native-spawn.log`: Python also observed about 254–258 ms for the same
  trivial children. This is not specific to the Node child-process API.
- Sandbox escalation, detached/non-detached launch, PTY launch, and documented
  I/O/QoS/application scheduling probes did not remove the floor. Their
  `gate-*.log` receipts are retained. Scheduling changes applied only to those
  short-lived diagnostic processes; none was added to the gate invocation.
- `gate-child-state.log` observes only the diagnostic children. Their process
  status remained runnable; it does not establish which host subsystem delayed
  execution.

The Git fixtures launch many real commands (fixture creation starts at
`tooling/pr-review-eval/test/local-git-repository.test.ts:24`). Opening each
pinned repository performs six sequential Git commands before blob reads
(`tooling/pr-review-eval/src/local-git-repository.ts:124`, `:132`, `:154`, `:313`).
A roughly 250 ms cost per child consumes the unchanged 5 s budget quickly;
serializing unrelated packages does not remove that fixed cost.

Both `vp env doctor` receipts pass. No timeouts, clocks, assertions, subprocess
implementations, or test filters were modified. The worker cap is the only
retained environment change. Cached or canceled suites are not claimed as a
complete gate pass.

## Remaining execution-context check

The attempted comparison through a fresh Terminal session was rejected by
Computer Use: `Computer Use is not allowed to use the app 'com.apple.Terminal'
for safety reasons.` No Terminal command ran, and no alternate route around
that restriction was attempted.

After the owner's target-contract run, the following is the unchanged gate
invocation to compare from an ordinary terminal at the repository root. Use a
new log name to preserve every prior failure:

```sh
VP_NODE_VERSION=24.21.0 \
NODE_OPTIONS="--require=$PWD/examples/durable-bench/results/prod-path/validation-env.cjs" \
vp run --no-cache ready > examples/durable-bench/results/prod-path/ready-terminal.log 2>&1
```

A successful independent invocation or a host execution fix is still required
before claiming full readiness. The failed gate alone does not establish a
product regression, and the sidecar does not supply a full-ready pass.
