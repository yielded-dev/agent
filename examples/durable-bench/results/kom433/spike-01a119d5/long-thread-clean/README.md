# Completed long-thread proof

Both required diagnostics passed on the frozen prototype. The command exited 0 with four complete worker batches and 56 successful samples including warmups. Each case has ten measured samples per revision.

Every recovery retained one original mutation, zero changed-handler calls, and zero whole-Thread reads, exports or observations. The read shape remained eight selected requests, 15 records and 13,100 bytes across both history ages and all three background-store sizes. The largest page contained five records and 3,166 bytes.

## Clean snapshot

The parent created an isolated local clone and applied the verified final patch. A synthetic validation commit made that clone genuinely clean for the existing diagnostic guard. The user worktree and branch were unchanged.

- Baseline: `ba5813ec33880a9063147be6e6cce94698b11725`.
- Validation commit: `4b4931979b689ea318987b3fc77eae24a86ab470`.
- Source fingerprint: `c1b557af834cfb57e30d74459876956089050fbcd23d8d0f31a52fe3d6eaabbf`.
- Candidate production build: `718658c2959cd5bc8734882d6d149722543e86d2654cf3db5c4dc1b682507352`.

All 11 changed source files and 284 production JavaScript files were byte-checked before execution. The snapshot installed its own frozen lockfile and reused the measured production build. The report records both revisions as clean and both original build hashes.

```sh
vp -C "$CLEAN_CANDIDATE" run perf:diagnose \
  --base-dir "$CLEAN_BASELINE" \
  --case long-thread-aging-256-131328 \
  --case long-thread-store-size \
  --out-dir "$NEW_OUTPUT"
```

Use new output directories. `snapshot.json` preserves the executed paths, and `exit.json` records exit 0. `verification.json` independently checks every sample and the unchanged working source. Raw results, fixture sources and worker logs remain in `diagnostics/`.

For replay, create clean local checkouts from the recorded baseline. The retained `prototype-validation.bundle` contains the validation commit with the baseline as a prerequisite; fetch its `HEAD` into the candidate checkout. Install each checkout with `vp install --frozen-lockfile`, restore the corresponding frozen `packages/*/dist` artifacts or rebuild through `vp run -F './packages/*' build`, and check the recorded identities before running the command. Observe the actual Node worker version; this run used `v24.16.0`.

This completed run supersedes the earlier dirty-checkout preflight failure retained under `../verification-20261008/long-thread/`. Elapsed diagnostic timings are informational and supply no CPU-improvement claim.
