# Effect build evidence — cf-bench-8914

Both prepared packages contain published exports pointing at `dist`, 500 JavaScript modules with matching declarations, source maps, source, and upstream package documentation. They were swapped into the local Effect installation to build and check the benchmark variants; the repository's original patched 4.0.0 installation was then restored. This directory records package preparation, not timing measurements.

| Variant | Exact revision | Prepared package root |
| --- | --- | --- |
| Merge-base | `757821fe99b7179f907d6d1a34a4e86de4173112` | `/private/tmp/cf-bench-8914-effect-Gv32WlqM/packages/base/effect` |
| PR head | `01c6222ccf74390848595633ef23410cbfa6983b` | `/private/tmp/cf-bench-8914-effect-Gv32WlqM/packages/head/effect` |

Both manifests retain their upstream version, **4.0.1**. The temporary parent is mode **0700**. Package roots are prepared copies; patched source checkouts remain under `base/` and `head/` in the same temporary parent. Copy a complete package root, including `package.json` and declarations; do not mix files from the two variants.

## Revision selection

The original acquisition commands were:

```sh
git clone --filter=blob:none --no-checkout https://github.com/Effect-TS/effect.git /private/tmp/cf-bench-8914-effect-Gv32WlqM/upstream
git -C /private/tmp/cf-bench-8914-effect-Gv32WlqM/upstream fetch origin 01c6222ccf74390848595633ef23410cbfa6983b refs/heads/main:refs/remotes/origin/main
git -C /private/tmp/cf-bench-8914-effect-Gv32WlqM/upstream rev-parse --is-shallow-repository refs/remotes/origin/main
git -C /private/tmp/cf-bench-8914-effect-Gv32WlqM/upstream merge-base --all 01c6222ccf74390848595633ef23410cbfa6983b refs/remotes/origin/main
```

The clone has full commit history (`false` for shallow status); blob filtering does not truncate ancestry. Fetched main was `13ace20e6c0501a0d01eb2b028672081c9780e42`. `merge-base --all` returned exactly one commit: `757821fe99b7179f907d6d1a34a4e86de4173112`. The base package uses that ancestor. No fork-point heuristic, current-main build, or PR synthetic merge was used. Reproduction pins the recorded main SHA so future branch movement cannot change the pair.

## Patch and build

[effect@4.0.0.patch](effect@4.0.0.patch) is the exact repository patch, SHA-256 `0051517e101fedd1c2822d1b9787accbc7025afab1624ac97bf5ba41d54a3229`.

The source section, retained as [LanguageModel.source.patch](LanguageModel.source.patch), was applied with `git apply --check --directory=packages/effect` followed by `git apply --directory=packages/effect` to each pristine checkout. Its SHA-256 is `5a54bc2c90c923120f613a08fdd48707643c3d5e3ff3cb2108ba1ec096b7ca41`. Both apply without manual edits. Generated JS comes from the patched source, so the repository patch's prebuilt JS section is not applied separately. Both actual applied diffs are retained as `base.applied.patch` and `head.applied.patch` and are byte-identical.

The patch caches the streaming response decoder by encoded-parameter mode and ordered tool name/parameter/success/failure schema identities. Request state and tool handlers remain live. The resulting LanguageModel source hash is `5c04d84ee7a93c98aa75185282c5dd8361c2418b68d93bdfe7f4a4c8e458bb02`; generated JS hash is `98924475a7653ccf62cadaabc28eaa2557b9aaadecfb4bf3e58089c1804382ea`. Both hashes are identical across variants.

Build command used:

```sh
cd /private/tmp/cf-bench-8914-effect-Gv32WlqM/tooling
vp node /Users/dan/dev/effect-agent/.worktrees/dan-cf-bench-effect-8914/examples/durable-bench/results/cf-bench-8914/effect-builds/build.mjs /private/tmp/cf-bench-8914-effect-Gv32WlqM
```

[build.mjs](build.mjs) uses the TypeScript **6.0.3 compiler API** with upstream compiler options, ES2022/NodeNext, rewritten `.ts` import extensions, `noCheck: true`, declaration/source maps, and release-style `stripInternal: true`. It then runs upstream-pinned Babel **8.0.6** with `babel-plugin-annotate-pure-calls` **0.5.0** and promotes `publishConfig.exports` into the prepared manifest. Node is **24.21.0**, macOS arm64. The isolated tooling manifest and frozen Bun lockfile are retained here. No lifecycle scripts were enabled. Babel's two large embedded HTTP UI styling notices appear for both builds and are preserved in [build.log](build.log).

This is a matched compiler-API build, **not the upstream TypeScript 7.0.2 CLI build or a semantic typecheck**. TypeScript 7 exposes no equivalent installed JavaScript compiler API. The same alternative pipeline was used on both exact source revisions; only the six expected source modules and their generated output differ. Affected consumer packages were checked separately for all three Effect builds; see [installation and checks](../checks/install-swap.json). Upstream tests were not run.

To recreate in a fresh private directory:

```sh
vp exec sh examples/durable-bench/results/cf-bench-8914/effect-builds/reproduce.sh
```

The script needs `git`, `vp`, network access, and the configured Bun 1.4.2/Node runtime. It installs build dependencies only under the new private task directory using `vp install --frozen-lockfile --ignore-scripts`, builds both packages, and prints their paths. The original first install needed network sandbox escalation; the task-local cache/temp paths keep build dependencies outside the product repository. `build.mjs` refuses non-pristine checkouts and existing package outputs. The reconstruction script was syntax-checked; its acquisition, install, patch and build operations were performed in this run. A second full clone/build was not needed.

## Verification and hashes

[build.json](build.json) records exact source/tree SHAs, versions, tool versions, patch hashes, paths and key generated module hashes. [validation.json](validation.json) records import checks, export/declaration checks, differing files and script hashes. Root and AI namespace imports succeeded in separate Node processes. All 22 concrete manifest exports exist; every one of the 500 JavaScript modules has a declaration file. Every retained per-file content hash was recomputed and matched.

Package content identity is the SHA-256 of a sorted manifest of `sha256  relative/path` lines, including a final newline:

| Variant | File count | Manifest | SHA-256 of manifest |
| --- | --- | --- | --- |
| Merge-base | 2,581 | [base.sha256](base.sha256) | `141d0b8a65bfb4e80d70040752ad4bfc277cdf1132412b96efb860941366c143` |
| PR head | 2,581 | [head.sha256](head.sha256) | `38458c207859d25d110bf28f79ebd5f7773760c1e31dab0281c67b8bb3379a4b` |

These are package content manifests, not archive hashes. No tarballs were needed. Generated source maps use relative source paths. Exactly 23 package files differ, arising from `Context`, `MutableList`, `Queue`, `Scheduler`, `internal/core`, and `internal/effect`.

## KOM-433 count harness assessment

**Counts skipped as authorized.** The copied `source/harness-corrected/counts.mjs` cannot be used unchanged for a comparable result. An incomplete port would report an artificial head reduction. Its failure-closed checks also reject the head because `this._stack = [];` now occurs in both the fiber constructor and exit cleanup.

The changes needed for a defensible future port are:

1. Keep all nine old constructor hooks, then add dedicated `Success`, `Failure`, `WithFiber`, `WithFiberSucceed` in `dist/internal/core.js`, and `Sync`, `Suspend` in `dist/internal/effect.js`. Normalize these to the corresponding old `ExitPrimitive:<op>` / `PrimitiveImpl:<op>` labels if comparing operation categories. Generic constructor hooks no longer observe these allocations.
2. Extend the module selector to `dist/Queue.js` and count six new constructors: `QueueOffer`, `QueueTake`, `QueueTakeBetween`, `QueuePoll`, `QueueWithdrawTaker`, `QueueWithdrawOffer`. Queue operations now carry operands in fields and can reuse the same primitive on retry. The previous closure/primitive mix has genuinely changed; allocation totals still describe selected primitive objects, not closure allocations, all heap objects, or bytes. These new operation categories do not have one-to-one old queue labels.
3. Define the evaluation metric explicitly. The head's `runLoop` pushes a `ContImpl` and `continue`s before generic dispatch when no tracer context is installed. For a metric comparable to the old loop-dispatch count, insert one hook immediately before this branch as well as handling the generic path exactly once, or move the common hook ahead of both paths. Preserve the branch's `current` value before replacing it with the wrapped effect. Separately counting generic dispatches and inline continuation dispatches would expose the distinction. A hook only on the old `current[evaluate]` line misses head work.
4. Preserve `succeedWith` as a count of that method's calls. The new `continueWith` is shared by `Success`, `Sync`, `WithFiberSucceed` and queue primitives as well as `succeedWith`; moving the old inline-success hook there would silently change its meaning. Any additional continuation-delivery metric needs explicit corresponding base hooks. Neither old loop counting nor that move counts every nested direct `[evaluate]` call; queue primitives can invoke exit evaluation directly.
5. Anchor fiber ancestry inheritance to the `FiberImpl` constructor, preferably via AST ownership, instead of the now-ambiguous `_stack = []` replacement. Resetting inheritance during exit cleanup would misattribute stages. `IteratorImpl` moved out of its IIFE but still has one matching function constructor; retain its hook.
6. Revalidate stage-selector ownership against the selected product source and compare plain/count-instrumented correctness fingerprints before accepting results. Keep the same tracer configuration: a tracer context disables the head's inline `ContImpl` branch. Include hashes of all three transformed Effect modules and the variant-specific hook inventory in any evidence.

This requires a revised measurement definition and coverage proof, beyond a mechanical constructor rename. No adapted counting harness, instrumented package or count run was used in this task.
