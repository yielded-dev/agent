# Reducing iteration cost

Use focused correctness checks, work counts, and profiles to locate changes. Select only the affected benchmark cases during iteration; use the same harness for the final comparison and required CI gates. A quick screen must identify what it cannot resolve. Expand samples or scenarios for the remaining acceptance question, rather than running the whole matrix after each edit.

Measure setup, build, queue, execution, and teardown time as well as the product metric. Cache expensive fixtures and build inputs outside disposable worktrees, keyed by the versions and formats that determine validity. Restore clean measured state and honor cold/warm semantics; cached setup must not change the experiment. Bound retries and waits, expose progress, and retain completed work when a run fails. Fix a recurring harness bottleneck at its owner.

If concurrent runs collide on output or mutable state, first isolate what each owns. Use [reliable operations](../../library-first/references/reliable-operations.md) when sharing is genuinely required; do not introduce machine-wide serialization to compensate for a harness design problem.
