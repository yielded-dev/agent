# Retries, recovery, and concurrent state

Use this guidance when the supported workflow actually includes retries, resumable work, or concurrent writers, or an observed failure requires them. A state-changing operation alone does not justify a recovery system. Implement only the guarantees current callers need, using existing runtime and storage capabilities.

Where recovery is required, resume from observable state and reconcile toward the intended outcome. Make partial work recognizable and cleanup ownership explicit, so a crash does not require guessing what happened.

Where an effect must happen once, use a stable operation identity and the storage or provider's actual atomicity and deduplication guarantees. A separate read-then-write check is not protection against concurrent retries. If an external action may have succeeded before its response was lost, reconcile its outcome before retrying. State remaining uncertainty when the provider cannot resolve it; do not claim exactly-once behavior without the required guarantees.

Before introducing a lock, check whether writers need one shared object. Independent runs can own distinct stages, keys, files, or output directories and combine results when reading. Where a shared invariant is real, use structural enforcement such as a single writer, transaction, or atomic conditional update. A prose ownership convention alone does not prevent races.

In Effect code, use the installed resource, fiber, scheduling, and concurrency primitives. Give resources and child work an explicit Scope owner; prefer scoped acquisition and fibers over detached Promises or custom lifecycle machinery. Finalizers handle normal scope closure and interruption, but cannot replace durable recovery after process loss. In-memory coordination protects only participants sharing that primitive; a local Semaphore does not enforce a cross-worker storage invariant.

Keep expected failures typed and preserve defects, interruption, and cleanup failures when translating or recovering. Retry only classified safe failures; cancellation is not a request to restart the operation. Use the existing [Effect error-handling guide](../../effect/references/errors.md) for Cause-sensitive recovery and finalization, without rewriting ordinary catches that already preserve the required semantics.

Do not turn runtime concurrency requirements into a laptop-wide benchmark lock. [Feedback loops](../../feedback-loops/SKILL.md) owns execution placement and [Alchemy's temporary stacks](../../alchemy/references/temporary-stacks.md) owns isolated deployment and cleanup.

Verify the promised behavior or observed failure through the existing public-boundary harness. Add a focused regression test when fixing a real case; do not build a failure-injection matrix for hypothetical guarantees.
