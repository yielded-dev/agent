# Failure-first development

Use this workflow for an explicitly requested TDD cycle or a justified test that isolates a system, including integration tests with substitutes. Apply [test selection](selection.md) first. Use the repository's testing policy, command authority, and public boundaries. Reuse the behavior and test boundary the user already named. When those are unspecified, choose the relevant public boundary from the existing design; ask only if the choice changes the intended behavior or scope.

1. Before implementing or fixing the scoped behavior, record the relevant inputs, expected outcomes, and observable failure modes in working notes. Include only applicable cases such as invalid input, authority, state transitions, partial writes, cancellation, or recovery. Identify uncertainty rather than inventing an exhaustive matrix.
2. Map those failures to existing evidence first. Write only the necessary missing tests, with expectations derived independently from the public contract. Confirm meaningful failures for the behavior being changed, rather than broken setup or missing infrastructure. Record existing guarantees that already pass separately; there is no test-per-item quota.
3. Implement enough to pass, then refactor when it improves the code while keeping the suite green. If a new failure mode emerges, reproduce it before changing the implementation for it.

A regression permits a reproducer before the fix; it does not justify unit assertions added after the fix merely to match the code. When the implementation is already written, use existing checks and direct workflow verification. Do not manufacture a retrospective red-green cycle or build a new E2E suite to replace an unnecessary isolated test.

For a difficult choice of boundary or substitute, read [test design](test-design.md). Avoid a new abstraction solely to make implementation details easier to mock.

Keep the final suite green and obey the repository's evidence and placement rules. A TDD request authorizes test-first work within the requested task; it does not require a separate confirmation at every cycle.
