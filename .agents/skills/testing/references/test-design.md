# Test design

Observe behavior through the public boundary that owns it. Assert an expected result derived from the requirement, a worked example, or independent fixture, rather than repeating the implementation's calculation.

Use the real workflow by default. If isolation is justified, choose the public boundary that owns the failure with the fewest irrelevant moving parts and follow [failure-first development](tdd.md) before changing the implementation. Persistence requirements may need real storage and migrations; UI interaction failures may need a browser. Use the existing runtime and consumer entrypoint required by the failure. Avoid mocks of internal collaborators that only prove the current wiring, or a private helper chosen solely because it is easy to mock.

Control nondeterministic inputs such as time or an external service when the failure requires it. Use the repository's existing clock controls rather than wall-clock sleeps. A substitute must faithfully provide the behavior its interface promises. Keep its use local unless several justified tests need it.

Use assertions that distinguish the failure from success and survive unrelated refactors. Do not add companion tests merely because a new source file exists.

Record the command or replay steps, relevant environment, expected result, and observed result when they are needed to repeat the proof. Reuse existing output, traces, screenshots, or resulting files as evidence; do not create a reporting system to document a small check. Keep credentials and private payloads out of retained artifacts. Follow repository placement and runner conventions, and audit callers before removing public testing helpers or shared acceptance infrastructure.
