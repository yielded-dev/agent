# Test selection

## Default to existing verification

Committed tests must prove durable value. Tests are permanent product code, not a reflexive companion to each source file. A request to implement or verify a feature does not by itself require new automation. Prefer existing checks and direct workflow observation; a missing suite does not oblige you to build one.

Before adding or expanding a committed test, both must hold:

- The user explicitly requested automated tests, TDD, or a named test seam, or you are reproducing a real regression.
- You can name the concrete uncovered failure, explain why existing tests, types, static checks, or direct workflow verification are insufficient, and justify the continuing maintenance and runtime cost.

Apply this gate to cases, assertions, fixtures, mocks, and harnesses inside existing suites as well as new files. Do not invent failure scenarios to qualify a test. Honor an explicit test request within its scope; the following criteria guide which cases and boundaries provide useful protection.

## Choose durable protection

Write a test only when every condition holds:

- It protects stable product behavior rather than an implementation detail.
- A regression is plausible and would be costly.
- The behavior is observable through a stable public seam.
- The test will survive likely refactors.
- The expected result is independent of the implementation.
- Existing evidence cannot cover the risk adequately.

If any condition fails, skip the test and use the cheapest sufficient verification instead. Moving MVP behavior often warrants no committed tests.

Test capabilities, not files. Use the fewest tests that protect the risk, and ignore coverage targets unless the user explicitly asks for them. Never turn a rejected unit test into a larger integration suite, browser test, or E2E harness. Do not add replacements merely to preserve the count or apparent coverage of removed tests. Temporary verification scripts also need a concrete question and proportional cost; leaving a harness uncommitted does not justify building it.

If isolation is necessary, explain the failure that the real workflow cannot adequately exercise and follow [failure-first development](tdd.md) before changing the implementation. Convenience, package size, and coverage targets do not establish that need. Map already-covered cases to existing proof rather than adding a test for each possible failure.

## Reuse evidence and audit retained tests

Reuse passing evidence until a relevant source, dependency, configuration, or environment change invalidates it. After a fix, rerun the affected proof; repeat the wider workflow only when its earlier evidence is invalid. An unchanged failure does not justify another broad run. Keep blocked required verification explicit and do not turn unrelated findings into new delivery requirements.

Judge existing tests by the failures they catch. Remove redundant checks and tests that only repeat schemas, constants, types, implementation calculations, or internal call order when test cleanup is in scope. Retain independent protection for costly failures that the remaining checks cannot reliably expose; age alone is not a reason to delete it. Remove orphaned fixtures and runner entries with the tests that needed them, while preserving shared infrastructure with live callers.

Never commit an intentionally failing or red-only suite. Keep time-driven tests deterministic instead of stabilizing them with wall-clock sleeps. A shared fake or in-memory implementation must fully provide the behavior its name promises; keep partial substitutes local to the tests that need them.
