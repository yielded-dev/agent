---
name: testing
description: Decide whether committed tests are warranted, design regression tests, or follow explicitly requested TDD.
license: MIT
metadata:
  source: "https://github.com/danieljvdm/dev-kit/tree/2bda762e1573fbfdaf5c5aa6c06abe952d812a0b/skills/testing"
  source_commit: "2bda762e1573fbfdaf5c5aa6c06abe952d812a0b"
  source_skill: "testing"
---

# Testing

Follow the repository's testing policy and the user's requested scope. Default to no new tests or test infrastructure; verification remains required. Use existing checks and direct observation of the real workflow first. Add committed automation only for an explicit user request for tests or a reproduced regression, with a concrete gap that existing evidence cannot adequately cover. Read test selection before expanding tests, fixtures, mocks, or harnesses.

- Whether to add, retain, or remove a test: [test selection](references/selection.md).
- A test boundary, substitute, or mocking decision: [test design](references/test-design.md).
- A justified isolated test, or an explicit TDD, red-green, or test-first request: [failure-first development](references/tdd.md). Ordinary feature verification does not require this workflow.

Honor an explicit testing request within its scope. Express expectations independently of implementation, exercise a stable public boundary, and follow repository evidence and placement rules. Never replace an unnecessary unit test with a larger integration or E2E suite. Reuse valid evidence, keep the final suite green, and stop when the requested outcomes are sufficiently verified.

TDD attribution is in [NOTICE](NOTICE); terms are in [LICENSE](LICENSE).
