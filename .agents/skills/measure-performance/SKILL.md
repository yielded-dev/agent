---
name: measure-performance
description: Investigate performance or verify optimization claims and changes likely to materially affect a demonstrably important, performance-sensitive path. Do not use for routine UI or feature changes solely because they touch an API, agent loop, or shared code.
---

# Measure and protect performance

Use this workflow when the user asks for performance work, when making a performance claim, or when there is a concrete reason to expect a material performance impact on an important path. Identify that path and reason before deciding to benchmark. High frequency, user-visible latency, substantial resource cost, or an established performance budget can make a path important; an API, agent loop, prompt, instrumentation, or shared library is not automatically critical just because of its category.

Routine UI and feature changes, small refactors, and changes with no plausible material impact do not require a benchmark, baseline, or performance tradeoff approval. Ordinary correctness checks and sound engineering judgment are enough. If performance risk is uncertain, start with a proportionate inspection or existing evidence; measure only if that reveals a meaningful concern. When explicitly asked to investigate or optimize performance, use the workflow even if the path's importance is not yet established.

Name the affected path, primary outcome, representative workload, and relevant budgets. Inspect added sequential calls, queries, serialization, or scaling work before implementing. Being under an absolute budget does not excuse a meaningful regression. For optimizations, state the improvement sought.

Capture or identify a comparable baseline before changing the path. Iterate with the smallest reusable loop: focused correctness checks, operation counts, and profiles when attribution is needed. Reuse valid evidence while relevant code, workload, and environment remain unchanged; do not rerun the full matrix or recapture profiles after every edit.

- Defining API/agent-loop metrics or choosing instrumentation: [critical paths](references/critical-paths.md), then only the relevant [observability](../o11y/SKILL.md) references.
- Running, interpreting, or reporting a measurement: [comparison](references/comparison.md), including baselines, noise, completed work, and evidence.
- Missing, duplicated, or slow verification tooling: [feedback-loops](../feedback-loops/SKILL.md).

Measure Cloudflare paths on deployed resources. No idle-laptop waits or global CPU locks; use pre-authorized [temporary Alchemy stacks](../alchemy/references/temporary-stacks.md).

Before calling a change complete or ready to land, provide matched before/after evidence for the actual user path. Functional checks and reduced operation counts alone do not prove performance. Early PRs allowed by repository policy must identify the gap.

Fix or drop material regressions. If a material regression is unavoidable, explain the quantified tradeoff and alternatives and seek the user's decision; ordinary measurement noise or a negligible cost does not require approval. Insufficient evidence leaves performance unverified. Revert ineffective optimizations unless another stated benefit justifies them within scope.
