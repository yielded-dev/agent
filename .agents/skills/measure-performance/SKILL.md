---
name: measure-performance
description: Measure and protect latency, throughput, CPU, and resource use. Use for changes on critical user paths such as APIs and agent loops, including prompt or tracing changes, and for performance investigations or optimization claims.
---

# Measure and protect performance

Performance is required behavior. A feature request does not authorize a regression. Apply this workflow to critical-path changes, including prompts, refactors, and instrumentation.

Name the affected path, primary outcome, representative workload, and relevant budgets. Inspect added sequential calls, queries, serialization, or scaling work before implementing. Being under an absolute budget does not excuse a meaningful regression. For optimizations, state the improvement sought.

Capture or identify a comparable baseline before changing the path. Iterate with the smallest reusable loop: focused correctness checks, operation counts, and profiles when attribution is needed. Reuse valid evidence while relevant code, workload, and environment remain unchanged; do not rerun the full matrix or recapture profiles after every edit.

- Defining API/agent-loop metrics or choosing instrumentation: [critical paths](references/critical-paths.md), then only the relevant [observability](../o11y/SKILL.md) references.
- Running, interpreting, or reporting a measurement: [comparison](references/comparison.md), including baselines, noise, completed work, and evidence.
- Missing, duplicated, or slow verification tooling: [feedback-loops](../feedback-loops/SKILL.md).

Measure Cloudflare paths on deployed resources. No idle-laptop waits or global CPU locks; use pre-authorized [temporary Alchemy stacks](../alchemy/references/temporary-stacks.md).

Before calling a change complete or ready to land, provide matched before/after evidence for the actual user path. Functional checks and reduced operation counts alone do not prove performance. Early PRs allowed by repository policy must identify the gap.

Fix or drop material regressions. Accept a quantified tradeoff only with the user's explicit decision. Insufficient evidence leaves performance unverified. Revert ineffective optimizations unless another stated benefit justifies them within scope.
