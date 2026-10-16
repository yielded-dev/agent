---
name: feedback-loops
description: Design or improve reusable development feedback loops, benchmark harnesses, and verification commands when checks are slow, duplicated, unreliable, or missing.
---

# Fast, reusable feedback loops

Optimize time to a trustworthy answer. Name the behavior, invariant, or metric that decides the next step and run the smallest check that answers it. Reuse valid evidence. Broaden or repeat checks for changed inputs, a failure, or an unresolved risk; do not verify every action independently.

Use the existing app or library harness. When a recurring question lacks one, improve its owner's tooling and share the real operation across agent commands, tests, and CI. Small exploratory probes are useful; promote recurring measurements into the harness. Keep raw run evidence in artifacts.

- Building a harness, CI gate, or durable check for a recurring mistake: [harness design](references/harness-design.md).
- Selecting cases, caching fixtures, or fixing slow setup, queues, and failures: [iteration cost](references/iteration-cost.md).

Use isolated Cloudflare resources when they shorten feedback or provide the actual runtime. Their creation and teardown through Alchemy are pre-authorized; [temporary stacks](../alchemy/references/temporary-stacks.md) owns isolation, reuse, and cleanup.

Never build or wait for a laptop-wide CPU lock or an idle machine. Local deterministic checks need no exclusivity. Move contention-sensitive timing to isolated cloud resources and run independent jobs concurrently within actual provider/project limits. Account for cloud noise too.

For traces and attribution, select [observability](../o11y/SKILL.md). For comparable measurements and performance acceptance, use [measure-performance](../measure-performance/SKILL.md); reuse already loaded guidance.
