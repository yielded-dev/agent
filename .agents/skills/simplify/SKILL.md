---
name: simplify
description: Review or simplify code and workflows by questioning architecture and removing unnecessary machinery while preserving required behavior.
disable-model-invocation: true
---

# Simplify

Start from the requested outcome and what it actually requires. Follow repository guidance and keep the review focused on that outcome. Look for unnecessary work, complexity, and weak assumptions in the solution. Distinguish inherited assumptions from user requirements and external contracts. Current code and tests establish what happens, not by themselves what must remain.

Prefer deleting over simplifying, simplifying over optimizing, and optimizing over automating. Question whether whole responsibilities, subsystems, dependencies, alternate paths, and handoffs need to exist before cleaning up their implementation. Sketch the smallest design that satisfies the actual requirements; treat the current architecture as a candidate, not a constraint. Reassess what becomes unnecessary after each proposed removal. Optimize or automate only when the task calls for it.

Match the requested deliverable. A review produces findings and tradeoffs for a reviewer to decide; an implementation request authorizes justified changes within scope. For a large feature, branch, or architecture review, follow the [architecture review workflow](references/architecture-review.md) and write a short memo of what to cut, what replaces it, and what stays. When implementation is already requested, use that reasoning to guide the work without adding a report approval gate. Small, focused changes do not need the full workflow.

Check relevant callers, stored data, and operational dependencies before removing behavior. Use a reversible candidate diff and focused checks when dependencies are uncertain. If deletion would change the assignment, make that choice explicit. If no worthwhile simplification is supported, leave the implementation alone and explain what makes the current machinery necessary. Judge simplification by responsibilities and maintenance removed, not code moved, renamed, or a deletion quota.

Keep the minimum useful implementation, including inside libraries. Preserve a small boundary that owns reusable behavior; remove machinery that the current contract does not need. The [library-first](../library-first/SKILL.md) guidance owns that distinction. In Effect code, preserve service requirements, typed failures, and scoped lifetimes; use existing primitives rather than replacing them with custom wrappers.

- Reviewing durability, validation, verification stages, or agent prompts: [durability and verification](references/durability-and-verification.md).
- Explicitly requested pre-production removal of legacy compatibility: [no-legacy pass](references/no-legacy.md). Ordinary simplification does not establish that premise.

Preserve required contracts and usable pending or error states. Use existing evidence and relevant checks; small changes do not need a new audit, checklist, benchmark suite, or permanent proof artifact. Critical-path changes still need [performance evidence](../measure-performance/SKILL.md). Report the removed mechanism and any constraint that required it to remain when useful to review the change.
