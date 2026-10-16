---
name: simplify
description: Simplify code or workflows by removing unnecessary machinery while preserving required behavior.
disable-model-invocation: true
---

# Simplify

Start from the requested outcome and what it actually requires. Follow repository guidance and keep the review focused on that outcome. Look for unnecessary work, complexity, and weak assumptions in the solution. Distinguish inherited assumptions from user requirements and external contracts.

Prefer deleting over simplifying, simplifying over optimizing, and optimizing over automating. First look for dependencies, alternate paths, and handoffs that can disappear entirely. Reassess what becomes simpler once they are gone. Optimize or automate only when the task calls for it.

Check relevant callers, stored data, and operational dependencies before removing behavior. Make justified changes within scope; do not stop at recommendations. Use a reversible candidate diff and focused checks when dependencies are uncertain. If deletion would change the assignment, make that choice explicit. If no worthwhile simplification is supported, leave the implementation alone and say so; a review need not produce a diff.

Keep the minimum useful implementation, including inside libraries. Preserve a small boundary that owns reusable behavior; remove machinery that the current contract does not need. The [library-first](../library-first/SKILL.md) guidance owns that distinction. In Effect code, preserve service requirements, typed failures, and scoped lifetimes; use existing primitives rather than replacing them with custom wrappers.

- Reviewing durability, validation, verification stages, or agent prompts: [durability and verification](references/durability-and-verification.md).
- Explicitly requested pre-production removal of legacy compatibility: [no-legacy pass](references/no-legacy.md). Ordinary simplification does not establish that premise.

Preserve required contracts and usable pending or error states. Use existing evidence and relevant checks; small changes do not need a new audit, checklist, benchmark suite, or permanent proof artifact. Critical-path changes still need [performance evidence](../measure-performance/SKILL.md). Report the removed mechanism and any constraint that required it to remain when useful to review the change.
