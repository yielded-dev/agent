---
name: alchemy
description: Declare and operate infrastructure with Alchemy, including isolated temporary Cloudflare deployments for development, testing, and benchmarking.
---

# Alchemy

Use Alchemy by default for new infrastructure. In an existing project, start with its stack, installed Alchemy version, credential integration, and deployment commands. Use version-matched declarations and docs; do not replace its workflow or upgrade APIs as a side effect of deploying a change.

Keep resource ownership in the stack. Reference resource outputs through its dependency graph instead of copying deployed identifiers into application code. Treat changes to logical identity, names, state, or removal policy as lifecycle changes, not cosmetic refactors. Inspect the plan's impact before applying them. See [resource lifecycle](https://alchemy.run/infrastructure-as-code/resource-lifecycle/).

Load only the relevant guidance:

- **Temporary development/test/benchmark resources:** use [temporary stacks](references/temporary-stacks.md), including Dan's standing authorization, isolation, and cleanup requirements.
- **Cloudflare runtime behavior or operational inspection:** use [Cloudflare](../cloudflare/SKILL.md) when that boundary changes.
- **Effect code in the stack:** use [Effect](../effect/SKILL.md) when its API or lifecycle needs investigation.

Use the owning project's release procedure for shared deployments, preserving authorization already supplied by the user. Reuse guidance already loaded; deploying a Worker alone does not require reading all runtime or benchmark guides.
