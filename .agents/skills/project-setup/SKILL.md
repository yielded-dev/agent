---
name: project-setup
description: Set up TypeScript repositories or workspace packages and integrate their tooling, Effect services, and Cloudflare deployment boundaries.
metadata:
  source: "https://github.com/danieljvdm/dev-kit/tree/2bda762e1573fbfdaf5c5aa6c06abe952d812a0b/skills/dev-kit"
  source_commit: "2bda762e1573fbfdaf5c5aa6c06abe952d812a0b"
  source_skill: "dev-kit"
---

# Project setup

Build from the user's requirements and leave ordinary repository-owned files. Start with the existing repository, package boundaries, and commands. Dan's defaults are Cloudflare and Effect for new backend work, Alchemy for infrastructure, and Vite+ for TypeScript tooling, with justified exceptions. Preserve established choices unless changing them is in scope.

Read only the relevant branch:

- Scaffold a repository or add a workspace package: [repository setup](references/repository-setup.md).
- Establish the Vite+ and TypeScript foundation: [TypeScript defaults](references/default-typescript-repository.md).
- Add or configure a Worker API: [Cloudflare API setup](references/cloudflare-worker-api.md).
- Implement Effect behavior: [Effect](../effect/SKILL.md).
- Declare infrastructure or deploy a temporary test stack: [Alchemy](../alchemy/SKILL.md).

Use [library-first](../library-first/SKILL.md) for reusable capabilities and [feedback-loops](../feedback-loops/SKILL.md) for shared validation and benchmark commands. Reuse instructions already loaded.

Generated projects must work independently of this skill catalog and the Dev Kit CLI. Keep version-sensitive API guidance in installed dependencies, and use the resulting project's commands to verify its changed entrypoints.
