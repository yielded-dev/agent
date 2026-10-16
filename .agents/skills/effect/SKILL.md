---
name: effect
description: Set up Effect, implement Atom React state, HTTP APIs or CLIs, and review architecture and error handling.
metadata:
  source: "https://github.com/danieljvdm/dev-kit/tree/2bda762e1573fbfdaf5c5aa6c06abe952d812a0b/skills/effect-development"
  source_commit: "2bda762e1573fbfdaf5c5aa6c06abe952d812a0b"
  source_skill: "effect-development"
---

# Effect development

Read the repository's `node_modules/effect/AGENTS.md` completely before writing or evaluating Effect code, then follow its relevant links. Installed declarations and repository policy own version-sensitive APIs and verification commands.

Read only the guide for the requested boundary:

- React queries, mutations, shared state, workflows, or SSR: [Effect Atom](references/atom/index.md).
- HTTP contracts, handlers, middleware, or server runtimes: [HTTP APIs](references/api/index.md).
- Scripts, CI automation, or command-line applications: [CLIs](references/cli/index.md).
- Error propagation, recovery, reporting, or cleanup failures: [error handling](references/errors.md).
- Trace/log instrumentation and OTLP export: [observability](../o11y/SKILL.md), then the shared [Effect OTLP snippets](../o11y/references/effect-otlp.md).
- A requested architecture review or service-boundary refactor: [architecture](references/architecture/index.md).
- Adding Effect dependencies or its agent guidance: [setup](references/setup.md).

For Effect React applications, Effect Atom owns shared state and server data; React renders and dispatches. View-local state can stay in React. Keep business logic in Effect workflows and declare cross-query invalidation as mutation reactivity keys. Read the Atom guide for runtime and registry ownership.

Preserve the requested scope and build the minimum current behavior. Use Effect's existing primitives; a service boundary does not justify extra policy layers, recovery machinery, or configuration for hypothetical uses. Treat the references as guidance for a needed boundary, not a checklist of features to implement. A component edit does not require a server rewrite or architecture audit. Follow repository policy for committed tests; changes to an atom, endpoint, or script do not by themselves justify a new test.

For reusable contracts, use [library-first](../library-first/SKILL.md). For critical-path changes or performance claims, use [measure-performance](../measure-performance/SKILL.md). Reuse instructions already loaded and read only the relevant references.
