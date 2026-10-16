---
name: show-me
description: Help the user understand the current topic visually with concise diagrams, code-shape sketches, and focused HTML artifacts.
metadata:
  source: "https://github.com/humanlayer/skills/tree/3c2629142c5d437428269b1b722b08c0b87f574d/plugins/show-me/skills/show-me"
  source_commit: "3c2629142c5d437428269b1b722b08c0b87f574d"
  source_skill: "show-me"
---

# Show me

Help the user understand the current topic visually. Skip the preamble and keep prose brief. Pick the smallest view that makes the key point clear; a simple answer may need no visual.

- Logic or algorithms: pseudocode.
- Runtime control flow: a call tree.
- UI structure: a component tree with the state and module boundaries that matter.
- File responsibility or a broad refactor: a shallow file tree.
- Component interaction or data flow: Mermaid; use a sequence diagram when call order matters.
- A change to an existing shape: a small `diff`. Show the whole block when it is mostly new, omitted context would hide ownership or order, or the user needs a copyable target.

Use [examples](references/examples.md) when choosing or shaping one of these views. Keep only the calls, files, props, states, and boundaries needed for the current question. Put each visual beside the short text it supports; one useful view is usually enough.

For a UI layout, state comparison, or concept that benefits from richer presentation, use an inline artifact if the client supports it, otherwise one focused HTML file. Match the product's visual language and use real labels and representative data. Add interaction only when it helps answer the question. Link the result and open it with the environment's available preview or browser tool when useful; do not assume a particular harness tool or operating-system command.

Match the destination's rendering support. GitHub PRs support Mermaid and fenced code; use the shared [PR explanation guidance](../pull-requests/references/explanation.md). Distinguish proposed or illustrative behavior from the running implementation, and do not present invented measurements as observed results.
