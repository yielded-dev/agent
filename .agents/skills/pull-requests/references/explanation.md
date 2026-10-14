# Explain architecture and APIs

Pick the smallest view that makes the change clear, and place it beside the
short explanation it supports. Prefer a diagram or example over a long prose
description; simple changes can stay prose-only. Keep only the calls, files,
props, states, and boundaries the reviewer needs.

The `show-me` views fit PR bodies well. Shape each one to the change:

- Logic change: pseudocode, or a `diff` of it.
- Runtime control flow: a call tree, or a `diff` of the call tree.
- UI structure: a component tree with the state and module boundaries that matter.
- Broad refactor or file responsibility: a shallow file tree, or a `diff` of it.
- Interaction or data flow: Mermaid; a sequence diagram when call order matters.

GitHub renders Mermaid and fenced code, not HTML, so skip `show-me`'s HTML option
in PRs.

- For meaningful changes to component ownership, boundaries, or data flow,
  include a focused Mermaid chart when it explains the change more clearly than
  short prose. Routine fixes and local branching need no diagram. Name the actual
  components, label the interactions, and make the changed responsibility or path
  clear without mapping the whole system.
- For new or changed APIs, show a concrete caller example when it clarifies a
  changed contract or subtle behavior: an HTTP request and response, or a typed
  function/SDK call and its result. An obvious edit needs no sample. Include the
  inputs, outputs, and error behavior relevant to the change. Use a small
  before/after diff when callers must migrate; show the complete example when
  the API is new.

Match diagrams and examples to the final implementation, use safe fixture data,
and distinguish illustrative or expected output from output actually observed
during validation. Include both a chart and an API example when they answer
different review questions, not just to fill sections.

## Performance claims

Support performance claims with a before/after table comparing the target-branch
baseline and PR candidate. Identify the revisions, workload, measurement
conditions, units, and relevant variability so reviewers can interpret the
comparison. Report measured results; label estimates and avoid claiming gains
without a comparable baseline.
