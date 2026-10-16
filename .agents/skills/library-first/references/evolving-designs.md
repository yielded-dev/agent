# Evolving a design

When a requirement no longer fits, consider what the design would look like if it had included that requirement initially. Use that to identify the right ownership boundary and remove special-case coordination. Deliver the smallest coherent change within scope; this question does not authorize a rewrite.

Remove obsolete behavior and duplicated decisions that the change actually replaces. Preserve working contracts and useful library boundaries. A smaller diff is not better if it leaves each consumer rebuilding the same workflow.

Sequence migrations around meaningful, checkable milestones. Batch dependent edits within a milestone and verify before building further on it. Reuse applicable evidence and required repository checks; do not run a full suite after each mechanical edit or create infrastructure unrelated to the next milestone.

For internal APIs whose callers are all controlled together, migrate those callers and remove the obsolete implementation in the same coordinated change. Public consumers, persisted data, and rolling deployments need their real compatibility and migration policy. Finish with coherent types, usage, examples, and documentation. Follow the target repository's publication policy and current user authorization; a refactor does not itself authorize a merge or release.
