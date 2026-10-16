# Public packages and upstream contributions

Write for the package's maintainers and users. Explain the reproducible library problem, supported use case, and resulting behavior without assuming knowledge of the downstream project that prompted it.

## Learn the maintainer's conventions

Read the contribution guide, PR template, and relevant maintainer feedback. Inspect a few recent merged PRs written or revised by active maintainers, preferably in the same subsystem and of similar scope. Start with two or three useful examples; reuse examples already examined in this session.

Adapt the title, amount of explanation, terminology, examples, code and test style, and changeset wording to those conventions. Observe what maintainers shorten, remove, or ask contributors to explain. Explicit repository instructions and current feedback take precedence over inferred patterns. A small fix should not inherit the ceremony of a major feature, and one unusual PR is not a universal rule.

If examples are unavailable, use the repository's documented conventions and a concise explanation of the change. Do not invent maintainer preferences or turn this into an exhaustive history review.

## Keep public context self-contained

Keep public titles, bodies, commit messages, changesets, examples, and review replies free of private Linear ticket IDs or URLs, internal project names, customer details, user-specific workflow instructions, and downstream work plans. Do not copy them from the task prompt or branch name, or replace a private link with an unexplained internal identifier. Relevant public issues and the repository's own tracking conventions are appropriate.

Use generic, reproducible examples while preserving the technical facts, limitations, and evidence maintainers need. For example: “Complete the stream when the provider sends an empty final event, so consumers do not wait indefinitely after the response ends.” The private ticket can retain the customer context and rollout plan.

When an internal ticket update is already authorized, link from that ticket to the public PR. Do not link back to private tracking from the public contribution. Private first-party repositories can follow their own ticket-linking conventions.
