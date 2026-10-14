---
name: pull-requests
description: Prepare, open, review, update, or land pull requests in any repository, including drafting titles and terse descriptions with diagrams or visual evidence, final diff review, and addressing review feedback.
---

# Pull requests

Make the change and its explanation useful to a reviewer who has not seen the
conversation. Follow the target repository's contributor instructions,
`AGENTS.md`, PR template, and current maintainer feedback. Scale the review and
description to the change. Repository guidance owns project commands, gates,
and publication constraints; keep those rules there instead of forking this
shared workflow.

## Review the change

- Reread the aggregate diff before opening or updating a PR. Check the observable
  behavior, supported contract, scope, and relevant regression coverage.
- Run the repository's required checks and relevant validation. Report material
  failures or limitations accurately; do not imply checks passed when they did not.
- When scope changes, rewrite the title and description around the final change.

## Write for the reviewer

Lead with the concrete problem and resulting behavior. Describe the final
aggregate diff; omit conversation history, intermediate commits, and abandoned
approaches unless they explain a tradeoff that helps assess the result. Simple
changes need one or two sentences. Use headings or lists only when complexity or
the repository template warrants them.

Show rather than narrate. Like `show-me`, pick the smallest view that makes the
change clear (a call tree, component or file tree, small diff, or Mermaid chart)
and place it beside the sentence it supports.

**No validation laundry list.** Keep test and check reporting out of the PR body:
no testing sections, checklists, or lists of commands run. Still run the required
checks, and state a material failure, risk, limitation, or manual step in one
sentence. Evidence that shows behavior is the exception: screenshots, video, and
performance tables belong in the body when they help a reviewer see the change.

Put enduring contracts in API comments; remove repeated implementation narration.
Write changesets as one or two short, consumer-facing imperative sentences about
the behavior users gain.

## Load what applies

- Ownership, data flow, or API changes: [diagrams and examples](references/explanation.md).
- UI or other visible behavior: [capture and publish evidence](references/evidence.md).
- Performance claims: [baseline and candidate comparisons](references/explanation.md#performance-claims).
- Commits, publication, readiness, review feedback, or landing: [PR workflow](references/publication.md).
- Upstream library contributions (Effect-TS/effect, effect-agent):
  [code and test practices](references/code-and-tests.md), applied while
  implementing or revising and during final review.
- Substantial takeovers, behavior changes, or new public APIs: [review pass](references/review.md).
- When a project or user-level companion skill or `AGENTS.md` has extra PR rules
  (for example upstream maintainer preferences or rules about private ticket
  references), load and follow it too.

Reuse verification and captures for unchanged inputs. Follow repository merge
policy and existing authorization; opening a PR does not authorize merging it.
