# Architecture review

Use this when asked whether a large feature, branch, or subsystem is overbuilt. The result is a short memo: what to cut, what replaces it, what the reviewer gives up, and what stays. A review stops at the memo; implement only when that is also requested, and then report actual changes instead of estimates.

## Investigate

1. Pin the scope: repository, reviewed head, and the real base and merge base for a branch or PR.
2. Separate requirements from behavior. Requirements come from the request, repository guidance, external contracts, persisted data, and real consumers. Code and tests show what happens, not what must remain.
3. List the major mechanisms in scope: jobs, queues, state machines, stores, adapters, service boundaries, and dependencies. For a whole feature, cover them all or say which you skipped.
4. Sketch the smallest design that meets the requirements as if nothing existed yet, then compare it with the current one.
5. For each mechanism, ask what would break if it vanished, whether an existing owner could do the job, and what else dies with it: callers, types, endpoints, UI states, tests, and docs. Follow callers beyond the diff.
6. Give each mechanism a verdict: cut, keep with the requirement that needs it, or your call with the decision it hinges on. Keeping is a fine result; do not manufacture cuts.

Use [durability and verification](durability-and-verification.md) when queues, journals, retries, or checks dominate the design.

## Size

Estimate production lines removed and added per cut, such as `−370 / +180`, from actual files and ranges at the reviewed head rather than the feature diff. Count shared deletions once and never add alternatives together. Mention tests or docs only when the change is large. Write `?` when the size is unknown. Moving code is not removing it.

## Write

Copy the [HTML template](../assets/review.html) and replace its bracketed text; its opening comment explains the step, tooltip, bar, diff, and decision markup. Write the HTML directly. There is no build step or data format, and the small script, which pins step details on tap and copies the reviewer's answers, stays as is. In order, the page shows the verdict and net lines, a before/after flow for each affected subsystem, one card per change pairing the reasoning and risk with a short real diff, the decisions the reviewer must make, and what stays or was not reviewed.

Let the diagrams and code carry the page. Put detail about a step in its tooltip rather than its label, keep each change to two sentences and one risk, and use plain names rather than proposal codes. Show actual code from the reviewed revision in diffs, mark any sketched replacement as such, and link evidence to that revision. Phrase each decision as a question with two concrete answers whose values read as instructions to the agent. For an answer in chat, use the same sections in Markdown.
