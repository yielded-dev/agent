# Open or update the PR

When the user asks to open a PR, create it ready for review unless they requested
a draft or the work has an actual readiness blocker. Explain any such blocker;
routine PR automation is not one.

A general "do not merge or deploy" instruction limits agent-initiated merges and
deployments. It does not by itself prohibit the repository's normal CI or PR
preview automation. Do not choose draft status or request extra confirmation
solely to suppress that automation. Honor explicit restrictions on previews or
automatic deployments.

Check authenticated push access to the target repository before publication.
Use the existing branch and review workflow. Once the implementation is ready:

1. Check the base for real conflicts, and review the branch diff and working tree
   for accidental changes. Reuse review, validation, and evidence already
   completed for unchanged inputs; `AGENTS.md` owns required checks.
2. Use Conventional Commits for commits and the title. Commit and push the
   intended changes, preserving unrelated work and published history.
3. Open or update the PR with the short body, useful diagrams or API examples,
   and existing evidence. With `gh`, use `--body-file` for multiline text.
4. Read back base/head, title, and body once with `gh pr view`. Run outstanding
   local validation alongside CI on the pushed commit; do not delay opening the
   PR until those checks finish. Pending validation alone is not a readiness
   blocker. Fix and push failures as they are found, without waiting for the
   previous CI run to finish.
5. Complete required local validation for the final implementation before
   declaring the work complete. No GitHub browser inspection or wait for CI is
   required to open the PR.

Follow `AGENTS.md` for merge approval and required checks; opening a PR does
not authorize merging it.

When an existing draft PR is the subject, interpret "open it" or "ready it"
as making it ready for review unless the user asks to view it. State the intended
transition before acting; use `gh pr ready` rather than opening a browser.

## Address review feedback

Make the requested change, reread the revised diff, and rerun relevant checks.
Rewrite the title and body if the scope moved. Keep review replies as
self-contained as the PR itself.
