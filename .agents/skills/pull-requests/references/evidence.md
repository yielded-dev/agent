# Capture visible behavior

For UI or visible features, capture the final running implementation during verification and reuse it for the PR. A screenshot is the default; use a short video when the sequence matters, such as an agent exchange or animation. Both are rarely needed. Nonvisual changes need no screenshots or recordings.

For changes to existing visible behavior, prefer a before/after table with published captures when the comparison helps reviewers see the change. Use the same viewport, fixture data, and interaction state for both captures. A new feature can use a final-state capture without a contrived before image.

Use existing capture tools; load `playwright-cli` for browser capture. Keep recordings focused, usually under 30 seconds, without changing product timing. Use safe fixture data and review the image or whole clip once for correctness and private content. Treat published assets as public; unreviewed media stays local. If inspection is unavailable, use a safe alternative or report the blocker. Do not build viewers, extract frame galleries, or reconstruct GitHub.

Use the repository's existing media publisher when available. Otherwise inspect `gh pr create --help` or `gh pr edit --help` for supported attachments, and use that capability when authenticated access allows it. Do not assume a particular publisher script exists in every consumer repository.

For a supported GitHub CLI attachment workflow, pass `--attach` when creating or editing the PR. A local Markdown image path in the body places the matching attachment inline; use the same path in the attachment argument:

```sh
gh pr create --title "fix: describe the change" --body-file /tmp/pr-body.md --attach './screenshot.png#What this shows'
gh pr edit <number> --attach ./interaction.mp4
```

Use the returned link or Markdown in the PR. Keep originals until publication succeeds. An attachment failure can still leave a created or updated PR with successful uploads; inspect the current PR before retrying only the missing files. Report unpublished files by their exact local paths. Never extract browser cookies, expose credentials in arguments, create asset branches, or invent an upload service.
