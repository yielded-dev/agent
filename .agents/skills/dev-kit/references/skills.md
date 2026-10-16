# Repository skills

The transient CLI copies skills directly into `.agents/skills`. Each copied skill is repository-owned. A `.dev-kit-origin.json` file inside that skill records only its selector, approved source, and base digest; it is not desired state and runs no lifecycle.

## Ownership and discovery

Shared engineering skills are authored in [danieljvdm/skills](https://github.com/danieljvdm/skills) and distributed through the pinned `dan-skills` source. Make reusable workflow improvements there, then refresh the approved catalog and installed copies. The Dev Kit repository owns its toolbox instructions and executable mechanics.

Keep shared copies aligned with their approved source. Put repository-specific commands, gates, and constraints in `AGENTS.md` or a focused document linked from it. Preserve intentional local deviations deliberately when updating. Repository copies and their linked support must keep fresh checkouts and cloud agents self-contained; do not replace them with links into a developer's home directory.

Global skills may legitimately have the same names as repository skills, including `effect`, `cloudflare`, and `pull-requests`. Required repository workflows should name the exact repository path, for example `Read .agents/skills/pull-requests/SKILL.md before opening a pull request.` Do not assume the host deduplicates same-named skills or gives one scope precedence. Inspect the host's loaded skill catalog, paths, and invocation policy when diagnosing what it loaded. `skills status` compares the selected directory with upstream; it is not a report of everything an agent host has loaded.

## Discover and add

Inspect repository capabilities before selecting skills. Search descriptions and prefer the narrowest matching skills:

```bash
bunx @danieljvdm/dev-kit@latest skills search cloudflare
bunx @danieljvdm/dev-kit@latest skills info cloudflare
bunx @danieljvdm/dev-kit@latest skills add cloudflare
```

Install only the skills relevant to the repository, such as `effect`, `cloudflare`, or `pull-requests`, plus the linked support needed by those workflows. Do not install the whole personal stack by default. Source families are broad selections; add one only when every member applies.

After copying, follow the selected skills' relative links through their references, scripts, assets, and required companion skills. Copy the relevant support and install needed companions until those repository-relative links resolve within the checkout. Check link targets from each file's own directory. The CLI copies each selected skill directory; selecting a skill does not automatically install the companion skills it links to.

## Refresh

Run `skills status` before updates. `skills update` fast-forwards a skill only when its repository copy still matches the recorded base. For an existing locally modified skill, inspect `skills diff` first. Move project policy into repository guidance and send reusable changes to the source skill where practical. For an intentional fork whose upstream also changed, merge the relevant upstream intent deliberately; never discard local instructions merely to make the status clean.

After a manual merge, preserve the merged content and advance its base with `skills update <name> --accept-local`. Recheck relative links and required companion support after updates. Current conflicts remain agent-owned rather than being overwritten.

## Renames

A renamed bundled skill keeps its previous selector. `skills add` with the old name installs the current skill. `skills status` and `skills diff` follow an existing origin receipt to the current skill. `skills update` moves an unmodified copy into the current directory and rewrites the receipt.

`open-pull-request` is now `pull-requests`, `effect-development` is now `effect`, and `cloudflare-workers` is now `cloudflare`. Existing bundled receipts can move to the shared Git source through the same update flow.

## Detach

Run `skills detach <name>` to remove only the origin receipt. The skill content stays in place as an ordinary local skill and future Dev Kit updates ignore it.
