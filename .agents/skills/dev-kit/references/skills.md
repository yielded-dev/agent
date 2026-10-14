# Repository skills

The transient CLI copies skills directly into `.agents/skills`. Each copied
skill is repository-owned. A `.dev-kit-origin.json` file inside that skill records
only its selector, approved source, and base digest; it is not desired state and
runs no lifecycle.

## Ownership and discovery

Keep shared skills identical to their approved source. Put repository-specific
commands, gates, and constraints in `AGENTS.md` or a focused document linked from
it. Make reusable workflow improvements in the source skill, then refresh its
installed copies. Repository copies keep fresh checkouts and cloud agents
self-contained; do not replace them with links into a developer's home directory.

Give genuinely different repository workflows distinct skill names. Codex can
expose both repository and user skills with the same name; do not rely on scope
to merge or override their instructions. Inspect the host's loaded skill catalog
when diagnosing duplicates, including each entry's path and invocation policy.
`skills status` compares the selected directory with upstream; it is not a report
of everything an agent host has loaded.

Personal fallbacks for repositories without these skills belong in the user's
configuration, under a distinct name. In Codex, use `~/.codex/skills` and set
`policy.allow_implicit_invocation: false` in `agents/openai.yaml` when the user
wants explicit invocation only. A fallback should load the repository's skill
when present, otherwise obtain the published source in temporary storage. Keep
one shared implementation rather than another edited copy of its instructions.

## Discover and add

Inspect repository capabilities before selecting skills. Search descriptions and
prefer the narrowest matching skills:

```bash
bunx @danieljvdm/dev-kit@latest skills search cloudflare
bunx @danieljvdm/dev-kit@latest skills info cloudflare-workers
bunx @danieljvdm/dev-kit@latest skills add cloudflare-workers
```

Treat source families as broad selections: add one only when every member applies.

## Refresh

Run `skills status` before updates. `skills update` fast-forwards a skill only
when its repository copy still matches the recorded base. For an existing
locally modified skill, inspect `skills diff` first. Move project policy into repository guidance
and send reusable changes to the source skill where practical. For an intentional
fork whose upstream also changed, merge the relevant upstream intent deliberately;
never discard local instructions merely to make the status clean.

After a manual merge, preserve the merged content and advance its base with
`skills update <name> --accept-local`. A future CLI may automate three-way
merges; current conflicts remain agent-owned rather than being overwritten.

## Renames

A renamed bundled skill keeps its previous selector. `skills add` with the old
name installs the current skill. `skills status` and `skills diff` follow an
existing origin receipt to the current skill. `skills update` moves an
unmodified copy into the current directory and rewrites the receipt.
`open-pull-request` is now `pull-requests`.

## Detach

Run `skills detach <name>` to remove only the origin receipt. The skill content
stays in place as an ordinary local skill and future Dev Kit updates ignore it.
