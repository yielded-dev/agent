# Learning more about the Effect

This repository uses the Effect Typescript library.

Before writing any Effect code, first read `node_modules/effect/AGENTS.md`
**completely**, and follow the links in the file when required.

If you need to learn more about particular Effect apis and concepts that the
guide doesn't cover, search through the source code in `node_modules/effect/src`.

# Effect Atom client boundary

This repository consumes APIs through Effect Atom clients (`@effect/atom-react`).
Keep business logic in Effect: compose multi-step client workflows as atoms,
declare cross-query invalidation as reactivity keys on mutations, and keep
promise-mode dispatches at the React boundary logic-free — no `.then` chains
in components or routes.

## Project command policy

Vite+ is the unified toolchain and command authority for this repository. It wraps Vite, Rolldown, Vitest, tsdown, Oxlint, Oxfmt, and Vite Task behind the `vp` CLI; Vite+ is distinct from Vite.

Run `vp help` for available commands and `vp <command> --help` for command-specific options. Documentation is available locally in `node_modules/vite-plus/docs` and online at https://viteplus.dev/guide/.

Use these repository commands:

- Install dependencies: `vp install`.
- Full handoff gate: `vp run ready`.
- Repository static and export checks: `vp run check`.
- Static checks: `vp check`.
- Format check: `vp fmt --check`; format fixes: `vp fmt`.
- Lint only: `vp lint`; lint fixes: `vp lint --fix`.
- Tests only: `vp test`.
- Other repository tasks and package scripts: `vp run <task>`.
- Toolchain or runtime troubleshooting: run `vp env doctor` and include its output when asking for help.

Do not use `bun run`, `npm run`, `pnpm run`, or `yarn run` in this repository. Do not invoke underlying tools such as `tsc`, `vitest`, `oxlint`, or `oxfmt` directly; use the Vite+ entry points above.

# Instructions for implementation agents

This repository is designed to be implemented by a large, parallel AI-assisted project. Every
agent must preserve a common domain language, dependency direction, and durability contract.

## Required reading

Before editing code:

1. Read `README.md`.
2. Read `GLOSSARY.md` when changing domain concepts or public terminology.
3. Read `docs/TOOLCHAIN.md`.
   Before changing runtime, storage, or platform packages, also read the authoritative
   [runtime model](docs/src/content/docs/concepts/runtime-model.mdx).
4. Read the relevant guide, API comments, and neighboring tests for the modules in scope.
5. Read `node_modules/effect/AGENTS.md` before writing Effect code (the canonical Effect
   guidance; `.agents/skills` carries the focused task skills).
6. Read `.agents/skills/effect-development/references/cli/index.md` before creating or
   changing repository scripts.
7. Inspect neighboring package tests before introducing a new pattern.

Keep user-facing behavior in existing guides, implementation contracts beside the code, and
verification evidence in the task or PR artifacts. Explain change rationale in the pull request.
Do not commit separate specifications, planning documents, decision registers, ADRs, roadmaps,
or investigation logs to the product repository.

## Documentation

Documentation helps humans evaluate and understand the current library, then use it.
Keep it terse and edit whole pages for a clear reading path. Agents can read source
and API comments; product documentation is not their implementation context.
A feature or fix usually needs a small change to existing guidance, not another section.

- Guides explain the mental model, ownership, and essential usage. Use one focused
  example and link to runnable examples for complete setup.
- References describe public options, defaults, and caller-visible limitations in
  a scannable form. Keep essential security and recovery caveats beside their API.
- Source, schemas, and API comments own implementation contracts. Keep SQL query
  plans, predicate inventories, internal state-machine walkthroughs, and exhaustive
  failure explanations out of guides and reference pages. Move an enduring contract
  into its owning API comment when it is missing there.
- PRs and release notes own release history, package-version availability,
  `main`/unreleased warnings, old-version fallbacks, migration stories, investigation
  findings, and verification results. Keep these out of product documentation,
  including banners, callouts, compatibility sections, and annotated examples.
- Review every changed page as a whole before handoff. Remove repetition and material
  that does not help a reader evaluate the library, use an API, or avoid a concrete
  pitfall. Preserve the actual constraint briefly; put its implementation rationale
  in source or the PR. Documentation is not a work log or feature inventory.
- Use small diagrams when they clarify ownership or usage. Draw architecture flows
  with `FlowMap` and request sequences with `Trace` from
  `@yielded/starlight-theme/components`, in `.mdx`. Give each `FlowMap` a `title` and
  `description`; the shared theme owns layout, ownership colors, and theming.
- Use `bun add` for consumer installation examples. Package manifests and the root
  catalog own package versions and peer compatibility. Never repeat Effect's current
  version in documentation or installation commands; use plain `effect`. Retain
  protocol identifiers and contributor toolchain requirements where needed for usage.

## Non-negotiable architecture rules

1. Public asynchronous operations return `Effect` or `Stream`, not naked `Promise` values.
2. Expected failures remain typed in `E`; dependency requirements remain visible in `R`.
3. Effect `Schema` is the canonical source for persisted, transported, tool, and structured model
   values.
4. Every acquired resource belongs to `Scope`. The engine must not create daemon fibers.
5. Use the pinned Effect v4 AI primitives directly. Do not introduce framework-owned copies of
   Effect AI `Tool`, `Toolkit`, `LanguageModel`, `Prompt`, `Response`, or `Model`.
6. Provider SDK values never become canonical thread records. Effect AI values may be used
   by the interpreter, but durable records remain explicit, versioned Schemas.
7. The canonical log is append-only and owns conversation content, operation evidence and terminal
   outcomes. An unfinished Run's versioned checkpoint is authoritative operational state, committed
   atomically with its new history under the single owner's mutation gate and revision check. Delete
   it at canonical settlement; unresolved operations and handoffs retain their own rows. Projections and lookup indexes
   remain disposable. `verify` checks retained history and active checkpoints, including original
   context consistency through `historyDigest`; it does not attest discarded intermediate
   checkpoint revisions or provide tamper evidence. Storage validates schema, version, identity,
   sequence and atomic commit boundaries without a hash chain or per-record integrity digests.
8. No code may claim exactly-once external side-effect execution.
9. An unresolved ordinary tool call is never automatically replayed after ownership loss.
   One process owns a Thread at a time on every backend. Local owner generations reject late
   results; distributed claims, ownership leases and multi-worker PostgreSQL are out of scope.
10. Tool/model/subagent concurrency is bounded and deterministic at commit time. Tool batches use
    Effect structured concurrency and Semaphore permits rather than a separate Promise scheduler.
11. Security decisions are fail-closed. Model output is untrusted input.
12. Node platform assumptions must not enter core domain modules.

## Package dependency direction

```text
effect <- ai-decision (thread model selection)
@yielded/agent <- storage-sql <- storage-sqlite / storage-postgres / storage-cloudflare
@yielded/agent <- storage-memory
@yielded/agent <- workflow
@yielded/agent + selected adapters <- platform packages
@yielded/agent <- sandbox-local
@yielded/agent <- testing
@yielded/agent <- pr-review
```

Within `packages/effect-agent/src`, dependencies point inward:
`core <- engine <- capabilities <- durable` and `core <- sandbox <- capabilities`.
Public module paths address these implementations directly; source directories are not separate
packages. Keep core and sandbox contracts platform-neutral. The export check enforces these
internal boundaries as well as package imports.

An inward package must not import an outward package. If a feature appears to require that, define
or deepen an inward port and implement an outward adapter.

Framework code lives only in `packages/*`; do not create an `apps/` workspace. Runnable consumer
benches live in `examples/*`, remain leaf workspaces, and may depend inward on public framework
packages and `@yielded/agent-testing`. Create a new framework package only for a
genuinely new framework concern agreed with the repository owner. Provider integrations remain upstream Effect AI Layers, not framework provider
packages.

## Toolchain rules

- Bun `1.4.2` is the package manager. Use `catalog:` for shared dependencies and `workspace:*`
  for repository packages.
- The root catalog is the single source for the exact Effect v4 version. Do not pin Effect
  independently in a package.
- After changing an Effect-family version, run `vp install` and `vp run check`.
- Contributor skills under `.agents/skills` are repo-owned. Dev Kit copies track their source
  in a `.dev-kit-origin.json` receipt. Check for upstream updates with
  `bunx @danieljvdm/dev-kit@latest skills status`, and fast-forward an unmodified skill with
  `bunx @danieljvdm/dev-kit@latest skills update <name>`; a skill with local edits is left for an
  agent merge instead of being overwritten. Add a new skill from the approved catalog with
  `bunx @danieljvdm/dev-kit@latest skills add <name>`.
- Contributor agent skills are repository tooling. They are not runtime Skill definitions and
  must not be imported by `@yielded/agent*`.
- Before handoff, run `vp run ready`.
- For live model calls, follow [live credentials](docs/TOOLCHAIN.md#live-credentials). When
  `INFISICAL_UNIVERSAL_AUTH_CLIENT_ID` is set, load keys through that machine identity rather than
  asking for a personal Infisical login; otherwise use provider keys already in the environment.
- For lockfile-only PR fixes, push after `vp install --frozen-lockfile` passes; finish full validation afterward.

## Change discipline

- This library is a v0 beta. Prefer simpler machinery and measured performance gains over
  compatibility with superseded APIs or storage formats.
- Add or update Effect Schema definitions before implementing new wire or persisted values.
- Use the [simplify skill](.agents/skills/simplify/SKILL.md) to consider removing unnecessary
  mechanisms within the affected workflow. Public APIs and persisted formats may change to
  simplify the architecture or meet the task's requirements. Do not preserve obsolete paths or
  add compatibility layers unless the user explicitly requires compatibility. Keep unrelated
  cleanup out of scope; the architecture and durability rules above still apply.
- Update existing guides or API comments when a change affects their documented behavior.
- Explain rejected alternatives in the pull request when a future agent could reasonably
  re-propose them.
- Do not silently widen errors to `unknown`, `Error`, or `any`.
- Do not use type assertions to cross a schema boundary.
- Change canonical history through export → re-encode → import, never through in-place payload
  rewrites. Export only idle Threads and keep the source quiescent until the archive is complete.
  Current Run state and operation state carry explicit versions. Storage-layout steps remain
  adapter-owned, ordered, and atomic.
  Refuse formats and layouts newer than the running code before mutation.
- Write changesets as one or two imperative sentences naming the consumer-visible change. Add only
  a short usage example or an explicit BEHAVIOR CHANGE note when consumers must act; keep IDs,
  root-cause, review and test stories, and implementation mechanics in the pull request.

## Testing policy

Default to no new tests or test infrastructure. Verify requested behavior with
existing checks and direct workflow evidence. Prefer E2E for complex features;
this does not require writing an E2E suite or a larger substitute for a rejected
unit test. Save a verifiable, repeatable artifact without building reporting
machinery.

Never write unit tests after implementation. If isolation is necessary, first
write the scoped failure inventory, then the necessary failing tests, then the
code. New or expanded committed automation requires a current regression or an
explicit human test request, plus a concrete gap existing proof cannot cover.
Being a library does not waive this bar or require tests for every transition.

Load the [testing skill](.agents/skills/testing/SKILL.md) before planning proof
or adding, retaining, or removing tests. It owns selection, failure-first
isolation, artifacts, evidence reuse and placement. Keep useful public-contract,
recovery and authority checks at their strongest boundary; remove redundant
matrices and implementation mirrors. The final `vp run ready` gate still applies.

For audits of existing tests, use [test-audit](.agents/skills/test-audit/SKILL.md)
for read-only discovery, candidate evidence, and retention checks before cleanup.

Use [open-pull-request](.agents/skills/open-pull-request/SKILL.md) for concise
PR descriptions. Add diagrams only for meaningful architecture changes and code
examples only when they clarify the change.

When a bot review blocks a PR, verify its findings against the current code before
changing anything. Authorized agents can clear a fully fixed, refuted, or explicitly
accepted review by commenting `@effect-agent dismiss <review-id-or-url>` with evidence
on subsequent lines. This dismisses the whole review and refreshes its check without
inference. See the [agent/CLI instructions](action/README.md#dismissing-a-review).
Resolving an inline conversation alone does not clear a blocking review.

## Parallel work

Parallel agents must own disjoint packages or documents. Shared domain schemas, error unions,
journal records, and public exports require one designated integrator.
Before integrating parallel work, review the combined diff and run
`vp run ready`. Add focused verification only for a concrete remaining question;
do not automatically repeat successful adapter, schema or crash suites.

## Completion standard

Finish when the requested observable outcomes have sufficient evidence, relevant
public contracts and operational limitations are documented, and `vp run ready`
passes. Choose failure, interruption, resource, recovery and security checks for
concrete risks in the change; this is not a mandatory scenario matrix. Preserve
blocked required proof explicitly. A new source file, state transition or public
API does not imply a new test file.

## Worktree startup

Create worktrees from cached refs; fetch explicitly when a newer base is needed.
Run `vp install` when dependencies or `node_modules/effect/AGENTS.md` are needed.
Installation uses Bun's shared store and keeps project-specific packages local.
Start only the development services needed for the task, explicitly. Worktree
creation itself should not install dependencies or start servers.
