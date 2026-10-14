# Default TypeScript repository

Use this branch when the user requests the Dev Kit default or chooses Vite+ as
the repository toolchain. Adapt package boundaries to the requested product;
the defaults below are invariants, not a fixed directory template.

## Foundation

- Use Bun as the package manager and declare its version through `packageManager`.
- Use Vite+ as the command authority for install, format, lint, tests, typecheck,
  builds, and repository tasks. Read its installed documentation before choosing
  config keys or commands.
- Keep `vite.config.ts`, TypeScript configs, lint plugins, ignore patterns, and
  task composition local and repository-owned.
- Give formatting, linting, tests, and pure typechecking distinct commands, then
  compose them into one full validation task.
- Make CI invoke the same repository commands developers use.
- Keep generated files and vendored source in explicit tool ignores.

## TypeScript

Build a root configuration around the repository's actual runtime targets and
workspace graph. Let child packages inherit shared strictness while declaring
only their environment-specific libraries, paths, and emitted output. Keep
compiler plugins at the configuration level where their file globs are correct.

When Effect TypeScript-Go is selected, install its commit-matched compiler and
language service according to the installed Effect guidance. Materialize any
required patch helper into a repository-owned script so installs never depend on
Dev Kit. Enable `preferTypedSchemaDecoder` at `warning` severity in the Effect
language-service diagnostics so already-typed inputs use the typed Schema decoder.
The current Dev Kit compiler baseline is `@effect/tsgo@0.48.1` with
`typescript@7.0.2`.

Set `unknownRuleName` to `"error"` and these simplification diagnostics to
`"warning"` (warnings block the compiler check by default):

```json
{
  "unknownRuleName": "error",
  "catchAllTagDispatchToCatchTag": "warning",
  "catchConditionalRefailToCatchIf": "warning",
  "catchIfTagToCatchTag": "warning",
  "catchRefailToTapError": "warning",
  "provideLayerSucceedToProvideService": "warning",
  "allOfMapToForEach": "warning",
  "flatMapConditionalToFilterOrFail": "warning",
  "flatMapIgnoredParamToAndThen": "warning",
  "optionMatchToFromOption": "warning",
  "timeoutCatchTagToTimeoutOrElse": "warning",
  "runOfExitToRunExit": "warning"
}
```

Keep the smaller simplification rules at their default suggestion severity and
`schemaSync` off; do not enable the global strict diagnostic preset. Retain
`unsafeEffectTypeAssertion` as a warning. Apply suggested rewrites only when
they preserve failure, interruption, and resource behavior.

Keep `unstableApiUsage` and `experimentalApiUsage` at their default warning
severity. Review reported APIs before adding project-specific exceptions through
`allowedUnstableApis` or `allowedExperimentalApis`: allow only an intentional
module/subtree or `package/module#export` API. Keep these lists empty in shared
defaults so each consuming project explicitly chooses its unstable dependencies.

## Workspaces

Create a package only for a real deployable, reusable boundary, or independently
validated unit. Give every package a narrow public surface and a pure typecheck
task. Configure the root validation task to cover every workspace without hiding
package failures behind a root-only compiler invocation.

## Completion

Run the full Vite+ validation and at least one real build or startup path. The
result must contain no import from `@danieljvdm/dev-kit` and no command that needs
the Dev Kit executable after setup.
