# Repository toolchain

Use Vite+ (`vp`) for commands and Bun for dependencies. Versions live in
[package.json](../package.json).

| Command            | Purpose                                             |
| ------------------ | --------------------------------------------------- |
| `vp install`       | Install dependencies and configure hooks            |
| `vp check`         | Check formatting and lint                           |
| `vp fmt`           | Format files                                        |
| `vp run typecheck` | Check types and Effect diagnostics with effect-tsrs |
| `vp run check`     | Run all static checks                               |
| `vp run test`      | Run all workspace tests                             |
| `vp run build`     | Build packages, docs, and the Action                |
| `vp run ready`     | Run check, test, and build before handoff           |
| `vp run docs:dev`  | Start the docs server                               |
| `vp env doctor`    | Diagnose local setup                                |

Use `vp run <task>` for other tasks and `vp help` for options.
Typechecking uses `effect-tsrs` for projects with Effect diagnostics and `tsrs` for
the documentation project. Effect warnings remain visible but do not fail checks;
TypeScript and configured Effect errors do. Neither checker requires patching after
installation.
`@effect/tsgo` remains available for its editor language service and tsconfig schema;
Oxlint handles linting.

## Postgres tests

Tests need a reachable Postgres server with permission to create databases.
Set `EFFECT_AGENT_TEST_POSTGRES_URL`, or start the default local server:

```sh
docker run -d -p 55432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=effect_agent postgres:17-alpine
```

## Live credentials

Live evaluations use provider keys from the environment. If your shell has an
Infisical machine identity, load the project's `dev` environment with it:

```sh
export INFISICAL_TOKEN="$(infisical login --method=universal-auth --silent --plain)"
infisical run --projectId="$(bun -p 'require("./.infisical.json").workspaceId')" --env=dev -- <command>
```

The [hosted checkout proof](../tooling/browser-run-worker-proof/README.md#run)
also needs Cloudflare credentials.
