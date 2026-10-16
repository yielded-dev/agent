# Temporary Cloudflare stacks

Dan pre-authorizes creating, using, and tearing down isolated temporary Cloudflare resources through Alchemy for development, testing, and benchmarking. Use them freely when they shorten feedback or provide the actual runtime. Do not request approval again for ordinary work in this scope. Production and other agents' resources are outside this standing authorization.

Use the project's configured account, credentials, and deployment entrypoint. Give each experiment an explicit, unique stage; a per-user default is not enough to isolate concurrent agents. Check that resource names and referenced bindings actually isolate its work. Review the initial or changed resource graph for unintended shared-resource changes, then use the supported noninteractive mode.

Prefer the existing app command. Where the installed CLI supports these flags, the underlying operations are:

```sh
alchemy deploy --config "$EXPERIMENT_STACK" --stage "$EXPERIMENT_STAGE" --yes
alchemy destroy --config "$EXPERIMENT_STACK" --stage "$EXPERIMENT_STAGE" --yes
```

Set both variables deliberately and retain the same account/profile and state store for cleanup. Verify flags against installed help and the [deploy](https://alchemy.run/cli/deploy/) and [destroy](https://alchemy.run/cli/destroy/) references.

Record ownership and deployment/version IDs in the shared harness's run output. Reuse the stage between iterations when measurement isolation permits. Keep expensive reusable fixtures outside disposable worktrees. Preserve deployment state until teardown; on a partial failure inspect what exists before retrying.

Clean up owned resources when finished or abandoning the experiment, including failure paths. Confirm cleanup against recorded resources; retained resources can outlive a successful destroy. Preserve shared fixtures and report any failed cleanup with exact ownership information. Do not use account-wide deletion as a substitute for stage cleanup.
