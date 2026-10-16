# CLI selection and discovery

Use the repository's command entrypoints and installed versions. Each tool has a different job:

- **Alchemy:** declare resources and manage their deployment lifecycle. Update the owning declaration instead of creating a competing resource with a CLI.
- **`cf`:** inspect and operate Cloudflare APIs, including deployed profiling. Resource commands can work alongside a Wrangler project without migrating it.
- **Wrangler:** keep using it where the project's development, types, or release commands depend on it. Do not migrate the project just to inspect a resource.

See the [CLI overview](https://developers.cloudflare.com/cf/) for `cf`/Wrangler compatibility.

For an unfamiliar `cf` action, search once with the action and resource type:

```sh
cf cli search "capture worker CPU profile"
```

Keep discovery queries anonymous: no account IDs, resource names, domains, credentials, or personal data. Select the relevant result, then read that command's `--help`. For generated API commands, replace the leading `cf` with `cf schema` to inspect request fields. Avoid walking nested help trees or repeating similar searches. See [agent command discovery](https://developers.cloudflare.com/cf/agents/).

Resolve the intended account, resource, and local/remote target before running the operation. Use the existing credential integration; `cf` does not inherit a Wrangler login. Its [authentication guide](https://developers.cloudflare.com/cf/get-started/) describes profiles and token-based automation.

Parse structured results directly and check pagination when a complete list is needed. Save binary output to an artifact. A partial failure calls for inspecting the operation's result before retrying it. Keep temporary-resource ownership and cleanup in [Alchemy's lifecycle](../../alchemy/references/temporary-stacks.md).
