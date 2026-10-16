---
name: cloudflare
description: Build, review, or operate Cloudflare Workers, Durable Objects, and Sandbox SDK applications, including CLI operations and deployed observability.
license: Apache-2.0
metadata:
  source: "https://github.com/danieljvdm/dev-kit/tree/2bda762e1573fbfdaf5c5aa6c06abe952d812a0b/skills/cloudflare-workers"
  source_commit: "2bda762e1573fbfdaf5c5aa6c06abe952d812a0b"
  source_skill: "cloudflare-workers"
---

# Cloudflare Workers

Use the repository's architecture, commands, installed types, and runtime compatibility settings. Current documentation explains APIs; a maintenance task does not by itself call for a dependency or compatibility-date upgrade.

Read for the changed boundary:

- Worker handlers, streams, bindings, and verification: [Workers](references/workers.md).
- Durable Object identity, storage, alarms, and WebSockets: [Durable Objects](references/durable-objects.md).
- Wrangler commands, environments, deployment, and secrets: [Wrangler](references/wrangler.md).
- Choosing between `cf`, Wrangler, and Alchemy, or discovering an API command: [CLI selection](references/cli.md).
- Tracing, CPU attribution, or deployed measurements: [observability](../o11y/SKILL.md), then its [Cloudflare reference](../o11y/references/cloudflare.md) as needed.
- Effect HTTP services and bindings at the Worker boundary: [Effect integration](../effect/references/api/cloudflare-workers.md).
- Infrastructure declarations and deployment lifecycle: [Alchemy](../alchemy/SKILL.md).
- An application using `@cloudflare/sandbox`: [Sandbox SDK](references/sandbox.md). Ordinary commands inside an agent sandbox do not require the SDK.

Resolve the account, environment, and local or remote target before mutations. Preserve deployed storage migrations, existing authorization, and host ownership of task containers, previews, authentication, and capability URLs. Follow the repository's testing policy and verify the behavior at its actual runtime boundary.

For critical-path changes, use [measure-performance](../measure-performance/SKILL.md). Reuse guidance already loaded and follow only the references needed for the current question.

Attribution is in [NOTICE](NOTICE); terms are in [LICENSE](LICENSE).
