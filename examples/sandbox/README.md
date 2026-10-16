# Sandbox coding agent

A signed-in coding workspace at `agent.yielded.dev/sandbox/<workspace-id>`.
Opening `/sandbox/` lists the account's saved workspaces, newest first, across browsers
and devices. Each has a permanent link and a title from its first request.
Workspace links select within that account; knowing an ID does not grant access.

`@yielded/agent` runs the conversation and tools in a durable Thread Object.
`@yielded/sandbox` supplies the isolated container, filesystem and process receipts.
The coding loop uses Effect's OpenAI provider; no Codex CLI is installed or invoked.

The composer offers GPT-6.1 Sol and GPT-6 Astra, reasoning effort, and processing
priority. New workspaces default to Sol with medium reasoning and ultrafast priority.
Each message retains its settings for recovery; reopening a workspace restores the
last settings used there.

The example exposes `read_file`, `write_file`, `edit_file`, `list_files`, `search`
and `bash`. Tool execution is serialized. Process tickets and results use Agent's
Durable Steps, and retries retain the original deadline and workspace identity.
The browser reconciles live assistant text and provider reasoning summaries with
canonical history, so reconnecting never resubmits work. `@yielded/ui` renders the
conversation, Markdown, and paired tool results; the example owns its Tailwind theme.

Prefix a message with `!` to run Bash directly, for example
`!python3 summarize.py sample.txt -n 2`. These commands do not call the model.
Each starts in `/workspace`, runs for up to 30 seconds, and retains its output and
exit status. There is no interactive stdin or persistent shell session. Commands
and agent work cannot overlap. A workspace retains up to 100 command receipts;
reconnecting or retrying the same request resumes its original process.

A workspace belongs to the authenticated account. Files are temporary: the
container expires after 30 minutes of inactivity. An expired workspace cannot be
silently replaced during recovery. Its conversation remains readable; the UI marks
its files as expired and stops accepting new work. Start a new workspace for fresh
work. Commands have no production credentials; network access is limited to the public npm
registry. The host caps concurrent containers at four.

## Development and deployment

Use the repository's Bun/Vite+ setup:

```sh
vp install
vp run -F @yielded/agent-example-sandbox check
vp run -F @yielded/agent-example-sandbox build
vp run -F @yielded/agent-example-sandbox worker
```

The account directory stores navigation only; Threads own conversation and execution.
After changing `src/schema.ts`, run `vp -C examples/sandbox run db:generate`, then
review and commit the generated Drizzle migration. Each directory applies it before
serving requests.

Docker is required to build and run the native container locally. Authentication
uses the shared Yielded gateway; local native verification supplies a controlled
issuer, never a production bypass.

The host owns the Cloudflare account, routes, model key and auth client secret.
Set Worker secrets `OPENAI_API_KEY` and `AUTH_YIELDED_CLIENT_SECRET`; the latter
must match the gateway's existing Agent client secret. The gateway must grant
`agent:sandbox` and recognize `/sandbox/` as a login destination. Model requests
also check the account's current hosted-model permission.

```sh
vp run -F @yielded/agent-example-sandbox deploy:production
```

The checked-in Wrangler configuration owns the Sandbox route and its Thread,
container, and account workspace-directory namespaces. It does not own the shared authentication gateway.
Wrangler 4.151 is scoped to this example because it supports the native
`durable_object` container policy and named images used by Sandbox.
