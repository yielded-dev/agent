# Sandbox coding agent

A signed-in coding workspace at `agent.yielded.dev/sandbox/<workspace-id>`.
Opening `/sandbox/` lists the account's saved workspaces, newest first, across browsers
and devices. Each has a permanent link and a short descriptive title.
Workspace links select within that account; knowing an ID does not grant access.

`@yielded/agent` runs the conversation and tools in a durable Thread Object.
`@yielded/sandbox` supplies the isolated container, filesystem and process receipts.
The coding loop uses Effect's OpenAI provider; no Codex CLI is installed or invoked.

The composer offers GPT-6.1 Sol and GPT-6 Astra, reasoning effort, and processing
priority. New workspaces default to Sol with medium reasoning and ultrafast priority.
Each message retains its settings for recovery; reopening a workspace restores the
last settings used there.

The example exposes `read_file`, `write_file`, `edit_file`, `list_files`, `search`
and `bash`, plus OpenAI’s native web search for current documentation and outside
information. Search runs in the coding model’s own response and retains citations;
the container also has public internet access for dependencies, documentation and
public repositories. Local tool execution is serialized. Process tickets and
results use Agent's Durable Steps, and retries retain the original deadline and
workspace identity.
Long tasks automatically summarize context and continue with the summary, recent
messages and existing files. The selected model supplies its context capacity
(Sol 500k, Astra 272k), with room reserved for the response. Conversation history is
kept. Each request can run for up to one hour and 1,000 model/tool calls; there is no
cumulative token cap.
The browser reconciles live assistant text and provider reasoning summaries with
canonical history, so reconnecting never resubmits work. `@yielded/ui` renders the
conversation, Markdown, and paired tool results; the example owns its Tailwind theme.

Prefix a message with `!` to run Bash directly, for example
`!python3 summarize.py sample.txt -n 2`. These commands do not call the model.
Each starts in `/workspace`, runs for up to 30 seconds, and retains its output and
exit status. There is no interactive stdin or persistent shell session. Commands
and agent work cannot overlap. A workspace retains up to 100 command receipts;
reconnecting or retrying the same request resumes its original process.

If container startup fails, use **Check recovery**. The server can close a coding
call only when its complete saved history proves no native operation started.
An operation that may have run stays blocked until its outcome is known; checking
recovery never resends the request or reruns the model.

Files are saved after agent work or a direct command settles. The Thread's existing
alarms retain a portable R2 backup, then attempt a native filesystem checkpoint for
the same settled work. New messages can be accepted while saving; execution waits
for that save to finish. The file panel shows when a portable save is retained and
offers a retry if saving failed. A failed native capture still leaves the portable
save available.
Containers still stop after 30 minutes idle. **Restore workspace** uses the native
checkpoint when available, or the R2 backup after expiry or an image change. It
also reads workspaces saved with only an R2 backup. Restoration keeps the same
account, URL and conversation and includes hidden and ignored files. Running
processes and native process receipts are not restored. An unresolved command
stays unresolved after restoration.

Changes since the last successful save can be lost when a container stops. Older
workspaces without a saved snapshot cannot recover files that have already expired.
Commands have no production credentials. The host caps concurrent containers at four.
The image includes Node, Bun, Python with pip/venv, Git, ripgrep and common shell tools.
Playwright Test and matching headless Chromium, system libraries and fonts are
preinstalled. New projects can import `@playwright/test` or `playwright` and run
`playwright test` without installing a browser. Existing projects keep their own
package manager and dependencies.

Open **Diagnostics** in the workspace header to inspect saved model responses,
token counts, tool timings and failures, or copy a page with its run and response
IDs. It reads existing history on demand. Tool elapsed time includes startup and
recording overhead; it is shown only when both boundaries are in the page.

Select an HTML file and switch between **Source** and **Preview** in the file viewer.
The preview embeds the workspace's HTML, scripts, CSS and images from an isolated
origin using Sandbox's native forwarding; it can also open in a separate tab.
External assets and dev servers are not supported.
Preview access lasts up to 15 minutes; preparing it again replaces the previous grant.
It permits reading workspace files, including through scripts in the HTML, and
remains valid after app sign-out until it expires or the container stops.

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

Create the `yielded-agent-sandbox-workspaces` R2 bucket before deploying; Wrangler
binds it as `BACKUPS`. Keep this bucket private. A capture interrupted before its
receipt reaches the Thread may leave an unreferenced object, because the Sandbox
SDK allocates backup IDs during capture. Recovery keeps the last confirmed snapshot
and does not repeat that capture automatically.

For previews, configure `PREVIEW_DOMAIN` with a dedicated domain and route its
wildcard to this Worker. The checked-in route uses `*.sandbox-preview.yielded.dev`;
it needs a proxied wildcard DNS record and a certificate covering that hostname.
Keep previews on a separate origin within the application's site so their same-site
cookie works in the embedded viewer. Only the configured application origin may
frame them. Their short-lived access
is transferred by a form POST into a host-only HttpOnly cookie, never a URL token.

Sandbox deploys automatically after successful push CI on `main`. The deployment
workflow skips superseded revisions and uses the repository's Cloudflare credentials,
preserving the Worker's existing model and auth secrets. The command above remains
available for manual deployment.
The checked-in Wrangler configuration owns the Sandbox route and its Thread,
container, and account workspace-directory namespaces. It does not own the shared authentication gateway.
Wrangler 4.151 is scoped to this example because it supports the native
`durable_object` container policy and named images used by Sandbox.
