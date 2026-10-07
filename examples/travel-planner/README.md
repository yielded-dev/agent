# Travel planner

An Effect Agent example for planning trips through chat or voice, researching destinations
with background agents, saving itineraries, and building standalone trip websites.

The planner lives at <https://agent.yielded.dev/travel/>. It shares host-only sign-in
cookies (`__Host-elsewhere-auth-*`, `Path=/`) with `/browser-use/`. Link to
`/travel/login?return=%2Fbrowser-use%2F` to return to the lab after sign-in;
only `/travel/` (the default) and `/browser-use/` are accepted return targets.
Generated trip apps stay on `*-trip.effect-agent.com`, outside the session origin.

`travel.effect-agent.com` redirects paths and queries to `/travel` with HTTP 301.
Users must sign in again; accounts and funding grants remain in `AUTH`/`AuthV1`/`PlannerAuth`.
Planner threads live in `PLANNER_THREADS`/`PlannerThreadsV2`/`PlannerThread`. Stores written
before thread storage format 16 stay in the retired `ACCOUNT_THREADS`/`AccountThreadsV1`/
`AccountPlannerThread` namespace, which current storage cannot open; it remains bound only so
Cloudflare keeps that data. Keep all three identities unchanged: changing one deletes its data.
The lab binds `PlannerAuth` on script `effect-agent-travel-planner`, instance
`auth-v1`; `/_internal/session` and `/_internal/funding/<id>` remain private.

Shared sign-in uses `https://auth.yielded.dev` as its OpenID issuer. Register client
`yielded-agent` there with callback
`https://agent.yielded.dev/travel/auth/yielded/callback`. Set `AUTH_YIELDED_ISSUER`
to that issuer and `AUTH_YIELDED_CLIENT_SECRET` to its separate client secret.
The central GitHub app's callback stays at Auth. Direct GitHub sign-in remains
under **Use an existing Agent account**, with callback
`https://agent.yielded.dev/travel/auth/github/callback`. If it shares a GitHub OAuth
app with Auth, register both exact callback entries and retain those needed by
other consumers; see [GitHub's callback rules](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps#redirect-urls). Set `AUTH_ORIGIN` to `https://agent.yielded.dev` and retain
`auth@effect-agent.com` as the email sender.
This stack owns only `agent.yielded.dev/travel*` on `yielded.dev`; the existing DNS
record and the browser lab's root and `/browser-use*` routes stay externally managed.

It uses Cloudflare:

- **Workers** to serve the TanStack Start UI and APIs.
- **Durable Objects with SQLite** to persist accounts, conversations, trips, and agent execution.
- **Browser Run** to research travel websites.
- **Artifacts** to version generated trip-site source code.
- **Workflows and Sandbox containers** to build sites, **R2** to store builds, and
  **Dynamic Workers** to serve them.
- **Email Sending** for email sign-in, alongside GitHub OAuth.

The app consumes the repository's Effect Agent workspace packages and uses Effect Atom for client state.
[alchemy.run.ts](alchemy.run.ts) defines the Cloudflare resources and required configuration.

New trip sites fork an immutable starter identified by its source contents. Updating the
starter affects new sites; existing app repositories and saved versions remain unchanged.
Source conflicts are returned to the editor so it can read the current files before editing.

Choose GPT-6 Astra or GPT-6 Luna in Settings. Astra remains the default; Luna also supports
turning reasoning off. Saved preferences for the previous Luna model select GPT-6 Luna on
the next message, preserving reasoning effort and processing speed. Already accepted work
retains its admitted model settings. Retained voice requests keep their original settings
when reconnecting or retrying.

Listing photos come from inspected page galleries. When a gallery yields no usable images,
the reader also checks Open Graph and Twitter image metadata within the same inspection deadline.
These source images do not verify amenities or availability; unavailable photos leave the cards usable.

The main planner uses native hosted web search in its own model call for focused questions.
It delegates longer or independent research to scouts and receives standard `WorkerUpdate`
and `WorkerCompletion` messages. New input can restart its disposable model response;
the preview clears cancelled text and marks interrupted search activity incomplete.
The early thinking indicator contains no private model reasoning.

The current planner and scout keep stable agent IDs and select one current binding per ID.
Retired planner v2–v15 and scout v1–v3 registrations and their custom report inputs are removed.
Start fresh conversations for those demo revisions; no migration or storage reset is supplied.
Keep their old stores and original executable release for inspection or reconciliation of unfinished
external actions. Existing current-agent records, saved trips, accounts, and report evidence are retained.
Scouts already waiting for research-plan approval remain paused. Cancel their active run and
start fresh research; removing the checkpoint does not approve existing requests.

Authentication uses `@yielded/auth` beta.12 with its compatible crypto, OpenID Client, and
Drizzle persistence companions. The app owns its SQLite tables and provides the adapter's
`SqliteDo.Database` through `databaseLayer`. It supplies session claims through each strategy's
`SessionClaims` service. The Atom client uses Fetch transport with
a 30-second request and response-body deadline; timed-out mutations are not retried automatically.
Provider sign-in creates flow IDs on the server; the browser retains only the
provider, returned flow ID and return target for callback completion. Effect Atom
owns this workflow, registration and query invalidation; React dispatches and renders.
**Continue with Yielded** reuses the current Auth account automatically, then
verifies issuer, audience, nonce, PKCE and the signed identity before establishing
an Agent session. Without an Auth session, Auth starts GitHub sign-in automatically. **Use another
Yielded account** explicitly requests account selection; new permissions require
consent. Cancelling returns to login with a retry action. Auth
and Agent have separate sessions: **Sign out of Agent** leaves Auth signed in.
The browser lab already consumes this Agent session, so the same login covers it;
Sync and docs are not connected by this change.

For a new Yielded identity, registration creates a new Agent account. Existing
accounts are never matched by email or display name. To preserve an existing
GitHub-owned account, an operator may set `AUTH_YIELDED_ACCOUNT_LINKS` before its
first Yielded sign-in, as a JSON array of `{ "yieldedSubject": "<Auth subject>",
"githubSubject": "<numeric GitHub ID>" }`. Independently verify both identities.
The migration requires exactly one active GitHub owner and rejects pre-existing
Yielded ownership, reservations or removed identities. It adds the credential to
that owner without changing their security revision, accounts, funding or threads.
An atomic receipt prevents later startup from restoring a subsequently removed
link; conflicting mappings fail closed and require explicit reconciliation.
Omit the mapping to keep registrations separate.

Auth storage format 2 adds only that receipt table to format 1. Older hosts reject
format 2; retain this migration when rolling back application code rather than
resetting deployed storage. Email verification keys are supplied through `ProofKeys` using `AUTH_PROOF_KEY`.

Keep the existing database, namespaces, and key IDs when upgrading this demo from Auth beta.7
to beta.11; its account and stateful-session formats are retained. Users upgrading from beta.5
should start a fresh email flow for codes requested before the upgrade because the proof template
identifier changed. Existing accounts and sessions are retained.

The production deployment workflow enables Cloudflare traces after verifying request URL
query-string redaction, keeping authentication callback parameters out of platform telemetry.
Direct Alchemy deployments leave traces disabled because its SDK does not yet expose that setting.
Manual dispatch of **Deploy travel planner** deploys the selected branch to production, so it
requires deployment authorization even for a PR branch. Automatic deployments remain on `main`.

For a local UI preview without cloud accounts or provider credentials, run:

```sh
vp install
vp run -F @yielded/agent-example-travel-planner preview
```

Open `https://127.0.0.1:4173/travel/` and accept the local certificate. Create an email account;
the terminal prints the file paths of locally delivered verification emails. After
registration, sign in with a fresh email code. Connect the synthetic key
`sk-preview-local` in Settings and send `complete travel cards fixture` to display
sample travel cards. Set `PREVIEW_PORT` to use another port.

This command builds the UI and supplies local SQLite auth and planner bindings.
It uses the real email authentication and session checks, an offline planner, and
fresh state that is removed when stopped. Outbound provider requests are blocked;
GitHub and Yielded sign-in, live research, voice, and published trip sites require the full app.
A raw `vp preview` does not provision these bindings and its session endpoint returns
503 when `AUTH` is missing.

For the full application, configure [.env.example](.env.example) with development-owned
credentials and use `vp run -F @yielded/agent-example-travel-planner dev` from the repository
root. Alchemy supplies the resources declared in [alchemy.run.ts](alchemy.run.ts), including
`AUTH` and `AUTH_EMAIL`. Authentication requires a canonical HTTPS `AUTH_ORIGIN`, a matching
GitHub OAuth callback at `${AUTH_ORIGIN}/travel/auth/github/callback`, a verified email sender, and
three independent persistent base64url-encoded 32-byte auth keys. Shared sign-in also
requires the issuer, registered callback and matching OpenID client secret above.
The deployment workflow reads `TRAVEL_PLANNER_YIELDED_CLIENT_SECRET` from GitHub
Actions secrets and optional `TRAVEL_PLANNER_YIELDED_ACCOUNT_LINKS` from repository
variables. Store the shared secret in the deployment secret manager too.

The conversation loads in stages. `GetPlanner` returns messages, trips, and the latest
source-record overview for up to eight scouts and the trip's editor without reading child
objects. Each `GetPlannerWorker` query then fills in its own status, public progress, and recent
activity. The client shares three request permits across scouts and editor, polls active or unavailable queries
two seconds after each response, and bounds each active read to three seconds. Finished views
stop polling until a new source request, mutation invalidation, or remount. A stalled or failed
worker leaves the conversation and other workers usable. Loading updates are distinct from
starting work or unavailable updates.

Worker activity belongs to its conversation. Switching trips closes its activity dialogs and
removes its cards. Provider failures remain visible in recorded activity; incompatible OpenAI
web-search actions are retained through the diagnostic redaction boundary for investigation.
An already failed scout requires a new user request to continue; observation never restarts it.

Worker query identity includes the signed-in account, conversation, worker, and canonical
request sequence. Changing conversations or replacing a task releases its subscriptions,
cancels queued/in-flight client reads, and prevents late replies from replacing the current
view. Existing mutations invalidate these queries through the shared `planner` reactivity key.
Cancellation stops observation; accepted durable work continues. Native Durable Object RPCs
are finite and separately time-limited on the receiver; cancelling browser fetch does not
promise immediate cancellation of an already dispatched native RPC.

The conversation object verifies the exact `WorkerInputRequested` record and the host's read
authorization before addressing a child. The child computes the compact view locally with one
lookup for that request, a nonterminal scan, and the final 100 canonical records. Limits apply
before decoding and activity projection. Status describes that selected request plus any
active work on the worker; an older pending delivery is not reconstructed as a new task.
Activity is a recent window, not a complete audit log. Reads never admit, recover, or replay
work. Diagnostics retain the existing redaction boundary.

These queries use the framework workspace dependencies in this example. They do not
change the framework's general `Subagent.inspect` or `Subagent.observe` contracts. Any future
framework optimization belongs in a separate library PR. Local Miniflare checks establish behavior and
work budgets; deployed latency requires a separately authorized deployment and measurement.
