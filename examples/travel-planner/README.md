# Travel planner

An Effect Agent example for planning trips through chat or voice, researching destinations
with background agents, saving itineraries, and building standalone trip websites.

The planner lives at <https://agent.yielded.dev/travel/>. It shares host-only sign-in
cookies (`__Host-elsewhere-auth-*`, `Path=/`) with `/browser-use/`. Link to
`/travel/login?return=%2Fbrowser-use%2F` to return to the lab after sign-in;
only `/travel/` (the default) and `/browser-use/` are accepted return targets.
Generated trip apps stay on `*-trip.effect-agent.com`, outside the session origin.

`travel.effect-agent.com` redirects paths and queries to `/travel` with HTTP 301.
Auth state lives in `AUTH_V2` / `AuthV2` / `PlannerAuthV2`; account-owned threads live in
`PLANNER_THREADS_V3` / `PlannerThreadsV3` / `PlannerThreadV3`. The beta.34 cutover replaces both
namespaces and removes the retired account-thread namespace, deleting previous accounts,
sessions, funding grants and planner conversations. Sign up again after deployment.
The main deployment detaches the browser lab from the retired auth namespace before
the reset and rebinds it afterward; shared lab sign-in is unavailable during that interval.
Alchemy keys these namespaces by env binding: a class-only rename preserves data;
new binding keys and new classes create empty stores and delete the removed namespaces.
The lab binds `PlannerAuthV2` on script `effect-agent-travel-planner`, instance
`auth-v1`; `/_internal/session` and `/_internal/funding/<id>` remain private.

Shared sign-in uses `https://auth.yielded.dev` as its OpenID issuer. Register client
`yielded-agent` there with callback
`https://agent.yielded.dev/travel/auth/yielded/callback`. Set `AUTH_YIELDED_ISSUER`
to that issuer and `AUTH_YIELDED_CLIENT_SECRET` to its separate client secret.
The central GitHub app's callback stays at Auth. Direct GitHub sign-in remains
with email and account switching under **Other sign-in options**, with callback
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
turning reasoning off. Accepted work retains its admitted model settings. Retained voice requests
keep their original settings when reconnecting or retrying.

Listing photos come from inspected page galleries. When a gallery yields no usable images,
the reader also checks Open Graph and Twitter image metadata within the same inspection deadline.
These source images do not verify amenities or availability; unavailable photos leave the cards usable.

The main planner uses native hosted web search in its own model call for focused questions.
It delegates longer or independent research to scouts and receives standard `WorkerUpdate`
and `WorkerCompletion` messages. New input can restart its disposable model response;
the preview clears cancelled text and marks interrupted search activity incomplete.
The early thinking indicator contains no private model reasoning.

The planner and scout select one current binding per agent ID.

Authentication uses `@yielded/auth` beta.34, its built-in GitHub and OpenID Connect
providers, the portable `@yielded/crypto` backend, and the Drizzle persistence adapter. The app owns
its SQLite tables and provides the adapter's `SqliteDo.Database` through `databaseLayer`.
It supplies session claims through each strategy's `SessionClaims` service. The Atom client uses Fetch transport with
a 30-second request and response-body deadline; timed-out mutations are not retried automatically.
Provider sign-in creates flow IDs on the server; the browser retains only the
provider, returned flow ID and return target for callback completion. Effect Atom
owns this workflow, registration and query invalidation; React dispatches and renders.
**Continue with Yielded** reuses the current Auth account automatically, then
verifies issuer, audience, nonce, PKCE and the signed identity before establishing
an Agent session. Agent requests `max_age=240` so authentication stays within its
five-minute evidence policy, including time to complete the callback. Auth starts
GitHub sign-in automatically when its session is absent or older than that limit;
the original `auth_time` is preserved. The initial screen shows one sign-in button;
**Other sign-in options** contains account switching and the existing direct GitHub/email
methods. **Use another Yielded account** explicitly requests account selection; new permissions require
consent. Cancelling returns to login with a retry action. Auth
and Agent have separate sessions: **Sign out of Agent** leaves Auth signed in.
The browser lab already consumes this Agent session, so the same login covers it;
Sync and docs are not connected by this change.

Each new provider identity creates a separate Agent account; email and display name
never link existing accounts. Auth starts from one initial schema with no old-data
conversion. Email codes use single-use proofs, and OAuth callbacks consume their
flow before exchanging a token. Email verification keys come from `ProofKeys` using
`AUTH_PROOF_KEY`. Auth owns one application scope per Object incarnation for provider resources,
bounded proof delivery and abuse counters. A proof receipt confirms admission, not email delivery.
Eviction resets in-memory counters and can interrupt pending delivery; proof expiry and
single-use consumption remain durable.

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

Open `https://127.0.0.1:4173/travel/` and accept the local certificate. Expand
**Other sign-in options** and create an email account;
the terminal prints the file paths of locally delivered verification emails. After
registration, sign in with a fresh email code. Connect the synthetic key
`sk-preview-local` in Settings and send `complete travel cards fixture` to display
sample travel cards. Set `PREVIEW_PORT` to use another port.

This command builds the UI and supplies local SQLite auth and planner bindings.
It uses the real email authentication and session checks, an offline planner, and
fresh state that is removed when stopped. Outbound provider requests are blocked;
GitHub and Yielded sign-in, live research, voice, and published trip sites require the full app.
A raw `vp preview` does not provision these bindings and its session endpoint returns
503 when `AUTH_V2` is missing.

For the full application, configure [.env.example](.env.example) with development-owned
credentials and use `vp run -F @yielded/agent-example-travel-planner dev` from the repository
root. Alchemy supplies the resources declared in [alchemy.run.ts](alchemy.run.ts), including
`AUTH_V2` and `AUTH_EMAIL`. Authentication requires a canonical HTTPS `AUTH_ORIGIN`, a matching
GitHub OAuth callback at `${AUTH_ORIGIN}/travel/auth/github/callback`, a verified email sender, and
three independent persistent base64url-encoded 32-byte auth keys. Shared sign-in also
requires the issuer, registered callback and matching OpenID client secret above.
The deployment workflow reads `TRAVEL_PLANNER_YIELDED_CLIENT_SECRET` from GitHub
Actions secrets. Store the shared secret in the deployment secret manager too.

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
