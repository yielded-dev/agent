# Travel planner

An Effect Agent example for planning trips through chat or voice, researching destinations
with background agents, saving itineraries, and building standalone trip websites.

The hosted planner lives at <https://agent.yielded.dev/travel/>. The Yielded Site
repository owns its sign-in gateway, identity mapping, permissions and administration.
This repository deploys the planner as an independent Worker. It consumes signed
access tokens; it does not create accounts or maintain a grant list. The browser-use example consumes the same
[shared integration](../shared/src/contract.ts) independently.

It uses Cloudflare:

- **Workers** to serve the TanStack Start UI and APIs.
- **Durable Objects with SQLite** to persist conversations, trips, and agent execution.
- **Browser Run** to research travel websites.
- **Artifacts** to version generated trip-site source code.
- **Workflows and Sandbox containers** to build sites, **R2** to store builds, and
  **Dynamic Workers** to serve them.

The app consumes the repository's Effect Agent workspace packages and uses Effect Atom for client state.
[deployment.ts](deployment.ts) exports the configurable Cloudflare composition;
[alchemy.run.ts](alchemy.run.ts) is its local entrypoint and
[alchemy.production.ts](alchemy.production.ts) configures the hosted Worker.

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

The host serves `/auth/login`, `/auth/session`, `/auth/refresh` and `/auth/logout`.
It sets the `__Host-yielded-access` HttpOnly cookie. The shared consumer verifies
its signature, issuer, resource audience, expiry and configured scope locally,
using public keys cached for five minutes. The browser refreshes before expiry
and retries a rejected pre-admission request once. Account changes dispose the
previous account's queries and drafts before the next account is rendered.

The token's canonical subject identifies the session; its signed `accountId`
selects the planner's existing data namespace. Model requests recheck hosted
funding with the issuer, including durable work resumed without a browser.
Personal keys take priority. Account permissions are managed in the hosted
[Yielded account settings](https://auth.yielded.dev/oauth-settings).

Keep `PLANNER_THREADS_V3` / `PlannerThreadsV3` / `PlannerThreadV3` stable when
adopting an existing deployment. The retained `AUTH_V2` / `AuthV2` /
`PlannerAuthV2` namespace is a read-only migration archive, reachable only through
private bindings. It has no live authentication or grant-management routes.

For a local UI preview without cloud accounts or provider credentials, run:

```sh
vp install
vp run -F @yielded/agent-example-travel-planner preview
```

Open `https://127.0.0.1:4173/travel/` and accept the local certificate. A loopback
preview gateway supplies a temporary test account and signed access cookie.
Connect `sk-preview-local` in Settings and send `complete travel cards fixture`
to display sample cards. `PREVIEW_PORT` selects another port. This preview uses
an offline planner and fresh state removed when stopped; outbound provider calls
are blocked. Live research, voice and published trip sites require the full app.

For the full application, configure [.env.example](.env.example) with your own
auth host and Cloudflare credentials, then run
`vp run -F @yielded/agent-example-travel-planner dev`. The host owns account and
permission policy; the example requires only its verification configuration,
private permission-check credential and persistent BYOK encryption key.

Deploy the hosted planner independently with:

```sh
vp run -F @yielded/agent-example-travel-planner plan:production
vp run -F @yielded/agent-example-travel-planner deploy:production --yes
```

Supply `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`,
`AUTH_YIELDED_CLIENT_SECRET`, the existing `BYOK_ENCRYPTION_KEY`, and optionally
`SERVER_OPENAI_KEY`. The token needs access to Workers, containers and the existing
Alchemy state store. Keep the production stack, Worker and storage identities stable.
The `Deploy travel planner` workflow runs on relevant changes to `main` or manual
dispatch. Its secrets are the two Cloudflare values above,
`TRAVEL_PLANNER_YIELDED_CLIENT_SECRET`, `TRAVEL_PLANNER_BYOK_ENCRYPTION_KEY`
and `OPENAI_API_KEY`. The `/auth/*` route belongs to Site.

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
