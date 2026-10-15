# Browser speed lab

Chat with an agent in a real Cloudflare browser, or try two quick demos: a **Wikipedia race**
(default **Mars → Nelson Mandela**) and a **coffee checkout**. The model writes JavaScript
through `browser_execute`, using CDP to inspect websites and choose interactions. Watch the
browser, follow up in chat, and expand conversation details to inspect timing or download traces.

**Chat with the agent** accepts an optional starting URL and messages up to 8,000 characters.
The agent can choose where to browse, answer questions, and ask for clarification. Follow-up
messages share the same Chromium browser and persisted Thread history. Press Enter to send,
Shift+Enter for a new line, and **New chat** to close the browser and clear the conversation.
The live view stays connected between messages. **Take over** enables interaction after the
agent finishes a message; **Return to agent** restores viewing and enables chat again.
Tell the agent what changed when you send the next message. **Reconnect live view** obtains
a fresh viewing connection without starting another agent run.
Chat runs one message at a time; model and browser comparisons remain in the demos.
If a response connection drops, chat keeps checking that run's status and restores its final
reply and browser when confirmed. Stop ends this wait; the message is never resubmitted.
Chat shows native assistant text as it arrives, returned reasoning summaries when available,
and search/browser activity separately. These summaries are provider-returned content, not
access to hidden reasoning. The final answer appears when the `finish` tool completes; its
JSON arguments are not rendered while streaming. You can draft the next message while the
agent works. Scrolling up pauses automatic scrolling; **Jump to latest** resumes it.

Intermediate text stays visible after completion, failure or Stop. Live output is limited to
64,000 characters and 256 text/summary blocks per run; the chat keeps the session's messages
and shows the latest 20 actions per message. Full action diagnostics remain in run details.
These display limits do not change the Thread history used for follow-ups.

OpenAI chat has three tools: `browser_execute`, hosted `OpenAiWebSearch`, and `finish`.
The agent is instructed to use search for discovery and recommendations, then the browser to
inspect websites and verify products before acting. Replies render clickable source links.
Search events appear in chat and the run trace; their timing overlaps the containing model call.
Search-enabled chat has a 64,000-token context target, with pruning retaining 24,000 tokens.
Workers AI uses search engines through the browser. Start a new chat when changing providers
after hosted search, since native provider tool history belongs to its original provider.
The canned demos retain only the browser and reply tools.

The Wikipedia race asks the agent to follow article links only, with no search, direct URL
entry, back button or extra tabs. The host resolves the destination, records main-page
navigations, and checks the final URL and canonical article. A passing run means **arrival
checked**. Code Mode does not enforce link-only play or prevent page-script tampering;
the route is observed navigation evidence, not proof that every move obeyed the game rules.
Leaving ordinary English Wikipedia articles or exceeding 20 hops fails the arrival check.

**Check out a bag of coffee** opens Hedge Coffee and prepares checkout with synthetic test
details. It instructs the agent to stop before submitting an order or payment. This demo is
available only to funded, signed-in accounts on the public site. There is no hard payment
guard, and its outcome is **unverified**. Use only stores you are authorized to operate.

The host explicitly permits arbitrary page CDP and JavaScript. There is no website allowlist,
private-network containment or payment guard. Generated code can trigger real actions,
including purchases. Use authorized goals and accounts. Page content is untrusted.
The isolated Dynamic Worker has no direct network or secret bindings; its browser capability
still has the browser's network and page authority.

## Run locally

From the repository root:

```sh
vp install
cp examples/browser-speed/.dev.vars.example examples/browser-speed/.dev.vars
vp run -F @yielded/agent-example-browser-speed build
```

Fill the ignored `.dev.vars` file with `CLOUDFLARE_ACCOUNT_ID`,
`BROWSER_RENDERING_API_TOKEN` (Browser Run Write), and `OPENAI_API_KEY`.
The project's Infisical `dev` folder `/browser-use` holds `BROWSER_USE_DEV_API_TOKEN` and
`BROWSER_USE_DEV_ACCOUNT_ID` for remote development; the OpenAI key is in `/`. Use the development
token for both Wrangler's `CLOUDFLARE_API_TOKEN` and the Worker's `BROWSER_RENDERING_API_TOKEN`,
and its account ID for `CLOUDFLARE_ACCOUNT_ID`. The remote browser binding and session credentials
must use the same Cloudflare account.

`WORKERS_AI_API_KEY` enables the optional Workers AI model. The `LOADER` Worker Loader
binding is configured in Wrangler for both environments and runs generated code in isolated
Dynamic Workers. Wrangler also needs the appropriate Cloudflare login or deployment credential.

In separate terminals:

```sh
vp run -F @yielded/agent-example-browser-speed worker --name effect-agent-browser-dev
vp run -F @yielded/agent-example-browser-speed dev
```

Open **http://127.0.0.1:5191/browser-use/** and send a message, or choose a demo.
Browser and model calls use real services. `worker --local` is useful for API checks but
disables the remote browser binding.
Use a separate development Worker name to keep its remote proxy session independent of the
deployed application's hostname and Cloudflare Access policy.

The OpenAI model picker offers GPT-6 Astra, GPT-6.1 Sol and GPT-6 Luna. Runs default to Fast
processing; Standard is also available. Ultrafast is available for Astra and Sol. Comparing
models with Ultrafast selected runs Luna on Fast, and switching to Luna displays Fast.
Direct API requests for Luna with Ultrafast are rejected before opening a browser.
Reasoning is independent of speed: Luna defaults to none, while Astra and Sol use low.
Model spans retain returned token counts and service tiers when available.

Chromium uses a host-owned Browser Session. Kitesurf uses its ephemeral CDP connection;
it has no persistent Live View and may differ in website or CDP support. A requested engine
never silently falls back. Screenshots and Live View display the original session page;
additional tabs remain accessible to the agent through CDP.

## Browser ownership

One browser tab owns one lab session and admits one run at a time. Each message or demo run
gets 120 model turns, 300 tool calls, 1,000,000 model tokens, a ten-minute agent deadline,
and 2,000 CDP operations. Each CDP result is bounded to 256 KiB; the platform allows at most
eight tabs. Each CDP command has a 60-second deadline; each Code Mode call gets two minutes,
with its own source, host-call and output limits.
The owner has a twelve-minute overall deadline and a durable cleanup alarm.

Demos prepare a fresh browser for each run and close it afterward. Chat retains the browser
for up to one hour, with ten minutes of idle time between messages. Completed messages persist
in the owner’s SQLite storage, so an idle Worker restart preserves the conversation and browser.
Chat captures screenshots only when requested. If the final screenshot fails, the chat ends
and browser cleanup runs.
Reloading the page restores this tab's session, saved reports and active conversation.
An expired chat refuses follow-up input; choose
**New chat** to start again. A failed or interrupted message is never replayed after restart.

Fresh browser preparation may retry once after confirmed cleanup. Follow-up attachment,
model calls and browser actions are not replayed. **Stop run** interrupts active work and
closes its browser; failed runs also close it. **Close browser** (or **New chat**) retries
cleanup or releases a recovered browser. Leaving the page does not guarantee immediate
cancellation; deadlines and alarms still apply. Unresolved requests stay fenced.

Taking over is available between messages, never concurrently with agent work. The lab rejects
run requests while human control is selected. Returning control replaces the embedded viewer;
it cannot revoke an interactive connection copied elsewhere. Closing the browser ends access.
When the agent needs you to act in the browser, it can show a handoff card with instructions
and a **Take over** button. Complete the step, choose **Return to agent**, then send a message
to continue. Ordinary questions stay in chat. Handoff cards remain in the conversation and
its traces; only the latest request can offer control of the current browser.

For native forms with username and password fields, the card can show a private sign-in form.
The entry goes directly to the host's credential-fill operation, outside conversation input
and trace payloads. A request can fill only its original form and is consumed before dispatch;
changed, expired, or uncertain requests are never automatically repeated. After filling, take
over to finish signing in. Dismiss the form to continue manually. The website and
browser provider receive these values, and later agent page access can still expose them.

Provider keys stay in the Worker unless visitors supply their own. Visitor keys are sent only
on run requests and excluded from stored owner state and reports. The UI keeps keys for the
browser session, or on the device when explicitly selected. Live View URLs and screenshots
are transient and excluded from exports. Goals and model output may still contain sensitive
website data; review exports before sharing them.

**Conversation details and traces** shows every message's timeline and cumulative active time,
excluding time waiting for the user. Each message's timestamps use its own admission-relative
Worker clock; work time starts after the initial page is loaded. CDP timing includes reads as
well as writes. These reports do not establish a comparative performance claim.

## Debug traces

Each admitted run saves diagnostic events incrementally in the owner's SQLite storage, then
archives a JSON file to the private `TRACES` R2 bucket on completion. R2 failure leaves the
SQLite copy available; downloading retries the upload. **Download conversation trace** exports
all messages together for another person or agent to inspect, without granting browser access.
Downloads write one run at a time to a temporary file in browser storage, requiring sufficient
disk space and a browser with the Origin Private File System enabled. The file remains until
the next download replaces it or the site's browser storage is cleared.
Each archive includes the report, prompts and native response metadata, provider request metadata,
generated JavaScript, CDP arguments/results, execution logs, failures, control transfers, agent
events and the deployed Worker version.
Its `otlp` array contains standard OTLP/JSON trace export bodies from Effect, including span
identities, parents, attributes, events and timing. These can be submitted to a collector's
`/v1/traces` endpoint individually. Interrupted requests retain incremental evidence; a report
still marked running is unresolved, not proof of completion.

Trace capture has a 32 MiB event budget per run and a 1 MB payload limit per event. Oversized
events carry a truncation marker and original size; the archive records dropped-event counts.
Raw model deltas retain metadata only; completed content is in model output and agent events.
Provider credentials, credential fields (including CDP cookies) and viewer URLs are redacted;
screenshot payloads are omitted from diagnostic copies. If the run's sensitive-value index
exceeds 1 MB, subsequent diagnostic text is omitted. Live browser return values are unchanged.
Website content and user-entered personal data can still appear in prompts, generated code and results. Review downloaded files before sharing.
Trace downloads require the owning lab session; the bucket has no public download route.

R2 keys use `browser-use/v1/<owner>/<conversation>/<run>.json`. Create the configured
`effect-agent-browser-traces` bucket before deploying this example:

```sh
vp exec wrangler r2 bucket create effect-agent-browser-traces
```

Local development uses emulated R2. Hosts without a `TRACES` binding keep downloadable SQLite
traces. Each lab session admits at most 100 runs; open a fresh tab for another session.

## Validate and deploy

```sh
vp run -F @yielded/agent-example-browser-speed check
vp run -F @yielded/agent-example-browser-speed test
vp run -F @yielded/agent-example-browser-speed build
```

The existing tests exercise Worker request validation, owner fencing, cleanup and interruption,
without replaying uncertain work. Live browser/model execution requires configured services.

The private Worker remains `effect-agent-browser-speed`, with the `BrowserLab` SQLite Durable
Object and its original `v1` Wrangler migration. Configure Worker secrets, protect the entire
`workers.dev` hostname with Cloudflare Access (including `/api/*`), then deploy:

```sh
vp run -F @yielded/agent-example-browser-speed deploy
```

Preview URLs are disabled. If using a custom domain instead, disable `workers_dev` and protect
that hostname too; a custom-domain Access policy does not protect `workers.dev`.

The hosted browser-use deployment is composed in the Yielded Site repository.
It consumes the shared authentication integration directly, without a planner
binding. Configure `AUTH_ISSUER`, `AUTH_RESOURCE`, `AUTH_JWKS_URL`,
`AUTH_REQUIRED_SCOPE` and `AUTH_HOSTED_MODEL_SCOPE` for your auth host. The
`__Host-yielded-access` cookie supplies a signed identity and scopes; a private
issuer check authorizes each hosted run using `AUTH_YIELDED_CLIENT_SECRET`.
Allowed accounts can use `FUNDED_OPENAI_API_KEY`; other visitors supply their
own key. Authentication or permission lookup failures never fund a request.
