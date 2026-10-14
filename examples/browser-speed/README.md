# Browser speed lab

Chat with an agent in a real Cloudflare browser, or try two quick demos: a **Wikipedia race**
(default **Mars → Nelson Mandela**) and a **coffee checkout**. The model writes JavaScript
through `browser_execute`, using CDP to inspect websites and choose interactions. Watch the
browser, follow up in chat, and expand run details to inspect timing or download reports.

**Chat with the agent** accepts an optional starting URL and messages up to 8,000 characters.
The agent can choose where to browse, answer questions, and ask for clarification. Follow-up
messages share the same Chromium browser and persisted Thread history. Press Enter to send,
Shift+Enter for a new line, and **New chat** to close the browser and clear the conversation.
Chat runs one message at a time; model and browser comparisons remain in the demos.

OpenAI chat has three tools: `browser_execute`, hosted `OpenAiWebSearch`, and `finish`.
The agent is instructed to use search for discovery and recommendations, then the browser to
inspect websites and verify products before acting. Replies render clickable source links.
Search events appear in the run trace; their timing overlaps the containing model call.
Search-enabled chat has a 64,000-token context target, with pruning retaining 24,000 tokens.
Workers AI uses search engines through the browser. Start a new chat when changing providers
after hosted search, since native provider tool history belongs to its original provider.
The canned demos retain only the browser and reply tools.

Recoverable code and page-script errors return diagnostics to the agent. The demo refuses
completion after these errors until a revised program makes a successful browser observation;
the agent is instructed to inspect current state before any further mutation and continue
permitted work. Browser transport failures and exhausted limits can still end the run.
This check does not verify that a product matches the request or that a cart is correct.

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
The project's Infisical `dev` folder `/browser-use` holds the REVE browser token and its
matching account ID; the OpenAI key is in `/`. Use the same Cloudflare account for Wrangler's
remote browser binding and the session credentials.

`WORKERS_AI_API_KEY` enables the optional Workers AI model. The `LOADER` Worker Loader
binding is configured in Wrangler for both environments and runs generated code in isolated
Dynamic Workers. Wrangler also needs the appropriate Cloudflare login or deployment credential.

In separate terminals:

```sh
vp run -F @yielded/agent-example-browser-speed worker
vp run -F @yielded/agent-example-browser-speed dev
```

Open **http://127.0.0.1:5191/browser-use/** and send a message, or choose a demo.
Browser and model calls use real services. `worker --local` is useful for API checks but
disables the remote browser binding.

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
Reloading the page starts a new UI session. An expired chat refuses follow-up input; choose
**New chat** to start again. A failed or interrupted message is never replayed after restart.

Fresh browser preparation may retry once after confirmed cleanup. Follow-up attachment,
model calls and browser actions are not replayed. **Stop run** interrupts active work and
closes its browser; failed runs also close it. **Close browser** (or **New chat**) retries
cleanup or releases a recovered browser. Leaving the page does not guarantee immediate
cancellation; deadlines and alarms still apply. Unresolved requests stay fenced.

Provider keys stay in the Worker unless visitors supply their own. Visitor keys are sent only
on run requests and excluded from stored owner state and reports. The UI keeps keys for the
browser session, or on the device when explicitly selected. Live View URLs and screenshots
are transient and excluded from exports. Goals and model output may still contain sensitive
website data; review exports before sharing them.

Run history and comparisons stay in the tab; the Durable Object keeps completed chat history,
its latest report and admission state.
Export before reloading. The report's timestamps use one admission-relative Worker clock;
work time starts after the initial page is loaded. CDP timing includes reads as well as writes.
These reports do not establish a comparative performance claim.

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

The `public` environment remains `yielded-agent-browser-use`, serving
`agent.yielded.dev/browser-use`. It rate-limits runs to six per minute per address. Visitors
supply an OpenAI key; same-origin travel planner accounts on the funding allowlist can use
`FUNDED_OPENAI_API_KEY`. Auth recognizes the shared Yielded owner and direct GitHub owner,
and checks active identities and funding grants for each run. Auth lookup failures leave
the request unfunded. The setup lists only missing requirements.

Every push to `main` runs [Deploy Agent demos](../../.github/workflows/deploy-travel-planner.yml),
which checks both applications and deploys the planner before the browser lab. Automatic
deployment uses the repository variable `TRAVEL_PLANNER_DEPLOY_ENABLED=true` and the existing
Cloudflare secrets. For a manual deployment of the lab alone:

```sh
vp run -F @yielded/agent-example-browser-speed deploy:public
```
