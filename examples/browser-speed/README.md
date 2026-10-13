# Browser lab

Give an agent a starting URL and a goal, then watch it browse in a real Cloudflare browser.
The model writes JavaScript through `browser_execute`, using CDP to read pages, navigate,
interact with forms, and manage tabs. The UI shows the live browser, screenshots, model and
CDP timings, and downloadable run reports.

The task-board presets provide a repeatable local page. On completion, the host compares the
entire saved board with the preset's expected result and returns mismatches for correction.
That check is independent of the agent's completion claim, but the page and its ledger remain
writable by page JavaScript. It is not tamper-resistant evidence. Arbitrary website results
are always marked **unverified**.

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
`WORKERS_AI_API_KEY` enables the optional Workers AI model. The `LOADER` Worker Loader
binding is configured in Wrangler for both environments and runs generated code in isolated
Dynamic Workers. Wrangler also needs the appropriate Cloudflare login or deployment credential.

In separate terminals:

```sh
vp run -F @yielded/agent-example-browser-speed worker
vp run -F @yielded/agent-example-browser-speed dev
```

Open **http://127.0.0.1:5191/browser-use/**, enter a URL and goal, and start browsing.
Browser and model calls use real services. `worker --local` is useful for API checks but
disables the remote browser binding.

The model picker uses a host-approved catalog. OpenAI runs default to Fast processing and no
reasoning; models that require reasoning use low instead. Fast and reasoning are independent
settings. Model spans retain returned token counts and service tiers when available.

Chromium uses a host-owned Browser Session. Kitesurf uses its ephemeral CDP connection;
it has no persistent Live View and may differ in website or CDP support. A requested engine
never silently falls back. Screenshots and Live View display the original session page;
additional tabs remain accessible to the agent through CDP.

## Browser ownership

One browser tab owns one lab session and admits one run at a time. Each run gets 40 model turns,
100 tool calls, 300,000 model tokens, a three-minute agent deadline, and 500 CDP operations.
Each CDP result is bounded to 256 KiB; the platform allows at most eight tabs. Code Mode adds
its own per-execution source, time, host-call and output limits. The owner has a four-minute
overall deadline, a ten-minute browser lifetime and a durable cleanup alarm.

A fresh browser is prepared for each run. Preparation may retry once after confirmed cleanup;
model and browser actions are not replayed. **Stop run** interrupts active work. **Close browser**
retries cleanup or releases a recovered browser. Leaving the page does not guarantee immediate
cancellation; the owner deadline and alarm still apply. Unresolved requests stay fenced.

Provider keys stay in the Worker unless visitors supply their own. Visitor keys are sent only
on run requests and excluded from stored owner state and reports. The UI keeps keys for the
browser session, or on the device when explicitly selected. Live View URLs and screenshots
are transient and excluded from exports. Goals and model output may still contain sensitive
website data; review exports before sharing them.

Run history stays in the tab; the Durable Object keeps its latest report and admission state.
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
and the board verifier. Live browser/model execution requires configured services.

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
`FUNDED_OPENAI_API_KEY`. Auth lookup failures leave the request unfunded.

```sh
vp run -F @yielded/agent-example-browser-speed deploy:public
```
