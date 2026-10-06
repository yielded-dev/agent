# Browser speed lab

Live at **[agent.yielded.dev/browser-use](https://agent.yielded.dev/browser-use/)**: visitors bring
their own keys, and allowlisted travel planner accounts run on the lab's.

Ask **Jev** or a model agent: **“Starting at the Wikipedia page for Mars, get to Nelson Mandela.”**
Watch a real Cloudflare browser follow article links and inspect the route, hop count, and
decision/action timings. The browser independently verifies arrival; claiming success cannot win.
The task-board presets remain available for repeatable form and batching measurements.

```text
React + Effect Atom → typed HTTP API → one browser owner per tab
                                        ├─ Jev loop or Effect Agent → observed links or controls
                                        ├─ Cloudflare Browser Run → Wikipedia or task board
                                        └─ independent verifier → route + trace + result
```

The default **Wikipedia race** accepts starting and destination article titles. It permits only
ordinary English Wikipedia article links in the current article body: no search, URL entry,
back button, namespaces, external links, or fragment shortcuts. Each observation contains an
article excerpt and up to 80 unique links for the model agent; it can page through the current article's
remaining links or filter their titles and labels with a case-insensitive substring. Filtering
only reads links already on the current article; offsets and counts refer to the matching links.
Refs expire when another link page is read or navigation occurs, and every click rechecks its anchor.
Clicks use the same guarded native adapter as the task board, including scrolling and
hit-target verification. A condition wait verifies the next article document after acknowledged input.

The host resolves the destination's canonical title through Wikipedia's title API before the
race; that lookup gives the agent no route. A win requires an observed link click, a valid article,
and matching browser URL and canonical destination. Redirects count as one hop. The route records
the clicked link and actual landing page. Navigation failures with uncertain outcomes fence further
clicks. The agent has 20 hops, 40 turns, 60 tool calls, 300,000 model tokens, and three minutes;
browser preparation and cleanup fit within the owner's four-minute deadline.
Native context pruning starts at 10,000 estimated tokens and retains 4,000 recent tokens, without
an extra summarization model call. Every observation carries the route so it survives pruning.
Exhausted budgets fail explicitly rather than asking the model to claim a result.

Live pages, routes, network conditions, and link pagination affect results. Compare success rate,
verified time, and hops for the **same start and destination**; a task-board winner is not a
Wikipedia winner. Page text is untrusted input. Tools do not expose arbitrary JavaScript or URLs.

The reusable browser tools come from `@yielded/agent/browser-use`: `BrowserUse.make({ mode })`
for the model agent and `BrowserUse.runJev` for Jev, both over the upstream native controller.
Wikipedia eligibility, route choice, verification, model configuration, and the comparison UI
remain here. [Consumer setup](../../docs/src/content/docs/guide/browser.md#let-jev-drive-the-browser).

The task board uses self-contained HTML and exposes observed clicks, fills, and selections.
Its preset verifier checks the complete saved board before accepting completion. A mismatch
returns the saved values so the agent can correct omissions within its remaining budget.
Free-form board requests are **unverified**.
Task-board agents have 30 turns, 100 tool calls, 300,000 model tokens and three minutes.
Mechanical pruning bounds each context to 10,000 estimated tokens and retains a 4,000-token
recent tail. Exhaustion fails explicitly; partial board changes cannot pass the verifier.
With **Driver → Jev**, `BrowserUse.runJev` operates the board from Jev observations and writes
field values with Mercury 2.5 when `OPENROUTER_API_KEY` is set, otherwise GPT-6 Luna. The same
verifier reads the saved board after Jev stops; a DONE claim alone never passes.

**Check out a bag of coffee** runs on a real store, [Hedge Coffee](https://www.hedge.coffee/store):
add one bag to the cart and open the checkout. The lab never enters details or pays:

- A guard keeps every page navigation on `www.hedge.coffee`; payment and captcha frames still load.
- The checkout page is read-only. Host authorization refuses every click, fill and key press there.
- After the driver stops, the host reads the order summary itself. The run passes only on the
  checkout's first step with exactly one bag, quantity 1.
- The public lab runs it only for allowlisted accounts, so visitors cannot fill a real merchant's
  store with abandoned carts. Every run still leaves one.

The model agent uses whole-page observations here, because "Add To Cart" sits below the fold and
the agent cannot scroll. Jev often adds two bags: Squarespace keeps the hidden "Added!" label in
the button, so Jev's observed name never changes after the click. Jev's naming matches
jev-ultrafast's, so changing it needs a new parity run.

## Run a standalone browser journey

The `journey` task takes a goal and uses the public native tools without the comparison UI.
Choose hosted `chromium` or `kitesurf`, or `local-chromium`; it never switches engines.
Supply the chosen browser and model credentials using [live credentials](../../docs/TOOLCHAIN.md#live-credentials).
Start local Maple on its OTLP HTTP port, then run:

```sh
BROWSER_JOURNEY_ENGINE=kitesurf \
BROWSER_JOURNEY_OUTPUT=/tmp/browser-journey-1 \
BROWSER_JOURNEY_GOAL='Navigate to https://example.com and report its visible heading.' \
vp run -F @yielded/agent-example-browser-speed journey
```

The output directory must be new. The default direct host uses GPT-6 Luna, Fast processing, no reasoning,
and a 1280 × 900 viewport; `BROWSER_JOURNEY_MODEL` chooses another compatible model
and `BROWSER_JOURNEY_REASONING` selects none, low, medium or high reasoning.
It permits 50 turns, 100 tools, eight minutes and two million cumulative tokens;
`BROWSER_JOURNEY_TOKEN_BUDGET` changes the token limit. Exhaustion fails the run.
The default direct journey uses viewport observations, settles after input, and caps condition
waits at five seconds, returning the current observation at the cap. Set
`BROWSER_JOURNEY_OPTIMIZED=false` to compare the original observation and wait settings.
Each turn starts from full canonical history. Existing mechanical compaction bounds each
model context to 60,000 estimated tokens, retaining an 8,000-token recent tail; older tool
results can be pruned before dispatch, and the full journal remains available in the export.
`result.json` records the actual CDP engine revision, goal, trace ID, agent claim and a
separate native DOM read. `thread.json` retains the existing conversation export;
`page.png` and `cleanup.json` retain the final page and closure acknowledgement. Traces
go to local Maple at `http://127.0.0.1:4318/v1/traces`. Review tool receipts and native
preparation/input spans before rerunning a failure.

Results remain `verified: false`: neither an agent claim nor an input acknowledgement
proves saved state. Independently verify the requested outcome and retain source revision,
inputs, reset rules and trace alongside that evidence. A failed setup can leave only
the cleanup record and command log. This host supplies no authenticated account.
For a disposable native reproduction, `BROWSER_JOURNEY_HTML`
accepts host-owned fixture HTML before the agent starts.

`BROWSER_JOURNEY_PATH=jev` runs `BrowserUse.runJev` over Jev observations and requires
`TYPESAFEAI_API_KEY`. A text model supplies field values; it defaults to GPT-6 Luna through
OpenAI. For Mercury 2.5, set `BROWSER_JOURNEY_TEXT_PROVIDER=openrouter`,
`BROWSER_JOURNEY_TEXT_MODEL=inception/mercury-2.5`, `BROWSER_JOURNEY_TEXT_REASONING=none`,
and supply `OPENROUTER_API_KEY`. Field generation has a five-second timeout and one retry
on timeout. The loop defaults to 60 steps (`BROWSER_JOURNEY_STEPS`) and eight minutes.
It caps each target question at 255 choices, uses enabled viewport controls and visible text,
and stops after three input proposals in a row leave the page unchanged, including refusals.
Page-wide decisions are refreshed after model latency. DONE remains an unverified claim.
`jev-result.json` records every step and receipt; `jev-requests.jsonl` keeps raw Jev requests.

`BROWSER_JOURNEY_PATH=code` uses the same tools and broker through the
production isolated Dynamic Worker executor hosted by local Miniflare. Compare
identical goals and settings before choosing a path.

Kitesurf currently has compatibility gaps in cross-origin classic script loading,
replacing nonempty number inputs, contenteditable input, native HTML dialogs and JavaScript dialogs. Keep those failures in its cohort;
Chromium success does not establish Kitesurf support. The task board requires native HTML
dialogs and refuses unsupported engines before model execution. Choose an engine before dispatch.
Starting another engine loses session continuity and cannot reconcile or replay uncertain input.

For local Chromium, set `BROWSER_JOURNEY_ENGINE=local-chromium` and
`BROWSER_TEST_EXECUTABLE` to Chrome's executable path. Human authentication setup can use
`BROWSER_JOURNEY_HEADLESS=false`, `BROWSER_JOURNEY_SETUP_URL`, and a host-owned
`BROWSER_JOURNEY_SETUP_SELECTOR`. The agent starts after that selector appears; setup
duration is recorded separately. `BROWSER_JOURNEY_START_URL` opens the task's page after
setup. An optional `BROWSER_JOURNEY_PROFILE` retains a dedicated local test profile;
keep its credentials outside evidence exports and remove it after testing.
`BROWSER_JOURNEY_ALLOWED_URL_PREFIX` limits operations to that origin and path.
Do not put credentials in goals, selectors, or artifact paths.

## Run locally

From the repository root:

```sh
vp install
vp run -F @yielded/agent-example-browser-speed build
cp examples/browser-speed/.dev.vars.example examples/browser-speed/.dev.vars
```

Fill in the ignored `.dev.vars` file:

| Variable                      | Purpose                                                                      |
| ----------------------------- | ---------------------------------------------------------------------------- |
| `CLOUDFLARE_ACCOUNT_ID`       | Account that owns the browser                                                |
| `BROWSER_RENDERING_API_TOKEN` | Account-scoped Browser Run Write token for lifecycle and read-only Live View |
| `OPENAI_API_KEY`              | Model agents, and Jev field text without OpenRouter                          |
| `WORKERS_AI_API_KEY`          | Optional Cloudflare Workers AI key for the Llama comparison                  |
| `TYPESAFE_API_KEY`            | Jev through native Effect DecisionModel                                      |
| `OPENROUTER_API_KEY`          | Optional Mercury 2.5 field text for Jev task-board runs                      |

Wrangler also needs its normal Cloudflare login or deployment credential for the remote browser
binding. Browser Run uses the real service during local development; agent runs also call the real
model. Start these commands in separate terminals:

```sh
vp run -F @yielded/agent-example-browser-speed worker
vp run -F @yielded/agent-example-browser-speed dev
```

Open **http://127.0.0.1:5191/browser-use/** and press **Start race**. Mars → Nelson Mandela is already filled in,
and Jev drives by default; choose **Model agent** to compare.
A real Cloudflare browser opens Wikipedia; its route and timings appear beside the live view.
The app connects automatically; there is no login or app token. Cloudflare and model credentials
stay in the Worker. Missing configuration disables runs with an explanation.
`worker --local` disables remote bindings for API checks; it cannot run a browser benchmark.

The model picker offers **GPT-6 Luna** (initial selection), **GPT-6 Sol**, and **Llama 3.3**. OpenAI models
use Responses; Llama uses Workers AI Chat Completions in the configured Cloudflare account.
Provider credentials stay on the server, and requests can select only this approved catalog.
Missing credentials disable the corresponding option.

The model agent starts with **Fast** processing and **none** reasoning, as do API requests that
omit them. **OpenAI speed** switches
between Fast and Standard; **Reasoning** exposes none, low, medium, high, xhigh, and max.
Fast is a service tier, independent of reasoning effort, and has premium OpenAI pricing.
The Worker explicitly sends both settings and records the tier actually returned by OpenAI.
Inspect a model span for the served tier, reasoning tokens, and any provider-returned reasoning
summary (bounded to 16,000 characters). A missing summary or tier remains unavailable; neither is
inferred. Llama has no equivalent controls. OpenAI calls allow 16,384 output tokens including
reasoning, within the task-specific token budget.

## Compare runs

**Browser** selects Kitesurf (beta) or Chromium. **Compare both browsers** repeats the same
task and model settings on each engine, rotating the order each round. Chromium is the default
in the UI and for API requests without an engine. A requested engine never falls back
to the other. Reports record `input.engine`, CDP product, revision, and user agent. History keeps
engines and revisions in separate cohorts; old reports without an engine used Chromium.
Both engines use the same Puppeteer actions, link rules, verification, 30-second native-command
deadline, and page-ready clock. Navigation and decision calls retain their narrower deadlines.
Kitesurf's site and CDP compatibility can differ; preparation and flow failures remain visible.
Chromium uses a host-owned persistent session; Kitesurf uses the documented ephemeral CDP
WebSocket and closes it at scope exit. The persistent-session POST endpoint currently ignores
`browser=kitesurf`, so the lab does not use it for Kitesurf. Preparation checks the provider's
`@kitesurf` CDP revision before starting the clock. Kitesurf has screenshots but no persistent Live View or
session resumption; interrupted actions are never replayed.

**Driver → Jev** on a Wikipedia race gives Jev the current article, excerpt, destination,
route so far, and every eligible article link's title, label and href. Jev chooses the next hop;
the browser clicks that observed link and verifies arrival. No language model, OpenAI key, or
planner fallback is used. Jev returns a choice distribution, not a prose reasoning summary.

Large pages use balanced groups of at most 255 links, batched in one request, followed
by a choice among group winners. Every eligible link participates; probabilities across groups
are never compared. The run fails explicitly above 5,000 unique eligible links or 10,000 source
anchors. Jev chooses the best exploratory hop even when the connection is indirect; there is no
abstention option. A page with just one eligible link follows it without a decision request.
Jev-only stops on invalid output, timeout, 20 hops, three minutes, or 300,000
reported decision tokens. Each decision request has a 15-second deadline and no retry. Route
choices have no probability cutoff: several routes may be useful, and probability does not
guarantee eventual success. The independent browser verifier remains authoritative.
Jev-only accepts two-decimal distribution rounding within 0.02 of total mass 1,
rescales those probabilities for native DecisionModel validation, and records the raw selected
probability and total in each trace. It never changes the chosen link or confidence. Missing,
out-of-range, nonfinite, or more divergent probabilities still fail.

Compare the **model** and **Jev** drivers. Jev sees all links at once; the model sees 80 per page.
Both stop explicitly above 10,000 source anchors instead of returning incomplete observations.
This compares complete navigation strategies, not model latency on identical observations.
The trace shows candidate/question counts, selected refs, probabilities, tokens, and separate
planner/Jev call counts. History keeps the route driver in its cohort settings.

Select a model, or **Compare configured models** to run the same task with each available model.
Each repetition rotates the starting model/browser configuration; runs execute sequentially and return to the starting article or reset the board.
The comparison table separates model, driver, reasoning, and requested tier,
includes failures in the flow success count, lists preparation failures separately, and shows
median ready-to-verified latency and successful race hop counts. Served tiers are reported
across the entire configuration, including `unknown` when no tier was returned. A few runs
do not establish a speed advantage.

The model driver offers three execution modes:

- **Scripted** runs a fixed UI sequence through the same action/observation implementation. It is
  a diagnostic baseline, not a claim about the fastest possible sequence.
- **Individual** lets the model choose one action per tool call.
- **Batched** allows up to eight sequential actions on already observed controls. A batch stops at
  its first failure and reports how many actions completed; completed actions must not be replayed.

Live View is provider-enforced read-only. Its URL is only sent to the current tab's UI and
excluded from reports. A final screenshot is always attempted after verification; optionally
capture one after every tool action/batch. Screenshot failures remain visible as failed spans.
The idle pane stays empty until the remote browser supplies a live view or screenshot.

The primary **Flow time** runs from the starting page and initial observation being ready to
independently verified success. Browser launch, Live View connection, initial navigation, and
destination resolution are preparation, outside this clock. Every run prepares a fresh browser
automatically; there are no cold/warm controls. A transient browser failure during preparation
gets at most one fresh-browser retry after confirmed closure. Both attempts remain in the trace;
invalid article requests are not retried. After readiness, actions and decisions are never retried
by the owner. Destination lookup has an eight-second request timeout and distinguishes unavailable
Wikipedia responses from missing articles.

**First action** uses the same ready boundary and ends when the first browser interaction completes.
**Preparation** is shown separately. A failed preparation has no flow time and stays in history as
**preparation failed**. A running or unsuccessful flow shows elapsed time, never a verified result.
The complete trace and exported timestamps remain admission-relative on one Worker clock:
`timing: "page-ready-v1"`, `readyAt`, `verifiedAt`, and `finishedAt`. Flow latency is
`verifiedAt - readyAt`; final capture and cleanup are excluded. Client request duration remains
in JSON for transport diagnostics and is a different clock domain. Model spans cover complete model
responses and include reported tokens and resolved model IDs; time to first token and CDP command
counts are not measured. Worker clocks can coarsen synchronous work. Overlapping spans are not
additive, and uninstrumented gaps remain visible rather than assigned to a component.

Repeat 1, 3, or 10 times. History retains failures. A single-model failure stops repetitions;
comparison runs continue to the next sample after an ordinary failure. Cancellation or failed
browser cleanup stops either sequence. Statistics compare the same preset, mode, timing protocol, model,
element selector, reasoning, requested tier, and capture settings. Older reports with
unspecified model settings stay in separate provider-default cohorts. Latency
percentiles use verified successes, with flow failures retained in the started-flow denominator.
Actual served tiers do not split that denominator; mixed tiers remain visible in the table and
individual model spans, so configuration medians may include different served tiers.
Preparation failures are counted separately; older admission-timed reports stay in separate cohorts. p95 appears
after 20 successes. Export JSON before reloading: history lives in the tab, while the owner retains
only its latest report. Record the tested commit and environment alongside exports for comparisons.
No performance improvement is established by the deterministic tests.

To link an independently published comparison from the UI, set
`VITE_BROWSER_BENCHMARK_REPORT_URL` when building. This optional link is hidden by default;
the example does not bundle historical benchmark reports.

## Ownership and limits

One tab owns one browser and admits one run at a time. Task-specific limits are listed above.
Every run has a four-minute overall deadline. Every tab is bounded to 100 admissions.
The remote browser has a ten-minute maximum lifetime and a durable cleanup alarm. Use **Stop run**
to interrupt active work and **Close browser** to retry failed cleanup or release a recovered browser.
Lost requests stay fenced; closing them never replays browser input. Leaving the tab does not
guarantee an immediate server-side cancellation; the run deadline and owner alarm still apply.

Persisted owner state uses a versioned Schema and atomic SQLite writes. Failpoints surround each
state transition and alarm mutation. Running spans are in-memory; a process loss can lose partial
timing data while the persisted admission prevents replay. Reports never claim complete telemetry
for a lost request. Unsupported owner state fails decoding without resetting stored data.

## Validate and deploy

```sh
vp run -F @yielded/agent-example-browser-speed check
vp run -F @yielded/agent-example-browser-speed test
vp run -F @yielded/agent-example-browser-speed build
```

The ordinary suite covers verification and lifecycle behavior without credentials. To exercise
task-board presets and Wikipedia navigation in real local Chromium, set `BROWSER_TEST_EXECUTABLE`:

```sh
BROWSER_TEST_EXECUTABLE="/path/to/chrome" vp run -F @yielded/agent-example-browser-speed test
```

That test substitutes only model HTTP responses; it retains native Effect AI decoding, tool
execution, Chromium, and independent verification. It does not establish hosted performance.

For a hosted lab, configure the same values as Worker secrets using the repository's credential
workflow, then run `vp run -F @yielded/agent-example-browser-speed deploy`. The Worker is named
`effect-agent-browser-speed`; its assets and browser-owner Durable Object are declared in
`wrangler.jsonc`. This configuration serves the lab on its `workers.dev` hostname: protect that
entire hostname with Cloudflare Access before deploying, including `/api/*`. The demo has no
application authentication. If using a custom domain instead, set `workers_dev: false` and
protect the custom hostname; an Access policy on a custom domain does not protect `workers.dev`.
Preview URLs are disabled so they cannot bypass that policy.

The public lab is the `public` environment: `vp run -F @yielded/agent-example-browser-speed deploy:public`
serves `agent.yielded.dev/browser-use`. Give it `CLOUDFLARE_ACCOUNT_ID` and a Browser Run Write
token as `BROWSER_RENDERING_API_TOKEN`. Visitors add TypeSafe, OpenRouter and OpenAI keys in the
page; each run sends them as request headers, and the lab uses them for that run without storing
or reporting them. The public lab has no scripted baseline or Workers AI model, and admits at most
six runs per minute per address.

Sign-in belongs to the [travel planner](../travel-planner) at `agent.yielded.dev/travel`. Both
apps share an origin, so the lab receives its session cookie and checks it through a binding to
the planner's auth Durable Object. When a same-origin run comes from an account on the planner's
funding allowlist, the lab fills any key the visitor left out from `FUNDED_OPENAI_API_KEY`,
`FUNDED_TYPESAFE_API_KEY` and `FUNDED_OPENROUTER_API_KEY`. Any other request, or any auth failure,
runs on visitor keys only.
