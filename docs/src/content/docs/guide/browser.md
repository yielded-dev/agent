---
title: Browser tools
description: Choose page capture, bounded crawling, or a host-owned interactive browser.
---

<a id="browser-tools"></a>

Choose the smallest browser capability the task needs. A capture returns one page result; a crawl
collects a bounded set of pages; an interactive session keeps a live page for navigation and input.
Your application owns destination access, credentials, and cleanup.

## Choose an adapter

| Need                                               | Capability                                | Host setup                                                    |
| -------------------------------------------------- | ----------------------------------------- | ------------------------------------------------------------- |
| Render Markdown, scrape, or take a PNG in a Worker | Quick Actions                             | Cloudflare browser binding                                    |
| Capture a page from Node or a Worker               | REST capture                              | Account ID, redacted token, Effect HttpClient                 |
| Crawl one HTTPS host into Markdown                 | REST crawl                                | REST credentials and a Scope                                  |
| Let an agent navigate and interact with websites   | BrowserUse + Code Mode + Browser Sessions | Worker Loader, browser binding, private lifecycle credentials |

## Give an agent a capture Tool

```ts twoslash
import { WebCapture } from "@yielded/agent";
import { Toolkit } from "effect/ai";

const ReadPage = WebCapture.make("read_page", {
  description: "Read example.com as rendered Markdown.",
  urls: ["example.com"],
  actions: ["markdown"],
  maxResponseBytes: 16 * 1024,
});

const BrowserTools = Toolkit.make(ReadPage.tool);
```

Use `BrowserTools` on the Agent. `CloudflareBrowser.layer(ReadPage, { browser })` supplies Worker
handlers; `CloudflareBrowserRest.layer` supplies REST capture with an HTTP client. Hosts and actions
are fixed before execution. Structured extraction additionally needs explicit authorization and
accounting for model inference.

## Capture and crawl

For rendered pages, use a finite `waitForSelector` for the content you need. Navigation completion
alone may return only a loading shell. Treat rendered text as untrusted, and distinguish missing
content from evidence that something is absent.

`PageCrawl` fixes the starting host, purposes, page/depth limits, bytes, and deadline. Scope closure
cancels a known-running crawl. Quick Actions have a different limit: interrupting local waiting
does not confirm termination of an unresolved remote browser request.

## Interact with a browser

`InteractiveBrowser` owns one bounded pass. Its immutable network policy makes the distinction
explicit: `ExactHosts` is a URL allowlist, **not** connection-time private-network isolation.
`PublicWeb` requires that isolation and is unsupported by the Cloudflare adapter. `Unrestricted`
opts out of both host and private-network containment; only trusted host code should select it.

`BrowserUse` gives the model one Code Mode tool. The model writes JavaScript for the website
it actually encounters, using Chrome DevTools Protocol (CDP) to inspect the DOM, evaluate page
JavaScript, navigate, or send native input. There is no framework classifier deciding which
elements are actionable.

```ts twoslash
import { BrowserUse } from "@yielded/agent";
import { Toolkit } from "effect/ai";

const browser = BrowserUse.make();
const tools = Toolkit.make(browser.tool);
```

Provide `BrowserUse.layer` with the scoped adapter from
`@yielded/agent-platform-cloudflare/browser-use`, then pass those handlers to
`CloudflareCodeMode.layer(browser, { loader, handlers })`. The adapter borrows an existing
`BrowserSession`; the executor runs each program in a fresh Worker with no ambient network,
bindings, or secrets. The browser remains attached across programs.

Inside a program, `cdp.send({ method, params, targetId?, frameId? })` sends a CDP command.
`cdp.tabs({})` discovers pages in the session's context, `cdp.newTab({})` opens a blank page,
and `cdp.closeTab({ targetId })` closes a secondary page. Omit `targetId` to address the original
page. Browser and Target protocol domains are reserved for the host.
Use `cdp.frames({ targetId? })` to discover frames, including cross-origin frames, and `frameId` to
route commands to a frame's session. `Page.createIsolatedWorld` supplies an execution context
for evaluating JavaScript in that frame.

Successful responses retain the CDP shape: `Runtime.evaluate` with `returnByValue: true`
returns the value at `response.result.value`, and `Runtime.getExceptionDetails` returns its
diagnostic metadata unchanged. Page execution exceptions reject `cdp.send` with
`BrowserUseError` code `evaluation`, carrying the page diagnostic into Code Mode's failure
report unless the program catches it. An evaluation may have changed the page before throwing;
inspect current state before further actions. The default tool guidance asks the model to correct
inspection errors with a smaller program and continue authorized work.

For example, the model can inspect a page with:

```js
async () => {
  const response = await cdp.send({
    method: "Runtime.evaluate",
    params: {
      expression: "document.body.innerText.slice(0, 6000)",
      returnByValue: true,
      awaitPromise: true,
    },
  });
  return response;
};
```

The host authorizes every command with current page and target metadata and sets command,
tab, and result limits. Code Mode adds execution and aggregate output limits. A result limit
does not bound the upstream CDP transfer; programs should request small results. CDP events
are not streamed to the program; inspect state or use bounded page-side conditions.

Arbitrary page JavaScript is a broad capability: a URL check or a tool named “read” cannot
guarantee read-only behavior, recipient restrictions, or safe checkout. Enforce network access
outside generated code and authorize the task's actual capabilities. Page content remains
untrusted. A completed command does not prove the website accepted an order, login, or upload;
verify the site's outcome independently. Never replay uncertain input automatically.

## Keep a browser across attempts

`BrowserSessions` keeps a private reference to one exact browser, context, and page while Attempts
borrow scoped attachments. The owner serializes attachments and retains responsibility for closing
the browser at completion, cancellation, or expiry. Disconnecting an attachment preserves the remote
session; a reference cannot recreate an expired page.

<a id="browser-session-options"></a>

| Creation option        | Default                        | Meaning                                                   |
| ---------------------- | ------------------------------ | --------------------------------------------------------- |
| `maxElapsedMillis`     | Required positive safe integer | Fixed session expiry                                      |
| `keepAliveMillis`      | `600_000`                      | Provider idle allowance; maximum `600_000`                |
| `commandTimeoutMillis` | `30_000`                       | Command limit, also bounded by remaining session lifetime |

Keep session references and Live View URLs private. Human takeover requires exclusive control;
read-only viewers can still see page data. Expiry of a viewing URL limits new connections, not
already connected viewers.

## Fill login or card credentials

`session.fillCredential` resolves host-owned material through `BrowserCredentialAccess`, then fills
supported native controls without submitting the form. The host authorizes the actual page, frame,
and form-recipient origins. Authorize filling itself: input handlers may send data immediately.

For private user entry, prepare the form before showing it to the user:

```ts
const binding = yield * session.prepareCredential(form);
// Retain only form, binding, expiry, and a one-use request ID.
// After private submission, atomically consume the request before filling.
const result = yield * session.fillCredential({ ...form, credential: requestId, binding });
```

Accept values through a separate authenticated endpoint and supply an invocation-scoped
`BrowserCredentialAccess` Layer there. Its resolver can use the submitted values instead of a vault.
Keep material out of conversation input, history, checkpoints, and telemetry. The binding pins the
page, document, form, and controls across attachments; a changed target requires a fresh request.
The host owns request expiry and one-use consumption. Retain only the outcome after filling;
process loss or an uncertain result never permits replaying the values.

Credentials stay out of the fill API's results and telemetry, but generated page JavaScript can
read filled fields and the website may echo values in later observations or screenshots.
CDP also has access to browser cookies. Your disclosure policy must cover those outputs. Uncertain fills or
actions must not be blindly retried; confirmed browser cleanup does not undo website effects.

The [browser-speed example](https://github.com/yielded-dev/agent/tree/main/examples/browser-speed)
shows a runnable interactive application. Consult the
[session API](https://github.com/yielded-dev/agent/blob/main/packages/platform-cloudflare/src/BrowserSession.ts)
for attachment and cleanup details.
