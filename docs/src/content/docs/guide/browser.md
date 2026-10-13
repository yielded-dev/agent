---
title: Browser tools
description: Choose page capture, bounded crawling, or a host-owned interactive browser.
---

<a id="browser-tools"></a>

Choose the smallest browser capability the task needs. A capture returns one page result; a crawl
collects a bounded set of pages; an interactive session keeps a live page for navigation and input.
Your application owns destination access, credentials, and cleanup.

## Choose an adapter

| Need                                               | Capability                             | Host setup                                       |
| -------------------------------------------------- | -------------------------------------- | ------------------------------------------------ |
| Render Markdown, scrape, or take a PNG in a Worker | Quick Actions                          | Cloudflare browser binding                       |
| Capture a page from Node or a Worker               | REST capture                           | Account ID, redacted token, Effect HttpClient    |
| Crawl one HTTPS host into Markdown                 | REST crawl                             | REST credentials and a Scope                     |
| Navigate, click, fill, or inspect a live page      | Interactive Browser / Browser Sessions | Worker binding and private lifecycle credentials |

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

Native `BrowserUse` tools operate through an existing Browser Session attachment. The host
authorizes each operation against current page and target information. Page content and model
arguments cannot supply authority. A successful input call proves SDK completion, not that a
website accepted an order, login, or upload; inspect the site's outcome separately.

Native observations and input support controls and frames in open and closed shadow roots.
Named elements with explicit keyboard focus or a pointer cursor can also receive refs, including
clickable cards. Inherited cursors do not create refs for every child; containers with nested
semantic controls are excluded. Input rechecks this evidence, visibility, and obstruction.
Closed-root discovery reads the frame's DOM through CDP; output limits do not bound that transfer.
Inspect again if a new closed root is attached after a condition wait starts.

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

`session.fillCredential` resolves a host-bound vault item after authorizing the actual page,
frame, and form-recipient origins. It fills supported native controls without submitting the form.
Authorize filling itself: input handlers may send data immediately.

Credentials stay out of the fill API's results and telemetry, but a website may echo them in later
observations or screenshots. Your disclosure policy must cover those outputs. Uncertain fills or
actions must not be blindly retried; confirmed browser cleanup does not undo website effects.

The [browser-speed example](https://github.com/yielded-dev/agent/tree/main/examples/browser-speed)
shows a runnable interactive application. Consult the
[session API](https://github.com/yielded-dev/agent/blob/main/packages/platform-cloudflare/src/BrowserSession.ts)
for attachment and cleanup details.
