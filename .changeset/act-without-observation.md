---
"@yielded/agent": minor
"@yielded/agent-platform-cloudflare": minor
---

Add `act(actions, { observe: false })` to `BrowserActions` for hosts that read the next page themselves. The result's `observation` is null and earlier references are invalidated, so inspect before the next action.
