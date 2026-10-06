---
"@yielded/agent": patch
---

`BrowserUse.runJev` waits on a page with no controls or text, such as a checkout still rendering, instead of asking Jev to decide on it.
