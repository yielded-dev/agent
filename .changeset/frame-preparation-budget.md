---
"@yielded/agent-platform-cloudflare": patch
---

Give actions on targets inside frames an 8 s preparation budget instead of 2 s, so a slow check of a payment frame no longer closes the browser session.
