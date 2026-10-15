---
"@yielded/agent": patch
---

Preserve host-confirmed, pre-dispatch browser policy refusals as `BrowserUsePolicyError` through BrowserUse and Code Mode. Let failure observers distinguish these refusals by tag without receiving page or provider content.
