---
"@yielded/agent": minor
"@yielded/agent-platform-cloudflare": minor
---

Replace ref-based browser actions and Jev with a Code Mode browser tool backed by host-authorized CDP commands. BEHAVIOR CHANGE: compose `BrowserUse.make().tool`, `BrowserUse.layer`, a scoped CDP adapter, and a Code Mode executor; authorize arbitrary page JavaScript as a broad browser capability.
