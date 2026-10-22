---
"@yielded/agent": patch
---

Release in-memory subagent reservation pools when their parent Run and attached children finish cleanup. Use the atomic `release` operation in place of `beginRelease`.
