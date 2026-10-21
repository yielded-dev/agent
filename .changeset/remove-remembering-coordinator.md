---
"@yielded/agent": patch
---

Remove the optional `Remembering` coordinator and its store protocol. **BEHAVIOR CHANGE:** Own background extraction through committed activity processing and memory writes; `SourcePosition` and `comparePosition` remain available from `@yielded/agent/remembering-store`.
