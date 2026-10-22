---
"@yielded/agent": patch
---

Use `WebSearch.native({ tool })` to run hosted search in the agent's own model call. **BEHAVIOR CHANGE:** Remove the separate `WebSearch.layer` handler and its tool, query, result, failure, and options exports; retain `WebSearch.Source` for citation validation.
