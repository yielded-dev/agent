---
"@yielded/agent": patch
---

Rebuild up to 256 canonical records per work-index pass while retaining the 32 MiB byte cap. Set the request's `limit` to select smaller passes.
