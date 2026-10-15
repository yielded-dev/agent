---
"@yielded/agent-platform-cloudflare": patch
---

Reduce Cloudflare client imports and validate submission envelopes once. Expose scoped live-text bytes for forwarding and validate frames at the final consumer with `decodeLiveText`.
