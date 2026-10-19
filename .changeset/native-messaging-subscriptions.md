---
"@yielded/agent": patch
---

Use native EventSource and Subscriptions APIs in place of GitHubWorkflowSource and SubscriptionTools. Require Crypto only for messaging tools that prepare a new message.
