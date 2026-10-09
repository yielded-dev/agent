---
"@yielded/agent-storage-sql": patch
---

Remove the unused SQL schema initializer and index-creation helpers.

BEHAVIOR CHANGE: Initialize storage through an adapter's frozen layout steps instead of `createStorageSchema`, `createWorkerControlIndexes`, `createNativeReadIndexes`, or `createMessageDeliveryPendingIndex`.
