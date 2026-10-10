---
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-postgres": patch
"@yielded/agent-storage-cloudflare": patch
---

Store one full canonical batch tail digest for replay and integrity checks, and remove `RawAppendRequest.batchDigest` from the SQL adapter SPI. BEHAVIOR CHANGE: require fresh layout-25 stores; retain predecessor stores with their matching release and transfer `thread@3` archives through export/import.
