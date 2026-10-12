---
"@yielded/agent-storage-sql": patch
---

Reduce Schema overhead in repeated SQL reads. BEHAVIOR CHANGE: Bind `decodeRows(schema)` or `decodeSingleRow(schema)` once, then pass `(table, rowKey, rows)` to the returned decoder.
