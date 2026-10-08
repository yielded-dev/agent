# Hosted control audit

Source inspection found no implementation or request mismatch explaining the baseline/control CPU gap. The retained baseline bundle and control bundle are identical; both deployed identities select `effectBatch`, with the same frozen five profiles, repetition counts, checksum consumer and one root Effect execution per RPC. The six raw CPU correlations agree with their reported values.

The audit identified two experimental weaknesses:

- Setup warmed the summary functions directly. The measured `effectBatch` and `synchronousBatch` wrappers were first exercised during measurement.
- Each form was assigned to a different Worker/Object and measured in fixed role order. Baseline and control requests had MIA/CDG ingress identifiers. Those identifiers establish ingress locations; they do not establish Durable Object placement or the cause of the CPU difference.

The appropriate controlled follow-up is a small within-Object crossover using one dual-implementation bundle, warming the exact batch paths first. Within-Object repeated forms reveal drift; the same-form observations across Workers describe cross-Worker variation. Retain the existing failed pilot unchanged.

The parent independently confirmed all seven previous Workers return Cloudflare error 10007 / HTTP 404 and all seven namespace IDs are absent from the complete account listing. See `parent-absence.json`. Earlier counter arithmetic, correlation and identity checks remain in `audit-checks.json` and `audit-absence.json`.

This audit performs source inspection and retained-data review. It establishes no new CPU improvement, no global per-evaluation price, and no result for the unimplemented append stage.
