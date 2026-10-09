# Publication allowlist

Only this `publish-terminal-reads/` directory is prepared for a public evidence branch. Do not copy its parent. Nothing has been published. Earlier codec/pi and prompt-reads evidence packages are retained separately.

| Allowed file | Contents |
|---|---|
| `PUBLISH.md` | Exact publication allowlist and validation. |
| `REPORT.md` | Measured baseline/terminal-reads tables, fingerprints, method, and limits. |
| `summary.json` | Compact exact counters, visits, tag counts, SQL aggregates, and fingerprints. |
| `provenance.json` | Revisions, versions, source/capture/input hashes, and replay verification. |
| `REPRODUCE.md` | Portable prerequisites, exact commands, and exact-byte replay limits. |
| `reproduce.sh` | Disposable-only local replay script; large output goes outside this directory. |
| `candidate-4417a095.patch` | Exact SQL-only functional candidate diff. |
| `source/counting/instrument.ts` | Preserved bundle-time hooks with explicit candidate source paths. |
| `source/counting/probe.js` | JSON-byte, schema, Effect, SQL, and visit counters. |
| `source/counting/run.ts` | Portable local driver with explicit fixture/output paths. |
| `source/counting/storage.ts` | Canonical transfer and transcript helper; no fixture data. |
| `source/counting/protocol.ts` | Transfer-schema source only; no runtime values. |
| `SHA256SUMS` | SHA-256 integrity manifest for every other allowed file. |

## Exclusions

No canonical archives, Worker/JavaScript bundles, SQLite or Miniflare databases, fixture snapshots, binary archives, raw captures, logs, environment diagnostics, workstation paths, credentials, deployed identifiers, copied third-party code, or complete product-module copies are included. Runtime protocol field names are schema declarations, not secret values. Input archive and raw-capture hashes are safe identifiers; their data remain local.

## Validation

- All included files are UTF-8 text; the exact inventory and absence of forbidden files, workstation paths, and credential values were checked.
- Candidate SQL patch reconstructs the exact captured module; native Records.ts matches baseline byte-for-byte. Counter hooks are unchanged apart from the pre-instrumentation candidate source selector.
- All four original candidate captures match baseline seed/measured fingerprints and normalized import inventories.
- Portable candidate replay at 50 matches every counter, SQL breakdown, visit histogram, model-boundary snapshot, normalized inventory, and fingerprint from the original capture. No new baseline repeat or full ready run was performed.
- The shell script passes syntax checking. Portable driver edits were formatted through vp. SHA256SUMS covers every other allowed file.
- Instrumentation and original fixtures remain local for another variant. No timing, CPU, memory, deployment, or billing improvement is claimed.
