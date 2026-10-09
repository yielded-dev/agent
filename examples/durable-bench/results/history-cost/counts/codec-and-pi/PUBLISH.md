# Publication allowlist

Copy only this `publish/` directory into the public evidence branch. Its entire file inventory is the allowlist below. Do not copy the parent evidence directory. Nothing has been published by this task.

Candidate 1 (`40485f33`) is rejected for lack of demonstrated warm improvement. The retained counts show work removed and added; they are not performance acceptance evidence.

| Allowed file | Contents |
|---|---|
| `PUBLISH.md` | Publication allowlist and exclusions. |
| `REPORT.md` | Measured 50/250/1000/3500 comparison, method, limitations, and rejected-candidate disposition. |
| `summary.json` | Compact exact operation counts and transcript fingerprints; no record payloads. |
| `provenance.json` | Revisions, pinned versions, original source hashes and portability changes. |
| `REPRODUCE.md` | Portable prerequisites, commands, and exact-byte replay limitations. |
| `reproduce.sh` | Local replay script; output is forced outside the publication bundle and checkout. |
| `candidate-40485f33.patch` | Small exact codec-only candidate diff. |
| `source/counting/instrument.ts` | Bundle-time counter hooks; candidate path supplied explicitly. |
| `source/counting/probe.js` | Counter, JSON-byte, SQL-cursor and visit-histogram implementation. |
| `source/counting/run.ts` | Local driver with explicit input/output CLI paths. |
| `source/counting/storage.ts` | Canonical transfer and transcript-verification helper source; no fixture data. |
| `source/counting/protocol.ts` | Transfer-schema declarations; no environment or credential values. |
| `SHA256SUMS` | Integrity hashes for the publication files. |

## Excluded from this deliverable

- Canonical `archive-*.json` files, Worker bundles, generated JavaScript bundles, binary archives, SQLite/Miniflare databases, fixture directories, and caches.
- Raw captures and trial directories, seed/run/debug logs, environment diagnostics, workstation paths, copied third-party sources, and complete product-module copies.
- Credentials, tokens with values, deployed resource identifiers, and internal issue references. The protocol file contains type declarations only; for example, a token field name is not a token value.

## Validation

- Every included file is UTF-8 text. Exact file inventory and forbidden file types were checked; workstation paths and internal identifiers were removed.
- The candidate patch reconstructs the captured Records.ts byte-for-byte.
- The portable driver was replayed at 50 turns for both baseline and candidate; counters, SQL breakdowns, visit histograms, and seed/measured fingerprints exactly match the original captures.
- Sources, local fixtures and full raw evidence are retained outside this directory for the next SQL candidate. The public copy changes only path configuration and packaging. Larger sizes use the already completed original captures; no new performance claim is made.
- `SHA256SUMS` covers every other file in this allowlist.
