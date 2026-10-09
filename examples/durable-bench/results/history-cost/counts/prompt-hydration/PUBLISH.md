# Publication allowlist

Copy only this publish directory. Its parent contains private/local data and must not be published. This is a new independent package; earlier codec/pi, input-filter and terminal-filter evidence is untouched. Nothing has been published.

| Allowed file | Contents |
|---|---|
| `PUBLISH.md` | Exact publication allowlist and validation. |
| `REPORT.md` | All four count comparisons, first-model boundary, digest attribution, limits. |
| `summary.json` | Exact compact counters, SQL aggregates, visits, fingerprints and separate digest supplement. |
| `provenance.json` | Revisions, versions, source/input/result hashes and verification. |
| `REPRODUCE.md` | Portable prerequisites, commands and boundaries. |
| `reproduce.sh` | Local disposable-only replay script for original counters and separate digest mode. |
| `candidate-0451aacb.patch` | Exact Records.ts change and new native-constructor helper. |
| `source/counting-hydration/instrument.ts` | Existing counters plus optional scoped digest attribution; candidate source selector. |
| `source/counting-hydration/probe.js` | Original counter and SQL-cursor implementation. |
| `source/counting-hydration/run.ts` | Portable driver, explicit fixture/archive/output paths. |
| `source/counting-hydration/storage.ts` | Transfer/transcript helper source; no data. |
| `source/counting-hydration/protocol.ts` | Transfer schemas; no runtime values. |
| `SHA256SUMS` | Integrity hashes covering every other allowed file. |

## Excluded

Canonical archives, fixture snapshots or symlink views, SQLite/Miniflare databases, generated Worker bundles, raw capture files, logs, temporary patch-verification product trees, copied full Records.ts, environment diagnostics, workstation paths and credentials. The small candidate patch intentionally includes the new private helper. Source probe.js is instrumentation source, not a generated bundle.

## Verification

- All four original-counter captures match baseline fingerprints, normalized imports, SQL statements/cursor totals, record/byte counts and journal visits.
- The optional digest scope is captured separately for baseline and candidate at250/1000. Removing its prefixed fields reproduces every original counter and model-boundary snapshot exactly.
- Portable candidate replay at50 matches counters, SQL, visits, all model snapshots, import inventory and both fingerprints.
- Both candidate files reconstruct byte-for-byte from baseline plus patch. Shell syntax and Vite+ formatting checks passed.
- All files are UTF-8 text; the exact file inventory and absence of workstation paths or credential values were checked. SHA256SUMS covers every other listed file.
- Earlier public evidence packages still pass their original manifests. Product correctness/equivalence and full-ready validation remain separately owned. No timing or deployed improvement is claimed.
