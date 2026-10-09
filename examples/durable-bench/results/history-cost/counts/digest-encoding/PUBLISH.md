Copy exactly these 15 files, preserving their relative paths. `SHA256SUMS` covers
the other 14 files. This is the finalized publication allowlist.

```text
PUBLISH.md
REPORT.md
REPRODUCE.md
SHA256SUMS
aggregate.py
candidate-source.patch
candidate/run-context.ts
provenance.json
reproduce.sh
source/counting-digest/instrument.ts
source/counting-digest/probe.js
source/counting-digest/protocol.ts
source/counting-digest/run.ts
source/counting-digest/storage.ts
summary.json
```

The bundle contains the aggregate, exact runner/source patch, provenance, and
reproduction commands. Retained canonical archives, fixture DBs, raw capture
files, logs, and generated worker bundles stay outside the publication. Raw local
capture paths and SHA-256 values are recorded in `provenance.json`.
