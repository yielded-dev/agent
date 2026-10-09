# Digest encoding operation counts

This bundle compares baseline `07f0272e7ba49a494064b6b74c6318b55514ae19` with the
captured `run-context.ts` digest encoder from candidate commit
`442c988c7485c6b088a10f56547daa70668fc0d6`. The counted source was verified
byte-for-byte against that commit. The source SHA-256, candidate patch,
input hashes, exact runner sources, and raw capture hashes are retained. This
is operation-count evidence, not a local or deployed latency measurement.

Use a disposable checkout at that baseline with its pinned dependencies already
installed through `vp install`. Supply the retained fixture directory containing
`yielded-{50,250,1000,3500}` and matching `.json` metadata, plus the retained
`archive-{size}.json` canonical exports. These large inputs are deliberately
excluded from this bundle; their SHA-256 values are in `provenance.json`.

```sh
sh reproduce.sh /path/to/disposable-checkout /path/to/fixtures /path/to/archives /path/to/output
```

The script creates only the new `examples/durable-bench/counting-digest` directory
in the disposable checkout and files beneath the output directory. It neither
edits product source nor updates package scripts. Miniflare needs permission to
bind local loopback ports. The counter runner never opens the fixture DBs: it
reads their metadata and canonical exports and imports into private temporary
stores. Those stores are removed after each capture. Canonical import naturally
drops historical Attempt leases; all remaining table counts must match the
retained source metadata.

Both sides retain the original counting-hydration `probe.js`, `storage.ts`, and
`protocol.ts`. Instrumentation differs only in its local storage import path and
the candidate source override: `run-context.ts` is replaced through esbuild
`onLoad`; `Records.ts` remains the baseline decoder. Both sides use the same
supplemental `historyDigest` scope hooks. The hooks add no Effect operations.

The capture starts before the existing turn workflow and retains its complete
canonical settlement and proof work. The first-callback snapshot ends at entry
to the first scripted model callback. SQL cursor totals cover the complete turn;
there is no extra SQL query at callback entry. All nine model snapshots, complete
count dictionaries, SQL statement groups, visit histograms, normalized table
counts, and transcript fingerprints remain in the raw JSON captures.

`summary.json` keeps the complete first-callback and full-turn count dictionaries,
digest scopes, compact snapshots of all model callbacks, SQL totals and full-SQL
hashes, and visit histograms. `aggregate.py` fails on differences in fingerprints,
tables, SQL, visits, byte/record/projection work, or any counter outside the
expected schema/Effect changes. The original capture additionally checks every
baseline counter and callback against the retained pre-candidate baseline
captures, ignoring only the added digest-scope keys.

The portable reproduction compares all aggregated counts against this bundle's
`summary.json`. Raw capture hashes can differ across checkout paths because the
raw files retain input paths and bundle hashes; all count snapshots and
transcript fingerprints must still match.

Run the portable script once in a fresh disposable checkout. The checks here
prove matched count workloads and transcript fingerprints for these fixtures;
the product's full ready gate and schema differential proof remain separate.
