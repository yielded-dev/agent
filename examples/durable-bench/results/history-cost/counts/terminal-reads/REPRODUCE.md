# Reproduce baseline versus terminal-reads

Use a fresh disposable checkout at `07f0272e7ba49a494064b6b74c6318b55514ae19`. Install pinned dependencies with:

```sh
vp install
vp run -F @yielded/agent-example-durable-bench vendor
```

Provide a local fixture directory containing `yielded-{50,250,1000,3500}` database directories and their adjacent `.json` metadata files. Fixtures are intentionally excluded from public evidence. Exact raw bytes require the retained original snapshots/archives. The existing baseline `seed` task can recreate the workload, but generating every prior turn is expensive and generated identities/timestamps can alter byte totals. Expected transcript fingerprints are in summary.json.

From this publication directory, supply absolute paths:

```sh
sh reproduce.sh "$CHECKOUT" "$FIXTURES" "$OUTPUT"
```

The script checks the baseline and tracked runtime/bench sources, installs the five task-local source files and a temporary count task, reconstructs the exact candidate SQL module from baseline plus candidate-4417a095.patch, checks its SHA-256, and runs both variants at all four sizes. Baseline Records.ts stays native. No codec candidate source is installed. Output must be outside the checkout and public bundle; it contains large canonical archives, generated Worker bundles, and raw local captures and must stay private/local.

The capture command for the new variant, after setup, is:

```sh
vp run --no-cache -F @yielded/agent-example-durable-bench count -- yielded terminal-reads 50 250 1000 3500 --fixtures-dir "$FIXTURES" --out-dir "$OUTPUT"
```

The baseline command uses `yielded baseline` with the same sizes and flags. The report reuses previously verified baseline captures; it did not repeat them. Existing `archive-N.json` inputs in OUTPUT are reused and their imported/model-visible fingerprint is checked. Miniflare needs local listener access. No provider keys, remote deployment, or external service is needed.

The one-Run input is `turn count-0 tools=8`. Compare raw outputs with summary.json for records, bytes, message passes, Schema parser calls, Effect evaluations, metadata/fold visits, SQL rows, and fingerprints. Counts include successful settlement; fingerprinting and fixture work are outside the window. They establish work removed for this benchmark only, not a speedup or broader correctness proof.

## Source fidelity

Candidate SQL is loaded before the original bundle-time counter hooks are applied. The input filter is absent. Native message hooks, probe.js, storage.ts, and protocol.ts are unchanged. Public run.ts exposes input/output paths through flags, and instrument.ts receives candidate file paths as arguments; these are outside measured execution. The dormant older codec selector remains in the preserved hook source but is never selected by the two commands above. provenance.json records captured-source hashes and validation. Full raw data remains local.
