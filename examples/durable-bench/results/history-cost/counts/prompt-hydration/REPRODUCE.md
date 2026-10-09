# Reproduce the constructor and digest counts

Start with a fresh disposable checkout at `07f0272e7ba49a494064b6b74c6318b55514ae19` and install dependencies with `vp install`. Provide the local `yielded-{50,250,1000,3500}` fixture database directories with their adjacent `.json` metadata. No pi vendor is needed for this Yielded-only capture.

From this publication directory, with absolute input/output paths:

```sh
sh reproduce.sh "$CHECKOUT" "$FIXTURES" "$OUTPUT"
```

The script checks the baseline, copies five counter/helper sources, reconstructs exact candidate Records.ts plus internal/history-prompt.ts from the small patch, and verifies both hashes. Only the private helper is placed at its real import path in disposable packages; baseline Records.ts stays unchanged. Candidate Records.ts is selected in the esbuild plugin. A temporary Vite+ task runs the comparison.

The script captures both original-counter variants at all four sizes, then captures both variants at250/1000 with the identical optional digest scope. These directories remain separate. The report's original-counter baseline values were reused from earlier verified captures; this independent reproducer includes baseline commands for readers who do not have them.

OUTPUT is forced outside the checkout and publication bundle. It receives local canonical archives, generated Worker bundles, temporary Miniflare state and raw captures; do not publish it. Shared input archives are reused with `--archive-dir` and never rewritten. Without that flag, setup can export missing archives from copied fixtures into its own output. Setup/import/fingerprinting are excluded from counters.

The exact candidate command, after setup, is:

```sh
vp run --no-cache -F @yielded/agent-example-durable-bench count-hydration -- yielded prompt-hydration 50 250 1000 3500 --fixtures-dir "$FIXTURES" --candidate-records "$CANDIDATE_RECORDS" --archive-dir "$ARCHIVES" --out-dir "$OUTPUT"
```

Here CANDIDATE_RECORDS is the Records.ts reconstructed under counting-hydration/candidate, and ARCHIVES contains archive-{50,250,1000,3500}. For the supplement, use only sizes250/1000, add `--digest-scope on`, and choose a new output directory; repeat with `yielded baseline` and the same scope flag. Miniflare needs local listener access. No credentials, remote service, deployment or publication is required.

The workload is one `turn count-0 tools=8`, nine scripted model calls through settlement. Verify seed and measured fingerprints and compare raw counters with summary.json. For the supplemental check, remove historyDigest-prefixed keys from counts and each modelSnapshots entry and compare to the original captures; SQL, visits and fingerprints must also match. Three scoped root parser calls do not mean three digest calls.

Original fixture identities/timestamps preserve exact byte totals. Independently regenerated seeds may retain transcript fingerprints but differ in raw bytes; replaying all historical turns is expensive. Fixtures and canonical archives are intentionally excluded from the public package.

The counter logic is preserved. The portable driver only permits archive export into its own output when no shared archive path was supplied; explicit shared inputs remain read-only. Report and provenance distinguish original-counter captures from supplemental attribution. No generated parser, AOT compiler or new production infrastructure is introduced.
