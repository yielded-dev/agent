# Reproduce the count experiment

Use a fresh disposable checkout at `07f0272e7ba49a494064b6b74c6318b55514ae19`. The script refuses another revision or an existing `examples/durable-bench/counting` directory. Install the pinned dependencies through Vite+:

```sh
vp install
vp run -F @yielded/agent-example-durable-bench vendor
```

Provide a local fixture directory containing `yielded-{50,250,1000,3500}` and `pi-{50,250,1000,3500}`, with one adjacent `.json` metadata file per database directory. These data are intentionally not published here. The original retained snapshots provide exact byte-for-byte replay. Independently recreated baseline snapshots can use the existing `seed` task, but generating every historical turn is expensive and generated record identities/timestamps can change raw byte totals. Expected transcript fingerprints are in `summary.json`.

From this publication bundle:

```sh
sh reproduce.sh "$CHECKOUT" "$FIXTURES" "$OUTPUT"
```

Each argument names an absolute local directory. `OUTPUT` must be outside the checkout and publication bundle: it receives generated canonical archives, Worker bundles, and raw local captures. The script adds a temporary `count` task to the disposable checkout, reconstructs the codec candidate with `candidate-40485f33.patch`, and runs each variant with the same source hooks and no task-cache reuse. Miniflare requires loopback listener access. No provider keys, deployment, or external service is needed.

The exact one-Run workload is `turn count-0 tools=8`, yielding nine scripted model calls. Each run verifies the model-visible seed fingerprint. Compare result JSON with `summary.json`: selected record counts, message passes, schema calls, Effect evaluations, and SQL reads are diagnostic operation metrics. They do not imply latency or CPU improvement.

Candidate 1 (`40485f33`) was rejected for lack of demonstrated warm improvement. Its captured work reduction remains useful evidence; this bundle makes no warm-performance claim.

## Portability changes

The hooks in `instrument.ts` are the captured hooks. Its candidate source path is now an explicit argument. `run.ts` replaces workstation paths with `--fixtures-dir`, `--out-dir`, and optional `--candidate-records` flags. The probe and transfer helpers are unchanged. Counter logic and measured boundaries were not redesigned. `provenance.json` identifies exact versions and binds the original hook and candidate files by hash.

The local full evidence includes SQL statement breakdowns, per-model snapshots, repeated captures, import inventories, and fingerprints. Those larger/raw items stay outside this compact publication allowlist. The full local checkout and instrumentation remain available for a later SQL candidate; that future result must use a new variant label and must not overwrite this candidate-1 summary.
