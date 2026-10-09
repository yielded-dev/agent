# cold-turn: measurement scope fixed before product changes

Baseline is origin/main b246f8aaa3a92d5f82934b1fc7a82356d1ad6664, including #823, #825 and #826.
The timing authority is the deployed Cloudflare driver (aws:us-west-1), not the controller or local runtime. The account was checked through the API as **Danieljmerwe@gmail.com's Account**. Resources and Alchemy state belong only to the cold-turn prefix; rebench is excluded.

Cold means the same persisted Object, seeded with completed history, whose previous incarnation has acknowledged `storage.sync()` and then been reset with `ctx.abort()`. The measured submit must be its first harness request, with a changed incarnation, identical Object identity and no earlier alarm. The same check applies to every baseline and candidate. An isolate marker is recorded separately; an incarnation reset is not treated as a new isolate.

The initial map uses 50 and 250 completed seed turns, instant and 400 ms first-token providers (the latter retains the existing 10 ms chunk spacing), two cold turns and four warm turns per Object, with two settling turns. Nine model calls must match independently generated reference transcripts per turn. Seed fingerprints are 50 `b017b487524e44a4` and 250 `dcea9f30b0917245`.

The primary outcome is driver submit-to-settlement latency. Secondary observations are receipt latency, first provider arrival bounds, provider gaps, settlement delivery bounds, Cloudflare invocation CPU, and native transaction/SQL/decoder counts. I/O clocks have integer-millisecond representation and can freeze through synchronous execution; individual transaction promises are not physical replication-roundtrip counts. Separate diagnostic beacon samples locate output-gated boundaries but are not candidate performance samples.

Candidate acceptance requires same-Object randomized interleaving inside one bundle, two identical baseline labels, and a driver improvement larger than the full observed baseline repeat range and candidate repeat range. Report every comparison, including negative and inconclusive results. Do not sum gains across separate cohorts. Warm samples must remain in their preceding variant's verified incarnation.

Reuse existing storage-open, version refusal, fencing, eviction, maintenance retry, publication and continuation recovery checks, then run `vp run ready` for each retained product change. No new committed tests are planned. Before any product patch, freeze the ordered deployed baseline map and its evidence. Product PRs target main, remain separate from this evidence branch, and are not merged. The warm-floor pre-arm deadline and settlement-tail code are outside this task.

The requested evidence extends the existing prod-admit and prod-turn maps. The copied deployment/controller/provider/observer/reducer comes from origin/dan/prod-turn-latency, itself based on origin/dan/bench-production-path. Exact source and bundle archives accompany every deployment. No credential, account/namespace identifier or Alchemy state belongs in the evidence. Cleanup must destroy the Alchemy stages and verify the empty prefix through the same account API.
