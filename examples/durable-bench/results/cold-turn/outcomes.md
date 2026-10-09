# Outcome inventory

The final capture contains 41,328 telemetry events representing 22,910 invocation records. Counts below cover **all phases**, including seeding, quiescence, resets, profiling and cleanup; they are not failure rates for the measured comparisons. Comparison 1 has 576/576 eligible turns, comparison 2 has 432/432, and the post-change map has 32/32. Every measured transcript matched. Seven of eight fresh-isolate diagnostics passed the stronger proof.

| Captured non-ok invocation outcome | Count | Evidence / interpretation |
| --- | ---: | --- |
| Durable Object fetch: aborted | 456 | Explicit Object resets are part of the harness. All individual records are retained; an aborted invocation is not relabeled `ok`. |
| Durable Object alarm: canceled | 647 | Includes seeding, reset and alarm lifecycle activity. Counts are observed telemetry, not proof that each alarm was needed or that canceled work completed. |
| Stateless fetch: canceled | 4 | Two keepalive `/identity` requests and two `/seed` requests. None is a measured turn. Seed controller responses and final seed fingerprints were verified. |
| **Total** | **1,107** | No captured CPU-limit or memory-limit invocation outcome. Telemetry can omit whole invocations. |

The four stateless cancellations are `compare1` 50/400 Object 0, `keepalive-m6-1` and `keepalive-m18-1`; and `compare2` 250/0 pi Object 1 `seed-210` and Yielded Object 0 `seed-140`. No unknown measured turn was automatically replayed.

Every captured invocation and its available context is in `telemetry-primary.json.gz` / `telemetry-provider.json.gz`. Every non-ok record is also in each final phase's `failed-outcomes.json.gz`. Outcome counts are repeated across those files because they cover the same whole-session capture; do not sum them.

| Other non-ok result | Count | Handling and artifact |
| --- | ---: | --- |
| Initial primary Worker deploy rejected global-scope randomness | 1 | Moved the observer's random isolate marker creation into an invocation; preserved the failed bundle and sanitized Alchemy log. `controller-errors.jsonl`, `alchemy/`. |
| Pre-profile `/run` HTTP 500 | 1 | Observer rejected `inline != production` before `meter.begin` and before `super.submitEncoded`. Only after proving no admission occurred was that input submitted with the selected variant. `profile-preparation.json`, requests and controller errors. |
| Optional version-metadata API HTTP 500 | 15 | Existing metadata API verified version/startup information after the optional endpoint failed. Every response is recorded in `controller-api-non-ok.jsonl`. No measured turn was repeated. |
| CPU profile API HTTP 429 | 3 | 250/0 warm, 250/400 cold and 250/400 warm. No retry. All underlying turns still completed with correct transcripts. `cpu-profiles/captures.json`. Five other captures returned 200. |
| Fresh-isolate identity/version proof rejected | 1 | Yielded 50/0 reached the prior version and existing isolate after readiness reached the new version. All nine actual transcript fingerprints matched. Excluded, no retry. `fresh-failures.json`, `fresh/adjudication.json`. The fresh controller exits 1 after recording all eight outcomes. |
| Contradictory constructor attribution in frozen baseline map | 1 | 50/400 `m1` had no prior application alarm entry, but constructor logs attached to an alarm. Excluded as cold; its incomplete formal cohort remains excluded. Original evidence is frozen under `map/`. |
| Cleanup Worker GET HTTP 404 | 2 | Expected deletion verification, one for each task Worker; empty namespace and whole-prefix checks also passed. `cleanup.json`. |

There is one failed controller HTTP request across the captured request ledger: the pre-profile 500. The fresh-isolate rejection is a successful HTTP response with failed proof, retained separately. All 1,008 randomized comparison requests were successful and eligible. SSE receipts cover all 9,072 model calls without falling back to provider logs.

Comparison 1 retains 340 observation/join issues; comparison 2 retains 249; post-change mapping retains 32; fresh-isolate diagnostics retain 6. These are observations, not necessarily unique invocation failures. The second comparison's 249 comprise 216 END-log context disagreements, 25 missing/conflicting provider-log joins, five driver joins, two inline joins and one submit join. START joins govern CPU attribution. Missing, ambiguous, non-ok or incomplete CPU stays null, and aggregate CPU pairs require the same complete Objects on both sides. The original map's frozen join inventory is unchanged. See each phase's full `failed-outcomes.json.gz` and `summary.json.gz` for all entries and coverage.

Local preparation/check attempts are not timing evidence. `preparation.json` records initial sandbox EPERM during frozen installation and sandbox DNS failure during account lookup; reviewed escalation allowed the same operations. Existing validation logs retain sandboxed-listener failures, an initial test-filter mismatch and formatting errors corrected before validation. `ready-runtime-initial.log.gz` records a full check invoked with the wrong Postgres environment variable and connection refusals; the corrected `EFFECT_AGENT_TEST_POSTGRES_URL` run passed. One local post-map reduction used an incorrect raw field name and exited with TypeError; correcting it verified the same 32 saved traces without dispatching a request. The final restored product tree passed uncached `vp run ready`, and `validation/local-postgres.json` verifies removal of its disposable local fixture.
