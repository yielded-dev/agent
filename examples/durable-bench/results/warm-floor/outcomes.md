# Outcome inventory

This inventory covers all captured seed, setup, measured, retired and cleanup invocations. Telemetry contains 40,718 events joined into 24,699 invocations. Canceled or aborted invocations are retained; they are not silently treated as successful model work. Raw evidence is in `telemetry-primary.json.gz`, `telemetry-provider.json.gz` and each phase’s compressed `failed-outcomes.json.gz`.

| Worker / execution / event / outcome | Captured invocations |
| --- | ---: |
| primary/stateless/fetch/ok | 2,965 |
| primary/durableObject/fetch/aborted | 268 |
| primary/durableObject/fetch/ok | 3,392 |
| primary/durableObject/alarm/canceled | 737 |
| primary/durableObject/jsrpc/ok | 1,186 |
| primary/durableObject/alarm/ok | 981 |
| primary/stateless/fetch/canceled | 5 |
| provider/stateless/fetch/ok | 15,165 |

All 268 aborted Object fetches were explicit cold-incarnation requests (`/cold`). The 737 canceled alarm deliveries and five canceled stateless fetches remain in the inventory; a cancellation alone does not establish whether a handler entered JavaScript or why it was canceled. No accepted measured turn had a non-completed settlement or transcript failure.

Two controller requests returned non-ok HTTP responses: the first pre-arm trial returned 500 after an obsolete provider query schema rejected its label, and the diagnostic read of that failed sample returned 409. The Object pair was retired. A later successful pi settlement changed incarnation within a warm series; its pair was also retired. The 50 attempts across both retired pairs are retained and never replayed. There are zero unreturned attempted requests after completion.

Four controller errors are recorded in `controller-errors.jsonl`: initial sandbox network access failed before deployment; the failed pre-arm turn; its 409 diagnostic read; and the warm-incarnation proof failure. The `retiredGroups` in [candidates-plan.json](candidates-plan.json) preserve replacement provenance. The separate undeployed probe-anchor build rejection is recorded in `undeployed-build-rejection.json`. Local gate failures and their resolutions are in [validation/results.json](validation/results.json).

The startup-metadata API returned 11 HTTP 500 responses; exact-upload/version verification used the alternate endpoint and succeeded. Every recorded API non-ok response is retained in `api-non-ok.jsonl`. The two expected cleanup Worker GET 404 responses are in [cleanup.json](cleanup.json).

| Phase | Accepted planned turns | Retired attempts | Missing planned turns | Join exceptions |
| --- | ---: | ---: | ---: | ---: |
| map | 32/32 | 0 | 0 | 32 |
| candidates | 432/432 | 50 | 0 | 295 |
| views | 272/272 | 0 | 0 | 159 |
| diagnostic | 128/128 | 0 | 0 | 146 |
| timing | 60/60 | 0 | 0 | 61 |

Join exceptions are disclosed separately from failed turns. Most are alarm END logs carrying the concurrently awaiting RPC context; CPU uses the unique START invocation join, never that conflicting END context. Missing/ambiguous telemetry joins remain missing CPU values, with complete-pair coverage shown in [cpu.md](cpu.md). The driver timestamps and returned native provider receipts are retained independently, so a missing log does not silently remove a valid latency sample. Every phase has zero plan or transcript-reference integrity failures.
