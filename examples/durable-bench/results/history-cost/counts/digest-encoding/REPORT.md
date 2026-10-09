# Digest encoder count evidence

Baseline `07f0272e7ba49a494064b6b74c6318b55514ae19`; candidate `442c988c7485c6b088a10f56547daa70668fc0d6`.

Counted source SHA-256: `85f7a1de02672c8e023559cfcb9bd337b93cb7bf5ab01b3ff6f9b1462f970420`. It exactly matches the committed file.

| Prior turns | First callback schema visits, baseline → candidate | Change | Digest schema visits, baseline → candidate | Change | First callback Effect evaluations, baseline → candidate | Digest Effect evaluations, baseline → candidate |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 50 | 21,247 → 19,153 | -9.86% | 5,601 → 3,507 | -37.39% | 15,349 → 15,343 | 579 → 574 |
| 250 | 97,840 → 87,492 | -10.58% | 27,703 → 17,355 | -37.35% | 56,123 → 56,117 | 2,753 → 2,747 |
| 1,000 | 368,020 → 326,672 | -11.24% | 110,703 → 69,355 | -37.35% | 208,859 → 208,852 | 10,861 → 10,854 |
| 3,500 | 1,268,711 → 1,124,017 | -11.40% | 387,401 → 242,707 | -37.35% | 718,221 → 718,213 | 37,887 → 37,879 |

The encoder removes approximately 37.35% of digest schema visits and 9.86–11.40% of first-callback schema visits. First-callback Effect evaluations decrease by 6–8. These are operation counts; they do not establish a latency improvement.

All four seed and measured transcript fingerprints, complete SQL statement/call/row snapshots, record visits, byte counts, projection counts, and normalized canonical table counts match exactly. The original nine model callbacks and full-turn proof work remain. All baseline counters and callbacks reproduce the retained baseline captures, ignoring only supplemental digest-scope keys.

| Prior turns | Seed transcript | Measured transcript | Selected records | Selected canonical bytes | Journal projections |
| ---: | --- | --- | ---: | ---: | ---: |
| 50 | `b017b487524e44a4` | `7b3b83c4979d51bb` | 268 | 204,113 | 1 |
| 250 | `dcea9f30b0917245` | `c29eb4d490969f50` | 1,334 | 1,024,025 | 1 |
| 1,000 | `ac520308146f2a8f` | `02f7ac7f9eb06bee` | 5,334 | 4,112,455 | 1 |
| 3,500 | `0a8c8e4b0d9a0794` | `6dd3a56608e79a1d` | 18,668 | 14,430,077 | 1 |

The candidate-only overlay preserves the baseline Records decoder and all original hook implementations. The supplemental digest scope is enabled identically on both sides. Thirty-eight retained input files were hashed before and after capture and remain unchanged. All private capture stores were removed.

`summary.json` contains the full first-callback and full-turn count dictionaries, compact snapshots of every model callback, SQL aggregates and exact-SQL hashes, visit histograms, and numeric deltas. `provenance.json` binds them to the committed candidate, source and input hashes, pinned dependencies, and raw captures. `REPRODUCE.md` and `reproduce.sh` reproduce the captures with the retained archives; no large archives or worker bundles are included here.

Parent implementation agent separately reports `vp run ready` PASS and a 24-case private differential proof (10 accepted, 14 rejected) with exact canonical JSON, SHA-256 digests, and failure class/messages. Those checks were not rerun by the count owner.
