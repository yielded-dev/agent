# KOM-433: deployed count map

**Count-map gate answered; select the complete `DoThreadStore.append` stage.** Its optimistic warmed evaluation-removal ceiling is **14.0–15.0%**, retaining observed Schema, digest, SQL/transaction and nested-read work. This is **not a CPU ceiling or speed prediction**. No optimization was implemented.

All **30 counted operations / 3 Objects** completed on real Cloudflare through two sequential Alchemy stacks. Both strict telemetry gates **failed**: only **15/20** and **9/10** operation/ingress pairs were available after 13 bounded polls. Failures and missing observations are retained; there are **zero uninstrumented CPU samples**. Both Workers and their exact namespace IDs are independently confirmed absent.

## Reproducible warm counts

Each operation used two scripted model rounds, two tool calls, two model-stream finalizers and 400 KB retained active context. Seeds were 10 and 1,000 canonical records. Phases 6–10 followed five operations without intermediate audits, in the same observed incarnation—not a JIT-state guarantee.

| Warm phase          | Evaluations | Inline successes, separate | Selected primitive allocations | SQL statements |
| ------------------- | ----------: | -------------------------: | -----------------------------: | -------------: |
| 6: compaction/reply |      14,845 |                        501 |                         16,467 |            100 |
| 7: fresh reply      |      13,264 |                        476 |                         14,505 |             91 |
| 8: fresh reply      |      13,040 |                        440 |                         14,295 |             91 |
| 9: compaction/reply |      15,431 |                        557 |                         17,360 |            100 |
| 10: fresh reply     |      13,512 |                        513 |                         14,827 |             91 |

**All three count columns matched exactly across all three Objects.** The corrected large Object also reproduced all ten earlier large-Object triples. Initial large-history operations were **80,697 / 44,960 evaluations**, versus **14,076 / 12,999** for the small Object. Large first-fresh acquisition/projection cost **25,984 / 6,812**, versus **883 / 264** at warm phase 7. Acquisition and projection are distinct; historical fresh-Thread Node numbers do not represent this retained-history workload.

All prefix/hash-chain, complete-export, retained-context, tool/settlement and continuation checks passed. Normalized final prompts matched across all three Objects; outputs were exactly `phase-1-ok` through `phase-10-ok`. Final canonical row counts were **168 / 1,158 / 1,158**, with **7,860,224 / 9,199,616 / 9,199,616 database bytes**. Retained evidence: [analysis and output hashes](analysis.json), [raw corrected stage/call counts](corrected/block-0/cohort-0/baseline/large/counts.json), [complete audit, gzip](corrected/block-0/cohort-0/baseline/large/evidence.json.gz).

## The old remainder, explained

The legacy labels leave **33,930 of 70,092 warmed evaluations** outside their regions. Finer source ownership explains **33,915/33,930 (99.956%)**. This is the legacy partition on the new hosted fixture, not a retroactive decomposition of historical local counts.

| Exclusive ownership of legacy remainder        | Evaluations |
| ---------------------------------------------- | ----------: |
| Interpreter/model/tool control                 |      10,084 |
| Attempt preparation/progress/control           |       6,790 |
| Ownership/ledger operations                    |       6,169 |
| Nested Schema runtime                          |       4,147 |
| Storage/cache/SQL                              |       4,057 |
| Commit/continuation/context pre/post-body work |         943 |
| Native AI stream/tool machinery                |         625 |
| Fixture producer/handler/operation             |         510 |
| Scope/fiber/host machinery                     |         445 |
| Digests                                        |         145 |
| **Unresolved host prelude**                    |      **15** |

These categories have an additive source census, not an opaque renamed engine bucket. Phase 7 examples: `SchemaParser.runWithCompiler:977` **588**; transaction-gated `OwnedState.read:23` and `ownedRows.matching:146` **389**; `beforeExecutionDeadline:8205,8210` clock/timeout wrappers **326**; native `Semaphore.withPermits` **349**; `RunContinuation.makeProgressWriter` provision/coverage checks **285**. Model-attempt/part/usage, tool preflight/authorization/execution/result admission, Channel, Layer, ownership release and settlement observation remain individually inspectable in [analysis.json](analysis.json) and [source selectors/transforms](source/corrected-build.tar.gz).

The irreducible **3 evaluations/operation** are `OnSuccess`, `Service`, `Success`, with birth at `ManagedRuntime.runPromise:378`: **0.0214% of the warmed total**, explicitly unresolved.

## Counting validity and fast paths

RPC roots begin before `DurableObject.RunSymbol` and end after Promise/event-Scope completion. Async-local roots, primitive/continuation provenance, immutable fiber birth and registering-finalizer ownership cover returned Effects/Streams after generator bodies close. Hooks use `globalThis.Boolean`, add no Effect wrappers, and count dispatches, inline successes and selected constructors separately—not JavaScript/heap/CPU shares.

All exclusive stage, legacy, legacy-to-fine and source partitions closed exactly. All counted fibers completed; no ancestry truncation or late-event diagnostic occurred. Six to eight generators/operation lacked a JavaScript `done` return at materialization/release failure propagation and abort/renewal watcher sites; that diagnostic remains retained, not mistaken for live fibers.

Phase 7 inventory: **1 acquisition, 1 complete projection, 5 public appends, 1 tool batch, 2 native model streams, 2 Toolkit handles**. Both scripted model bodies completed with **zero yields but returned Streams** whose work/finalizers remained attributed. There were **470 decode / 61 encode applications**, parser outcomes **297 same-Exit / 315 other Exit / 55 non-Exit Effect**, and synchronous-runner paths **657 Exit fast paths / 5 nested fibers**. Sync/result codecs were not assumed runtime-free.

## One-stage choice

`T` is the whole RPC; `S` includes the complete stage; `R` retains measured Schema, digest, SQL/transaction, other-storage and Scope work. `(S-R)/T` is an optimistic **evaluation-removal** bound. Inclusive stages overlap; do not sum them.

| Rank  | Complete candidate                                 |      Warm S | Retained R |          Ceiling |
| ----- | -------------------------------------------------- | ----------: | ---------: | ---------------: |
| **1** | `DoThreadStore.append`, including `appendPrepared` | 2,466–2,897 |    568–664 | **14.05–15.04%** |
| 2     | `RunJournal.projectRunJournalStream`               |     233–731 |       9–25 |       1.72–4.58% |
| 3     | `initialContext`, retaining storage/codecs         |   674–1,078 |  607–1,011 |       0.43–0.51% |

Compare **the whole public append**: capture/validation, encoding/digests, fencing, the real storage transaction, canonical/index/work/progress writes and result validation. Authority and atomicity are unchanged here. Reject codec-only or raw-SQL substitutes. Representative input selection remains necessary; the provisional 64 KiB recipe is not established for every observed batch. No selected narrow stage supports a 50% whole-operation evaluation claim.

## Identity, limits, receipts

- Clean framework baseline: **`8c05714de84d68961b14e5ab7a3b7d809599563f`**. Protected **`58359ea25a78727d18f74847c0aa7a6a4873bc61`** stayed unchanged. Only task-local fixture code and authorized evidence were written.
- Configured account SHA-256: **`3e8f88a45f1480a607b6192f4f156c5630072d175f6d49cc61160be5384837bd`**, confirmed through project `/usr/bin/env direnv exec .`; no personal login or inference keys.
- Exact source, fixture, lockfile, bundle and version identities: [pilot build](source/pilot-build.json), [corrected build](source/corrected-build.json). Production exports; Effect 4.0.0; effect-cf 0.53.0; Alchemy 2.0.0-beta.80; compatibility 2026-08-01 / nodejs_compat.

| Worker suffix, after `effect-agent-cpu-` | Deployed version                       | Bundle SHA-256                                                     |
| ---------------------------------------- | -------------------------------------- | ------------------------------------------------------------------ |
| `01a119d543300001-b0-baseline`           | `105f9b46-49bd-4ae3-88c8-7861c1692862` | `7b4e37d94820aecd028f38aafcc4cf781e16118b31985b34a976f67a42caa03e` |
| `01a119d543300004-b0-baseline`           | `91ceead7-1f78-40dd-bc69-a076fc66cedc` | `3ca0b017373c423d244243ac79f0336deb1a336645ff8393f10ec3a0083f2912` |

The follow-up corrected closure-captured response/tool commit labels and refined ownership/digest/source attribution; the original split is superseded, not discarded. Plans admitted **2 Objects/20 RPCs**, then **1/10**, one active operation, no amplification, ten-minute dispatch deadlines and a 16 MiB pilot stop beneath 32 MiB admission. They declared **21,900 / 14,000 estimated SQL statements** and **2,010 / 1,500 canonical record writes**, including seed. Actual seed-to-last-operation spans: **53.111 / 35.941 seconds**. These are workload bounds, not billing caps; [plans and raw receipts](pilot/plan.json) remain retained.

Counted seed/operation/audit roots observed **8,609 statements, 42,960 rows read, 32,210 rows written**, with complete cursor counters. Initialization/alarm SQL is excluded; exports retain 32 and 19 observed alarms, including cancellations. No uncertain RPC was retried.

Both commands exited **1** on telemetry incompleteness but completed Alchemy destruction. [Pilot](pilot/cleanup.json) and [corrected](corrected/cleanup.json) receipts record `complete=true`, `secretRemoved=true`, and all targets complete. The [independent check at 16:30:37Z](independent-cleanup.json) confirms both Worker 404s and absence of both namespace IDs/script associations in the complete account listing. **No live stack, private state or background job remains.**

[Exact commands, including context and cleanup](commands/recorded-commands.txt), static-check logs, baseline-relative patches, complete build archives and [artifact digests](artifact-manifest.json) are retained. Use fresh run/output identities for reproduction. Vite+ static checks passed. **No local runtime benchmark/test suite or `ready` suite ran; no paid model call, PR, push, merge or tracker message occurred.**

**Proceed means stage selection only.** Missing invocation correlations still block CPU evidence. The three unresolved host-prelude evaluations and excluded initialization/alarm SQL remain limitations; never turn evaluation shares into CPU shares.
