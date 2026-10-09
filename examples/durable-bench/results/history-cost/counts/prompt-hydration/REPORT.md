# Prompt hydration through native constructors: deterministic counts

Baseline `07f0272e7ba49a494064b6b74c6318b55514ae19`; independent candidate `0451aacb627ec3dbe76618c043bce9d6b91596ad`. Only PromptRecordPayload uses HistoryPrompt from the new private helper. Both SQL filtering candidates are absent.

All four benchmark fingerprints match baseline. Native constructor hydration preserves upstream encoded-message validation and removes the second native Message schema pass in this read path. The measured reduction is substantial in operation counts; no latency or CPU claim is made.

## Submission to first scripted model

| Seed turns | First-model Effects B → C | Whole-Run Effects B → C | readPrompt Effects B → C |
|---:|---:|---:|---:|
| 50 | 15,349 → 10,518 | 37,112 → 32,281 | 7,667 → 2,836 |
| 250 | 56,123 → 32,098 | 78,169 → 54,144 | 37,839 → 13,814 |
| 1,000 | 208,859 → 112,783 | 230,905 → 134,829 | 151,084 → 55,010 |
| 3,500 | 718,221 → 381,982 | 740,267 → 404,028 | 528,733 → 192,490 |

The first-model boundary is entry to the scripted provider callback, before benchmark transcript conversion. It includes inline admission/process and runtime setup, but not deployed HTTP or remote-provider transport. At 250/1000/3500, the remainder after the first callback is exactly 22,046 Effects in both variants (50: 21,763). No timing, scheduling, allocation, startup or deployment improvement follows directly from these counts.

## Schema work and unchanged history workload

| Turns | Records, unchanged | JSON bytes, unchanged | Encoded passes, unchanged | Native passes B → C | readPrompt schema nodes B → C | Decoder schema nodes B → C |
|---:|---:|---:|---:|---:|---:|---:|
| 50 | 268 | 204,113 | 184 | 184 → 0 | 12,494 → 9,544 | 9,524 → 6,574 |
| 250 | 1,334 | 1,024,025 | 917 | 917 → 0 | 62,107 → 47,432 | 47,367 → 32,692 |
| 1,000 | 5,334 | 4,112,455 | 3,667 | 3,667 → 0 | 248,287 → 189,612 | 189,382 → 130,707 |
| 3,500 | 18,668 | 14,430,077 | 12,834 | 12,834 → 0 | 868,946 → 663,596 | 662,795 → 457,445 |

| Turns | Decoder Effects B → C | Decoder root calls B → C | Decoder schema nodes / selected record B → C |
|---:|---:|---:|---:|
| 50 | 7,533 → 2,702 | 86 → 2 | 35.537 → 24.530 |
| 250 | 37,437 → 13,412 | 423 → 6 | 35.507 → 24.507 |
| 1,000 | 149,674 → 53,603 | 1,688 → 21 | 35.505 → 24.504 |
| 3,500 | 523,833 → 187,590 | 5,907 → 73 | 35.504 → 24.504 |

Schema-node calls count interpreted AST parser invocations, including structural and primitive fields; root calls count SchemaParser.runWithCompiler, not individual messages. Decoder scope excludes SQL/planning. Every selected historical record is JSON-decoded once; metadata and fold each visit it once, plus the new input. One journal projection, four full-envelope decodes and nine scripted model calls occur in each Run. Native constructors still perform JavaScript work; zero native schema passes does not mean zero hydration work.

| Turns | Metadata visits = fold visits, unchanged | Prompt SQL rows, unchanged | Whole-Run SQL rows, unchanged |
|---:|---:|---:|---:|
| 50 | 269 | 539 | 1,220 |
| 250 | 1,335 | 2,675 | 5,652 |
| 1,000 | 5,335 | 10,690 | 15,275 |
| 3,500 | 18,669 | 37,410 | 47,355 |

Complete normalized SQL breakdowns, visit histograms and importer inventories match baseline at every size. SQL rows are completed workerd cursor counters, not physical I/O or a billing metric.

## Separate history-digest supplement

The original comparison above uses the old counters. Separate fresh baseline/candidate captures add only enter/leave in JavaScript try/finally around the existing yield* withCrypto(digestRunHistory(...)) call. No Effect operation or Schema validation is added. Removing historyDigest-prefixed fields exactly reproduces each original count map and all nine model snapshots, with unchanged SQL/visits/fingerprints.

| Turns | Digest schema nodes B = C | Digest Effects B = C | Digest root parser calls B = C | Other schema nodes outside readPrompt and digest B = C |
|---:|---:|---:|---:|---:|
| 250 | 27,703 | 2,753 | 3 | 8,030 |
| 1,000 | 110,703 | 10,861 | 3 | 9,030 |

The digest accounts for 83,000 of the 84,000 additional schema nodes outside readPrompt from 250 → 1000 (98.81%); the remainder grows 8,030 → 9,030. Its Effect count grows by 8,108. Constructor hydration leaves this stage unchanged. Three root parser calls do not mean three digests. The scope includes full prior-Prompt encoding and digest computation; it does not separately count canonical JSON nodes, UTF-8 bytes, SHA work, or wall time.

## Method and limits

- Same retained canonical import archives and pinned baseline dependencies. 50/250 seed snapshots came from an existing benchmark fixture checkout; 1000/3500 continued private copies of its 250 fixture. Seed plan: repeating 1/1/0 tool cycle, 256-byte results with every 97th result 8 KiB, no compaction. New input: `turn count-0 tools=8`, nine scripted model calls through successful settlement.
- Exact candidate Records.ts is selected by the existing esbuild plugin; the committed helper is copied only into the disposable checkout at its real relative import path. Baseline Records.ts stays unchanged. Optional digest instrumentation is in a separate mode; original counter code, probe and transfer helpers remain unchanged.
- Import runs through the real canonical importer. Historical Attempt rows are omitted, with no copied attempt/claim rows. Fresh Miniflare executes the inline DurableAgentRuntime path. Native alarms are disabled in the counting bundle. Import, export, setup, fingerprinting, settlement polling and reporting are outside counters.
- The 50-turn pilot justified larger captures by its operation-count reduction. Prior baseline captures were reused for the main comparison. Both baseline and candidate were recaptured at250/1000 with the identical supplemental scope. Semantic equivalence beyond the benchmark and the product full gate are owned separately; this report does not substitute for them.
- Hooks count JSON.parse UTF-8 bytes, interpreted schema parser invocations, Effect runLoop iterations, actual journal visits and SQL cursor reads. Their global scopes are appropriate only for this isolated single-Run measurement. Hooks add JavaScript work and are unsuitable for timing claims.
- Existing artifacts are untouched. All new work is isolated here and in disposable counting-hydration sources. No credentials, product or deployed-bench checkout writes, deployment, publication, new indexes, storage representation changes, AOT/compiler lifecycle, or committed test infrastructure. Ownership-query findings remain separate.

## Fingerprints

| Turns | Seed | Last scripted model request |
|---:|---|---|
| 50 | `b017b487524e44a4` | `7b3b83c4979d51bb` |
| 250 | `dcea9f30b0917245` | `c29eb4d490969f50` |
| 1,000 | `ac520308146f2a8f` | `02f7ac7f9eb06bee` |
| 3,500 | `0a8c8e4b0d9a0794` | `6dd3a56608e79a1d` |

See REPRODUCE.md for the commands and PUBLISH.md for the exact text-only allowlist. Exact fixtures preserve record identities/timestamps and byte totals; regenerated equivalent transcripts may have different raw bytes. Source/patches and fingerprints are public-safe; archives, bundles, databases, raw logs and workstation paths are excluded.
