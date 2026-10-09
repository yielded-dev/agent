# @yielded/agent-storage-sql

## 0.1.0-beta.168

### Minor Changes

- [#768](https://github.com/yielded-dev/agent/pull/768) [`8214498`](https://github.com/yielded-dev/agent/commit/8214498e8dfffde58773837fef3aa0c1fde07e9f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Publish settlement intent atomically in the canonical log and remove the separate settlement reservation protocol. Combine eligible SQL receipt finalization with publication and exclusive-session input markers with their canonical append.

  BEHAVIOR CHANGE: custom durable assemblies must provide a co-owned `SettlementPublisher`; pair Memory ledger and thread layers with `Layer.provideMerge`, and use fresh thread storage or format 16.

- [#768](https://github.com/yielded-dev/agent/pull/768) [`8214498`](https://github.com/yielded-dev/agent/commit/8214498e8dfffde58773837fef3aa0c1fde07e9f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Capture canonical appends before asynchronous work so later caller mutations cannot change the persisted value or invalidate its digest. Reuse captured record JSON across hashing and SQL writes, and commit eligible readonly responses with their completed results.

  BEHAVIOR CHANGE: custom SQL adapters must prepare raw append requests with `prepareSqlAppend`; `RawAppendRequest` is now a typed value instead of a Schema factory, and `RawRecord` is removed.

- [#768](https://github.com/yielded-dev/agent/pull/768) [`8214498`](https://github.com/yielded-dev/agent/commit/8214498e8dfffde58773837fef3aa0c1fde07e9f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Recover unfinished tools from their committed model declarations and remove the separate preparation write and outstanding-operation index.

  BEHAVIOR CHANGE: a crash after declaration can leave a mutating tool outcome unknown; durable Runs require unique tool call IDs and reject responses exceeding 4,096 distinct IDs with `RunJournalError` before commit or dispatch; thread stores require fresh storage or format 16. Use runtime `explain` in place of `readOutstanding`, and `DeclaredToolCallEvidence` in custom reconcilers.

  Supply JSON tool arguments and results in history used by function-based approval hooks.

- [#768](https://github.com/yielded-dev/agent/pull/768) [`8214498`](https://github.com/yielded-dev/agent/commit/8214498e8dfffde58773837fef3aa0c1fde07e9f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Prevent recovery from reverting joined input while its host is still processing it. Recheck canonical input after acquiring recovery ownership and fence rollback against the current host.

  BEHAVIOR CHANGE: Custom `SubmissionLedger` adapters must validate a supplied `RevertJoiningRequest.guard` atomically with rollback; requests without a guard retain their existing behavior.

- [#800](https://github.com/yielded-dev/agent/pull/800) [`cd15a98`](https://github.com/yielded-dev/agent/commit/cd15a98fb4c7bd741ac69fcb983492df1a4b4b6e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Remove cumulative Thread record, worker-input, peer-message, and delivery limits while preserving live capacity and per-Run bounds, and refuse batches whose expanded identities and references cannot fit a complete transfer page. **BEHAVIOR CHANGE:** Use fresh layout-21 stores and `effect-agent/thread@3` records, provide `ThreadExportSource` for `streamExport({ threadId })`, and configure compaction within retained model-context bounds.

- [#768](https://github.com/yielded-dev/agent/pull/768) [`8214498`](https://github.com/yielded-dev/agent/commit/8214498e8dfffde58773837fef3aa0c1fde07e9f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Bind durable Attempts to scoped storage sessions and reduce repeated ownership and tail reads in managed Node hosts.

  BEHAVIOR CHANGE: provide `RunStorage` in manual runtime assemblies and provide `ThreadReader` for canonical read helpers (stock adapters include it). Managed Node hosts keep `SqlClient`, `ThreadStore`, and `SubmissionLedger` private and reject custom SQLite triggers; compose application SQL with a separate client.

### Patch Changes

- [#801](https://github.com/yielded-dev/agent/pull/801) [`2461681`](https://github.com/yielded-dev/agent/commit/2461681593d911b28900ebe916b1c0d21ad3ebf0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Keep work and recovery continuations representable for accepted Thread identities and reject oversized fresh admissions before retaining work. Bound fresh Memory identities independently of imported facts and preserve exact receipt replays when UUID generation is unavailable.

- [#792](https://github.com/yielded-dev/agent/pull/792) [`d95dbb3`](https://github.com/yielded-dev/agent/commit/d95dbb3c462a28ba92205bc77734f66734c9e491) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Resume durable Runs from canonical continuations and referenced context, expose `@yielded/agent/run-continuation`, and remove `ThreadStore.recoveryCheckpoints`. **BEHAVIOR CHANGE:** use fresh layout-21 storage and `effect-agent/thread@3` archives; predecessor stores and formats are rejected.

- [#784](https://github.com/yielded-dev/agent/pull/784) [`80deef0`](https://github.com/yielded-dev/agent/commit/80deef0af2af790c68c2fda8701b3fed77c07db9) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reduce Schema overhead in repeated SQL reads. BEHAVIOR CHANGE: Bind `decodeRows(schema)` or `decodeSingleRow(schema)` once, then pass `(table, rowKey, rows)` to the returned decoder.

- [#816](https://github.com/yielded-dev/agent/pull/816) [`d449cd5`](https://github.com/yielded-dev/agent/commit/d449cd5efface20b6527f6e11f56fd6987408b5a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reduce SQL adapter work on each durable turn without changing storage formats, validation, or producer fencing.

- [#804](https://github.com/yielded-dev/agent/pull/804) [`b237c75`](https://github.com/yielded-dev/agent/commit/b237c75373c864895409fbb829ccf67ad3875ee5) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Apply saved-context limits after compaction so new Runs can follow long compacted Runs. Reject unrepresentable queued facts before admission and detect orphan or out-of-range canonical data during Cloudflare startup verification.

- [#784](https://github.com/yielded-dev/agent/pull/784) [`80deef0`](https://github.com/yielded-dev/agent/commit/80deef0af2af790c68c2fda8701b3fed77c07db9) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reduce readiness and claim decoding work by selecting only the submission state, input, and authority fields those operations need.

- [#768](https://github.com/yielded-dev/agent/pull/768) [`8214498`](https://github.com/yielded-dev/agent/commit/8214498e8dfffde58773837fef3aa0c1fde07e9f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reduce nested runtime and SQL tracing overhead while preserving agent, model, tool, storage and recovery operation spans, attributes and typed failures.

  BEHAVIOR CHANGE: If your trace filters target removed private helper spans, use their enclosing operation instead; selected helpers no longer create spans or Effect call frames.

- [#784](https://github.com/yielded-dev/agent/pull/784) [`80deef0`](https://github.com/yielded-dev/agent/commit/80deef0af2af790c68c2fda8701b3fed77c07db9) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reduce tracing overhead by keeping operation spans and removing private helper spans and stack frames. BEHAVIOR CHANGE: Update filters that use private helper span names to use the enclosing agent, model, tool, storage, or recovery operation.

- [#810](https://github.com/yielded-dev/agent/pull/810) [`5e3e3a7`](https://github.com/yielded-dev/agent/commit/5e3e3a7aef84807253a1c8b5a0087ff748b1e864) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Store durable context as verified canonical ranges, eliminate duplicate hot SQL batch payloads, and skip unused token estimates. **BEHAVIOR CHANGE:** use fresh stores for the revised unreleased `effect-agent/thread@3` format; custom adapters must provide narrow `readPrompt` and snapshot-bound full history reads.

- [#786](https://github.com/yielded-dev/agent/pull/786) [`8dd7b6a`](https://github.com/yielded-dev/agent/commit/8dd7b6ab18e4f5dd6970b830b5d8aa602f522f52) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Export complete Thread archives and atomically import them into empty Threads with rebuilt ledger state and preserved admission facts.

  BEHAVIOR CHANGE: Quiesce the source and export/import into fresh storage for the layout-16 cutover; import only the current record format and convert archives explicitly for future semantic changes. Keep the source on destination admission conflicts or unsupported external obligations, and re-export older archives that lack batch identities. Decode custom exports through `ThreadExportRecord` to retain their wire, and pair the memory ledger and delivery store with the same `MemoryThreadStoreLive`.

- [#786](https://github.com/yielded-dev/agent/pull/786) [`8dd7b6a`](https://github.com/yielded-dev/agent/commit/8dd7b6ab18e4f5dd6970b830b5d8aa602f522f52) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Remove the unused SQL schema initializer and index-creation helpers.

  BEHAVIOR CHANGE: Initialize storage through an adapter's frozen layout steps instead of `createStorageSchema`, `createWorkerControlIndexes`, `createNativeReadIndexes`, or `createMessageDeliveryPendingIndex`.

- [#792](https://github.com/yielded-dev/agent/pull/792) [`d95dbb3`](https://github.com/yielded-dev/agent/commit/d95dbb3c462a28ba92205bc77734f66734c9e491) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Provide a mutation-scoped `ProgressAppendReader` when validating canonical Run progress. Update custom adapters to call `validateProgressAppend(records)` inside their write boundary.

- [#815](https://github.com/yielded-dev/agent/pull/815) [`32f8a07`](https://github.com/yielded-dev/agent/commit/32f8a0785ab3f639c86be27ed9ef0ae7a8e184ba) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Avoid repeated work-index catalog probes within a storage session, invalidating schema presence after maintenance, rebuilds, and failed queries.

- [#803](https://github.com/yielded-dev/agent/pull/803) [`fadafe5`](https://github.com/yielded-dev/agent/commit/fadafe50ea795d8f479723661d1b02153f881c94) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve newer SQL writer fences and independent canonical appends when an exclusive Run session's cached state is stale. Keep valid sessions usable after failed claims and bind operation resources to the caller's Scope.

- [#798](https://github.com/yielded-dev/agent/pull/798) [`bdaaf49`](https://github.com/yielded-dev/agent/commit/bdaaf49643e8bfd3a4a72e7bfc7cacc640993486) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Discover unfinished Thread work through `@yielded/agent/thread-work`, recover bounded pages, and explicitly rebuild disposable indexes. **BEHAVIOR CHANGE:** follow `runRecovery().cursor` to finish a scan and use fresh layout-21 stores; factual effect closure remains available after execution decisions and Run settlement and must agree with any committed tool result.

- [#768](https://github.com/yielded-dev/agent/pull/768) [`8214498`](https://github.com/yielded-dev/agent/commit/8214498e8dfffde58773837fef3aa0c1fde07e9f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reduce redundant writes when appending durable tool and worker records to SQLite or PostgreSQL.
- Updated dependencies [[`bdaaf49`](https://github.com/yielded-dev/agent/commit/bdaaf49643e8bfd3a4a72e7bfc7cacc640993486), [`2461681`](https://github.com/yielded-dev/agent/commit/2461681593d911b28900ebe916b1c0d21ad3ebf0), [`fadafe5`](https://github.com/yielded-dev/agent/commit/fadafe50ea795d8f479723661d1b02153f881c94), [`8c05714`](https://github.com/yielded-dev/agent/commit/8c05714de84d68961b14e5ab7a3b7d809599563f), [`80deef0`](https://github.com/yielded-dev/agent/commit/80deef0af2af790c68c2fda8701b3fed77c07db9), [`1903e2c`](https://github.com/yielded-dev/agent/commit/1903e2ce0e62f6839e1cb881ef9970d2c5070656), [`d95dbb3`](https://github.com/yielded-dev/agent/commit/d95dbb3c462a28ba92205bc77734f66734c9e491), [`8214498`](https://github.com/yielded-dev/agent/commit/8214498e8dfffde58773837fef3aa0c1fde07e9f), [`32f8a07`](https://github.com/yielded-dev/agent/commit/32f8a0785ab3f639c86be27ed9ef0ae7a8e184ba), [`8214498`](https://github.com/yielded-dev/agent/commit/8214498e8dfffde58773837fef3aa0c1fde07e9f), [`80deef0`](https://github.com/yielded-dev/agent/commit/80deef0af2af790c68c2fda8701b3fed77c07db9), [`32f8a07`](https://github.com/yielded-dev/agent/commit/32f8a0785ab3f639c86be27ed9ef0ae7a8e184ba), [`8214498`](https://github.com/yielded-dev/agent/commit/8214498e8dfffde58773837fef3aa0c1fde07e9f), [`8214498`](https://github.com/yielded-dev/agent/commit/8214498e8dfffde58773837fef3aa0c1fde07e9f), [`8214498`](https://github.com/yielded-dev/agent/commit/8214498e8dfffde58773837fef3aa0c1fde07e9f), [`9437078`](https://github.com/yielded-dev/agent/commit/9437078991252ae70d835dc192787ab8f479612e), [`fadafe5`](https://github.com/yielded-dev/agent/commit/fadafe50ea795d8f479723661d1b02153f881c94), [`80deef0`](https://github.com/yielded-dev/agent/commit/80deef0af2af790c68c2fda8701b3fed77c07db9), [`b237c75`](https://github.com/yielded-dev/agent/commit/b237c75373c864895409fbb829ccf67ad3875ee5), [`cd15a98`](https://github.com/yielded-dev/agent/commit/cd15a98fb4c7bd741ac69fcb983492df1a4b4b6e), [`28e922d`](https://github.com/yielded-dev/agent/commit/28e922d68c03c51a40879ae4942c6920fd4c7f1c), [`f7652ff`](https://github.com/yielded-dev/agent/commit/f7652ff8197999e0155d6182b13cf8f70f552be2), [`8214498`](https://github.com/yielded-dev/agent/commit/8214498e8dfffde58773837fef3aa0c1fde07e9f), [`b246f8a`](https://github.com/yielded-dev/agent/commit/b246f8aaa3a92d5f82934b1fc7a82356d1ad6664), [`80deef0`](https://github.com/yielded-dev/agent/commit/80deef0af2af790c68c2fda8701b3fed77c07db9), [`2f062d4`](https://github.com/yielded-dev/agent/commit/2f062d4ed30afa9b11fb7d24cb3508d10358f3bd), [`8214498`](https://github.com/yielded-dev/agent/commit/8214498e8dfffde58773837fef3aa0c1fde07e9f), [`5e3e3a7`](https://github.com/yielded-dev/agent/commit/5e3e3a7aef84807253a1c8b5a0087ff748b1e864), [`8dd7b6a`](https://github.com/yielded-dev/agent/commit/8dd7b6ab18e4f5dd6970b830b5d8aa602f522f52), [`f38cc1a`](https://github.com/yielded-dev/agent/commit/f38cc1a8582515341b6e255d6b509331b0059a21), [`51bb46c`](https://github.com/yielded-dev/agent/commit/51bb46c6f240c579cf26e72f6127a434c8697a34), [`a4a1c4b`](https://github.com/yielded-dev/agent/commit/a4a1c4bb50cd037b114fcdaaf595492b775a2ad2), [`d449cd5`](https://github.com/yielded-dev/agent/commit/d449cd5efface20b6527f6e11f56fd6987408b5a), [`32f8a07`](https://github.com/yielded-dev/agent/commit/32f8a0785ab3f639c86be27ed9ef0ae7a8e184ba), [`d95dbb3`](https://github.com/yielded-dev/agent/commit/d95dbb3c462a28ba92205bc77734f66734c9e491), [`8214498`](https://github.com/yielded-dev/agent/commit/8214498e8dfffde58773837fef3aa0c1fde07e9f), [`45f0eeb`](https://github.com/yielded-dev/agent/commit/45f0eeb8c72835716d4538ef925a649dc02a9734), [`bdaaf49`](https://github.com/yielded-dev/agent/commit/bdaaf49643e8bfd3a4a72e7bfc7cacc640993486), [`a1fda46`](https://github.com/yielded-dev/agent/commit/a1fda46d689a278beaf60fbbcfe4a19048005ced), [`32f8a07`](https://github.com/yielded-dev/agent/commit/32f8a0785ab3f639c86be27ed9ef0ae7a8e184ba)]:
  - @yielded/agent@0.1.0-beta.168

## 0.1.0-beta.167

### Patch Changes

- Updated dependencies [[`776aaca`](https://github.com/yielded-dev/agent/commit/776aaca3809ca5959327ebff5623d525503e8e34)]:
  - @yielded/agent@0.1.0-beta.167

## 0.1.0-beta.166

### Minor Changes

- [#766](https://github.com/yielded-dev/agent/pull/766) [`a1fb42a`](https://github.com/yielded-dev/agent/commit/a1fb42a651eccef46b8775fe4373d3f04d85e8de) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Move Effect Agent to `@yielded/agent` and its `@yielded/agent-*` adapters. Update dependencies and import prefixes together; APIs, service identities, and stored formats remain unchanged.

### Patch Changes

- [#759](https://github.com/yielded-dev/agent/pull/759) [`00995dd`](https://github.com/yielded-dev/agent/commit/00995dd9049e11190588b143a32ba5c520686e7f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reduce latency for durable settlement, streamed responses, retained SQL history reads, and idle SQL ledger claims. Reduce cold startup time for Node hosts.

- [#749](https://github.com/yielded-dev/agent/pull/749) [`08d1384`](https://github.com/yielded-dev/agent/commit/08d1384e625091d83a6cb6eeb9c95e28cff2cc69) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require Effect 4.0.0 and use its current module paths and encoding APIs. Require `effect-cf@^0.53.0` for the Cloudflare adapter.

  BEHAVIOR CHANGE: upgrade Effect and matching provider, platform, SQL, and Atom packages to 4.0.0; replace `effect/unstable/*` imports with `effect/*` and use `effect/http-api` for HTTP APIs. Cloudflare logical alarms now back off from one second and park for hourly recovery after eight attempts without reported source progress.

- Updated dependencies [[`00995dd`](https://github.com/yielded-dev/agent/commit/00995dd9049e11190588b143a32ba5c520686e7f), [`2cfa8f7`](https://github.com/yielded-dev/agent/commit/2cfa8f75258edd32898958e264e549a737368c9e), [`08d1384`](https://github.com/yielded-dev/agent/commit/08d1384e625091d83a6cb6eeb9c95e28cff2cc69), [`a1fb42a`](https://github.com/yielded-dev/agent/commit/a1fb42a651eccef46b8775fe4373d3f04d85e8de)]:
  - @yielded/agent@0.1.0-beta.166

## 0.1.0-beta.165

### Patch Changes

- [#753](https://github.com/danieljvdm/effect-agent/pull/753) [`9d5f4d7`](https://github.com/danieljvdm/effect-agent/commit/9d5f4d7baac8e7311efa07b088f074406264bbc3) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reduce fresh durable Run latency on retained SQL histories while preserving all messages and validation.

- Updated dependencies [[`c426ed7`](https://github.com/danieljvdm/effect-agent/commit/c426ed78de30b9b186b4967e2e42bd19870ddc84), [`4ac924a`](https://github.com/danieljvdm/effect-agent/commit/4ac924aa14e3cfbfdac9ffaa7026164115943f11), [`656f964`](https://github.com/danieljvdm/effect-agent/commit/656f9643beecce1b6233e02672987ba413b5c5fc), [`de31c1e`](https://github.com/danieljvdm/effect-agent/commit/de31c1ece882beed14880628636c6bbd04f2ba52)]:
  - effect-agent@0.1.0-beta.165

## 0.1.0-beta.164

### Patch Changes

- Updated dependencies [[`d3ef7ea`](https://github.com/danieljvdm/effect-agent/commit/d3ef7ea8d24e6d7a5d4d057d497badfc72d3f5de)]:
  - effect-agent@0.1.0-beta.164

## 0.1.0-beta.163

### Patch Changes

- Updated dependencies [[`58f80d2`](https://github.com/danieljvdm/effect-agent/commit/58f80d2c1d2a832eba47176e24cc05010ee056a8)]:
  - effect-agent@0.1.0-beta.163

## 0.1.0-beta.162

### Patch Changes

- [#737](https://github.com/danieljvdm/effect-agent/pull/737) [`aa50237`](https://github.com/danieljvdm/effect-agent/commit/aa502375beb6e45a0979cb46a8324dccf6c22304) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Batch lifecycle publication acknowledgements, and reduce recovery queries and redundant maintenance for exclusive SQLite owners. Preserve exact identities, persisted retry budgets, and compatibility with custom publication storage.

- Updated dependencies [[`aa50237`](https://github.com/danieljvdm/effect-agent/commit/aa502375beb6e45a0979cb46a8324dccf6c22304)]:
  - effect-agent@0.1.0-beta.162

## 0.1.0-beta.161

### Patch Changes

- Updated dependencies [[`ff7f6c3`](https://github.com/danieljvdm/effect-agent/commit/ff7f6c30988b3c714f8cf4f4d26748c6baea1ccb)]:
  - effect-agent@0.1.0-beta.161

## 0.1.0-beta.160

### Patch Changes

- [#731](https://github.com/danieljvdm/effect-agent/pull/731) [`9a0b5bf`](https://github.com/danieljvdm/effect-agent/commit/9a0b5bf0f8f7a862f805b889a7305664e82ccaf5) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reuse eligible compacted Thread context across fresh durable Runs, refreshing it from new canonical records while preserving full replay for incompatible histories. Validate stored checkpoints through indexed canonical batch lookups.

- [#733](https://github.com/danieljvdm/effect-agent/pull/733) [`8c25dfd`](https://github.com/danieljvdm/effect-agent/commit/8c25dfdd2a16271df44bdfae4258b0d5c7114c38) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Coalesce Cloudflare maintenance scheduling writes within each transaction and reuse its queue view without changing retry, publication, or recovery behavior.

- Updated dependencies [[`9a0b5bf`](https://github.com/danieljvdm/effect-agent/commit/9a0b5bf0f8f7a862f805b889a7305664e82ccaf5), [`8c25dfd`](https://github.com/danieljvdm/effect-agent/commit/8c25dfdd2a16271df44bdfae4258b0d5c7114c38)]:
  - effect-agent@0.1.0-beta.160

## 0.1.0-beta.159

### Patch Changes

- [#724](https://github.com/danieljvdm/effect-agent/pull/724) [`8a839e6`](https://github.com/danieljvdm/effect-agent/commit/8a839e685444bff89ba236e0de316adf40ee010f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Remove obsolete runtime aliases, frozen context tools, and unused storage failpoint controls. Use indexed canonical reads for selected Thread records instead of decoding a cached full history.

  BEHAVIOR CHANGE: Use `Subagent.make`, `ContextTools.toolkit` with `ContextTools.layer`, and the registered `runResolvedWorker` in place of `Subagent.define`, legacy context tools, and `runWorker`; classify delegation with `DelegationTool` metadata instead of name helpers. Replace the removed `DoStorageFailpointTestControl` and SQLite testing module with the corresponding storage failpoint service Layers.

- Updated dependencies [[`4e78cc4`](https://github.com/danieljvdm/effect-agent/commit/4e78cc4c0011b9d60146397fa6e0143c70b8c9ca), [`04889a9`](https://github.com/danieljvdm/effect-agent/commit/04889a95338a4f786bdd7fdf45f23236e4dbb1ef), [`b6d171d`](https://github.com/danieljvdm/effect-agent/commit/b6d171de3d385960a062ce7f8e3415ffe22bbb93), [`8a839e6`](https://github.com/danieljvdm/effect-agent/commit/8a839e685444bff89ba236e0de316adf40ee010f), [`976f337`](https://github.com/danieljvdm/effect-agent/commit/976f3371a63b7a9f1a92b26f88371350042735b3)]:
  - effect-agent@0.1.0-beta.159

## 0.1.0-beta.158

### Patch Changes

- [#720](https://github.com/danieljvdm/effect-agent/pull/720) [`9a7d358`](https://github.com/danieljvdm/effect-agent/commit/9a7d3581930b1a36fed318d13c966ff8efc4baa5) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Park accepted message deliveries without status polling and acknowledge native sources on terminal settlement. Bound no-progress Cloudflare maintenance and reuse canonical hydration codecs and a bounded multi-page cache.

  BEHAVIOR CHANGE: generic host envelopes need an exact terminal `Complete` acknowledgement or explicit receipt recovery. Draining a host lane resets its scheduling budget; eight unchanged self-rearming passes park pending work for hourly recovery and report once through the installed error reporter. New source-transaction `recordProgress` or a strictly increasing retained commit cursor resumes it immediately. Canonical hydration retention is bounded by one shared eight-MiB serialized-byte budget per isolate.

- Updated dependencies [[`9a7d358`](https://github.com/danieljvdm/effect-agent/commit/9a7d3581930b1a36fed318d13c966ff8efc4baa5), [`a52c77f`](https://github.com/danieljvdm/effect-agent/commit/a52c77fe67ac7f729f3ea1c637474f4b8036b2dd)]:
  - effect-agent@0.1.0-beta.158

## 0.1.0-beta.157

### Patch Changes

- [#715](https://github.com/danieljvdm/effect-agent/pull/715) [`b6ec526`](https://github.com/danieljvdm/effect-agent/commit/b6ec526daf05a71d318fec0b31c5b31db54fed35) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Publish committed Run and Subagent start progress while native execution continues. Reduce SQLite statements for warm Durable Object turns while preserving recovery and ownership fencing.

- Updated dependencies [[`b6ec526`](https://github.com/danieljvdm/effect-agent/commit/b6ec526daf05a71d318fec0b31c5b31db54fed35), [`ee41eb7`](https://github.com/danieljvdm/effect-agent/commit/ee41eb7ef96a9edfa7e8d2dd4b2a50b8944f7af3)]:
  - effect-agent@0.1.0-beta.157

## 0.1.0-beta.156

### Patch Changes

- [#713](https://github.com/danieljvdm/effect-agent/pull/713) [`d2d83a2`](https://github.com/danieljvdm/effect-agent/commit/d2d83a2fa815fe11224421e64135145ed58e7bca) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reduce Durable Object SQLite work with shared write-through reads and asynchronous lifecycle batches after native execution, preserving durable receipts across eviction. Use the SQL Memory Layer's `SqlMemoryBatchWriter.changeMany` to commit up to 128 ordered commands atomically and combine their writes.

- Updated dependencies [[`d2d83a2`](https://github.com/danieljvdm/effect-agent/commit/d2d83a2fa815fe11224421e64135145ed58e7bca)]:
  - effect-agent@0.1.0-beta.156

## 0.1.0-beta.155

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.155

## 0.1.0-beta.154

### Patch Changes

- Updated dependencies [[`826fb91`](https://github.com/danieljvdm/effect-agent/commit/826fb911a7dfd6548182e02f103c747f1a9a567f)]:
  - effect-agent@0.1.0-beta.154

## 0.1.0-beta.153

### Patch Changes

- Updated dependencies [[`4e16d81`](https://github.com/danieljvdm/effect-agent/commit/4e16d81311ad26f1666fdedd23bb409f2100a669), [`6f9e913`](https://github.com/danieljvdm/effect-agent/commit/6f9e91364cd03174bb8cf0223d0e7b468c3d7c1e), [`8085bda`](https://github.com/danieljvdm/effect-agent/commit/8085bda3355ef97bc3f7ed82612a105581648440)]:
  - effect-agent@0.1.0-beta.153

## 0.1.0-beta.152

### Patch Changes

- Updated dependencies [[`cff65f6`](https://github.com/danieljvdm/effect-agent/commit/cff65f61b7b46fb00ebecd1a6242c1d04cf0bd24)]:
  - effect-agent@0.1.0-beta.152

## 0.1.0-beta.151

### Patch Changes

- Updated dependencies [[`61cb29f`](https://github.com/danieljvdm/effect-agent/commit/61cb29f025fedd14483e013d7b33d099439b8074)]:
  - effect-agent@0.1.0-beta.151

## 0.1.0-beta.150

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.150

## 0.1.0-beta.149

### Patch Changes

- Updated dependencies [[`08e4acf`](https://github.com/danieljvdm/effect-agent/commit/08e4acf1cd791b0a615f5ba751b698e915c3b8be)]:
  - effect-agent@0.1.0-beta.149

## 0.1.0-beta.148

### Patch Changes

- Updated dependencies [[`f2726bb`](https://github.com/danieljvdm/effect-agent/commit/f2726bb4f48848a7cbaa0878a9911f68220b8255)]:
  - effect-agent@0.1.0-beta.148

## 0.1.0-beta.147

### Patch Changes

- Updated dependencies [[`27877c8`](https://github.com/danieljvdm/effect-agent/commit/27877c820b42cbffcbeecca42dc7c4b6f4a382cc), [`af24505`](https://github.com/danieljvdm/effect-agent/commit/af2450560f185e75d725a425349e9f611741645c)]:
  - effect-agent@0.1.0-beta.147

## 0.1.0-beta.146

### Minor Changes

- [#681](https://github.com/danieljvdm/effect-agent/pull/681) [`9252c1a`](https://github.com/danieljvdm/effect-agent/commit/9252c1ad4707035308ff72527ed303a685027a28) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Publish retained lifecycle facts asynchronously in ordered owner batches without delaying model attempts, with atomic receipts and bounded retries that park exhausted work.

  BEHAVIOR CHANGE: Implement `LifecyclePublicationHandler.publish(batch)` for a nonempty array of at most eight facts and commit the entire batch idempotently in one host transaction; custom lifecycle storage implementations must return bounded owner batches, replace `defer` with `claim`, and implement `retryParked` instead of `pendingDeadlineFor`.

### Patch Changes

- Updated dependencies [[`9252c1a`](https://github.com/danieljvdm/effect-agent/commit/9252c1ad4707035308ff72527ed303a685027a28), [`c2fc81a`](https://github.com/danieljvdm/effect-agent/commit/c2fc81a2882deec908868955d1325fdec400b979)]:
  - effect-agent@0.1.0-beta.146

## 0.1.0-beta.145

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.145

## 0.1.0-beta.144

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.144

## 0.1.0-beta.143

### Patch Changes

- [#669](https://github.com/danieljvdm/effect-agent/pull/669) [`c68edc7`](https://github.com/danieljvdm/effect-agent/commit/c68edc7aa1cdedb92ad6f31b63ba048bb718b2c6) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add an optional typed lifecycle publication handler with atomically retained native obligations and acknowledgement retries through existing Cloudflare maintenance. Publish application records from exact native admissions and transitions without scanning execution history.

- Updated dependencies [[`c68edc7`](https://github.com/danieljvdm/effect-agent/commit/c68edc7aa1cdedb92ad6f31b63ba048bb718b2c6)]:
  - effect-agent@0.1.0-beta.143

## 0.1.0-beta.142

### Patch Changes

- [#665](https://github.com/danieljvdm/effect-agent/pull/665) [`03831c5`](https://github.com/danieljvdm/effect-agent/commit/03831c5554b568bbf87ba79dcf1f030444d35e90) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose PostgreSQL storage with an application-provided Effect SQL client and Crypto layer, preserving native connection pooling, codecs, and schema defaults.

  BEHAVIOR CHANGE: Replace the `client` option and `PostgresStorageClient` with `PostgresStorage.layer` (or `layerWith(options)`) and native Layers. Install the service values returned by shared `makeSqlThreadStore` and `makeSqlSubmissionLedger` factories with `Layer.effect` for their corresponding ports; yield `makeSqlQuery(namespace?)` and call shared schema and index creation helpers as functions, optionally supplying a namespace.

- [#597](https://github.com/danieljvdm/effect-agent/pull/597) [`161aab3`](https://github.com/danieljvdm/effect-agent/commit/161aab335dbbb9bf704d005bf95015be2e04858b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Share SQL persistence implementations through `@effect-agent/storage-sql` while preserving SQLite storage formats and adapter APIs. BEHAVIOR CHANGE: import SQL subscription, message-delivery, native-read, and upgrade helpers from `@effect-agent/storage-sql` instead of `effect-agent`, and pass custom transactions through the factory options.

- Updated dependencies [[`161aab3`](https://github.com/danieljvdm/effect-agent/commit/161aab335dbbb9bf704d005bf95015be2e04858b)]:
  - effect-agent@0.1.0-beta.142
