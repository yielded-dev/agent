# Lost-ACK seed prefix assessment

Source review only, 2026-10-08. No Worker/API calls, deployments, replay, or harness changes were made. The diagnostics below were reported by the coordinating agent, not collected by this review. SQL examples are proposed reads, not executed proof. Run any future diagnostic only after the active 50-turn measurement finishes.

| Fixture | Last acknowledged prefix | Candidate durable prefix | Reported evidence | Resume only after proof |
| --- | ---: | ---: | --- | --- |
| pinned `1000_2` | 820 | 830 | submissions=attempts=830; ownership/work=0 | begin with `h830` |
| tardie `250_1` | 207 | 208 | messages=208; events=2708; checkpoint=2507; watchdog empty; alarm null | begin with `h208` |

Counts show admission/activity, not successful completion. Both stores have persisted terminal evidence that can resolve the missing acknowledgement without submitting the same input again. A missing or inconsistent terminal record means **do not skip or replay**; retain the fixture as ambiguous for separate recovery handling.

## Read-only wrapper boundary

Use a narrowly scoped diagnostic on the existing object's `ctx.storage.sql`, with fixed SELECT statements and validated/bound parameters. Read the stored prefix directly; do not call `wake`, `turn`, `seed`, `resume`, `recover`, `processThreadResolved`, or open a runtime/reference to inspect it. Tardie's `methodState` looks observational but calls `open`; even its `records()` goes through a lazily initialized journal. Raw SELECTs avoid those paths.

Collect each object's SQL proof synchronously without an `await` between its statements. Capture the canonical tail before and after, and require identical tails; if reads are paged or interrupted, repeat against a fixed tail and reject changes. Alarm/watchdog/scheduler reads are separate observations, not an atomic cross-object snapshot. Return only proof counts, requested identity, tail, terminal IDs/outcomes, mismatches, and hashes; retain raw rows locally only if needed to validate a mismatch. Do not install a general SQL execution endpoint.

The proof concerns the completed seed prefix, not whether all future alarm CPU has finished. Read-only inspection can still activate/warm an object, so it belongs outside measurement. The coordinating agent owns the subsequent cold preparation.

## Pinned: canonical submission settlement

The [original Yielded bench](../../../src/yielded.ts) admits each input with principal `bench`, idempotency key `hN`, and raw string `turn hN tools=K`. It accepts a turn only when `processThreadResolved` returns its submission with outcome `completed`. The [Cloudflare layout](../../../../../packages/storage-cloudflare/src/internal/migrations.ts) has submission state and canonical records; attempts have no terminal-outcome column. [Canonical `SubmissionSettled`](../../../../../packages/effect-agent/src/durable/Records.ts) is the outcome authority, and the [ledger finalization](../../../../../packages/storage-cloudflare/src/DoSubmissionLedger.ts) copies that outcome to the submission and removes ownership.

The saved pinned bundle contains this same layout (version 21). First identify the logical thread from storage, rather than assuming its SQL `thread_id` equals the namespace object name `main`:

```sql
SELECT thread_id, tail_sequence, tail_digest
FROM effect_agent_threads;

SELECT thread_id, principal, state, settled_outcome, COUNT(*) AS n
FROM effect_agent_submissions
GROUP BY thread_id, principal, state, settled_outcome;
```

For the identified thread, fetch all 830 admissions with their canonical input, settlement, and Run completion. Bind that thread ID to both placeholders. This includes the published archive fallback used by the [native canonical reader](../../../../../packages/storage-sql/src/SqlThreadArchiveRange.ts); an unavailable canonical payload must fail proof rather than being silently skipped.

```sql
WITH canonical AS (
  SELECT r.sequence, r.record_id, r.record_tag, r.run_id,
         COALESCE(r.record_json,
           CASE WHEN j.state = 'archived' AND j.locator IS NOT NULL
                THEN a.record_json END) AS wire
  FROM effect_agent_canonical_records r
  LEFT JOIN effect_agent_archive_records a
    ON a.thread_id = r.thread_id AND a.sequence = r.sequence
  LEFT JOIN effect_agent_journal_ranges j
    ON j.thread_id = a.thread_id AND j.first_sequence = a.range_first_sequence
  WHERE r.thread_id = ?
    AND r.record_tag IN ('UserInputRecorded', 'SubmissionSettled', 'RunCompleted')
)
SELECT s.queue_sequence, s.idempotency_key, s.principal,
       s.agent_id, s.deployment_id, s.submission_id, s.receipt_id,
       s.input_json, s.input_applied_record_id, s.input_applied_sequence,
       s.state, s.settled_outcome, s.settled_record_id, s.finalized_at,
       s.joined_host_submission_id,
       i.sequence AS input_sequence, i.wire AS input_record,
       z.sequence AS settlement_sequence, z.wire AS settlement_record,
       r.sequence AS completion_sequence, r.wire AS completion_record
FROM effect_agent_submissions s
LEFT JOIN canonical i ON i.record_id = s.input_applied_record_id
LEFT JOIN canonical z ON z.record_id = 'settlement:' || s.submission_id
LEFT JOIN canonical r
  ON r.record_tag = 'RunCompleted'
 AND r.run_id = json_extract(z.wire, '$.payload.runId')
WHERE s.thread_id = ?
ORDER BY s.queue_sequence;
```

Decode the canonical wires against the deployed record schema; JSON field presence alone is not validation. Require:

- Exactly 830 rows, queue sequences 1–830, and exact idempotency-key set `h0`…`h829`, in that order; no extra admissions. For index `i`, decode `input_json` to `turn h{i} tools={CYCLE[i % 3]}` with `CYCLE=[1,1,0]`; principal `bench`, agent `bench`, deployment `durable-bench`.
- Every submission is `state='settled'`, `settled_outcome='completed'`, has non-null finalization, and has no joined host. Every linked canonical input is `UserInputRecorded`, `kind='user'`, with the same submission, input, sequence, and `runId='run:' + submission_id`. Its record ID is `input:` plus the submission ID.
- Exactly one valid canonical `SubmissionSettled` per submission, ID and settlement ID `settlement:` plus the submission ID, matching receipt/submission/Run IDs, `outcome='completed'`, and result `done after K lookups`. The row's `settled_record_id` must match it. Reject budget-exhaustion/failed/aborted outcomes, even if there is a final-looking answer.
- Exactly one matching `RunCompleted`, with the same expected output and ordinary completion metadata; input precedes Run completion, which precedes settlement. Detect extra or duplicate terminal facts, not just missing joins. In particular, independently count `SubmissionSettled` and `RunCompleted` records for this fixture and require 830 of each.
- No live ownership for this thread and no work entries. Read these directly as supporting checks, not as substitutes for the canonical terminal facts:

```sql
SELECT COUNT(*) AS ownership
FROM effect_agent_submission_ownership o
JOIN effect_agent_submissions s USING (submission_id)
WHERE s.thread_id = ?;

SELECT COUNT(*) AS work
FROM effect_agent_work_entries WHERE thread_id = ?;
```

With the previously acknowledged 820-prefix accepted, the newly uncertain range is `h820`…`h829`. Checking the complete ordered key set plus terminal facts for all 830 is inexpensive at this size and avoids relying solely on the last row. Attempt count is corroborating evidence, not a completion criterion.

## Tardie: persisted method input, Turn settlement, final model reply

The Thread journal uses `actor='events'`. [sql-journal.ts](../../../third-party/node_modules/tardie/src/platform/shared/sql-journal.ts) persists the entire recorded envelope as JSON in `experimental_events.event`; `experimental_messages` indexes admissions. [receive](../../../third-party/node_modules/tardie/src/core/runtime/execution.ts) wraps the domain `TurnRequested` inside `MessageReceived.body` and stores original method metadata alongside it. Therefore a query for top-level `TurnRequested` alone misses these inputs.

[agentReply](../../../third-party/node_modules/tardie/src/agent/actor/methods.ts) reports completion only when the persisted inference projection has a completed turn and answer. [inferState/turnOutput](../../../third-party/node_modules/tardie/src/agent/atoms/durable/inference.ts) ties completed `TurnSettled.callId` to an inference `ModelCalled` for that turn and a `ModelReturned` with no further tool calls. These are the facts to inspect.

```sql
SELECT COUNT(*) AS n, MIN(seq) AS first_seq, MAX(seq) AS last_seq
FROM experimental_events WHERE actor = 'events';

SELECT id, seq FROM experimental_messages
WHERE actor = 'events' ORDER BY seq;

WITH normalized AS (
  SELECT seq, event AS wire,
    CASE
      WHEN json_extract(event, '$.event.type') = 'MessageReceived'
       AND json_type(event, '$.message.inReplyTo') IS NULL
      THEN json_extract(event, '$.event.body')
      ELSE json_extract(event, '$.event')
    END AS domain_event
  FROM experimental_events WHERE actor = 'events'
)
SELECT m.id, m.seq AS input_seq, i.wire AS input_record,
       s.seq AS settlement_seq, s.domain_event AS settlement,
       c.seq AS model_call_seq, c.domain_event AS model_call,
       r.seq AS model_return_seq, r.domain_event AS model_return
FROM experimental_messages m
JOIN normalized i ON i.seq = m.seq
LEFT JOIN normalized s
  ON json_extract(s.domain_event, '$.type') = 'TurnSettled'
 AND json_extract(s.domain_event, '$.turnId') = m.id
LEFT JOIN normalized c
  ON json_extract(c.domain_event, '$.type') = 'ModelCalled'
 AND json_extract(c.domain_event, '$.purpose') = 'inference'
 AND json_extract(c.domain_event, '$.turnId') = m.id
 AND json_extract(c.domain_event, '$.callId') = json_extract(s.domain_event, '$.callId')
LEFT JOIN normalized r
  ON json_extract(r.domain_event, '$.type') = 'ModelReturned'
 AND json_extract(r.domain_event, '$.purpose') = 'inference'
 AND json_extract(r.domain_event, '$.callId') = json_extract(s.domain_event, '$.callId')
WHERE m.actor = 'events'
ORDER BY m.seq;
```

Require exactly 208 indexed messages in order `h0`…`h207`, no extras, and a contiguous event journal (reported 2708 rows would mean seq 0…2707). For each message, validate its recorded schema, `message.id`, `message.invocation.method='message'`, original input text, and `MessageReceived.body` with `type='TurnRequested'`, matching `turnId`, source `user`, and invocation ref `{method:'message', id:'hN'}`. Require exactly one completed `TurnSettled`, matching final `ModelCalled`/`ModelReturned`, empty final `toolCalls`, and answer `done after K lookups`; input < model call < model return < settlement. The actual fixture emits the `callId` settlement variant; do not silently accept an unexpected alternative shape. Count all settlements independently to detect unmatched extras. For `h207`, the expected answer is `done after 1 lookups`.

For a cheap implementation, one ordered SELECT and linear JavaScript maps keyed by message/turn/call ID can perform the same joins without repeatedly evaluating JSON joins in SQLite. Bound the read and fail if it exceeds the bound; never accept a truncated prefix.

If stronger method-wait equivalence is required, export the fixed journal with `SELECT seq,event FROM experimental_events WHERE actor='events' ORDER BY seq` and fold the validated events through the vendor's pure inference projection outside the executing runtime. Follow [event-source.ts](../../../third-party/node_modules/tardie/src/core/runtime/event-source.ts): unwrap non-reply inbox bodies and convert stored `EffectRequested` records with `observeRequest` to the payload-free acceptance shape before passing core events to the inference reducer. Require no unsettled turn, no outstanding tool, no unreturned model call, and no pending effect. `EffectSettled` with a promise handle is not final completion; its eventual `PromiseSettled` must also exist. A replay here means pure event reduction only, never executing actor effects.

The checkpoint is an acceleration snapshot, not the current terminal authority. The [default checkpoint policy](../../../third-party/node_modules/tardie/src/core/runtime/execution.ts) is every 500 events when an eligible checkpoint can be produced. Position 2507 can legitimately lag a 2708-event journal; reading only that checkpoint misses 201 records. Full journal SELECTs are simpler for this one small proof and avoid checkpoint decoding/reconstruction.

The reported empty watchdog and null alarm are supportive, not completion proof. For a separate settled-state observation, read `tardie:watchdog:` and `tardie:scheduler:` entries and `getAlarm()` on **both Thread and Actor**; do not clear anything. Empty Thread watchdog state alone does not inspect the Actor or scheduler. A null alarm can also occur while a handler is running. Keep these observations distinct from terminal-prefix proof.

## Durable transcript check and exact expected hashes

Both original benches hash module-local `seen`, assigned when the model is called. It is the **last model input**, excluding that call's final assistant answer. Eviction or another fixture in the same module can invalidate this observation. Derive the candidate transcript from persisted records instead and verify the terminal answer separately.

Pinned: fetch canonical records in sequence order with the same published-archive fallback. The existing pure [promptFromCanonicalRecords](../../../../../packages/effect-agent/src/durable/RunJournal.ts) reconstructs the durable Prompt without executing effects. Apply the original bench's normalization: omit system messages, concatenate text parts, extract `lookup` parameter `n` into `calls`, and normalize tool results as in `yielded.ts`. Important: the first `ModelResponseRecorded` of each Run already contains evaluated instructions and user input; adding a second user message from `UserInputRecorded` would duplicate it. A small fixture-specific fold is possible only after rejecting compaction/alternate history paths and validating declaration/result pairing; the existing pure projection is the stronger option.

Tardie: unwrap `MessageReceived` as above, then fold [trajectoryState](../../../third-party/node_modules/tardie/src/agent/atoms/durable/trajectory.ts) over validated domain events in journal order. Normalize each trajectory message exactly as the original model: assistant `toolCalls[].input.n` becomes `calls`, and tool `text` is JSON-decoded once. Preserve ordering. Reject tool errors, unmatched calls/results, model failures, and unexpected compaction/correction events. The fixture's context window is 1e9 tokens and its tool results are below the clipping threshold; validate absence of compaction rather than assuming it. Large stored `EffectRequested` inputs are often only `InputDigest` values ([input-digest.ts](../../../third-party/node_modules/tardie/src/core/runtime/input-digest.ts)); there is no guaranteed full last prompt to read directly from that row.

For either reconstructed complete transcript, first validate the exact expected history from [plan.ts](../../../src/plan.ts): all user IDs/texts, `[1,1,0]` tool pattern, global sequential lookup numbers, exact `payload(n)` contents, and final answers. Then remove **only the proven final assistant answer for the last turn** to reproduce the legacy `seen` hash. Hash `JSON.stringify(messages.map(m => [m.role, m.text, m.calls ?? []]))`, UTF-8 SHA-256, first eight digest bytes in lowercase hex.

| Prefix | Tool results | Model calls | Complete messages | Last-model-input messages | Last-model-input hash | Complete-transcript hash |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| 208 | 139 | 347 | 694 | 693 | `ac4a4e755b4dc648` | `beffa35b944de283` |
| 830 | 554 | 1384 | 2768 | 2767 | `8f55f0fb51a9296b` | `69f13137f27c30c8` |

These expectations were independently reproduced locally from the deterministic plan, without running a bench or timing workload. They are not observed remote hashes. A hash derived only from a newly generated expected transcript proves nothing about storage; compute the actual side from the validated persisted records. These 64-bit hashes are fixture checks, not a substitute for schema/identity/terminal validation.

## Resume decision

Accept only an exact, stable persisted prefix with successful canonical terminals and no unresolved work; use the durable transcript comparison when asserting that model-visible history survived. Save the proof's object/class, generation/version, logical thread, captured tail, completed count, unresolved-work counts, hashes and source bundle hash in task evidence. Preserve the failed request as a lost ACK; never relabel it as a received successful response. Resume from 830 or 208 only after that proof passes. Missing facts, a partial final turn, a mismatched hash, an unexpected later admission, or a changing tail leave the fixture quarantined for explicit recovery.

Source bundle snapshots inspected locally:

- pinned `/private/tmp/cf-bench-8914-bundles/pinned/bench.mjs`: SHA-256 `c823d1745273673543621a777765ef3531a3392682dbc3bddea3981c5000defb`.
- tardie `/private/tmp/cf-bench-8914-bundles/tardie/bench.mjs`: SHA-256 `c8fbbb722e67090856e46048ab8197f49f197b64900aa4513c48adba4a909484`.

Confirm those still match the deployed original bench module before implementing the diagnostic. This memo does not establish either remote prefix as completed.
