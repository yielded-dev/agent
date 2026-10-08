import { RunId } from "@yielded/agent/identifiers";
import { Digest, RecordEnvelope, RecordId } from "@yielded/agent/records";
import { Effect, Schema } from "effect";

import { CYCLE, history } from "../../../src/plan.ts";
import { BenchError, Identity } from "./contracts.ts";

const MAX_ROWS = 20_000;
const Positive = Schema.Natural.check(Schema.isGreaterThan(0));
const Prefix = Positive.check(Schema.isLessThanOrEqualTo(MAX_ROWS));

const YieldedTail = Schema.Struct({
  thread_id: Schema.NonEmptyString,
  tail_sequence: Positive,
  tail_digest: Digest,
});

const TardieTail = Schema.Struct({
  n: Prefix,
  first_seq: Schema.Literal(0),
  last_seq: Schema.Natural,
});

const CommonProof = {
  identity: Identity,
  completed: Prefix,
  keys: Schema.Array(Schema.NonEmptyString).check(Schema.isMaxLength(MAX_ROWS)),
  success: Schema.Literal(true),
  tailStable: Schema.Literal(true),
};

/** Persisted terminal-prefix evidence only; full history is checked by the final fingerprint. */
export const SeedProof = Schema.Union([
  Schema.Struct({
    ...CommonProof,
    role: Schema.Literal("pinned"),
    basis: Schema.Literal("yielded-canonical-terminals"),
    tail: YieldedTail,
    terminalChecks: Schema.Struct({
      inputs: Prefix,
      runCompleted: Prefix,
      submissionSettled: Prefix,
      ownership: Schema.Literal(0),
      workEntries: Schema.Literal(0),
    }),
  }),
  Schema.Struct({
    ...CommonProof,
    role: Schema.Literal("tardie"),
    basis: Schema.Literal("tardie-journal-terminals"),
    tail: TardieTail,
    terminalChecks: Schema.Struct({
      inputs: Prefix,
      turnSettled: Prefix,
      finalModelReturned: Prefix,
    }),
  }),
]).check(
  Schema.makeFilter(
    (proof) =>
      proof.keys.length === proof.completed &&
      proof.keys.every((key, index) => key === `h${index}`) &&
      proof.terminalChecks.inputs === proof.completed &&
      (proof.role === "pinned"
        ? proof.terminalChecks.runCompleted === proof.completed &&
          proof.terminalChecks.submissionSettled === proof.completed
        : proof.terminalChecks.turnSettled === proof.completed &&
          proof.terminalChecks.finalModelReturned === proof.completed &&
          proof.tail.last_seq === proof.tail.n - 1),
    { title: "Seed proof counts and ordered keys describe the same completed prefix" },
  ),
);

export type SeedProof = typeof SeedProof.Type;

// SELECT * contains additional ledger columns; only the proof's required columns are decoded.
const Submission = Schema.Struct({
  submission_id: Schema.NonEmptyString,
  thread_id: Schema.NonEmptyString,
  queue_sequence: Positive,
  principal: Schema.Literal("bench"),
  idempotency_key: Schema.NonEmptyString,
  agent_id: Schema.Literal("bench"),
  deployment_id: Schema.Literal("durable-bench"),
  input_json: Schema.fromJsonString(Schema.String),
  receipt_id: Schema.NonEmptyString,
  state: Schema.Literal("settled"),
  settled_outcome: Schema.Literal("completed"),
  settled_record_id: RecordId,
  finalized_at: Schema.DateTimeUtcFromString,
  input_applied_record_id: RecordId,
  input_applied_sequence: Positive,
  joined_host_submission_id: Schema.Null,
});

const CanonicalRow = Schema.Struct({
  sequence: Positive,
  record_id: RecordId,
  record_tag: Schema.Literals(["UserInputRecorded", "SubmissionSettled", "RunCompleted"]),
  // SqlThreadNativeReads.canonicalRecordMetadata JSON-encodes scalar identifier columns.
  run_id: Schema.fromJsonString(RunId),
  record_json: Schema.fromJsonString(RecordEnvelope),
});

const YieldedRaw = Schema.Struct({
  kind: Schema.Literal("yielded"),
  identity: Identity,
  threads: Schema.Tuple([YieldedTail]),
  submissions: Schema.Array(Submission).check(Schema.isMaxLength(MAX_ROWS)),
  records: Schema.Array(CanonicalRow).check(Schema.isMaxLength(MAX_ROWS)),
  ownership: Schema.Literal(0),
  workEntries: Schema.Literal(0),
  tailAfter: Schema.Tuple([YieldedTail]),
});

const TardieRaw = Schema.Struct({
  kind: Schema.Literal("tardie"),
  identity: Identity,
  messages: Schema.Array(Schema.Struct({ id: Schema.NonEmptyString, seq: Schema.Natural })).check(
    Schema.isMaxLength(MAX_ROWS),
  ),
  events: Schema.Array(Schema.Struct({ seq: Schema.Natural, event: Schema.String })).check(
    Schema.isMaxLength(MAX_ROWS),
  ),
  tailBefore: Schema.Tuple([TardieTail]),
  tailAfter: Schema.Tuple([TardieTail]),
});

// Fixture wire subsets of tardie@0.44.0's agent/contracts/events.ts and
// core/{services/journal,actor/message}.ts. Keep its separate Effect runtime out
// of this client. Runtime bookkeeping events are not folded into a new projection.
const Recorded = Schema.Struct({
  event: Schema.Record(Schema.String, Schema.Json),
  message: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)),
  recordedAt: Schema.optionalKey(Schema.Natural),
});

const EventType = Schema.Struct({ type: Schema.NonEmptyString });

const InputRecord = Schema.Struct({
  event: Schema.Struct({
    type: Schema.Literal("MessageReceived"),
    body: Schema.Struct({
      type: Schema.Literal("TurnRequested"),
      turnId: Schema.NonEmptyString,
      text: Schema.String,
      source: Schema.Literal("user"),
      invocationRef: Schema.Struct({
        method: Schema.Literal("message"),
        id: Schema.NonEmptyString,
      }),
    }),
  }),
  message: Schema.Struct({
    id: Schema.NonEmptyString,
    from: Schema.Struct({ kind: Schema.Literal("external"), id: Schema.NonEmptyString }),
    invocation: Schema.Struct({
      method: Schema.Literal("message"),
      input: Schema.Struct({ text: Schema.String }),
    }),
  }),
  recordedAt: Schema.optionalKey(Schema.Natural),
});

const ModelCalled = Schema.Struct({
  type: Schema.Literal("ModelCalled"),
  purpose: Schema.Literal("inference"),
  model: Schema.Struct({
    provider: Schema.Literal("scripted"),
    model_id: Schema.Literal("scripted-1"),
  }),
  contextWindowTokens: Schema.Literal(1e9),
  turnId: Schema.NonEmptyString,
  callId: Schema.NonEmptyString,
});

const ModelReturned = Schema.Struct({
  type: Schema.Literal("ModelReturned"),
  purpose: Schema.Literal("inference"),
  callId: Schema.NonEmptyString,
  text: Schema.String,
  toolCalls: Schema.Array(
    Schema.Struct({
      callId: Schema.String,
      name: Schema.String,
      input: Schema.Json,
      providerId: Schema.String,
    }),
  ),
  reasoning: Schema.optionalKey(Schema.String),
  continuation: Schema.optionalKey(
    Schema.Struct({
      provider: Schema.String,
      protocol: Schema.String,
      model: Schema.String,
      payload: Schema.Json,
    }),
  ),
  usage: Schema.optionalKey(
    Schema.Struct({
      input: Schema.optionalKey(Schema.Natural),
      output: Schema.optionalKey(Schema.Natural),
      usd: Schema.NullOr(Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))),
    }),
  ),
});

const TurnSettled = Schema.Struct({
  type: Schema.Literal("TurnSettled"),
  turnId: Schema.NonEmptyString,
  outcome: Schema.Literal("completed"),
  callId: Schema.NonEmptyString,
});

const invalid = (message: string) => new BenchError({ message: `Seed proof: ${message}` });
const malformed = (label: string) => () => invalid(`${label} does not match its wire schema`);
const decodeYielded = Schema.decodeUnknownEffect(YieldedRaw);
const decodeTardie = Schema.decodeUnknownEffect(TardieRaw);

const decodeRecorded = Schema.decodeUnknownEffect(Schema.fromJsonString(Recorded), {
  onExcessProperty: "error",
});

const decodeEventType = Schema.decodeUnknownEffect(EventType);
const decodeInput = Schema.decodeUnknownEffect(InputRecord, { onExcessProperty: "error" });
const decodeCall = Schema.decodeUnknownEffect(ModelCalled, { onExcessProperty: "error" });
const decodeReturn = Schema.decodeUnknownEffect(ModelReturned, { onExcessProperty: "error" });
const decodeSettlement = Schema.decodeUnknownEffect(TurnSettled, { onExcessProperty: "error" });

const validateYielded = Effect.fnUntraced(function* (
  raw: unknown,
  expected: number,
): Effect.fn.Return<SeedProof, BenchError> {
  const proof = yield* decodeYielded(raw).pipe(Effect.mapError(malformed("Yielded proof")));
  const tail = proof.threads[0];
  const after = proof.tailAfter[0];

  if (
    tail.thread_id !== after.thread_id ||
    tail.tail_sequence !== after.tail_sequence ||
    tail.tail_digest !== after.tail_digest ||
    proof.submissions.length !== expected ||
    proof.records.length !== expected * 3
  ) {
    return yield* invalid("Yielded tail changed or admission/record counts differ from the prefix");
  }

  const inputs = new Map<string, typeof CanonicalRow.Type>();
  const completions = new Map<string, typeof CanonicalRow.Type>();
  const settlements = new Map<string, typeof CanonicalRow.Type>();
  const recordIds = new Set<string>();
  let previousSequence = 0;

  for (const row of proof.records) {
    const wire = row.record_json;
    const payload = wire.payload;

    if (
      row.sequence <= previousSequence ||
      row.sequence > tail.tail_sequence ||
      recordIds.has(row.record_id) ||
      row.record_id !== wire.recordId ||
      row.record_tag !== payload._tag ||
      wire.deploymentId !== "durable-bench" ||
      !("runId" in payload) ||
      row.run_id !== payload.runId
    ) {
      return yield* invalid("Yielded canonical identity, ordering, or SQL metadata mismatch");
    }

    previousSequence = row.sequence;
    recordIds.add(row.record_id);

    if (payload._tag === "UserInputRecorded" && payload.submissionId !== undefined) {
      if (inputs.has(payload.submissionId)) return yield* invalid("duplicate canonical input");
      inputs.set(payload.submissionId, row);
    } else if (payload._tag === "RunCompleted") {
      if (completions.has(payload.runId)) return yield* invalid("duplicate Run completion");
      completions.set(payload.runId, row);
    } else if (payload._tag === "SubmissionSettled") {
      if (settlements.has(payload.submissionId)) return yield* invalid("duplicate settlement");
      settlements.set(payload.submissionId, row);
    } else {
      return yield* invalid("unexpected canonical fact in the terminal proof");
    }
  }

  if (inputs.size !== expected || completions.size !== expected || settlements.size !== expected) {
    return yield* invalid("Yielded canonical input/terminal cardinalities differ from the prefix");
  }

  const plan = history(0, expected);
  const receiptIds = new Set<string>();
  let previousSettlement = 0;

  for (const [index, turn] of plan.entries()) {
    const row = proof.submissions[index];

    if (
      row === undefined ||
      row.queue_sequence !== index + 1 ||
      row.idempotency_key !== turn.id ||
      row.thread_id !== tail.thread_id ||
      row.input_json !== turn.text ||
      receiptIds.has(row.receipt_id)
    ) {
      return yield* invalid(`Yielded admission ${turn.id} is not the exact ordered input`);
    }

    receiptIds.add(row.receipt_id);

    const runId = `run:${row.submission_id}`;
    const input = inputs.get(row.submission_id);
    const completed = completions.get(runId);
    const settled = settlements.get(row.submission_id);
    const answer = `done after ${CYCLE[index % CYCLE.length]} lookups`;

    if (
      input === undefined ||
      completed === undefined ||
      settled === undefined ||
      input.record_json.payload._tag !== "UserInputRecorded" ||
      completed.record_json.payload._tag !== "RunCompleted" ||
      settled.record_json.payload._tag !== "SubmissionSettled"
    ) {
      return yield* invalid(`Yielded ${turn.id} lacks its canonical input or terminal`);
    }

    const inputFact = input.record_json.payload;
    const completion = completed.record_json.payload;
    const settlement = settled.record_json.payload;

    if (
      input.record_id !== `input:${row.submission_id}` ||
      row.input_applied_record_id !== input.record_id ||
      row.input_applied_sequence !== input.sequence ||
      inputFact.kind !== "user" ||
      inputFact.runId !== runId ||
      inputFact.input !== turn.text ||
      inputFact.messageAdmission !== undefined ||
      completed.record_id !== `run-completed:${runId}` ||
      completion.output !== answer ||
      completion.finishReason !== undefined ||
      completion.exhausted !== undefined ||
      completion.runDisposition !== undefined ||
      settled.record_id !== `settlement:${row.submission_id}` ||
      row.settled_record_id !== settled.record_id ||
      settlement.settlementId !== `settlement:${row.submission_id}` ||
      settlement.receiptId !== row.receipt_id ||
      settlement.runId !== runId ||
      settlement.outcome !== "completed" ||
      settlement.result !== answer ||
      settlement.finishReason !== undefined ||
      settlement.exhausted !== undefined ||
      settlement.runDisposition !== undefined ||
      settlement.policyLimit !== undefined ||
      !(previousSettlement < input.sequence && input.sequence < completed.sequence) ||
      completed.sequence >= settled.sequence
    ) {
      return yield* invalid(`Yielded ${turn.id} has inconsistent or nonordinary completion`);
    }

    previousSettlement = settled.sequence;
  }

  return {
    role: "pinned",
    basis: "yielded-canonical-terminals",
    identity: proof.identity,
    completed: expected,
    keys: plan.map((turn) => turn.id),
    success: true,
    tailStable: true,
    tail,
    terminalChecks: {
      inputs: inputs.size,
      runCompleted: completions.size,
      submissionSettled: settlements.size,
      ownership: proof.ownership,
      workEntries: proof.workEntries,
    },
  };
});

const validateTardie = Effect.fnUntraced(function* (
  raw: unknown,
  expected: number,
): Effect.fn.Return<SeedProof, BenchError> {
  const proof = yield* decodeTardie(raw).pipe(Effect.mapError(malformed("Tardie proof")));
  const tail = proof.tailBefore[0];
  const after = proof.tailAfter[0];

  if (
    tail.n !== after.n ||
    tail.first_seq !== after.first_seq ||
    tail.last_seq !== after.last_seq ||
    tail.last_seq !== tail.n - 1 ||
    proof.events.length !== tail.n ||
    proof.messages.length !== expected
  ) {
    return yield* invalid("Tardie tail changed, journal is incomplete, or message count differs");
  }

  const inputs = new Map<string, { seq: number; record: typeof InputRecord.Type }>();
  const calls = new Map<string, { seq: number; event: typeof ModelCalled.Type }>();
  const returns = new Map<string, { seq: number; event: typeof ModelReturned.Type }>();
  const settlements = new Map<string, { seq: number; event: typeof TurnSettled.Type }>();
  let finalReturns = 0;

  for (const [index, row] of proof.events.entries()) {
    if (row.seq !== index) return yield* invalid("Tardie journal is not contiguous from zero");

    const record = yield* decodeRecorded(row.event).pipe(
      Effect.mapError(malformed(`Tardie event ${index}`)),
    );

    const { type } = yield* decodeEventType(record.event).pipe(
      Effect.mapError(malformed(`Tardie event ${index} discriminator`)),
    );

    if (type === "MessageReceived") {
      const input = yield* decodeInput(record).pipe(
        Effect.mapError(malformed(`Tardie input at ${index}`)),
      );

      if (inputs.has(input.message.id)) return yield* invalid("duplicate Tardie message");
      inputs.set(input.message.id, { seq: row.seq, record: input });
    } else if (type === "ModelCalled") {
      const call = yield* decodeCall(record.event).pipe(
        Effect.mapError(malformed(`Tardie model call at ${index}`)),
      );

      if (calls.has(call.callId)) return yield* invalid("duplicate Tardie model call");
      calls.set(call.callId, { seq: row.seq, event: call });
    } else if (type === "ModelReturned") {
      const returned = yield* decodeReturn(record.event).pipe(
        Effect.mapError(malformed(`Tardie model return at ${index}`)),
      );

      if (returns.has(returned.callId)) return yield* invalid("duplicate Tardie model return");
      returns.set(returned.callId, { seq: row.seq, event: returned });
      if (returned.toolCalls.length === 0) finalReturns++;
    } else if (type === "TurnSettled") {
      const settled = yield* decodeSettlement(record.event).pipe(
        Effect.mapError(malformed(`Tardie turn settlement at ${index}`)),
      );

      if (settlements.has(settled.turnId)) return yield* invalid("duplicate Tardie turn terminal");
      settlements.set(settled.turnId, { seq: row.seq, event: settled });
    } else if (type === "TurnRequested") {
      return yield* invalid("unindexed Tardie turn request outside MessageReceived");
    }
  }

  if (
    inputs.size !== expected ||
    settlements.size !== expected ||
    finalReturns !== expected ||
    calls.size !== returns.size
  ) {
    return yield* invalid("Tardie has missing/extra inputs, terminals, or unmatched model calls");
  }

  const plan = history(0, expected);
  let previousSettlement = -1;

  for (const [index, turn] of plan.entries()) {
    const message = proof.messages[index];
    const input = inputs.get(turn.id);
    const settlement = settlements.get(turn.id);

    if (
      message === undefined ||
      message.id !== turn.id ||
      input === undefined ||
      message.seq !== input.seq ||
      input.record.event.body.turnId !== turn.id ||
      input.record.event.body.invocationRef.id !== turn.id ||
      input.record.event.body.text !== turn.text ||
      input.record.message.invocation.input.text !== turn.text ||
      settlement === undefined
    ) {
      return yield* invalid(
        `Tardie ${turn.id} lacks its exact indexed input or completed terminal`,
      );
    }

    const call = calls.get(settlement.event.callId);
    const returned = returns.get(settlement.event.callId);

    if (
      call === undefined ||
      returned === undefined ||
      call.event.turnId !== turn.id ||
      returned.event.toolCalls.length !== 0 ||
      returned.event.text !== `done after ${CYCLE[index % CYCLE.length]} lookups` ||
      !(previousSettlement < input.seq && input.seq < call.seq) ||
      !(call.seq < returned.seq && returned.seq < settlement.seq)
    ) {
      return yield* invalid(`Tardie ${turn.id} does not link to its ordinary final model answer`);
    }

    previousSettlement = settlement.seq;
  }

  // Even nonterminal model calls must belong to a proven turn and have returned
  // before its settlement. This is a join over facts, not a runtime/prompt fold.
  for (const [callId, call] of calls) {
    const input = inputs.get(call.event.turnId);
    const settled = settlements.get(call.event.turnId);
    const returned = returns.get(callId);

    if (
      input === undefined ||
      settled === undefined ||
      returned === undefined ||
      !(input.seq < call.seq && call.seq < returned.seq && returned.seq < settled.seq)
    ) {
      return yield* invalid("Tardie has an unmatched or out-of-turn model call/return");
    }
  }

  return {
    role: "tardie",
    basis: "tardie-journal-terminals",
    identity: proof.identity,
    completed: expected,
    keys: plan.map((turn) => turn.id),
    success: true,
    tailStable: true,
    tail,
    terminalChecks: {
      inputs: inputs.size,
      turnSettled: settlements.size,
      finalModelReturned: finalReturns,
    },
  };
});

/** Decode the fixed seedProof RPC payload; any missing or inconsistent evidence fails closed. */
export const validateSeedProof = Effect.fnUntraced(function* (
  raw: unknown,
  role: "pinned" | "tardie",
  expected: number,
): Effect.fn.Return<SeedProof, BenchError> {
  const count = yield* Schema.decodeUnknownEffect(Prefix)(expected).pipe(
    Effect.mapError(malformed("expected prefix")),
  );

  const checkedRole = yield* Schema.decodeUnknownEffect(Schema.Literals(["pinned", "tardie"]))(
    role,
  ).pipe(Effect.mapError(malformed("proof role")));

  return yield* checkedRole === "pinned" ? validateYielded(raw, count) : validateTardie(raw, count);
});
