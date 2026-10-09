import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import type { RunId, ThreadId } from "../core/Identifiers.ts";
import { IntegrityCheck, IntegrityReport, type IntegrityCheckName } from "./Admin.ts";
import { EMPTY_TAIL_DIGEST } from "./Digest.ts";
import { CanonicalSequence, type CanonicalRecordEnvelope, type RecordId } from "./Records.ts";
import { canonicalRunIds, reference, verifyRunContinuations } from "./RunContinuation.ts";
import {
  submissionInputRecordId,
  submissionSettlementRecordId,
  workerTerminalFromRecord,
  type SubmissionSnapshot,
} from "./SubmissionLedger.ts";
import {
  ThreadArchive,
  makeThreadImportProgress,
  prepareImportPage,
  finishThreadImport,
  rebuildImportedSubmission,
  verifyImportedReferences,
  verifyImportedSettlementOrder,
  ThreadImportRejected,
  ThreadImportReader,
} from "./ThreadImport.ts";
import { ThreadExport, ThreadStoreError, type ThreadCheckpoint } from "./ThreadStore.ts";

/** All reads share the export snapshot. No lifetime identity sets or materialization. */
export interface ThreadInvariantInput<E = ThreadStoreError, R = never> {
  readonly threadId: ThreadId;
  readonly pages: Stream.Stream<ThreadExport, E, R>;
  /** Complete directory, including settled rows, ordered by queueSequence. */
  readonly submissions: Stream.Stream<SubmissionSnapshot, E, R>;
  readonly runs: Stream.Stream<RunId, E, R>;
  readonly checkpoint?: ThreadCheckpoint;
  readonly checkpointsSupported?: boolean;
  readonly requireAllSettled?: boolean;
}

/** Indexed streaming verification; bounded Run evidence is the only retained selection. */
export const verifyThreadInvariants = Effect.fnUntraced(function* <E, R>(
  input: ThreadInvariantInput<E, R>,
) {
  const reader = yield* ThreadImportReader;
  const failures = new Map<IntegrityCheckName, string>();

  const fail = (name: IntegrityCheckName, detail: string) => {
    if (!failures.has(name)) failures.set(name, detail.slice(0, 4096));
  };

  const progress = makeThreadImportProgress();

  // Canonical pages, not the disposable Run directory, determine what must be recomputed.
  // Retain only one bounded selection; interleaved Runs may require another explicit read.
  let verifiedRun:
    | { readonly runId: RunId; readonly records: ReadonlyMap<RecordId, CanonicalRecordEnvelope> }
    | undefined;

  const readVerifiedRun = Effect.fnUntraced(function* (runId: RunId) {
    if (verifiedRun?.runId === runId) return verifiedRun.records;
    verifiedRun = undefined;
    const records = yield* reader.runRecords(runId);
    let after = 0;

    for (const entry of records) {
      if (
        entry.threadId !== input.threadId ||
        entry.sequence <= after ||
        entry.sequence > (progress.manifest?.tailSequence ?? 0) ||
        !canonicalRunIds(entry.record).includes(runId)
      )
        return yield* ThreadStoreError.make({
          operation: "verify Run continuation",
          message: "Run evidence has foreign ownership or invalid canonical ordering",
        });
      after = entry.sequence;
    }
    yield* verifyRunContinuations(records);
    const byId = new Map(records.map((entry) => [entry.record.recordId, entry]));

    verifiedRun = { runId, records: byId };

    return byId;
  });

  let lastInputQueue = -1;
  let checkpointDigest = input.checkpoint?.throughSequence === 0 ? EMPTY_TAIL_DIGEST : undefined;

  yield* Stream.runForEach(input.pages, (page) =>
    Effect.gen(function* () {
      if (page.threadId !== input.threadId)
        fail("record-identity", "Export belongs to another Thread");

      const prepared = yield* Schema.encodeEffect(ThreadExport)(page).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(ThreadArchive)),
        Effect.flatMap((archive) =>
          prepareImportPage(progress, archive, { allowExternalObligations: true }),
        ),
        Effect.match({
          onFailure: (e) => {
            for (const name of [
              "schema-round-trip",
              "sequence-contiguity",
              "digest-chain",
            ] as const)
              fail(name, e.message);

            return undefined;
          },
          onSuccess: (p) => p,
        }),
      );

      if (prepared === undefined) return;
      for (const batch of prepared.batches)
        if (batch.lastSequence === input.checkpoint?.throughSequence)
          checkpointDigest = batch.tailDigest;
      for (const entry of prepared.records) {
        yield* Effect.gen(function* () {
          for (const runId of canonicalRunIds(entry.record)) {
            const selected = (yield* readVerifiedRun(runId)).get(entry.record.recordId);

            if (
              selected === undefined ||
              selected.sequence !== entry.sequence ||
              selected.batchId !== entry.batchId ||
              (yield* reference(selected.record)).digest !== (yield* reference(entry.record)).digest
            )
              fail(
                "continuation-evidence",
                `Canonical record ${entry.record.recordId} is missing or differs from recomputed Run evidence`,
              );
          }
        }).pipe(
          Effect.match({
            onSuccess: () => undefined,
            onFailure: (e) => fail("continuation-evidence", e.message),
          }),
        );
        yield* verifyImportedReferences(entry).pipe(
          Effect.match({
            onSuccess: () => undefined,
            onFailure: (e) => fail("continuation-evidence", e.message),
          }),
        );
        const exact = yield* reader.record(entry.record.recordId);

        if (
          exact === undefined ||
          exact.sequence !== entry.sequence ||
          exact.threadId !== entry.threadId ||
          exact.batchId !== entry.batchId
        )
          fail(
            "record-identity",
            `Record ${entry.record.recordId} has conflicting indexed identity`,
          );
        const payload = entry.record.payload;

        if (payload._tag === "UserInputRecorded" && payload.submissionId !== undefined) {
          const admission = yield* reader.admission(payload.submissionId);

          if (
            admission === undefined ||
            entry.record.recordId !== submissionInputRecordId(payload.submissionId)
          )
            fail("ledger-canonical-agreement", "Canonical input has no exact immutable admission");
          else if (admission.queueSequence <= lastInputQueue)
            fail("fifo-input-order", "Canonical inputs violate immutable admission order");
          else lastInputQueue = admission.queueSequence;
        }
        if (payload._tag === "SubmissionSettled") {
          if (entry.record.recordId !== submissionSettlementRecordId(payload.submissionId))
            fail("terminal-uniqueness", "Settlement has a noncanonical identity");
          if ((yield* reader.admission(payload.submissionId)) === undefined)
            fail("ledger-canonical-agreement", "Settlement has no immutable admission");
        }
      }
    }),
  );
  yield* finishThreadImport(progress).pipe(
    Effect.match({
      onSuccess: () => undefined,
      onFailure: (e) => {
        fail("sequence-contiguity", e.message);
        fail("digest-chain", e.message);
      },
    }),
  );
  yield* Stream.runForEach(input.runs, (runId) =>
    readVerifiedRun(runId).pipe(
      Effect.match({
        onSuccess: () => undefined,
        onFailure: (e) => fail("continuation-evidence", e.message),
      }),
    ),
  );
  verifiedRun = undefined;
  let submissionCount = 0;
  const workerSeal = progress.manifest?.workerSeal;
  let sealTerminalEvidence = workerSeal?.terminal === undefined;
  let lastQueue = -1;

  yield* Stream.runForEach(input.submissions, (row) =>
    Effect.gen(function* () {
      submissionCount++;
      if (row.threadId !== input.threadId || row.queueSequence <= lastQueue)
        fail("record-identity", "Admission directory is not unique and ordered");
      lastQueue = row.queueSequence;
      const admission = yield* reader.admission(row.submissionId);

      if (admission === undefined) {
        fail("ledger-canonical-agreement", "Ledger row has no immutable admission");

        return;
      }
      yield* rebuildImportedSubmission(admission).pipe(
        Effect.match({
          onFailure: (e) => {
            fail("ledger-canonical-agreement", e.message);

            return undefined;
          },
          onSuccess: (s) => s,
        }),
      );
      const settlement = yield* reader.record(submissionSettlementRecordId(row.submissionId));

      if (
        row.state === "settled" &&
        settlement !== undefined &&
        workerTerminalFromRecord(row, settlement.record) === workerSeal?.terminal
      )
        sealTerminalEvidence = true;
      const payload = settlement?.record.payload;

      if (row.state === "settled" && payload?._tag !== "SubmissionSettled")
        fail("terminal-uniqueness", "Settled ledger row has no canonical settlement");
      if (
        row.state === "settled" &&
        (payload?._tag !== "SubmissionSettled" || row.settledOutcome !== payload.outcome)
      )
        fail(
          "ledger-canonical-agreement",
          "Ledger input or terminal projection differs from canonical evidence",
        );
      if (input.requireAllSettled && row.state !== "settled")
        fail("all-settled", `Submission ${row.submissionId} is ${row.state}`);
      yield* verifyImportedSettlementOrder(row).pipe(
        Effect.catchIf(Schema.is(ThreadImportRejected), (e) =>
          Effect.sync(() => fail("fifo-settlement-order", e.message)),
        ),
      );
    }),
  );
  if (submissionCount !== progress.counts.admissions)
    fail("ledger-canonical-agreement", "Admission directory differs from the captured fact count");
  if (!sealTerminalEvidence)
    fail("ledger-canonical-agreement", "Worker seal terminal has no exact settled worker evidence");
  if (input.requireAllSettled && submissionCount === 0)
    fail("all-settled", "No submissions are known");
  if (
    input.checkpoint !== undefined &&
    (input.checkpoint.threadId !== input.threadId ||
      checkpointDigest !== input.checkpoint.tailDigest)
  )
    fail("checkpoint-binding", "Checkpoint is not bound to a recomputed batch tail");

  const names: Array<IntegrityCheckName> = [
    "schema-round-trip",
    "record-identity",
    "sequence-contiguity",
    "digest-chain",
    "continuation-evidence",
    "fifo-input-order",
    "fifo-settlement-order",
    "terminal-uniqueness",
    "ledger-canonical-agreement",
    "checkpoint-binding",
  ];

  if (input.requireAllSettled) names.push("all-settled");

  const checks = names.map((name) =>
    IntegrityCheck.make({
      name,
      status: failures.has(name)
        ? "failed"
        : name === "checkpoint-binding" &&
            input.checkpoint === undefined &&
            !input.checkpointsSupported
          ? "skipped"
          : "passed",
      ...(failures.has(name) ? { detail: failures.get(name) } : {}),
    }),
  );

  return IntegrityReport.make({
    threadId: input.threadId,
    tailSequence: progress.manifest?.tailSequence ?? CanonicalSequence.make(0),
    recordCount: progress.sequence,
    submissionCount,
    checks,
    ok: failures.size === 0,
  });
});
