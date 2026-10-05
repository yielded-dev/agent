import { type ThreadId } from "@yielded/agent/identifiers";
import * as MemoryNamespace from "@yielded/agent/memory-namespace";
import type { MemoryKey } from "@yielded/agent/memory-store";
import { MemoryReader } from "@yielded/agent/memory-store";
import type { CanonicalRecordEnvelope } from "@yielded/agent/records";
import { CanonicalSequence } from "@yielded/agent/records";
import { ThreadRead, ThreadStore, ThreadTailRequest } from "@yielded/agent/thread-store";
import { Effect, Schema, Stream } from "effect";

import { EvaluationError, type RunContinuationEvidence } from "./contracts.ts";

export const notesNamespace = MemoryNamespace.define({
  name: "example/context-continuity-notes",
  version: 1,
  identity: Schema.Struct({ threadId: Schema.String }),
});

export const readLog = Effect.fn("ContextContinuity.readLog")(function* (threadId: ThreadId) {
  const store = yield* ThreadStore;
  const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));
  const records: Array<CanonicalRecordEnvelope> = [];
  let cursor = 0;

  while (cursor < tail.tailSequence) {
    const limit = Math.min(64, tail.tailSequence - cursor);

    const page = yield* store
      .read(
        ThreadRead.make({
          threadId,
          afterSequence: yield* Schema.decodeEffect(CanonicalSequence)(cursor),
          limit,
        }),
      )
      .pipe(Stream.runCollect);

    if (
      page.length !== limit ||
      page.some((record, index) => record.sequence !== cursor + index + 1)
    )
      return yield* EvaluationError.make({
        stage: "evidence",
        message: "Canonical evidence was not contiguous",
      });
    records.push(...page);
    cursor += page.length;
  }

  return records;
});

export const readNotes = Effect.fn("ContextContinuity.readNotes")(function* (key: MemoryKey) {
  const reader = yield* MemoryReader;
  const document = yield* reader.get(key);

  if (document?._tag === "WithdrawnMemoryDocument")
    return yield* EvaluationError.make({
      stage: "notes",
      message: "Evaluation notes were unexpectedly withdrawn",
    });

  return { revision: document?.source.revision ?? null, text: document?.content.text ?? "" };
});

/** Inspect the oracle already captured for verification, outside any measured operation. */
export const continuationEvidence = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
): RunContinuationEvidence => {
  const entry = records.findLast(({ record }) => record.payload._tag === "RunContinuation");
  const cursor = entry?.record.payload;

  return entry !== undefined && cursor?._tag === "RunContinuation"
    ? {
        status: "present",
        sequence: entry.sequence,
        runId: cursor.runId,
        revision: cursor.revision,
        recordBytes: cursor.recordBytes,
        turnBytes: cursor.turnBytes,
      }
    : { status: "missing" };
};
