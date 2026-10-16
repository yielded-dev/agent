import { type RunId, type ThreadId } from "@yielded/agent/identifiers";
import * as MemoryNamespace from "@yielded/agent/memory-namespace";
import type { MemoryKey } from "@yielded/agent/memory-store";
import { MemoryReader } from "@yielded/agent/memory-store";
import type { CanonicalRecordEnvelope } from "@yielded/agent/records";
import { CanonicalSequence } from "@yielded/agent/records";
import {
  ThreadRead,
  ThreadStore,
  ThreadTailRequest,
  type ExecutionCheckpoint,
} from "@yielded/agent/thread-store";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

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

/** Read current progress directly; completed Runs normally have no execution checkpoint. */
export const continuationEvidence = Effect.fn("ContextContinuity.continuationEvidence")(function* (
  threadId: ThreadId,
  runId: RunId | undefined,
) {
  if (runId === undefined) return { status: "missing" } satisfies RunContinuationEvidence;
  const store = yield* ThreadStore;
  const execution = yield* store.readExecution({ threadId, runId });

  return Option.isSome(execution)
    ? ({ status: "present", checkpoint: execution.value } satisfies RunContinuationEvidence)
    : ({ status: "missing" } satisfies RunContinuationEvidence);
});

export const originalExecutionFacts = (execution: ExecutionCheckpoint) =>
  execution.operational.filter(
    ({ payload }) => payload._tag === "RunStarted" || payload._tag === "RunContextRecorded",
  );
