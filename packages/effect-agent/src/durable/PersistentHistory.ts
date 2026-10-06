import { DateTime, Effect, Layer, Schema, Stream } from "effect";
import { Prompt } from "effect/ai";

import { type ThreadId, type RunId } from "../core/Identifiers.ts";
import { type RunCompleted as RunCompletedEvent } from "../core/RunEvent.ts";
import {
  ThreadHistory,
  ThreadHistoryError,
  type ThreadHistoryRun,
} from "../engine/ThreadHistory.ts";
import {
  BatchId,
  CanonicalBatch,
  DeploymentId,
  ModelCompleted,
  PersistedJson,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  RunCompleted,
  UserInputRecorded,
} from "./Records.ts";
import { promptFromCanonicalRecords } from "./RunJournal.ts";
import {
  ThreadIdentityRequest,
  SelectedThreadRead,
  type ThreadIdentity,
  ThreadMaterialization,
  ThreadStore,
  FencedAppendRequest,
  type ThreadStoreFailure,
} from "./ThreadStore.ts";

const HISTORY_EPOCH = Schema.decodeSync(ProducerEpoch)(0);
const HISTORY_DEPLOYMENT = Schema.decodeSync(DeploymentId)("persistent-history");
const batchId = Schema.decodeSync(BatchId);
const producer = Schema.decodeSync(ProducerId);
const recordId = Schema.decodeSync(RecordId);

const historyError = (
  threadId: ThreadId,
  reason: ThreadHistoryError["reason"],
  message: string,
  cause?: unknown,
) =>
  ThreadHistoryError.make({
    threadId,
    reason,
    message,
    ...(cause === undefined ? {} : { cause }),
  });

const storageError = (threadId: ThreadId, cause: ThreadStoreFailure) =>
  historyError(
    threadId,
    cause._tag === "AppendConflict"
      ? "conflict"
      : cause._tag === "FenceRejected"
        ? "fenced"
        : cause._tag === "ThreadNotMaterialized"
          ? "not-found"
          : "storage",
    cause._tag === "ThreadStoreError" ? cause.message : cause._tag,
    cause,
  );

/**
 * Provide retained history to the normal AgentRuntime entry points. Each successful Run appends
 * one three-record batch. Staging is private to that Run; interruption discards it. Epoch zero
 * and the loaded tail fence stale writers without replaying external execution. A storage failure
 * after append may leave the whole Run recorded. Adapter failpoints cover both durable mutations.
 */
export const layer = Layer.effect(
  ThreadHistory,
  Effect.gen(function* () {
    const store = yield* ThreadStore;

    const readHistory = Effect.fnUntraced(function* (base: ThreadIdentity) {
      const threadId = base.threadId;

      const selected = (tag: "LatestModelCompleted" | "DurableHistoryOwner") =>
        store
          .read(
            SelectedThreadRead.make({
              threadId,
              selection: { _tag: tag, throughSequence: base.tailSequence },
              page: { limit: 2 },
            }),
          )
          .pipe(
            Stream.take(3),
            Stream.runCollect,
            Effect.mapError((cause) => storageError(threadId, cause)),
          );

      if (
        base.admissions > 0 ||
        base.producerEpoch !== HISTORY_EPOCH ||
        (yield* selected("DurableHistoryOwner")).length > 0
      )
        return yield* historyError(
          threadId,
          "incompatible",
          "This Thread belongs to durable accepted work; use a separate history Thread",
        );
      if (base.tailSequence === 0) return Prompt.empty;
      const first = base.records[0]?.record.payload;

      if (
        base.records.length !== 1 ||
        first?._tag !== "UserInputRecorded" ||
        first.submissionId !== undefined
      )
        return yield* historyError(
          threadId,
          "incompatible",
          "This Thread has another canonical history owner",
        );
      const records = yield* selected("LatestModelCompleted");
      const latest = records[0];

      if (
        records.length !== 1 ||
        latest?.threadId !== threadId ||
        latest.sequence > base.tailSequence ||
        latest.record.payload._tag !== "ModelCompleted" ||
        latest.record.payload.history === undefined
      )
        return yield* historyError(
          threadId,
          "encoding",
          "The retained model-context snapshot is missing or invalid",
        );

      return yield* promptFromCanonicalRecords(records).pipe(
        Effect.mapError((cause) => historyError(threadId, "encoding", cause.message, cause)),
      );
    });

    const load = Effect.fnUntraced(function* (threadId: ThreadId) {
      const base = yield* store
        .readIdentity(ThreadIdentityRequest.make({ threadId }))
        .pipe(Effect.mapError((cause) => storageError(threadId, cause)));

      return yield* readHistory(base);
    });

    const open = Effect.fnUntraced(function* ({
      threadId,
      runId,
    }: {
      readonly threadId: ThreadId;
      readonly runId: RunId;
    }): Effect.fn.Return<ThreadHistoryRun, ThreadHistoryError> {
      const error = (reason: ThreadHistoryError["reason"], message: string, cause?: unknown) =>
        historyError(threadId, reason, message, cause);

      const persisted = (value: unknown) =>
        Schema.decodeUnknownEffect(PersistedJson)(value).pipe(
          Effect.mapError((cause) =>
            error("limit", "Run data exceeds canonical persistence bounds", cause),
          ),
        );

      yield* store
        .materialize(ThreadMaterialization.make({ threadId, producerEpoch: HISTORY_EPOCH }))
        .pipe(Effect.mapError((cause) => storageError(threadId, cause)));

      const base = yield* store
        .readIdentity(ThreadIdentityRequest.make({ threadId }))
        .pipe(Effect.mapError((cause) => storageError(threadId, cause)));

      const prompt = yield* readHistory(base);

      const expectedTailSequence = base.tailSequence;
      const expectedTailDigest = base.tailDigest;
      const initialPromptLength = prompt.content.length;

      const createdAt = yield* DateTime.now;

      const record = (id: string, payload: RecordEnvelope["payload"], timestamp = createdAt) =>
        RecordEnvelope.make({
          recordId: recordId(id),
          family: "thread",
          schemaVersion: 1,
          deploymentId: HISTORY_DEPLOYMENT,
          createdAt: timestamp,
          payload,
        });

      let input: RecordEnvelope | undefined;
      let messages: PersistedJson | undefined;
      let history: PersistedJson | undefined;
      let stagedSource: ReadonlyArray<Prompt.Message> = [];
      let encodedMessages: ReadonlyArray<Prompt.MessageEncoded> = [];

      return {
        prompt,
        stageInput: Effect.fnUntraced(function* (encodedInput: unknown) {
          input = record(
            `history-input:${runId}`,
            UserInputRecorded.make({
              kind: "user",
              runId,
              input: yield* persisted(encodedInput),
            }),
          );
        }),
        stageHistory: Effect.fnUntraced(function* ({
          source: next,
          modelContext,
        }: {
          readonly source: Prompt.Prompt;
          readonly modelContext: Prompt.Prompt;
        }) {
          const source = next.content.slice(initialPromptLength);
          let retained = 0;

          while (
            retained < source.length &&
            retained < stagedSource.length &&
            source[retained] === stagedSource[retained]
          ) {
            retained++;
          }

          // Normal engine updates append messages. Rewritten or shortened histories retain only
          // their unchanged prefix, so direct staging callers keep the same replacement behavior.
          const suffix = yield* Schema.encodeEffect(Prompt.Prompt)(
            Prompt.fromMessages(source.slice(retained)),
          ).pipe(
            Effect.mapError((cause) =>
              error("encoding", "Run history could not be encoded", cause),
            ),
          );

          const nextEncoded = [...encodedMessages.slice(0, retained), ...suffix.content];
          const nextMessages = yield* persisted({ content: nextEncoded });

          const nextHistory = yield* Schema.encodeEffect(Prompt.Prompt)(modelContext).pipe(
            Effect.flatMap(persisted),
            Effect.mapError((cause) =>
              error(
                "limit",
                "Retained model context exceeds persistence bounds; configure context compaction",
                cause,
              ),
            ),
          );

          // Aggregate validation stays at every history update: per-message limits would admit
          // oversized Runs and move failures past the next model or Tool call.
          messages = nextMessages;
          history = nextHistory;
          encodedMessages = nextEncoded;
          stagedSource = source;
        }),
        commit: Effect.fnUntraced(function* (completion: RunCompletedEvent) {
          if (input === undefined || messages === undefined || history === undefined) {
            return yield* error("encoding", "Run completed without its encoded input and history");
          }
          const output = yield* persisted(completion.output);

          const runDisposition =
            completion.runDisposition === undefined
              ? undefined
              : yield* persisted(completion.runDisposition);

          const completedAt = yield* DateTime.now;

          const modelCompleted = yield* ModelCompleted.makeEffect({
            runId,
            output,
            messages,
            history,
          }).pipe(
            Effect.mapError((cause) =>
              error("encoding", "Run history is not a canonical Prompt", cause),
            ),
          );

          yield* store
            .append(
              FencedAppendRequest.make({
                threadId,
                producerEpoch: HISTORY_EPOCH,
                expectedTailSequence,
                expectedTailDigest,
                batch: CanonicalBatch.make({
                  batchId: batchId(`history:${runId}`),
                  producerId: producer(`history:${runId}`),
                  records: [
                    input,
                    record(`history-output:${runId}`, modelCompleted, completedAt),
                    record(
                      `run-completed:${runId}`,
                      RunCompleted.make({
                        runId,
                        output,
                        ...(runDisposition === undefined ? {} : { runDisposition }),
                        ...(completion.finishReason === "budget-exhausted"
                          ? {
                              finishReason: completion.finishReason,
                              exhausted: completion.exhausted,
                            }
                          : {}),
                      }),
                      completedAt,
                    ),
                  ],
                }),
              }),
            )
            .pipe(Effect.mapError((cause) => storageError(threadId, cause)));
        }),
      };
    });

    return ThreadHistory.of({ retention: "on-success", load, open });
  }),
);
