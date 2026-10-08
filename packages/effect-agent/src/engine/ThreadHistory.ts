import type { Prompt } from "effect/ai";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { ThreadId, type RunId } from "../core/Identifiers.ts";
import { type RunCompleted } from "../core/RunEvent.ts";
import {
  Store as ConversationStore,
  layerMemory,
  toPrompt,
  type ThreadError,
  type ThreadMessage,
} from "../core/Thread.ts";
import type { ContextCompactionState } from "./internal/compaction.ts";

/** Process-local view state without Run-local guards; official Thread messages remain append-only. */
export type RetainedCompaction = Pick<
  ContextCompactionState,
  "replacement" | "clearedThrough" | "nativeWindows"
>;

// The first immutable record owns the sidecar lifetime, including scoped Store eviction.
const retainedCompactions = new WeakMap<ThreadMessage, RetainedCompaction>();
const closedRuns = new WeakMap<ThreadMessage, ReadonlySet<RunId>>();

/** A history adapter rejected a read, staged value, or completed Run commit. */
export class ThreadHistoryError extends Schema.TaggedError<ThreadHistoryError>()(
  "ThreadHistoryError",
  {
    threadId: ThreadId,
    reason: Schema.Literals([
      "not-found",
      "conflict",
      "fenced",
      "incompatible",
      "limit",
      "encoding",
      "storage",
    ]),
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/**
 * One Run's history owner. Incremental adapters retain each stageHistory update; on-success
 * adapters stage privately until commit. Before commit, the interpreter
 * finishes run-owned work and closes resources acquired for that Run, then validates the result
 * for run/start.await. Shared model, provider, and client services supplied by an enclosing
 * application Layer remain owned by that application's Scope and may outlive multiple Runs.
 * Commit must succeed before RunCompleted becomes observable. Schema-encoded input is opaque
 * here; the adapter owns its persistence boundary. A failed commit may have reached storage;
 * callers must inspect history before retrying external execution.
 */
export interface ThreadHistoryRun {
  readonly prompt: Prompt.Prompt;
  readonly compaction?: RetainedCompaction | undefined;
  /** Native coverage requires terminal prior Runs, not merely a currently settled Tool batch. */
  readonly validateNativeCompaction?:
    | ((through: number) => Effect.Effect<void, ThreadHistoryError>)
    | undefined;
  /** Infallible local lifecycle evidence after execution Scope closure, including interruption. */
  readonly end?: Effect.Effect<void> | undefined;
  readonly stageCompaction?:
    | ((state: RetainedCompaction) => Effect.Effect<void, ThreadHistoryError>)
    | undefined;
  readonly stageInput: (input: unknown) => Effect.Effect<void, ThreadHistoryError>;
  readonly stageHistory: (history: {
    /** Exact source messages; compaction changes only the model view. */
    readonly source: Prompt.Prompt;
    /** Retained context after native compaction, excluding transient recall. */
    readonly modelContext: Prompt.Prompt;
  }) => Effect.Effect<void, ThreadHistoryError>;
  readonly commit: (completed: RunCompleted) => Effect.Effect<void, ThreadHistoryError>;
}

/**
 * History shared by Runs with the same Thread ID. The default layer retains native messages
 * incrementally in memory for its application Scope, including completed updates before a failure.
 * PersistentHistory.layer from @yielded/agent/persistent-history instead commits successful
 * Runs to an explicit ThreadStore. Durable hosts retain history through their journal hooks.
 * No implementation may retry model or Tool execution or claim interrupted-work recovery.
 */
export class ThreadHistory extends Context.Service<
  ThreadHistory,
  {
    readonly retention: "incremental" | "on-success";
    /** Present only for the in-memory adapter; identifies its exact Store ownership view. */
    readonly memoryStore?: ConversationStore["Service"];
    readonly open: (request: {
      readonly threadId: ThreadId;
      readonly runId: RunId;
    }) => Effect.Effect<ThreadHistoryRun, ThreadHistoryError>;
    readonly load: (threadId: ThreadId) => Effect.Effect<Prompt.Prompt, ThreadHistoryError>;
  }
>()("@effect-agent/engine/ThreadHistory") {
  /** Bind incremental history to the supplied Thread.Store, including scoped ownership views. */
  static readonly layerFromStore = Layer.effect(
    ThreadHistory,
    Effect.gen(function* () {
      const threads = yield* ConversationStore;

      const historyError = (cause: ThreadError): ThreadHistoryError =>
        ThreadHistoryError.make({
          threadId: cause.threadId,
          reason:
            cause._tag === "ThreadNotFound"
              ? "not-found"
              : cause._tag === "ThreadLimitExceeded"
                ? "limit"
                : cause._tag === "ThreadHistoryDiverged"
                  ? "conflict"
                  : cause._tag === "ThreadOwnershipError"
                    ? cause.reason === "closed"
                      ? "fenced"
                      : "conflict"
                    : "encoding",
          message:
            cause._tag === "ThreadNotFound"
              ? "Thread history is not present in this application Scope"
              : cause._tag === "ThreadLimitExceeded"
                ? `In-memory history exceeds the ${cause.limit} limit of ${cause.limitValue}`
                : cause.message,
          cause,
        });

      return ThreadHistory.of({
        retention: "incremental",
        memoryStore: threads,
        load: (threadId) =>
          threads.snapshot(threadId).pipe(Effect.map(toPrompt), Effect.mapError(historyError)),
        open: Effect.fnUntraced(function* ({ threadId, runId }) {
          const snapshot = yield* threads.create(threadId).pipe(Effect.mapError(historyError));

          return {
            prompt: toPrompt(snapshot),
            compaction:
              snapshot.messages[0] === undefined
                ? undefined
                : retainedCompactions.get(snapshot.messages[0]),
            validateNativeCompaction: (through: number) =>
              threads.snapshot(threadId).pipe(
                Effect.mapError(historyError),
                Effect.flatMap((current) => {
                  const anchor = current.messages[0];
                  const closed = anchor === undefined ? undefined : closedRuns.get(anchor);

                  return through > 0 &&
                    through <= current.messages.length &&
                    current.messages
                      .slice(0, through)
                      .every(
                        (entry) =>
                          entry.runId !== undefined &&
                          entry.runId !== runId &&
                          closed?.has(entry.runId) === true,
                      )
                    ? Effect.void
                    : Effect.fail(
                        ThreadHistoryError.make({
                          threadId,
                          reason: "incompatible",
                          message: "Native compaction requires complete retained prior Runs",
                        }),
                      );
                }),
              ),
            end: threads.snapshot(threadId).pipe(
              Effect.match({
                // Missing or closed history cannot authorize a later native prefix.
                onFailure: () => undefined,
                onSuccess: (current) => {
                  const anchor = current.messages[0];

                  if (
                    anchor !== undefined &&
                    current.messages.some((entry) => entry.runId === runId)
                  )
                    closedRuns.set(anchor, new Set([...(closedRuns.get(anchor) ?? []), runId]));
                },
              }),
            ),
            stageCompaction: (state: RetainedCompaction) =>
              threads.snapshot(threadId).pipe(
                Effect.mapError(historyError),
                Effect.flatMap((current) => {
                  const anchor = current.messages[0];

                  if (
                    anchor === undefined ||
                    (state.replacement?.through ?? 0) > current.messages.length
                  )
                    return ThreadHistoryError.make({
                      threadId,
                      reason: "conflict",
                      message: "Native view has no retained source prefix",
                    });

                  return Effect.sync(() => {
                    retainedCompactions.set(anchor, {
                      replacement: state.replacement,
                      clearedThrough: state.clearedThrough,
                      nativeWindows: [...state.nativeWindows],
                    });
                  });
                }),
              ),
            stageInput: () => Effect.void,
            stageHistory: ({ source }) =>
              threads
                .recordHistory(threadId, runId, source)
                .pipe(Effect.asVoid, Effect.mapError(historyError)),
            commit: () =>
              threads.snapshot(threadId).pipe(Effect.asVoid, Effect.mapError(historyError)),
          };
        }),
      });
    }),
  );

  /** Retain bounded history for the application Scope. Separate Layer builds own separate stores. */
  static readonly layer = ThreadHistory.layerFromStore.pipe(Layer.provideMerge(layerMemory));
}

/** Retain in-memory conversation history for the application Scope. */
export const layer = ThreadHistory.layer;
