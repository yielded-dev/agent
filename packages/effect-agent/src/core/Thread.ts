/**
 * Native Effect AI conversation history with append-only messages, portable snapshots, and explicit
 * ownership.
 *
 * @since 0.1.0
 */

import * as Prompt from "effect/ai/Prompt";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as SynchronizedRef from "effect/SynchronizedRef";

import { ThreadId, RunId } from "./Identifiers.ts";
import { utf8ByteLength } from "./internal/utf8.ts";

/**
 * Bound applied to one text projection before it can enter a model context.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ThreadText = Schema.String.check(Schema.isMaxLength(64 * 1024));
const MAX_MESSAGE_CONTENT_BYTES = 4 * 1024 * 1024;
const MAX_THREADS = 256;
const MAX_STORE_CONTENT_BYTES = 64 * 1024 * 1024;

/**
 * One append-only native Effect AI message in a conversation.
 *
 * @category models
 * @since 0.1.0
 */
export class ThreadMessage extends Schema.Class<ThreadMessage>(
  "@effect-agent/capabilities/ThreadMessage",
)({
  threadId: ThreadId,
  sequence: Schema.Natural,
  runId: Schema.optionalKey(RunId),
  message: Prompt.Message,
  encodedBytes: Schema.Natural.check(Schema.isLessThanOrEqualTo(MAX_MESSAGE_CONTENT_BYTES)),
  timestamp: Schema.DateTimeUtcFromString,
}) {}

/**
 * An identified, ordered conversation snapshot. Storage and execution recovery are separate concerns.
 *
 * @category models
 * @since 0.1.0
 */
export class Thread extends Schema.Class<Thread>("@effect-agent/capabilities/ThreadSnapshot")({
  version: Schema.Literal(1),
  threadId: ThreadId,
  nextSequence: Schema.Natural,
  contentBytes: Schema.Natural,
  messages: Schema.Array(ThreadMessage),
}) {}

/**
 * Portable conversation snapshot. Its format is independent of the durable execution journal.
 *
 * @category models
 * @since 0.1.0
 */
export class ThreadExport extends Schema.Class<ThreadExport>(
  "@effect-agent/capabilities/ThreadExport",
)({
  format: Schema.Literal("effect-agent/ephemeral-thread@1"),
  exportedAt: Schema.DateTimeUtcFromString,
  snapshot: Thread,
}) {}

/**
 * Native message accepted by the append-only thread.
 *
 * @category models
 * @since 0.1.0
 */
export class ThreadAppend extends Schema.Class<ThreadAppend>(
  "@effect-agent/capabilities/ThreadAppend",
)({
  runId: Schema.optionalKey(RunId),
  message: Prompt.Message,
}) {}

/**
 * The requested Thread has not been created in this store.
 *
 * @category errors
 * @since 0.1.0
 */
export class ThreadNotFound extends Schema.TaggedError<ThreadNotFound>()("ThreadNotFound", {
  threadId: ThreadId,
}) {}

/**
 * A message or the process-local host exceeded capacity; no bound counts a Thread's lifetime.
 *
 * @category errors
 * @since 0.1.0
 */
export class ThreadLimitExceeded extends Schema.TaggedError<ThreadLimitExceeded>()(
  "ThreadLimitExceeded",
  {
    threadId: ThreadId,
    limit: Schema.Literals(["content-bytes", "threads", "store-content-bytes"]),
    limitValue: Schema.Natural,
    observedValue: Schema.Natural,
  },
) {}

/**
 * Engine history was not an append-only extension of the official snapshot.
 *
 * @category errors
 * @since 0.1.0
 */
export class ThreadHistoryDiverged extends Schema.TaggedError<ThreadHistoryDiverged>()(
  "ThreadHistoryDiverged",
  { threadId: ThreadId, message: Schema.String },
) {}

/**
 * Native Prompt encoding failed before a bounded state mutation.
 *
 * @category errors
 * @since 0.1.0
 */
export class ThreadEncodingError extends Schema.TaggedError<ThreadEncodingError>()(
  "ThreadEncodingError",
  { threadId: ThreadId, message: Schema.String },
) {}

/**
 * A scoped view cannot access another owner's history or be used after closure.
 *
 * @category errors
 * @since 0.1.0
 */
export class ThreadOwnershipError extends Schema.TaggedError<ThreadOwnershipError>()(
  "ThreadOwnershipError",
  { threadId: ThreadId, reason: Schema.Literals(["closed", "foreign"]), message: Schema.String },
) {}

/**
 * Expected lookup, capacity, append-only history, encoding, and ownership failures from a Thread
 * store.
 *
 * @category models
 * @since 0.1.0
 */
export type ThreadError =
  | ThreadNotFound
  | ThreadLimitExceeded
  | ThreadHistoryDiverged
  | ThreadEncodingError
  | ThreadOwnershipError;

/**
 * Reconstruct exact Effect AI Prompt history, including tools, reasoning, files, and options.
 *
 * @category operations
 * @since 0.1.0
 */
export const toPrompt = (snapshot: Thread): Prompt.Prompt =>
  Prompt.fromMessages(snapshot.messages.map((entry) => entry.message));

/**
 * Process-local state for conversation history. Updates remain visible even
 * when a Run later fails; this service has no successful-Run commit boundary or durable recovery.
 * ThreadHistory.layer uses this store for ordinary Runs and interactive history.
 * Scope closure releases all in-memory state.
 */
interface StoreService {
  /**
   * Acquire an isolated ownership view over this same bounded store. Newly created Threads
   * belong to the supplied Scope; closing it releases only that view's Threads and bytes.
   * Supplied by layerMemory; custom stores may omit this capability.
   * The view rejects foreign IDs and all access after closure. Keep its Scope open through
   * all users, including streams, child Runs, snapshots and history hooks.
   */
  readonly scoped?: Effect.Effect<StoreService, never, Scope.Scope>;
  readonly create: (
    threadId: ThreadId,
  ) => Effect.Effect<Thread, ThreadLimitExceeded | ThreadOwnershipError>;
  readonly append: (
    threadId: ThreadId,
    message: ThreadAppend,
  ) => Effect.Effect<
    Thread,
    ThreadNotFound | ThreadLimitExceeded | ThreadEncodingError | ThreadOwnershipError
  >;
  /**
   * Record an engine-emitted full history only when it is an append-only
   * extension of the exact official Prompt already stored. The entire
   * suffix commits in one transaction or not at all: concurrent writers are
   * serialized, a writer whose history no longer extends the committed
   * official history fails with ThreadHistoryDiverged, and a limit
   * failure inside the suffix records nothing. Earlier updates remain committed if the Run
   * later fails or is interrupted; this transaction covers one update, not the whole Run.
   */
  readonly recordHistory: (
    threadId: ThreadId,
    runId: RunId,
    history: Prompt.Prompt,
  ) => Effect.Effect<Thread, ThreadError>;
  readonly snapshot: (
    threadId: ThreadId,
  ) => Effect.Effect<Thread, ThreadNotFound | ThreadOwnershipError>;
  readonly export: (
    threadId: ThreadId,
  ) => Effect.Effect<ThreadExport, ThreadNotFound | ThreadOwnershipError>;
}

/**
 * Process-local conversation store supplied by layerMemory.
 *
 * @category services
 * @since 0.1.0
 */
export class Store extends Context.Service<Store, StoreService>()(
  "@effect-agent/capabilities/EphemeralThreads",
) {}

const encodeMessage = (
  threadId: ThreadId,
  message: Prompt.Message,
): Effect.Effect<string, ThreadEncodingError> =>
  Schema.encodeEffect(Prompt.Message)(message).pipe(
    Effect.map((encoded) => JSON.stringify(encoded)),
    Effect.mapError((error) =>
      ThreadEncodingError.make({
        threadId,
        message: `Could not encode native Effect AI message: ${error.message}`,
      }),
    ),
  );

interface Owner {
  closed: boolean;
  readonly parent?: Owner;
}

interface Entry {
  readonly snapshot: Thread;
  readonly owner: Owner;
}

interface State {
  readonly threads: ReadonlyMap<ThreadId, Entry>;
  readonly contentBytes: number;
}

const belongsTo = (owner: Owner, ancestor: Owner): boolean => {
  for (let current: Owner | undefined = owner; current !== undefined; current = current.parent) {
    if (current === ancestor) return true;
  }

  return false;
};

const access = (state: State, owner: Owner, threadId: ThreadId) => {
  for (let current: Owner | undefined = owner; current !== undefined; current = current.parent) {
    if (current.closed) {
      return Effect.fail(
        ThreadOwnershipError.make({
          threadId,
          reason: "closed",
          message: "The in-memory conversation owner is closed",
        }),
      );
    }
  }
  const entry = state.threads.get(threadId);

  return entry !== undefined && entry.owner !== owner
    ? Effect.fail(
        ThreadOwnershipError.make({
          threadId,
          reason: "foreign",
          message: "The Thread belongs to another in-memory conversation owner",
        }),
      )
    : Effect.succeed(entry?.snapshot);
};

const findSnapshot = Effect.fnUntraced(function* (state: State, owner: Owner, threadId: ThreadId) {
  const snapshot = yield* access(state, owner, threadId);

  return snapshot === undefined ? yield* ThreadNotFound.make({ threadId }) : snapshot;
});

const put = (state: State, owner: Owner, snapshot: Thread): State => ({
  threads: new Map(state.threads).set(snapshot.threadId, { snapshot, owner }),
  contentBytes:
    state.contentBytes +
    snapshot.contentBytes -
    (state.threads.get(snapshot.threadId)?.snapshot.contentBytes ?? 0),
});

const messageSize = (threadId: ThreadId, encoded: string) => {
  const observedValue = utf8ByteLength(encoded);

  return observedValue <= MAX_MESSAGE_CONTENT_BYTES
    ? Effect.succeed(observedValue)
    : Effect.fail(
        ThreadLimitExceeded.make({
          threadId,
          limit: "content-bytes",
          limitValue: MAX_MESSAGE_CONTENT_BYTES,
          observedValue,
        }),
      );
};

const appendEncoded = Effect.fnUntraced(function* (
  state: State,
  owner: Owner,
  current: Thread,
  append: ThreadAppend,
  encoded: string,
  timestamp: DateTime.Utc,
) {
  const threadId = current.threadId;

  const messageBytes = yield* messageSize(threadId, encoded);
  const contentBytes = current.contentBytes + messageBytes;
  const storeBytes = state.contentBytes + messageBytes;

  if (storeBytes > MAX_STORE_CONTENT_BYTES) {
    return yield* ThreadLimitExceeded.make({
      threadId,
      limit: "store-content-bytes",
      limitValue: MAX_STORE_CONTENT_BYTES,
      observedValue: storeBytes,
    });
  }

  const message = ThreadMessage.make({
    threadId,
    sequence: current.nextSequence,
    ...(append.runId === undefined ? {} : { runId: append.runId }),
    message: append.message,
    encodedBytes: messageBytes,
    timestamp,
  });

  const next = Thread.make({
    version: current.version,
    threadId: current.threadId,
    nextSequence: current.nextSequence + 1,
    contentBytes,
    messages: [...current.messages, message],
  });

  return [next, put(state, owner, next)] as const;
});

/**
 * In-memory storage shared for the consumer's Scope; it does not survive process loss.
 *
 * @category constructors
 * @since 0.1.0
 */
export const makeMemoryStore = Effect.gen(function* () {
  const state = yield* SynchronizedRef.make<State>({ threads: new Map(), contentBytes: 0 });
  const root: Owner = { closed: false };

  const release = (owner: Owner) =>
    SynchronizedRef.update(state, (current) => {
      owner.closed = true;
      const threads = new Map(current.threads);
      let contentBytes = current.contentBytes;

      for (const [id, entry] of threads) {
        if (belongsTo(entry.owner, owner)) {
          threads.delete(id);
          contentBytes -= entry.snapshot.contentBytes;
        }
      }

      return { threads, contentBytes };
    });

  yield* Effect.addFinalizer(() => release(root));

  const view = (owner: Owner): StoreService => ({
    scoped: Effect.acquireRelease(
      Effect.sync((): Owner => ({ closed: false, parent: owner })),
      release,
    ).pipe(Effect.map(view)),
    create: (threadId) =>
      SynchronizedRef.modifyEffect(
        state,
        Effect.fnUntraced(function* (threads) {
          const existing = yield* access(threads, owner, threadId);

          if (existing !== undefined) return [existing, threads] as const;
          if (threads.threads.size >= MAX_THREADS) {
            return yield* ThreadLimitExceeded.make({
              threadId,
              limit: "threads",
              limitValue: MAX_THREADS,
              observedValue: threads.threads.size + 1,
            });
          }

          const created = Thread.make({
            version: 1,
            threadId,
            nextSequence: 0,
            contentBytes: 0,
            messages: [],
          });

          return [created, put(threads, owner, created)] as const;
        }),
      ),
    append: Effect.fnUntraced(function* (threadId, message) {
      const encoded = yield* encodeMessage(threadId, message.message);
      const timestamp = DateTime.toUtc(DateTime.makeUnsafe(yield* Clock.currentTimeMillis));

      return yield* SynchronizedRef.modifyEffect(
        state,
        Effect.fnUntraced(function* (threads) {
          const current = yield* findSnapshot(threads, owner, threadId);

          return yield* appendEncoded(threads, owner, current, message, encoded, timestamp);
        }),
      );
    }),
    recordHistory: Effect.fnUntraced(function* (threadId, historyRunId, history) {
      const incoming = yield* Effect.forEach(history.content, (message) =>
        encodeMessage(threadId, message).pipe(Effect.map((encoded) => ({ message, encoded }))),
      );

      // Publish the complete suffix only after verification and every bounded append succeed.
      return yield* SynchronizedRef.modifyEffect(
        state,
        Effect.fnUntraced(function* (threads) {
          const current = yield* findSnapshot(threads, owner, threadId);

          const currentEncoded = yield* Effect.forEach(current.messages, (entry) =>
            encodeMessage(threadId, entry.message),
          );

          if (
            incoming.length < currentEncoded.length ||
            currentEncoded.some((encoded, index) => encoded !== incoming[index]?.encoded)
          ) {
            return yield* ThreadHistoryDiverged.make({
              threadId,
              message: "Engine history is not an append-only extension of official history",
            });
          }
          const timestamp = DateTime.toUtc(DateTime.makeUnsafe(yield* Clock.currentTimeMillis));

          if (incoming.length === currentEncoded.length) return [current, threads] as const;

          const messages = [...current.messages];
          let contentBytes = current.contentBytes;
          let storeBytes = threads.contentBytes;
          let nextSequence = current.nextSequence;

          for (const entry of incoming.slice(currentEncoded.length)) {
            const append = ThreadAppend.make({ runId: historyRunId, message: entry.message });

            const messageBytes = yield* messageSize(threadId, entry.encoded);

            contentBytes += messageBytes;
            storeBytes += messageBytes;
            if (storeBytes > MAX_STORE_CONTENT_BYTES) {
              return yield* ThreadLimitExceeded.make({
                threadId,
                limit: "store-content-bytes",
                limitValue: MAX_STORE_CONTENT_BYTES,
                observedValue: storeBytes,
              });
            }
            messages.push(
              ThreadMessage.make({
                threadId,
                sequence: nextSequence,
                ...(append.runId === undefined ? {} : { runId: append.runId }),
                message: append.message,
                encodedBytes: messageBytes,
                timestamp,
              }),
            );
            nextSequence += 1;
          }

          const snapshot = Thread.make({
            version: current.version,
            threadId,
            nextSequence,
            contentBytes,
            messages,
          });

          return [snapshot, put(threads, owner, snapshot)] as const;
        }),
      );
    }),
    snapshot: (threadId) =>
      SynchronizedRef.get(state).pipe(Effect.flatMap((all) => findSnapshot(all, owner, threadId))),
    export: Effect.fnUntraced(function* (threadId) {
      const snapshot = yield* findSnapshot(yield* SynchronizedRef.get(state), owner, threadId);

      return ThreadExport.make({
        format: "effect-agent/ephemeral-thread@1",
        exportedAt: DateTime.toUtc(DateTime.makeUnsafe(yield* Clock.currentTimeMillis)),
        snapshot,
      });
    }),
  });

  return view(root);
});

/**
 * Provide an in-memory Thread store whose retained state belongs to the Layer’s Scope.
 *
 * @category layers
 * @since 0.1.0
 */
export const layerMemory = Layer.effect(Store, makeMemoryStore);
