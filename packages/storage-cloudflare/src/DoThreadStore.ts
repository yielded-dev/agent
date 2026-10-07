import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { makeSqlThreadImport } from "@yielded/agent-storage-sql/sql-thread-import";
import {
  makeSelectedReads,
  SelectedReadOwner,
} from "@yielded/agent-storage-sql/sql-thread-native-reads";
import { EMPTY_TAIL_DIGEST } from "@yielded/agent/digest";
import { ExportRecord } from "@yielded/agent/record-format";
import {
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  ObservationOffset,
} from "@yielded/agent/records";
import { SqlStorageOwner } from "@yielded/agent/sql-memory-store";
import { DEFAULT_OWNERSHIP_LEASE_DURATION } from "@yielded/agent/submission-ledger";
import { MAX_ARCHIVE_RANGE_PAGE } from "@yielded/agent/thread-archive-range";
import { ThreadImport } from "@yielded/agent/thread-import";
import {
  type ThreadExportRequest,
  AppendConflict,
  AppendResult,
  CheckpointRejected,
  ThreadCheckpoint,
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadObservation,
  ThreadReadRequest,
  ThreadStore,
  ThreadReader,
  type ThreadCheckpoints,
  ThreadStoreError,
  ThreadStoreDiagnostic,
  ThreadTail,
  ThreadTailRequest,
  FenceRejected,
  type FencedAppendRequest,
  PreparedAppend,
  LoadCheckpointRequest,
  SaveCheckpointRequest,
} from "@yielded/agent/thread-store";
import {
  Clock,
  Context,
  Crypto,
  Duration,
  Effect,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import * as SqlClientService from "effect/sql/SqlClient";
import { isSqlError } from "effect/sql/SqlError";

import {
  DEFAULT_MAX_STORED_VALUE_BYTES,
  DoStorageConfig,
  DoStorageConfigValue,
} from "./DoStorageConfig.ts";
import {
  type DoStorageCompatibilityError,
  DoAppendConflict,
  DoCheckpointConflict,
  DoFenceRejected,
  type DoStorageFailpointLocation,
  DoStorageCorruptionError,
  DoStorageError,
} from "./DoStorageError.ts";
import { DoStorageFailpoint, type DoStorageFailpointHandler } from "./DoStorageFailpoint.ts";
import { DO_OFFSET_PREFIX, prepareCanonicalAppend } from "./internal/canonical-append.ts";
import {
  initializeDoJournal,
  RawCheckpoint,
  RawReadRequest,
  type DoJournal,
} from "./internal/do-journal.ts";
import { readDoStorageHeader } from "./internal/migrations.ts";
import { invalidateOwnedState, ownedState } from "./internal/owned-state.ts";
import { isAppendContention, withStorageSpan } from "./internal/storage-span.ts";

/**
 * Convenience-layer construction options. `storage` is the Durable Object's own
 * `ctx.storage` handle, injected as a value (DEPLOY-010: platform bindings enter only
 * through Layers; this package never imports `cloudflare:workers`).
 */
export interface DoStorageOptions {
  readonly storage: DurableObjectStorage;
  readonly observationPollInterval?: number | undefined;
  /**
   * Submission ownership lease duration in milliseconds (D5). Defaults to
   * `DEFAULT_OWNERSHIP_LEASE_DURATION` from `@yielded/agent/submission-ledger`.
   */
  readonly ownershipLeaseDuration?: number | undefined;
  /**
   * Maximum bytes for any single stored value; must stay under the platform's 2 MB
   * per-value limit. Defaults to `DEFAULT_MAX_STORED_VALUE_BYTES`.
   */
  readonly maxStoredValueBytes?: number | undefined;
  /**
   * Re-verify Thread journal payloads, ownership, and digest chains in bounded pages while
   * opening the store, including orphan rows and rows outside declared ranges. Defaults to
   * off: per-operation Schema decoding and the digest chain already fail clearly on corrupt
   * rows without scanning the whole database on every open.
   */
  readonly verifyOnOpen?: boolean | undefined;
  readonly failpoint?: DoStorageFailpointHandler | undefined;
}

export type DoStorageInitializationError =
  | DoStorageCompatibilityError
  | DoStorageCorruptionError
  | DoStorageError;

const OffsetText = Schema.String.check(Schema.isMaxLength(4 * 1024));
const ZERO_CANONICAL_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);
const isDigest = Schema.is(Digest);
const isDoFenceRejected = Schema.is(DoFenceRejected);
const isDoAppendConflict = Schema.is(DoAppendConflict);
const isDoCheckpointConflict = Schema.is(DoCheckpointConflict);
// Reuse the same AST/parser across reads. Reconstructing a JSON codec per row
// defeats Schema's parser cache and repeatedly walks the canonical union.
const CanonicalRecordJson = Schema.fromJsonString(ExportRecord);
const ThreadCheckpointJson = Schema.fromJsonString(ThreadCheckpoint);
const decodeCanonicalRecord = Schema.decodeEffect(CanonicalRecordJson);

const storeError = (
  operation: string,
  error: { readonly message: string; readonly diagnostic?: ThreadStoreDiagnostic },
) =>
  ThreadStoreError.make({
    cause: error,
    operation,
    message: error.message,
    ...(error.diagnostic === undefined ? {} : { diagnostic: error.diagnostic }),
  });

const schemaStoreError = (operation: string, error: Schema.SchemaError) =>
  ThreadStoreError.make({
    operation,
    message: "A Thread storage value does not satisfy its schema",
    diagnostic: ThreadStoreDiagnostic.make({
      causeTag: error._tag,
      operation,
      issueTag: error.issue._tag,
    }),
  });

const makeOffset = (
  threadId: ThreadMaterialization["threadId"],
  sequence: number,
): Effect.Effect<ObservationOffset, ThreadStoreError> =>
  Schema.decodeEffect(CanonicalSequence)(sequence).pipe(
    Effect.flatMap((validatedSequence) =>
      Schema.decodeEffect(ObservationOffset)(
        `${DO_OFFSET_PREFIX}${encodeURIComponent(threadId)}:${validatedSequence}`,
      ),
    ),
    Effect.mapError((error) => schemaStoreError("encode observation offset", error)),
  );

const parseOffset = Effect.fnUntraced(function* (
  threadId: ThreadMaterialization["threadId"],
  offset: ObservationOffset | undefined,
): Effect.fn.Return<CanonicalSequence, ThreadStoreError> {
  if (offset === undefined) return ZERO_CANONICAL_SEQUENCE;

  const text = yield* Schema.decodeEffect(OffsetText)(offset).pipe(
    Effect.mapError((error) => schemaStoreError("decode observation offset", error)),
  );

  const threadPrefix = `${DO_OFFSET_PREFIX}${encodeURIComponent(threadId)}:`;

  if (!text.startsWith(threadPrefix)) {
    return yield* ThreadStoreError.make({
      operation: "decode observation offset",
      message: "The observation offset belongs to a different adapter, storage version, or Thread.",
    });
  }
  const sequenceText = text.slice(threadPrefix.length);

  if (!/^(0|[1-9][0-9]*)$/.test(sequenceText)) {
    return yield* ThreadStoreError.make({
      operation: "decode observation offset",
      message: "The observation offset is malformed.",
    });
  }

  return yield* Schema.decodeEffect(CanonicalSequence)(Number(sequenceText)).pipe(
    Effect.mapError((error) => schemaStoreError("decode observation offset", error)),
  );
});

const mapFence = (threadId: ThreadMaterialization["threadId"], error: DoFenceRejected) =>
  FenceRejected.make({
    threadId,
    actualEpoch: error.actualEpoch,
    attemptedEpoch: error.producerEpoch,
  });

const encodeCheckpoint = (checkpoint: ThreadCheckpoint): Effect.Effect<string, ThreadStoreError> =>
  Schema.encodeEffect(ThreadCheckpointJson)(checkpoint).pipe(
    Effect.mapError((error) => schemaStoreError("encode checkpoint", error)),
  );

const envelopes = new WeakMap<object, CanonicalRecordEnvelope>();

const decodeEnvelope = Effect.fnUntraced(function* (row: {
  readonly batch_id: string;
  readonly thread_id: string;
  readonly record_json: string;
  readonly sequence: CanonicalSequence;
}) {
  const cached = envelopes.get(row);

  if (cached !== undefined) return cached;

  const record = yield* decodeCanonicalRecord(row.record_json).pipe(
    Effect.mapError((error) =>
      ThreadStoreError.make({
        operation: "decode canonical record",
        message: "The canonical record does not satisfy its schema",
        diagnostic: ThreadStoreDiagnostic.make({
          causeTag: error._tag,
          operation: "decode canonical record",
          decoder: "CanonicalRecord",
          sequence: row.sequence,
          issueTag: error.issue._tag,
        }),
      }),
    ),
    Effect.tapError((error) =>
      Effect.annotateCurrentSpan({
        "storage.failure.operation": error.diagnostic?.operation,
        "storage.failure.cause": error.diagnostic?.causeTag,
        "storage.failure.decoder": error.diagnostic?.decoder,
        "storage.failure.sequence": row.sequence,
        "storage.failure.issue": error.diagnostic?.issueTag,
      }),
    ),
  );

  const threadId = yield* Schema.decodeEffect(CanonicalRecordEnvelope.fields.threadId)(
    row.thread_id,
  ).pipe(Effect.mapError((error) => schemaStoreError("decode thread identity", error)));

  const offset = yield* makeOffset(threadId, row.sequence);

  const batchId = yield* Schema.decodeEffect(CanonicalRecordEnvelope.fields.batchId)(
    row.batch_id,
  ).pipe(Effect.mapError((error) => schemaStoreError("decode batch identity", error)));

  const envelope = new CanonicalRecordEnvelope(
    { threadId, batchId, sequence: row.sequence, offset, record },
    { disableChecks: true },
  );

  envelopes.set(row, envelope);

  return envelope;
});

const decodeCheckpoint = (
  checkpointJson: string,
): Effect.Effect<ThreadCheckpoint, ThreadStoreError> =>
  Schema.decodeEffect(Schema.fromJsonString(ThreadCheckpoint))(checkpointJson).pipe(
    Effect.mapError((error) => schemaStoreError("decode checkpoint", error)),
  );

const requireThread = Effect.fnUntraced(function* (
  journal: DoJournal,
  threadId: ThreadMaterialization["threadId"],
) {
  const rows = yield* journal
    .getThread(threadId)
    .pipe(Effect.mapError((error) => storeError("read thread", error)));

  if (rows.length === 0) {
    return yield* ThreadNotMaterialized.make({ threadId });
  }

  return rows[0];
});

const tailDigestAt = Effect.fnUntraced(function* (
  journal: DoJournal,
  threadId: ThreadMaterialization["threadId"],
  sequence: CanonicalSequence,
) {
  if (sequence === 0) return EMPTY_TAIL_DIGEST;

  const digests = yield* journal
    .getTailDigestAt(threadId, sequence)
    .pipe(Effect.mapError((error) => storeError("read checkpoint digest", error)));

  if (digests.length !== 1) {
    return yield* CheckpointRejected.make({
      threadId,
      reason: "digest-mismatch",
    });
  }

  return yield* Schema.decodeEffect(Digest)(digests[0]).pipe(
    Effect.mapError((error) => schemaStoreError("decode checkpoint digest", error)),
  );
});

/** Audit excluded rows before paging owners and bounded ranges; never collect Thread payloads. */
const decodeStartupPayloads = Effect.fnUntraced(function* (journal: DoJournal) {
  yield* journal.archiveRanges.verifyCoverage;
  let afterThreadId: string | undefined;

  while (true) {
    const owners = yield* journal.archiveRanges.threadPage(afterThreadId);

    for (const owner of owners) {
      yield* journal.archiveRanges.verifyThread(owner.thread_id);
      afterThreadId = owner.thread_id;
    }
    if (owners.length < MAX_ARCHIVE_RANGE_PAGE) break;
  }
});

const makeServices = Effect.fnUntraced(function* () {
  const config = yield* DoStorageConfig;
  const failpoint = yield* DoStorageFailpoint;
  const sql = yield* SqlClientService.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const journal = yield* initializeDoJournal(sql, failpoint.hit, config.maxStoredValueBytes);

  if (config.verifyOnOpen) {
    yield* journal.state.read(decodeStartupPayloads(journal)).pipe(
      Effect.mapError((cause) =>
        DoStorageCorruptionError.make({
          table: "effect_agent_journal_ranges",
          rowKey: "verification",
          message: cause.message,
        }),
      ),
    );
  }

  const hitFailpoint = (
    location: DoStorageFailpointLocation,
  ): Effect.Effect<void, ThreadStoreError> =>
    failpoint
      .hit(location)
      .pipe(Effect.mapError((error) => storeError(`storage failpoint ${location}`, error)));

  const materialize: ThreadStore["Service"]["materialize"] = Effect.fnUntraced(
    function* (request: ThreadMaterialization) {
      const validated = yield* Schema.decodeEffect(Schema.toType(ThreadMaterialization))(
        request,
      ).pipe(Effect.mapError((error) => schemaStoreError("validate materialization", error)));

      const now = yield* Clock.currentTimeMillis;

      yield* hitFailpoint("materialize:before");
      yield* journal
        .materialize(
          validated.threadId,
          new Date(now).toISOString(),
          EMPTY_TAIL_DIGEST,
          validated.producerEpoch,
        )
        .pipe(
          Effect.mapError((error) =>
            error._tag === "DoFenceRejected"
              ? mapFence(validated.threadId, error)
              : storeError("materialize thread", error),
          ),
        );
      yield* hitFailpoint("materialize:after");
    },
    withStorageSpan("DoThreadStore.materialize", (error) => error._tag === "FenceRejected"),
  );

  const append: ThreadStore["Service"]["append"] = Effect.fnUntraced(
    function* (request: FencedAppendRequest) {
      const validated = yield* PreparedAppend.capture(request);
      const observed = yield* requireThread(journal, validated.threadId);

      const { raw: rawRequest } = yield* prepareCanonicalAppend(validated).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
      );

      yield* hitFailpoint("append:before");

      const result = yield* journal.append(rawRequest, observed).pipe(
        Effect.mapError((error) => {
          if (isDoFenceRejected(error)) {
            return mapFence(validated.threadId, error);
          }
          if (isDoAppendConflict(error)) {
            return error.actualTailSequence !== undefined && isDigest(error.actualTailDigest)
              ? AppendConflict.make({
                  threadId: validated.threadId,
                  batchId: validated.batch.batchId,
                  reason: error.reason,
                  actualTailSequence: error.actualTailSequence,
                  actualTailDigest: error.actualTailDigest,
                })
              : AppendConflict.make({
                  threadId: validated.threadId,
                  batchId: validated.batch.batchId,
                  reason: error.reason,
                });
          }

          return storeError("append canonical batch", error);
        }),
        Effect.flatMap((result) =>
          Schema.decodeEffect(AppendResult)(result).pipe(
            Effect.mapError((error) => schemaStoreError("decode append result", error)),
          ),
        ),
      );

      yield* hitFailpoint("append:after");

      return result;
    },
    withStorageSpan("DoThreadStore.append", isAppendContention),
  );

  const loadRecords = Effect.fnUntraced(function* (request: RawReadRequest) {
    const result = yield* journal
      .read(request)
      .pipe(Effect.mapError((error) => storeError("read canonical records", error)));

    return {
      count: result.count,
      records: result.records.pipe(
        Stream.mapError((error) => storeError("read canonical records", error)),
        Stream.mapEffect(decodeEnvelope),
      ),
    };
  });

  const readEffect = Effect.fnUntraced(function* (request: ThreadReadRequest) {
    const validated = yield* Schema.decodeEffect(Schema.toType(ThreadReadRequest))(request).pipe(
      Effect.mapError((error) => schemaStoreError("validate thread read", error)),
    );

    if ("selection" in validated) return Stream.fromIterable(yield* selectedReads.read(validated));
    yield* requireThread(journal, validated.threadId);

    const result = yield* loadRecords(
      RawReadRequest.make({
        threadId: validated.threadId,
        fromSequenceExclusive: validated.afterSequence ?? ZERO_CANONICAL_SEQUENCE,
        limit: validated.limit,
      }),
    );

    return result.records;
  });

  const read: ThreadStore["Service"]["read"] = (request) => Stream.unwrap(readEffect(request));

  const observeEffect = Effect.fnUntraced(function* (request: ThreadObservation) {
    const validated = yield* Schema.decodeEffect(Schema.toType(ThreadObservation))(request).pipe(
      Effect.mapError((error) => schemaStoreError("validate thread observation", error)),
    );

    yield* requireThread(journal, validated.threadId);
    const initialSequence = yield* parseOffset(validated.threadId, validated.afterOffset);
    const cursor = yield* Ref.make(initialSequence);
    let polls = 0;
    let emptyPolls = 0;
    let deliveredRecords = 0;

    const poll = Effect.fnUntraced(function* () {
      polls++;
      const fromSequenceExclusive = yield* Ref.get(cursor);

      const result = yield* loadRecords(
        RawReadRequest.make({
          threadId: validated.threadId,
          fromSequenceExclusive,
          limit: 1_024,
        }),
      );

      if (result.count === 0) {
        emptyPolls++;
        yield* Effect.sleep(config.observationPollInterval);

        return Stream.empty;
      }

      return result.records.pipe(
        Stream.tap((record) => {
          deliveredRecords++;

          return Ref.set(cursor, record.sequence);
        }),
      );
    });

    return Stream.fromEffectRepeat(poll()).pipe(
      Stream.flatten,
      Stream.ensuring(
        Effect.suspend(() =>
          Effect.annotateCurrentSpan({
            "effect_agent.observation.polls": polls,
            "effect_agent.observation.empty_polls": emptyPolls,
            "effect_agent.observation.records": deliveredRecords,
          }),
        ),
      ),
    );
  });

  const observe: ThreadStore["Service"]["observe"] = (request) =>
    Stream.unwrap(observeEffect(request)).pipe(Stream.withSpan("DoThreadStore.observe"));

  const transfer = yield* makeSqlThreadImport({
    offsetPrefix: DO_OFFSET_PREFIX,
    maxValueBytes: config.maxStoredValueBytes,
    // A zero cursor lets the normal publication source regenerate intent from the imported log.
    afterImport: ({ result }) =>
      journal.lifecycle === undefined
        ? Effect.void
        : sql`INSERT INTO effect_agent_lifecycle_cursors(thread_id, through_sequence) VALUES (${result.threadId}, 0)`.pipe(
            Effect.asVoid,
            Effect.catchTag("SqlError", (cause) => storeError("rebuild lifecycle cursor", cause)),
          ),
    afterThreadRead: hitFailpoint("export:after-thread-read"),
    read: (body) =>
      journal.owner.transaction(body).pipe(
        Effect.provideService(SqlClientService.SqlClient, sql),
        Effect.catchIf(isSqlError, (cause) => storeError("export transaction", cause)),
      ),
    write: (body) =>
      journal.owner.transaction(body.pipe(Effect.tap(() => journal.state.invalidate))).pipe(
        Effect.provideService(SqlClientService.SqlClient, sql),
        Effect.catchIf(isSqlError, (cause) => storeError("import transaction", cause)),
      ),
  });

  const inspectTail: ThreadStore["Service"]["inspectTail"] = Effect.fnUntraced(function* (
    request: ThreadTailRequest,
  ) {
    const validated = yield* Schema.decodeEffect(Schema.toType(ThreadTailRequest))(request).pipe(
      Effect.mapError((error) => schemaStoreError("validate tail inspection", error)),
    );

    const thread = yield* requireThread(journal, validated.threadId);

    const tailDigest = yield* Schema.decodeEffect(Digest)(thread.tail_digest).pipe(
      Effect.mapError((error) => schemaStoreError("decode tail digest", error)),
    );

    return ThreadTail.make({
      threadId: validated.threadId,
      tailSequence: thread.tail_sequence,
      tailDigest,
      producerEpoch: thread.producer_epoch,
    });
  });

  const saveCheckpoint: ThreadCheckpoints["save"] = Effect.fnUntraced(function* (
    request: SaveCheckpointRequest,
  ) {
    const validated = yield* Schema.decodeEffect(Schema.toType(SaveCheckpointRequest))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate checkpoint", error)));

    const thread = yield* requireThread(journal, validated.checkpoint.threadId);

    if (validated.checkpoint.throughSequence > thread.tail_sequence) {
      return yield* CheckpointRejected.make({
        threadId: validated.checkpoint.threadId,
        reason: "ahead-of-tail",
      });
    }

    const canonicalDigest = yield* tailDigestAt(
      journal,
      validated.checkpoint.threadId,
      validated.checkpoint.throughSequence,
    );

    if (canonicalDigest !== validated.checkpoint.tailDigest) {
      return yield* CheckpointRejected.make({
        threadId: validated.checkpoint.threadId,
        reason: "digest-mismatch",
      });
    }
    const checkpointJson = yield* encodeCheckpoint(validated.checkpoint);

    const raw = RawCheckpoint.make({
      threadId: validated.checkpoint.threadId,
      throughSequence: validated.checkpoint.throughSequence,
      tailDigest: validated.checkpoint.tailDigest,
      checkpointJson,
    });

    yield* hitFailpoint("save-checkpoint:before");
    yield* journal.saveCheckpoint(raw).pipe(
      Effect.mapError((error) =>
        isDoCheckpointConflict(error)
          ? CheckpointRejected.make({
              threadId: validated.checkpoint.threadId,
              reason: "digest-mismatch",
            })
          : storeError("save checkpoint", error),
      ),
    );
    yield* hitFailpoint("save-checkpoint:after");
  });

  const loadCheckpoint: ThreadCheckpoints["load"] = Effect.fnUntraced(function* (
    request: LoadCheckpointRequest,
  ) {
    const validated = yield* Schema.decodeEffect(Schema.toType(LoadCheckpointRequest))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate checkpoint lookup", error)));

    const thread = yield* requireThread(journal, validated.threadId);

    const rows = yield* journal
      .loadCheckpoint(validated.threadId, validated.atOrBeforeSequence ?? thread.tail_sequence)
      .pipe(Effect.mapError((error) => storeError("load checkpoint", error)));

    if (rows.length === 0) return Option.none();
    if (rows.length !== 1) {
      return yield* ThreadStoreError.make({
        operation: "load checkpoint",
        message: `Expected at most one checkpoint row but found ${rows.length}.`,
      });
    }
    const row = rows[0];
    const checkpoint = yield* decodeCheckpoint(row.checkpoint_json);

    if (
      row.thread_id !== validated.threadId ||
      checkpoint.threadId !== row.thread_id ||
      checkpoint.throughSequence !== row.through_sequence ||
      checkpoint.tailDigest !== row.tail_digest
    ) {
      return yield* ThreadStoreError.make({
        operation: "load checkpoint",
        message: "Stored checkpoint metadata does not match its canonical row.",
      });
    }

    const canonicalDigest = yield* tailDigestAt(
      journal,
      checkpoint.threadId,
      checkpoint.throughSequence,
    );

    if (canonicalDigest !== checkpoint.tailDigest) {
      return yield* CheckpointRejected.make({
        threadId: checkpoint.threadId,
        reason: "digest-mismatch",
      });
    }

    return Option.some(checkpoint);
  });

  const selectedReads = yield* makeSelectedReads(decodeEnvelope).pipe(
    Effect.provideService(SelectedReadOwner, {
      snapshot: journal.state.read,
      tail: (threadId) =>
        journal.getThread(threadId).pipe(
          Effect.flatMap((rows) =>
            Schema.decodeEffect(
              Schema.UndefinedOr(
                Schema.Struct({
                  tail_sequence: CanonicalSequence,
                  tail_digest: Digest,
                  producer_epoch: ThreadMaterialization.fields.producerEpoch,
                }),
              ),
            )(rows[0]),
          ),
          Effect.mapError((error) => storeError("native thread tail", error)),
        ),
    }),
  );

  const threadStore = ThreadStore.of({
    archives: journal.archives,
    work: journal.work.storage,
    ...(journal.lifecycle === undefined
      ? {}
      : { lifecyclePublications: journal.lifecycle.storage }),
    readIdentity: selectedReads.readIdentity,
    readWorkerCapacity: selectedReads.readWorkerCapacity,
    verification: transfer.verification,
    countPeerMessages: selectedReads.countPeerMessages,
    append,
    export: transfer.export,
    inspectTail,
    materialize,
    observe,
    read,
    checkpoints: { save: saveCheckpoint, load: loadCheckpoint },
  });

  return Context.make(ThreadStore, threadStore).pipe(Context.add(ThreadImport, transfer.importer));
});

/**
 * Durable Object Thread Store implementation with configuration, failpoint, SQL, and
 * Crypto authority kept visible in its input channel.
 */
export const threadStoreLayer: Layer.Layer<
  ThreadStore | ThreadReader | ThreadImport,
  DoStorageInitializationError,
  DoStorageConfig | DoStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = ThreadReader.layer().pipe(Layer.provideMerge(Layer.effectContext(makeServices())));

/**
 * Validated Durable Object storage configuration Layer with the documented defaults applied.
 * Shared by the ThreadStore and SubmissionLedger convenience layers so their defaults
 * cannot drift.
 */
export const storageConfigLayer = (
  options: DoStorageOptions,
): Layer.Layer<DoStorageConfig, DoStorageError> =>
  Layer.effect(DoStorageConfig)(
    Schema.decodeEffect(DoStorageConfigValue)({
      observationPollInterval: options.observationPollInterval ?? 25,
      ownershipLeaseDuration:
        options.ownershipLeaseDuration ?? Duration.toMillis(DEFAULT_OWNERSHIP_LEASE_DURATION),
      maxStoredValueBytes: options.maxStoredValueBytes ?? DEFAULT_MAX_STORED_VALUE_BYTES,
      verifyOnOpen: options.verifyOnOpen ?? false,
    }).pipe(
      Effect.mapError((error) =>
        DoStorageError.make({
          cause: error,
          operation: "configure Durable Object storage",
          message: error.message,
        }),
      ),
    ),
  );

/** The failpoint Layer selected by convenience options: explicit handler or the no-op default. */
export const storageFailpointLayer = (
  options: DoStorageOptions,
): Layer.Layer<DoStorageFailpoint> =>
  options.failpoint === undefined
    ? DoStorageFailpoint.layer
    : Layer.succeed(DoStorageFailpoint)({ hit: options.failpoint });

/**
 * A composition-root convenience Layer for canonical Threads inside one Durable Object,
 * built over `ctx.storage`. Durable accepted work is served by the separate SubmissionLedger
 * port; point both at the SAME `ctx.storage` so claims fence the same producer epochs
 * (ADR-0011 D7's "same file" rule, transposed to one object's private database).
 */
export const layer = (
  options: DoStorageOptions,
): Layer.Layer<ThreadStore | ThreadReader | ThreadImport, DoStorageInitializationError> =>
  Layer.unwrap(
    Effect.map(DoStorageConfig, (config) =>
      threadStoreLayer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(DoStorageConfig)(config),
            storageFailpointLayer(options),
            SqliteClient.layer({ storage: options.storage }),
            BrowserCrypto.layer,
          ),
        ),
      ),
    ),
  ).pipe(Layer.provide(storageConfigLayer(options)));

/** Read a quiesced current-layout Object without initializing its layout or ownership. */
export const exportThread = Effect.fn("DoThreadStore.exportThread")(function* (
  options: Pick<DoStorageOptions, "storage">,
  request: ThreadExportRequest,
) {
  return yield* Effect.gen(function* () {
    const sql = yield* SqlClientService.SqlClient;
    const state = yield* ownedState(sql);

    return yield* state
      .transaction(
        Effect.gen(function* () {
          yield* readDoStorageHeader();

          const transfer = yield* makeSqlThreadImport({
            offsetPrefix: DO_OFFSET_PREFIX,
            read: (body) => body,
            write: () => Effect.die("A read-only exporter cannot import"),
          });

          return yield* transfer.export(request);
        }),
      )
      .pipe(
        Effect.catchTag("SqlError", (cause) =>
          DoStorageError.make({
            operation: "export Thread snapshot",
            message: cause.message,
            cause,
          }),
        ),
      );
  }).pipe(
    Effect.provide(
      Layer.merge(SqliteClient.layer({ storage: options.storage }), BrowserCrypto.layer),
    ),
  );
});

/**
 * Discard the Object's derived thread and ledger state after direct SQL maintenance.
 * Quiesce port operations during the raw write and invalidation, and enroll the write with
 * the host mutation gate. Ordinary ThreadStore/SubmissionLedger writes maintain this view.
 */
export const invalidate = invalidateOwnedState;

/** Share the Object's transaction gate with owner-local SQL Memory compositions. */
export const sqlOwnerLayer = Layer.effect(SqlStorageOwner)(
  Effect.flatMap(SqlClientService.SqlClient, ownedState),
);
