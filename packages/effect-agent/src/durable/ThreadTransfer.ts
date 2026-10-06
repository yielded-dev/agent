import { Context, Effect, Schema } from "effect";

import type { ThreadId } from "../core/Identifiers.ts";
import { canonicalJson, digestJson, EMPTY_TAIL_DIGEST, utf8ByteLength } from "./Digest.ts";
import { transferRecordDependencies } from "./internal/transfer-dependencies.ts";
import { ExportRecord } from "./RecordFormat.ts";
import {
  type RecordId,
  CURRENT_RECORD_FORMAT,
  Digest,
  CanonicalSequence,
  type CanonicalRecordEnvelope,
} from "./Records.ts";
import {
  MAX_THREAD_EXPORT_PAGE_BYTES,
  ThreadExport,
  ThreadExportRequest,
  ThreadExportSnapshot,
  type ThreadNotMaterialized,
  ThreadStoreError,
} from "./ThreadStore.ts";

export const transferSections = [
  "admissions",
  "aborts",
  "approvals",
  "resolutions",
  "deliveries",
] as const;

export type TransferSection = (typeof transferSections)[number];
export type TransferFacts = Pick<ThreadExport, "admissions" | "commands" | "deliveries">;

export type TransferManifest = Pick<
  ThreadExport,
  | "threadId"
  | "format"
  | "tailSequence"
  | "tailDigest"
  | "snapshot"
  | "externalObligations"
  | "workerSeal"
>;

/** Reads share the adapter's single page transaction. Batch reads hydrate archives. */
export class ThreadExporterReader extends Context.Service<
  ThreadExporterReader,
  {
    readonly snapshot: (
      threadId: ThreadId,
    ) => Effect.Effect<TransferManifest, ThreadStoreError | ThreadNotMaterialized>;
    readonly batch: (
      threadId: ThreadId,
      fromSequence: number,
    ) => Effect.Effect<
      Pick<ThreadExport, "records" | "batches">,
      ThreadStoreError | ThreadNotMaterialized
    >;
    /** A keyset read of at most 256 facts, subject to the transfer byte bound. */
    readonly facts: (
      threadId: ThreadId,
      section: TransferSection,
      after: string | undefined,
    ) => Effect.Effect<
      {
        readonly facts: TransferFacts;
        readonly after: string;
      },
      ThreadStoreError
    >;
  }
>()("@effect-agent/thread/ThreadExporterReader") {}

const Position = Schema.Struct({
  manifest: Schema.Struct({
    threadId: ThreadExport.fields.threadId,
    format: ThreadExport.fields.format,
    tailSequence: ThreadExport.fields.tailSequence,
    tailDigest: Digest,
    snapshot: ThreadExportSnapshot,
    workerSeal: ThreadExport.fields.workerSeal,
    externalObligations: ThreadExport.fields.externalObligations,
  }),
  snapshotId: Digest,
  fromSequence: Schema.Int.check(Schema.isGreaterThan(0)),
  previousTailDigest: Digest,
  section: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 5 })),
  consumed: Schema.Natural,
  after: Schema.optionalKey(Schema.String),
});

const failure = (message: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation: "Thread transfer",
    message,
    ...(cause === undefined ? {} : { cause }),
  });

const nextSection = (position: typeof Position.Type) => {
  const { after: _, ...retained } = position;

  return { ...retained, section: position.section + 1, consumed: 0 };
};

export const transferSnapshotId = (manifest: TransferManifest) =>
  Schema.encodeEffect(Position.fields.manifest)(manifest).pipe(
    Effect.mapError(() => failure("Invalid transfer manifest")),
    Effect.flatMap(digestJson),
    Effect.mapError(() => failure("Cannot digest transfer snapshot")),
  );

/** Exact evidence references outside this page, including references into archived ranges. */
export const transferDependencies = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
): ReadonlyArray<RecordId> => {
  return transferRecordDependencies(records.map((entry) => entry.record));
};

export const checkTransferPageBytes = (page: ThreadExport) =>
  Schema.encodeEffect(ThreadExport)(page).pipe(
    Effect.mapError(() => failure("Transfer page does not encode its canonical wire")),
    Effect.flatMap((wire) =>
      utf8ByteLength(canonicalJson(wire)) <= MAX_THREAD_EXPORT_PAGE_BYTES
        ? Effect.void
        : Effect.fail(failure("Transfer page exceeds 32 MiB")),
    ),
  );

/** Opaque cursors pin the tail AND every independently mutable fact owner. No lifetime collection. */
export const exportThreadPage = Effect.fnUntraced(function* (input: ThreadExportRequest) {
  const reader = yield* ThreadExporterReader;

  const request = yield* Schema.decodeEffect(ThreadExportRequest)(input).pipe(
    Effect.mapError(() => failure("Invalid export request")),
  );

  const manifest = yield* reader.snapshot(request.threadId);
  const snapshotId = yield* transferSnapshotId(manifest);

  let position =
    request.cursor === undefined
      ? {
          manifest,
          snapshotId,
          fromSequence: 1,
          previousTailDigest: EMPTY_TAIL_DIGEST,
          section: 0,
          consumed: 0,
        }
      : yield* Schema.decodeEffect(Schema.fromJsonString(Position))(request.cursor).pipe(
          Effect.mapError((cause) => failure("Invalid transfer cursor", cause)),
        );

  if (position.snapshotId !== snapshotId || position.manifest.threadId !== request.threadId)
    return yield* failure("Source changed during transfer; restart from its first page");
  if (
    manifest.format !== CURRENT_RECORD_FORMAT ||
    position.fromSequence > manifest.tailSequence + 1
  )
    return yield* failure("Unsupported format or invalid canonical transfer position");
  let canonical: Pick<ThreadExport, "records" | "batches"> = { records: [], batches: [] };

  let facts: TransferFacts = {
    admissions: [],
    commands: { aborts: [], approvals: [], resolutions: [] },
    deliveries: [],
  };

  const fromSequence = position.fromSequence;
  const previousTailDigest = position.previousTailDigest;

  if (fromSequence <= manifest.tailSequence) {
    canonical = yield* reader.batch(request.threadId, fromSequence);
    if (
      canonical.batches.length !== 1 ||
      canonical.records.length === 0 ||
      canonical.records.length > 256
    )
      return yield* failure("A transfer page must contain a complete bounded canonical batch");
    const batch = canonical.batches[0]!;

    for (const [index, entry] of canonical.records.entries())
      if (
        entry.threadId !== request.threadId ||
        entry.sequence !== fromSequence + index ||
        entry.batchId !== batch.batchId
      )
        return yield* failure("Canonical batch identity or sequence mismatch");

    const records = yield* Effect.forEach(canonical.records, (entry) =>
      Schema.encodeEffect(ExportRecord)(entry.record).pipe(
        Effect.mapError(() => failure("Canonical record lost its exact wire")),
      ),
    );

    const tail = yield* digestJson({ previousTailDigest, batch: { ...batch, records } }).pipe(
      Effect.mapError(() => failure("Cannot digest transfer batch")),
    );

    position = {
      ...position,
      fromSequence: fromSequence + records.length,
      previousTailDigest: tail,
    };
    if (position.fromSequence > manifest.tailSequence && tail !== manifest.tailDigest)
      return yield* failure("Canonical transfer chain does not match its captured tail");
  } else {
    while (position.section < transferSections.length) {
      const section = transferSections[position.section]!;

      if (position.consumed > manifest.snapshot[section])
        return yield* failure("Transfer fact cursor exceeds its captured count");
      if (position.consumed === manifest.snapshot[section]) {
        const probe = yield* reader.facts(request.threadId, section, position.after);

        const remaining =
          section === "admissions"
            ? probe.facts.admissions.length
            : section === "deliveries"
              ? probe.facts.deliveries.length
              : probe.facts.commands[section].length;

        if (remaining !== 0) return yield* failure("Native transfer count omits retained facts");
        position = nextSection(position);
        continue;
      }
      const page = yield* reader.facts(request.threadId, section, position.after);

      facts = page.facts;

      const count =
        section === "admissions"
          ? facts.admissions.length
          : section === "deliveries"
            ? facts.deliveries.length
            : facts.commands[section].length;

      if (
        count === 0 ||
        count > 256 ||
        page.after === position.after ||
        position.consumed + count > manifest.snapshot[section]
      )
        return yield* failure("Transfer fact page is missing or exceeds its captured count");
      position = { ...position, consumed: position.consumed + count, after: page.after };
      break;
    }
  }
  while (
    position.section < transferSections.length &&
    position.consumed === manifest.snapshot[transferSections[position.section]!]
  ) {
    const section = transferSections[position.section]!;
    const probe = yield* reader.facts(request.threadId, section, position.after);

    const count =
      section === "admissions"
        ? probe.facts.admissions.length
        : section === "deliveries"
          ? probe.facts.deliveries.length
          : probe.facts.commands[section].length;

    if (count !== 0) return yield* failure("Native transfer count omits retained facts");
    position = nextSection(position);
  }

  const more =
    position.fromSequence <= manifest.tailSequence || position.section < transferSections.length;

  if (!more && position.previousTailDigest !== manifest.tailDigest)
    return yield* failure("Transfer tail differs from the captured snapshot");

  const cursor = more
    ? yield* Schema.encodeEffect(Schema.fromJsonString(Position))(position).pipe(
        Effect.mapError((cause) => failure("Cannot encode transfer cursor", cause)),
      )
    : undefined;

  const page = yield* ThreadExport.makeEffect({
    ...manifest,
    transferFormat: "effect-agent/thread-transfer@1",
    snapshotId,
    fromSequence: Schema.decodeSync(CanonicalSequence)(fromSequence),
    previousTailDigest,
    ...canonical,
    ...facts,
    dependencies: transferDependencies(canonical.records),
    ...(cursor === undefined ? {} : { cursor }),
  }).pipe(Effect.mapError(() => failure("Transfer page exceeds its schema bounds")));

  yield* checkTransferPageBytes(page);

  return page;
});
