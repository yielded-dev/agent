import { Context, Effect, Schema } from "effect";

import type { ThreadId } from "../core/Identifiers.ts";
import { canonicalJson, utf8ByteLength } from "./Digest.ts";
import { ImportSourcePhase } from "./internal/thread-transfer-source.ts";
import { transferRecordDependencies } from "./internal/transfer-dependencies.ts";
import { ExportRecord } from "./RecordFormat.ts";
import {
  type RecordId,
  CURRENT_RECORD_FORMAT,
  CanonicalSequence,
  type CanonicalRecordEnvelope,
} from "./Records.ts";
import {
  MAX_THREAD_EXPORT_PAGE_BYTES,
  ThreadExport,
  ThreadExportRequest,
  ThreadExportSnapshot,
  ThreadExportRejected,
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
  "threadId" | "format" | "tailSequence" | "snapshot" | "workerSeal"
>;

/** Reads share the adapter's single page transaction. Batch reads hydrate archives. */
export class ThreadExporterReader extends Context.Service<
  ThreadExporterReader,
  {
    readonly snapshot: (
      threadId: ThreadId,
      captured?: TransferManifest,
    ) => Effect.Effect<
      TransferManifest,
      ThreadStoreError | ThreadNotMaterialized | ThreadExportRejected
    >;
    readonly batch: (
      threadId: ThreadId,
      fromSequence: number,
    ) => Effect.Effect<
      Pick<ThreadExport, "records" | "batches">,
      ThreadStoreError | ThreadNotMaterialized
    >;
    /** A keyset read of at most 256 facts. Keys stay bounded independently of fact identities. */
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

export const TransferManifest = Schema.Struct({
  threadId: ThreadExport.fields.threadId,
  format: ThreadExport.fields.format,
  tailSequence: ThreadExport.fields.tailSequence,
  snapshot: ThreadExportSnapshot,
  workerSeal: ThreadExport.fields.workerSeal,
});

const Position = Schema.Struct({
  manifest: TransferManifest,
  fromSequence: Schema.Int.check(Schema.isGreaterThan(0)),
  section: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: transferSections.length })),
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

/** Exact manifest equality replaces a redundant stored snapshot fingerprint. */
export const sameTransferManifest = Schema.toEquivalence(TransferManifest);

/** Must run BEFORE a source gate, transaction or routed RPC is acquired. */
export const checkExportSource = (threadId: ThreadId) =>
  Effect.flatMap(ImportSourcePhase, (phase) =>
    phase?.writing
      ? Effect.fail(ThreadExportRejected.make({ threadId, reason: "live-import-source" }))
      : Effect.void,
  );

/** Exact evidence references outside this canonical page. */
export const transferDependencies = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
): ReadonlyArray<RecordId> =>
  [...new Set(transferRecordDependencies(records.map((entry) => entry.record)))].sort();

const factCount = (facts: TransferFacts, section: TransferSection): number =>
  section === "admissions"
    ? facts.admissions.length
    : section === "deliveries"
      ? facts.deliveries.length
      : facts.commands[section].length;

export const checkTransferPageBytes = (page: ThreadExport) =>
  Schema.encodeEffect(ThreadExport)(page).pipe(
    Effect.mapError(() => failure("Transfer page does not encode its canonical wire")),
    Effect.flatMap((wire) =>
      utf8ByteLength(canonicalJson(wire)) <= MAX_THREAD_EXPORT_PAGE_BYTES
        ? Effect.void
        : Effect.fail(failure("Transfer page exceeds 32 MiB")),
    ),
  );

/** Stateless pages require application quiescence; no transfer counter is maintained on writes. */
export const exportThreadPage = Effect.fnUntraced(function* (input: ThreadExportRequest) {
  const reader = yield* ThreadExporterReader;

  const request = yield* Schema.decodeEffect(ThreadExportRequest)(input).pipe(
    Effect.mapError(() => failure("Invalid export request")),
  );

  const captured =
    request.cursor === undefined
      ? undefined
      : yield* Schema.decodeEffect(Schema.fromJsonString(Position))(request.cursor).pipe(
          Effect.mapError((cause) => failure("Invalid transfer cursor", cause)),
        );

  const manifest = yield* reader.snapshot(request.threadId, captured?.manifest);
  let position = captured ?? { manifest, fromSequence: 1, section: 0, consumed: 0 };

  if (!sameTransferManifest(position.manifest, manifest) || manifest.threadId !== request.threadId)
    return yield* ThreadExportRejected.make({
      threadId: request.threadId,
      reason: "source-changed",
    });
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

    position = {
      ...position,
      fromSequence: fromSequence + records.length,
    };
  } else {
    while (position.section < transferSections.length) {
      const section = transferSections[position.section]!;

      if (position.consumed > manifest.snapshot[section])
        return yield* failure("Transfer fact cursor exceeds its captured count");
      if (position.consumed === manifest.snapshot[section]) {
        const probe = yield* reader.facts(request.threadId, section, position.after);

        const remaining = factCount(probe.facts, section);

        if (remaining !== 0) return yield* failure("Native transfer count omits retained facts");
        position = nextSection(position);
        continue;
      }
      const page = yield* reader.facts(request.threadId, section, position.after);

      facts = page.facts;

      const count = factCount(facts, section);

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

    const count = factCount(probe.facts, section);

    if (count !== 0) return yield* failure("Native transfer count omits retained facts");
    position = nextSection(position);
  }

  const more =
    position.fromSequence <= manifest.tailSequence || position.section < transferSections.length;

  const cursor = more
    ? yield* Schema.encodeEffect(Schema.fromJsonString(Position))(position).pipe(
        Effect.mapError((cause) => failure("Cannot encode transfer cursor", cause)),
      )
    : undefined;

  const page = yield* ThreadExport.makeEffect({
    ...manifest,
    transferFormat: "effect-agent/thread-transfer@4",
    fromSequence: Schema.decodeSync(CanonicalSequence)(fromSequence),
    ...canonical,
    ...facts,
    dependencies: transferDependencies(canonical.records),
    ...(cursor === undefined ? {} : { cursor }),
  }).pipe(Effect.mapError(() => failure("Transfer page exceeds its schema bounds")));

  yield* checkTransferPageBytes(page);

  return page;
});
