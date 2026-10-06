import { Effect, Layer, Schema, Stream } from "effect";

import type { ThreadId } from "../core/Identifiers.ts";
import {
  ContextHistory,
  ContextHistoryError,
  ContextHistoryHit,
  ContextHistoryPage,
  ContextHistoryRead,
  ContextHistorySearch,
} from "../engine/ContextHistory.ts";
import { CanonicalRecordEnvelope, CanonicalSequence, RecordId } from "./Records.ts";
import type {
  ContextHistoryBoundary,
  ContextHistoryEvidence,
} from "./ThreadContextHistoryProjection.ts";
import {
  project,
  normalizeQuery,
  matchText,
  windowIdFor,
} from "./ThreadContextHistoryProjection.ts";
import {
  SelectedThreadRead,
  ThreadRead,
  ThreadStore,
  ThreadTail,
  ThreadTailRequest,
} from "./ThreadStore.ts";

/** Per-search work and deadline bounds; exact archived reads are independent of Thread age. */
export const ThreadContextHistoryOptions = Schema.Struct({
  maxRecords: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65_536 })),
  ),
  timeoutMillis: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 300_000 })),
  ),
});

export type ThreadContextHistoryOptions = typeof ThreadContextHistoryOptions.Type;

const unavailable = () =>
  ContextHistoryError.make({
    reason: "unavailable",
    message: "Canonical context history is unavailable",
  });

const invalid = (message: string) => ContextHistoryError.make({ reason: "invalid-input", message });

/**
 * Provides bounded lexical search and paged reads over retained canonical transcript evidence.
 * Each operation captures one tail. Search examines at most `maxRecords` (default 16,384),
 * newest first in pages of 8, plus one bounded tail page and exact anchor/window lookups. A
 * `timeoutMillis` deadline defaults to 10 seconds. Exhausting the search budget before finding
 * a complete result page fails explicitly. No mutable archive or background work is created.
 * An exact `beforeRecordId` lookup starts the search immediately before that retained evidence;
 * unrelated newer history does not consume its scan budget. Storage verification owns global
 * integrity; search validates the canonical pages and native window locators it reads.
 *
 * Rollover coverage boundaries assign subsequent records to the new window, including later Runs.
 * Before the first rollover, each Run uses its initial `context:<runId>:0` identity. Pruning and
 * summarization never remove searchable source records. Tool results remain exactly as retained,
 * including any truncation envelope; discarded output bytes and transient recall are unavailable.
 *
 * The host must supply an authorized ThreadStore. This adapter does not grant access based on a
 * Thread ID. Context Tools select the current Run's Thread, never a model-selected Thread.
 */
export const layer = (
  options: ThreadContextHistoryOptions = {},
): Layer.Layer<ContextHistory, ContextHistoryError, ThreadStore> =>
  Layer.effect(
    ContextHistory,
    Effect.gen(function* () {
      const decoded = yield* Schema.decodeEffect(ThreadContextHistoryOptions)(options).pipe(
        Effect.mapError(() => invalid("Invalid context history limits")),
      );

      const maxRecords = decoded.maxRecords ?? 16_384;
      const timeoutMillis = decoded.timeoutMillis ?? 10_000;
      const store = yield* ThreadStore;

      const captureTail = (threadId: ThreadId) =>
        store.inspectTail(ThreadTailRequest.make({ threadId })).pipe(
          Effect.mapError(unavailable),
          Effect.flatMap((value) =>
            Schema.decodeEffect(Schema.toType(ThreadTail))(value).pipe(
              Effect.mapError(unavailable),
            ),
          ),
          Effect.filterOrFail((tail) => tail.threadId === threadId, unavailable),
        );

      const lookup = Effect.fnUntraced(function* (
        threadId: ThreadId,
        recordId: string,
        through: CanonicalSequence,
      ) {
        const records = yield* store
          .read(
            SelectedThreadRead.make({
              threadId,
              selection: {
                _tag: "RecordId",
                recordId: yield* Schema.decodeEffect(RecordId)(recordId).pipe(
                  Effect.mapError(() => invalid("Invalid retained record identity")),
                ),
              },
              page: { limit: 2 },
            }),
          )
          .pipe(Stream.take(3), Stream.runCollect, Effect.mapError(unavailable));

        if (records.length > 1) return yield* unavailable();
        const entry = records[0];

        if (
          entry !== undefined &&
          (entry.threadId !== threadId || entry.record.recordId !== recordId)
        )
          return yield* unavailable();

        const selected =
          entry === undefined || entry.sequence > through
            ? undefined
            : (yield* project(entry)).evidence;

        if (selected === undefined)
          return yield* ContextHistoryError.make({
            reason: "not-found",
            message: "Retained context record was not found in this Thread",
          });

        return selected;
      });

      const windowId = Effect.fnUntraced(function* (
        threadId: ThreadId,
        selected: ContextHistoryEvidence,
        through: CanonicalSequence,
      ) {
        const records = yield* store
          .read(
            SelectedThreadRead.make({
              threadId,
              selection: {
                _tag: "ContextWindowBoundary",
                atSequence: selected.sequence,
                throughSequence: through,
              },
              page: { limit: 2 },
            }),
          )
          .pipe(Stream.take(3), Stream.runCollect, Effect.mapError(unavailable));

        if (records.length > 1) return yield* unavailable();
        const boundaries: Array<ContextHistoryBoundary> = [];

        for (const entry of records) {
          if (entry.threadId !== threadId || entry.sequence > through) return yield* unavailable();
          const boundary = (yield* project(entry)).boundary;

          if (boundary === undefined || boundary.coversThrough >= selected.sequence)
            return yield* unavailable();
          boundaries.push(boundary);
        }

        return windowIdFor(selected, boundaries);
      });

      const page = Effect.fnUntraced(function* (
        threadId: ThreadId,
        after: number,
        through: number,
      ) {
        const limit = through - after;

        if (limit === 0)
          return {
            after,
            through,
            projected: [],
            firstCoverage: undefined,
            lastCoverage: undefined,
          };

        const records = yield* store
          .read(ThreadRead.make({ threadId, afterSequence: CanonicalSequence.make(after), limit }))
          .pipe(Stream.take(limit + 1), Stream.runCollect, Effect.mapError(unavailable));

        if (records.length !== limit) return yield* unavailable();

        const projected: Array<{
          readonly sequence: number;
          readonly evidence: ContextHistoryEvidence | undefined;
        }> = [];

        let firstCoverage: number | undefined;
        let lastCoverage: number | undefined;

        for (let index = 0; index < records.length; index++) {
          const record = yield* Schema.decodeEffect(Schema.toType(CanonicalRecordEnvelope))(
            records[index],
          ).pipe(Effect.mapError(unavailable));

          if (record.threadId !== threadId || record.sequence !== after + index + 1)
            return yield* unavailable();
          const value = yield* project(record);

          if (value.boundary !== undefined) {
            if (lastCoverage !== undefined && value.boundary.coversThrough < lastCoverage)
              return yield* unavailable();
            firstCoverage ??= value.boundary.coversThrough;
            lastCoverage = value.boundary.coversThrough;
          }
          projected.push({ sequence: record.sequence, evidence: value.evidence });
        }

        return { after, through, projected, firstCoverage, lastCoverage };
      });

      const search = Effect.fnUntraced(
        function* (input: ContextHistorySearch) {
          const request = yield* Schema.decodeEffect(Schema.toType(ContextHistorySearch))(
            input,
          ).pipe(Effect.mapError(() => invalid("Invalid context history search")));

          const query = yield* normalizeQuery(request.query);
          const tail = yield* captureTail(request.threadId);

          const head = yield* page(
            request.threadId,
            Math.max(0, tail.tailSequence - 8),
            tail.tailSequence,
          );

          const anchor =
            request.beforeRecordId === undefined
              ? undefined
              : yield* lookup(request.threadId, request.beforeRecordId, tail.tailSequence);

          let cursor = anchor === undefined ? tail.tailSequence : anchor.sequence - 1;
          let examined = 0;
          let laterCoverage = head.firstCoverage;
          const matches: Array<ContextHistoryHit> = [];

          while (cursor > 0 && examined < maxRecords && matches.length < request.limit) {
            const limit = Math.min(8, cursor, maxRecords - examined);
            const after = cursor - limit;

            const block =
              after >= head.after && cursor <= head.through
                ? {
                    ...head,
                    projected: head.projected.filter(
                      (item) => item.sequence > after && item.sequence <= cursor,
                    ),
                  }
                : yield* page(request.threadId, after, cursor);

            if (after < head.after) {
              if (
                cursor <= head.after &&
                laterCoverage !== undefined &&
                block.lastCoverage !== undefined &&
                block.lastCoverage > laterCoverage
              )
                return yield* unavailable();
              laterCoverage = block.firstCoverage ?? laterCoverage;
            }
            for (const { evidence } of block.projected.toReversed()) {
              examined++;
              if (evidence === undefined) continue;
              const text = matchText(evidence.text, query);

              if (text === undefined) continue;
              matches.push(
                ContextHistoryHit.make({
                  recordId: evidence.recordId,
                  windowId: yield* windowId(request.threadId, evidence, tail.tailSequence),
                  text,
                }),
              );
              if (matches.length === request.limit) break;
            }
            cursor = after;
          }
          if (cursor > 0 && matches.length < request.limit)
            return yield* ContextHistoryError.make({
              reason: "limit",
              message: `Context history search exceeded its ${maxRecords} record scan budget`,
            });

          return matches;
        },
        Effect.timeoutOrElse({
          duration: timeoutMillis,
          orElse: () =>
            Effect.fail(
              ContextHistoryError.make({
                reason: "limit",
                message: "Context history search exceeded its time limit",
              }),
            ),
        }),
      );

      const read = Effect.fnUntraced(
        function* (input: ContextHistoryRead) {
          const request = yield* Schema.decodeEffect(Schema.toType(ContextHistoryRead))(input).pipe(
            Effect.mapError(() => invalid("Invalid context history read")),
          );

          const tail = yield* captureTail(request.threadId);
          const selected = yield* lookup(request.threadId, request.recordId, tail.tailSequence);
          const selectedWindowId = yield* windowId(request.threadId, selected, tail.tailSequence);

          if (request.offset > selected.text.length)
            return yield* invalid("Context history offset is beyond the retained record");
          const end = Math.min(selected.text.length, request.offset + request.maxChars);

          return ContextHistoryPage.make({
            recordId: selected.recordId,
            windowId: selectedWindowId,
            text: selected.text.slice(request.offset, end),
            nextOffset: end < selected.text.length ? end : null,
          });
        },
        Effect.timeoutOrElse({
          duration: timeoutMillis,
          orElse: () =>
            Effect.fail(
              ContextHistoryError.make({
                reason: "limit",
                message: "Context history read exceeded its time limit",
              }),
            ),
        }),
      );

      return ContextHistory.of({ search, read });
    }),
  );
