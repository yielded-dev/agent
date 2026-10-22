/**
 * Bounded physical ranges of complete canonical batches and the storage-owner maintenance port used
 * to seal, publish, and verify them. Archiving preserves Thread order and unresolved obligations.
 *
 * @since 0.1.0
 */

import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { ThreadId } from "../core/Identifiers.ts";
import { CanonicalSequence } from "./Records.ts";
import type { ThreadNotMaterialized, ThreadStoreError } from "./ThreadStore.ts";

/**
 * Maximum number of canonical records in one sealed archive range: 1,024.
 *
 * @category constants
 * @since 0.1.0
 */
export const MAX_ARCHIVE_RANGE_RECORDS = 1_024;
/**
 * Maximum encoded size of one sealed archive range: 32 MiB.
 *
 * @category constants
 * @since 0.1.0
 */
export const MAX_ARCHIVE_RANGE_BYTES = 32 * 1024 * 1024;
/**
 * Maximum encoded size of one atomic canonical batch: 16 MiB.
 *
 * @category constants
 * @since 0.1.0
 */
export const MAX_CANONICAL_BATCH_BYTES = 16 * 1024 * 1024;
/**
 * Maximum number of archive ranges returned in one page: 32.
 *
 * @category constants
 * @since 0.1.0
 */
export const MAX_ARCHIVE_RANGE_PAGE = 32;

/**
 * A physical range of complete batches in the continuous Thread log; never an execution epoch.
 *
 * @category models
 * @since 0.1.0
 */
export class ThreadArchiveRange extends Schema.Class<ThreadArchiveRange>(
  "@effect-agent/thread/ThreadArchiveRange",
)({
  format: Schema.Literal("effect-agent/thread-range@2"),
  threadId: ThreadId,
  firstSequence: CanonicalSequence.check(Schema.isGreaterThan(0)),
  lastSequence: CanonicalSequence.check(Schema.isGreaterThan(0)),
  recordCount: Schema.Int.check(
    Schema.isBetween({ minimum: 1, maximum: MAX_ARCHIVE_RANGE_RECORDS }),
  ),
  batchCount: Schema.Int.check(
    Schema.isBetween({ minimum: 1, maximum: MAX_ARCHIVE_RANGE_RECORDS }),
  ),
  byteCount: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: MAX_ARCHIVE_RANGE_BYTES })),
  state: Schema.Literals(["sealed", "archived"]),
  /** Published only after the exact archive contents have been validated for identity, order and completeness. */
  locator: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(4_096))),
}) {}

/**
 * Keyset request for a Thread's physical archive ranges after an optional canonical sequence.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ThreadArchiveRangeRead = Schema.Struct({
  threadId: ThreadId,
  afterSequence: Schema.optionalKey(CanonicalSequence),
  limit: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: MAX_ARCHIVE_RANGE_PAGE })),
  ),
});

/**
 * Decoded value of {@link ThreadArchiveRangeRead}.
 *
 * @category models
 * @since 0.1.0
 */
export type ThreadArchiveRangeRead = typeof ThreadArchiveRangeRead.Type;

/**
 * A bounded archive-range listing with an optional sequence cursor for the next page.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ThreadArchiveRangePage = Schema.Struct({
  ranges: Schema.Array(ThreadArchiveRange).check(Schema.isMaxLength(MAX_ARCHIVE_RANGE_PAGE)),
  afterSequence: Schema.optionalKey(CanonicalSequence),
});

/**
 * Decoded value of {@link ThreadArchiveRangePage}.
 *
 * @category models
 * @since 0.1.0
 */
export type ThreadArchiveRangePage = typeof ThreadArchiveRangePage.Type;

/**
 * Requests sealing of the next complete-batch range for one Thread.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ThreadArchiveRangeSeal = Schema.Struct({
  threadId: ThreadId,
});

/**
 * Decoded value of {@link ThreadArchiveRangeSeal}.
 *
 * @category models
 * @since 0.1.0
 */
export type ThreadArchiveRangeSeal = typeof ThreadArchiveRangeSeal.Type;

/**
 * Addresses a sealed range by Thread and its first canonical sequence.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ThreadArchiveRangeRequest = Schema.Struct({
  threadId: ThreadId,
  firstSequence: CanonicalSequence.check(Schema.isGreaterThan(0)),
});

/**
 * Decoded value of {@link ThreadArchiveRangeRequest}.
 *
 * @category models
 * @since 0.1.0
 */
export type ThreadArchiveRangeRequest = typeof ThreadArchiveRangeRequest.Type;

/**
 * Requests durable publication of the sealed range addressed by Thread and first sequence.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ThreadArchiveRangePublish = Schema.Struct({
  ...ThreadArchiveRangeRequest.fields,
});

/**
 * Decoded value of {@link ThreadArchiveRangePublish}.
 *
 * @category models
 * @since 0.1.0
 */
export type ThreadArchiveRangePublish = typeof ThreadArchiveRangePublish.Type;

type ReadFailure = ThreadStoreError | ThreadNotMaterialized;

/**
 * Storage-owner maintenance. Rotation retains the same canonical order, owner,
 * live obligations, and exact identities. Archive publication must preserve a reachable
 * hot copy until durable contents and their locator have passed integrity verification.
 *
 * @category models
 * @since 0.1.0
 */
export interface ThreadArchiveStorage {
  readonly page: (
    request: ThreadArchiveRangeRead,
  ) => Effect.Effect<ThreadArchiveRangePage, ReadFailure>;
  /** Explicitly seal the next bounded complete-batch range. Ordinary appends create no range rows. */
  readonly seal: (
    request: ThreadArchiveRangeSeal,
  ) => Effect.Effect<Option.Option<ThreadArchiveRange>, ReadFailure>;
  readonly archive: (
    request: ThreadArchiveRangePublish,
  ) => Effect.Effect<ThreadArchiveRange, ReadFailure>;
  readonly verify: (
    request: ThreadArchiveRangeRequest,
  ) => Effect.Effect<ThreadArchiveRange, ReadFailure>;
}
