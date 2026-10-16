import type { Effect, Option } from "effect";
import * as Schema from "effect/Schema";

import { ThreadId } from "../core/Identifiers.ts";
import { CanonicalSequence } from "./Records.ts";
import type { ThreadNotMaterialized, ThreadStoreError } from "./ThreadStore.ts";

export const MAX_ARCHIVE_RANGE_RECORDS = 1_024;
export const MAX_ARCHIVE_RANGE_BYTES = 32 * 1024 * 1024;
export const MAX_CANONICAL_BATCH_BYTES = 16 * 1024 * 1024;
export const MAX_ARCHIVE_RANGE_PAGE = 32;

/** A physical range of complete batches in the continuous Thread log; never an execution epoch. */
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

export const ThreadArchiveRangeRead = Schema.Struct({
  threadId: ThreadId,
  afterSequence: Schema.optionalKey(CanonicalSequence),
  limit: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: MAX_ARCHIVE_RANGE_PAGE })),
  ),
});

export type ThreadArchiveRangeRead = typeof ThreadArchiveRangeRead.Type;

export const ThreadArchiveRangePage = Schema.Struct({
  ranges: Schema.Array(ThreadArchiveRange).check(Schema.isMaxLength(MAX_ARCHIVE_RANGE_PAGE)),
  afterSequence: Schema.optionalKey(CanonicalSequence),
});

export type ThreadArchiveRangePage = typeof ThreadArchiveRangePage.Type;

export const ThreadArchiveRangeSeal = Schema.Struct({
  threadId: ThreadId,
});

export type ThreadArchiveRangeSeal = typeof ThreadArchiveRangeSeal.Type;

export const ThreadArchiveRangeRequest = Schema.Struct({
  threadId: ThreadId,
  firstSequence: CanonicalSequence.check(Schema.isGreaterThan(0)),
});

export type ThreadArchiveRangeRequest = typeof ThreadArchiveRangeRequest.Type;

export const ThreadArchiveRangePublish = Schema.Struct({
  ...ThreadArchiveRangeRequest.fields,
});

export type ThreadArchiveRangePublish = typeof ThreadArchiveRangePublish.Type;

type ReadFailure = ThreadStoreError | ThreadNotMaterialized;

/**
 * Storage-owner maintenance. Rotation retains the same canonical order, owner,
 * live obligations, and exact identities. Archive publication must preserve a reachable
 * hot copy until durable contents and their locator have passed integrity verification.
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
