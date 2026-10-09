import { canonicalBatchHeaderJson } from "@yielded/agent-storage-sql/sql-thread-archive-range";
import { canonicalRecordMetadata } from "@yielded/agent-storage-sql/sql-thread-native-reads";
import { utf8ByteLength } from "@yielded/agent/digest";
import type { FencedAppendRequest } from "@yielded/agent/thread-store";
import {
  canonicalBatchFitsTransfer,
  PreparedAppend,
  ThreadStoreError,
} from "@yielded/agent/thread-store";
import * as Effect from "effect/Effect";

import type { RawAppendRequest } from "./do-journal.ts";

export const DO_OFFSET_PREFIX = "effect-agent-do@1:";

/** Prepare the publisher's privately captured canonical request before the writer. */
export const prepareCanonicalAppend = Effect.fnUntraced(function* (input: FencedAppendRequest) {
  const request = yield* PreparedAppend.capture(input);

  const offsetBytes =
    utf8ByteLength(JSON.stringify(DO_OFFSET_PREFIX)) + 3 * utf8ByteLength(request.threadId) + 25;

  if (!canonicalBatchFitsTransfer(request.threadId, request.batch, request.batchBytes, offsetBytes))
    return yield* ThreadStoreError.make({
      operation: "prepare canonical append",
      message: "Canonical batch exceeds its bounded transfer representation",
    });

  const records = request.records.map((record) =>
    Object.freeze({ ...record, readMetadata: canonicalRecordMetadata(record) }),
  );

  const tailDigest = yield* request.digest().pipe(
    Effect.mapError((cause) =>
      ThreadStoreError.make({
        operation: "digest canonical append",
        message: cause.message,
        cause,
      }),
    ),
  );

  const raw: RawAppendRequest = {
    threadId: request.threadId,
    producerEpoch: request.producerEpoch,
    expectedTailSequence: request.expectedTailSequence,
    expectedTailDigest: request.expectedTailDigest,
    batchId: request.batch.batchId,
    batchDigest: tailDigest,
    tailDigest,
    batchJson: request.batchJson,
    batchHeaderJson: canonicalBatchHeaderJson(request.batch),
    batchBytes: request.batchBytes,
    records,
    progress: request.progress,
  };

  return { request, raw };
});
