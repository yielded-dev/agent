import { canonicalRecordMetadata } from "@yielded/agent-storage-sql/sql-thread-native-reads";
import type { FencedAppendRequest } from "@yielded/agent/thread-store";
import { PreparedAppend, ThreadStoreError } from "@yielded/agent/thread-store";
import { Effect } from "effect";

import type { RawAppendRequest } from "./do-journal.ts";

/** Prepare the publisher's privately captured canonical request before the writer. */
export const prepareCanonicalAppend = Effect.fnUntraced(function* (input: FencedAppendRequest) {
  const request = yield* PreparedAppend.capture(input);

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
    batchBytes: request.batchBytes,
    records,
    progress: request.progress,
  };

  return { request, raw };
});
