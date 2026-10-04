import type { FencedAppendRequest } from "@yielded/agent/thread-store";
import { PreparedAppend, ThreadStoreError } from "@yielded/agent/thread-store";
import { Crypto, Effect } from "effect";

import type { RawAppendRequest } from "./do-journal.ts";

/** Prepare the publisher's privately captured canonical request before the writer. */
export const prepareCanonicalAppend = Effect.fnUntraced(function* (
  input: FencedAppendRequest,
  crypto: Crypto.Crypto,
) {
  const request = yield* PreparedAppend.capture(input);

  const tailDigest = yield* request.digest().pipe(
    Effect.provideService(Crypto.Crypto, crypto),
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
    records: request.records,
  };

  return { request, raw };
});
