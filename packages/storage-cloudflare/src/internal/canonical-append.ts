import { Crypto, Effect, Schema } from "effect";
import { digestCanonicalBatch } from "effect-agent/digest";
import { CanonicalBatch, CanonicalRecord } from "effect-agent/records";
import type { FencedAppendRequest } from "effect-agent/thread-store";
import { ThreadStoreError } from "effect-agent/thread-store";

import { RawAppendRequest } from "./do-journal.ts";

/** Prepare the publisher's privately captured canonical request before the writer. */
export const prepareCanonicalAppend = Effect.fnUntraced(function* (
  input: FencedAppendRequest,
  crypto: Crypto.Crypto,
) {
  const request = input;

  const tailDigest = yield* digestCanonicalBatch(request.expectedTailDigest, request.batch).pipe(
    Effect.provideService(Crypto.Crypto, crypto),
    Effect.mapError((cause) =>
      ThreadStoreError.make({
        operation: "digest canonical append",
        message: cause.message,
        cause,
      }),
    ),
  );

  const batchJson = yield* Schema.encodeEffect(Schema.fromJsonString(CanonicalBatch))(
    request.batch,
  ).pipe(
    Effect.mapError((cause) =>
      ThreadStoreError.make({ operation: "encode canonical batch", message: cause.message, cause }),
    ),
  );

  const records = yield* Effect.forEach(request.batch.records, (record) =>
    Schema.encodeEffect(Schema.fromJsonString(CanonicalRecord))(record).pipe(
      Effect.map((recordJson) => ({ recordId: record.recordId, recordJson })),
      Effect.mapError((cause) =>
        ThreadStoreError.make({
          operation: "encode canonical record",
          message: cause.message,
          cause,
        }),
      ),
    ),
  );

  const raw = yield* Schema.decodeEffect(RawAppendRequest)({
    threadId: request.threadId,
    producerEpoch: request.producerEpoch,
    expectedTailSequence: request.expectedTailSequence,
    expectedTailDigest: request.expectedTailDigest,
    batchId: request.batch.batchId,
    batchDigest: tailDigest,
    tailDigest,
    batchJson,
    records,
  }).pipe(
    Effect.mapError((cause) =>
      ThreadStoreError.make({
        operation: "encode canonical append",
        message: cause.message,
        cause,
      }),
    ),
  );

  return { request, raw };
});
