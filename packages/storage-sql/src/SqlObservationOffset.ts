import { CanonicalSequence, type ObservationOffset } from "@yielded/agent/records";
import { ThreadStoreError, type ThreadMaterialization } from "@yielded/agent/thread-store";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const OffsetText = Schema.String.check(Schema.isMaxLength(4 * 1024));
const ZERO_CANONICAL_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);

/** Encode the shared wire syntax; callers retain their native offset validation. */
export const formatObservationOffset = (
  offsetPrefix: string,
  threadId: ThreadMaterialization["threadId"],
  sequence: CanonicalSequence,
): string => `${offsetPrefix}${encodeURIComponent(threadId)}:${sequence}`;

/** Decode the existing offset format while preserving the adapter's schema diagnostics. */
export const makeObservationOffsetParser = (
  offsetPrefix: string,
  schemaStoreError: (operation: string, error: Schema.SchemaError) => ThreadStoreError,
) =>
  Effect.fnUntraced(function* (
    threadId: ThreadMaterialization["threadId"],
    offset: ObservationOffset | undefined,
  ): Effect.fn.Return<CanonicalSequence, ThreadStoreError> {
    if (offset === undefined) return ZERO_CANONICAL_SEQUENCE;

    const text = yield* Schema.decodeEffect(OffsetText)(offset).pipe(
      Effect.mapError((error) => schemaStoreError("decode observation offset", error)),
    );

    const threadPrefix = `${offsetPrefix}${encodeURIComponent(threadId)}:`;

    if (!text.startsWith(threadPrefix)) {
      return yield* ThreadStoreError.make({
        operation: "decode observation offset",
        message:
          "The observation offset belongs to a different adapter, storage version, or Thread.",
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
