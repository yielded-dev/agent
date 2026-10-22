import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  MemoryDocument,
  type MemoryKey,
  MemoryReader,
  MemoryStorageError,
} from "../MemoryStore.ts";

const decodeDocument = Schema.decodeUnknownEffect(Schema.NullOr(MemoryDocument.Wire));

/**
 * Read the current source through the host's authorized reader, then validate its schema and
 * exact namespace/key identity. Callers own visibility and stale-candidate policy; no source
 * snapshot is cached across invocations.
 * @internal
 */
export const readMemoryDocument = Effect.fnUntraced(function* (key: MemoryKey) {
  const reader = yield* MemoryReader;

  const document = yield* reader
    .get(key)
    .pipe(
      Effect.flatMap((value) =>
        decodeDocument(value).pipe(
          Effect.mapError(() =>
            MemoryStorageError.make({ operation: "validate source view", reason: "corrupt" }),
          ),
        ),
      ),
    );

  if (
    document !== null &&
    (document.key.namespace.address !== key.namespace.address ||
      document.key.id !== key.id ||
      document.source.id !== key.id)
  )
    return yield* MemoryStorageError.make({
      operation: "validate source identity",
      reason: "corrupt",
    });

  return document;
});
