import { Effect, Schema } from "effect";

import { copyJson } from "../../core/internal/json.ts";
import {
  isPersistedJson,
  MAX_PERSISTED_JSON_BYTES,
  MAX_PERSISTED_JSON_DEPTH,
} from "../../core/internal/persisted-json.ts";
import { CompactionError, NativeCompaction } from "../ContextCompactor.ts";
import { boundedCanonicalJsonSnapshot } from "./provider-result-staging.ts";

function freezeJson(value: Schema.Json): void {
  if (value === null || typeof value !== "object") return;
  for (const entry of Object.values<Schema.Json>(value)) freezeJson(entry);
  Object.freeze(value);
}

/** Effect structural equality needs iterable arrays, unlike the prototype-free bounded snapshot. */
export function ownNativeCompaction(native: NativeCompaction): NativeCompaction {
  const data = copyJson(native.context.data);

  freezeJson(data);
  Object.freeze(native.affinity);
  Object.freeze(native.usage.inputTokens);
  Object.freeze(native.usage.outputTokens);
  if (native.usage.response !== undefined) Object.freeze(native.usage.response);
  Object.freeze(native.usage);

  return Object.freeze({
    ...native,
    context: Object.freeze({ ...native.context, data }),
  });
}

const NativeCompactions = Schema.Array(NativeCompaction).check(Schema.isMaxLength(1_024));

/** Retained sidecars cannot share mutable data with a history adapter or strategy. */
export const snapshotNativeCompactions = Effect.fnUntraced(function* (
  windows: ReadonlyArray<NativeCompaction>,
): Effect.fn.Return<ReadonlyArray<NativeCompaction>, CompactionError> {
  const encoded = yield* Schema.encodeEffect(NativeCompactions)(windows).pipe(
    Effect.mapError(() => CompactionError.make({ message: "Invalid retained native context" })),
  );

  const snapshot = boundedCanonicalJsonSnapshot(
    encoded,
    MAX_PERSISTED_JSON_BYTES,
    MAX_PERSISTED_JSON_DEPTH,
  );

  if (snapshot === undefined || !isPersistedJson(snapshot.value))
    return yield* CompactionError.make({ message: "Retained native context exceeds its bounds" });

  const decoded = yield* Schema.decodeUnknownEffect(NativeCompactions)(snapshot.value).pipe(
    Effect.mapError(() => CompactionError.make({ message: "Invalid retained native context" })),
  );

  return Object.freeze(decoded.map(ownNativeCompaction));
});
