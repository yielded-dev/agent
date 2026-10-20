import type { R2ObjectBody } from "@cloudflare/workers-types";
import type { ThreadId } from "@yielded/agent/identifiers";
import {
  MAX_STORED_TOOL_RESULT_BYTES,
  ToolResultPage,
  ToolResultRead,
  ToolResultStorage,
  ToolResultStorageError,
} from "@yielded/agent/tool-result-storage";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

/** Native R2 surface used here, independent of Worker/DOM Headers type differences. */
export interface R2ToolResultBucket {
  readonly put: (
    key: string,
    value: Uint8Array,
    options: {
      readonly httpMetadata: { readonly contentType: string };
      readonly onlyIf: { readonly etagDoesNotMatch: string };
    },
  ) => Promise<unknown>;
  readonly get: (key: string) => Promise<
    | (Pick<R2ObjectBody, "size" | "bodyUsed" | "text"> & {
        readonly body: Pick<R2ObjectBody["body"], "cancel">;
      })
    | null
  >;
}

const unavailable = (cause?: unknown) => {
  const error = ToolResultStorageError.make({
    reason: "unavailable",
    message: "The saved tool output is temporarily unavailable.",
  });

  Object.defineProperty(error, "cause", { value: cause });

  return error;
};

const missing = () =>
  ToolResultStorageError.make({
    reason: "not-found",
    message: "This tool output is not available in the current thread.",
  });

/**
 * Private, immutable output objects scoped to one host-authorized Thread. The bucket
 * owner controls retention and cleanup of `tool-results/`, including orphan writes.
 * Keep the bucket private; an ID alone grants no authority to another Thread.
 */
export const layer = (bucket: R2ToolResultBucket, threadId: ThreadId): Layer.Layer<never> => {
  const key = (id: string) => `tool-results/${encodeURIComponent(threadId)}/${id}.json`;

  return Layer.succeed(ToolResultStorage, {
    save: Effect.fnUntraced(function* (request) {
      if (request.threadId !== threadId) return yield* missing();
      const bytes = new TextEncoder().encode(request.text);

      if (bytes.byteLength > MAX_STORED_TOOL_RESULT_BYTES) {
        return yield* ToolResultStorageError.make({
          reason: "too-large",
          message: "Full tool output exceeds the 8 MiB storage limit.",
        });
      }

      const digest = yield* Effect.tryPromise({
        try: () => crypto.subtle.digest("SHA-256", bytes),
        catch: unavailable,
      });

      const id = Array.from(new Uint8Array(digest), (byte) =>
        byte.toString(16).padStart(2, "0"),
      ).join("");

      yield* Effect.tryPromise({
        try: () =>
          bucket.put(key(id), bytes, {
            httpMetadata: { contentType: "application/json; charset=utf-8" },
            onlyIf: { etagDoesNotMatch: "*" },
          }),
        catch: unavailable,
      });

      return id;
    }),
    read: Effect.fnUntraced(function* (input) {
      const request = yield* Schema.decodeUnknownEffect(ToolResultRead)(input).pipe(
        Effect.mapError(() =>
          ToolResultStorageError.make({
            reason: "invalid-output",
            message: "Invalid saved-output read request.",
          }),
        ),
      );

      if (request.threadId !== threadId) return yield* missing();

      return yield* Effect.acquireUseRelease(
        Effect.tryPromise({ try: () => bucket.get(key(request.id)), catch: unavailable }),
        Effect.fnUntraced(function* (object) {
          if (object === null) return yield* missing();
          if (object.size > MAX_STORED_TOOL_RESULT_BYTES) return yield* unavailable();
          const text = yield* Effect.tryPromise({ try: () => object.text(), catch: unavailable });
          let offset = Math.min(request.offset, text.length);
          const first = text.charCodeAt(offset);

          if (first >= 0xdc00 && first <= 0xdfff) offset += 1;
          let end = Math.min(offset + request.maxChars, text.length);
          const last = text.charCodeAt(end - 1);

          if (end < text.length && last >= 0xd800 && last <= 0xdbff) end -= 1;

          return ToolResultPage.make({
            id: request.id,
            offset,
            text: text.slice(offset, end),
            nextOffset: end < text.length ? end : null,
          });
        }),
        (object) =>
          object === null || object.bodyUsed
            ? Effect.void
            : Effect.tryPromise({ try: () => object.body.cancel(), catch: unavailable }).pipe(
                Effect.ignore,
              ),
      );
    }),
  });
};
