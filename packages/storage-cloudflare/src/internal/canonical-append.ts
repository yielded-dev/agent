import { prepareSqlAppend } from "@yielded/agent-storage-sql/sql-thread-store";
import type { AppendRequest } from "@yielded/agent/thread-store";
import { PreparedAppend } from "@yielded/agent/thread-store";
import * as Effect from "effect/Effect";

/** @internal */
export const DO_OFFSET_PREFIX = "effect-agent-do@1:";

/** Prepare the publisher's privately captured canonical request before the writer.
 * @internal
 */
export const prepareCanonicalAppend = Effect.fnUntraced(function* (input: AppendRequest) {
  const request: PreparedAppend = yield* PreparedAppend.capture(input);

  const raw = yield* prepareSqlAppend(request, DO_OFFSET_PREFIX);

  return { request, raw };
});
