import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type * as Stream from "effect/Stream";

import type { ThreadId } from "../../core/Identifiers.ts";
import type { CanonicalRecordEnvelope, RecordId } from "../Records.ts";
import {
  getRecord,
  ThreadReader,
  type ThreadNotMaterialized,
  type ThreadStoreError,
  type ThreadPromptRead,
} from "../ThreadStore.ts";
import type { JournalRecordEnvelope } from "./journal-metadata.ts";

/** Canonical history access bound by the recovery or verification-snapshot owner. */
export class RunContextReader extends Context.Service<
  RunContextReader,
  {
    /** Recovery decodes the prompt view; verification supplies full canonical envelopes. */
    readonly read: (
      request: ThreadPromptRead,
    ) => Stream.Stream<JournalRecordEnvelope, ThreadStoreError | ThreadNotMaterialized>;
    readonly record: (
      recordId: RecordId,
    ) => Effect.Effect<
      CanonicalRecordEnvelope | undefined,
      ThreadStoreError | ThreadNotMaterialized
    >;
  }
>()("@effect-agent/thread/RunContextReader") {
  static layer(threadId: ThreadId) {
    return Layer.effect(
      RunContextReader,
      Effect.gen(function* () {
        const reader = yield* ThreadReader;

        return RunContextReader.of({
          read: reader.readPrompt,
          record: (recordId) =>
            getRecord({ threadId, recordId }).pipe(
              Effect.provideService(ThreadReader, reader),
              Effect.map(Option.getOrUndefined),
            ),
        });
      }),
    );
  }
}
