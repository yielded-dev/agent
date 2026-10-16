import {
  ActivityProcessorKey,
  type ActivityProcessorStore,
  type ActivityStoreFailure,
  type PreparedActivity,
} from "@yielded/agent/activity-store";
import {
  ActivityPassLimits,
  processCommittedActivity,
  type ActivityProcessingError,
} from "@yielded/agent/committed-activity";
import { type DigestError } from "@yielded/agent/digest";
import { ThreadId } from "@yielded/agent/identifiers";
import { type PersistedJson } from "@yielded/agent/records";
import {
  type ThreadNotMaterialized,
  type ThreadStore,
  type ThreadStoreError,
} from "@yielded/agent/thread-store";
import * as Context from "effect/Context";
import type * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

type Equal<L, R> =
  (<T>() => T extends L ? 1 : 2) extends <T>() => T extends R ? 1 : 2
    ? (<T>() => T extends R ? 1 : 2) extends <T>() => T extends L ? 1 : 2
      ? true
      : false
    : false;
type Assert<T extends true> = T;

class ExtractFailure extends Schema.TaggedError<ExtractFailure>()("ExtractFailure", {}) {}
class ApplyFailure extends Schema.TaggedError<ApplyFailure>()("ApplyFailure", {}) {}
class Extractor extends Context.Service<
  Extractor,
  {
    readonly read: Effect.Effect<PersistedJson, ExtractFailure, Scope.Scope>;
  }
>()("test/activity/Extractor") {}
class Destination extends Context.Service<
  Destination,
  {
    readonly apply: (work: PreparedActivity) => Effect.Effect<void, ApplyFailure, Scope.Scope>;
  }
>()("test/activity/Destination") {}

const pass = processCommittedActivity({
  key: ActivityProcessorKey.make({
    processorId: "test",
    processorVersion: "1",
    threadId: Schema.decodeSync(ThreadId)("test"),
  }),
  limits: ActivityPassLimits.make({
    maxRecords: 1,
    pageSize: 1,
    timeoutMillis: 100,
  }),
  extract: Effect.fnUntraced(function* () {
    return yield* (yield* Extractor).read;
  }),
  apply: Effect.fnUntraced(function* (work) {
    yield* (yield* Destination).apply(work);
  }),
});

type Errors = Assert<
  Equal<
    Effect.Error<typeof pass>,
    | ExtractFailure
    | ApplyFailure
    | ActivityStoreFailure
    | ActivityProcessingError
    | DigestError
    | ThreadStoreError
    | ThreadNotMaterialized
  >
>;
type Requirements = Assert<
  Equal<
    Effect.Services<typeof pass>,
    Extractor | Destination | ActivityProcessorStore | ThreadStore | Crypto.Crypto
  >
>;

export const proof: readonly [Errors, Requirements] = [true, true];
