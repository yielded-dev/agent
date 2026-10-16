import type { SubmissionId } from "@yielded/agent/identifiers";
import { CanonicalBatch } from "@yielded/agent/records";
import { RunStorage } from "@yielded/agent/run-storage";
import { ClaimRequest } from "@yielded/agent/submission-ledger";
import type { AppendRequest } from "@yielded/agent/thread-store";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import { BenchmarkError } from "./contracts.js";

/** Seed through the public owner and settlement protocol, outside operation timing. */
export const publishSeedSettlement = Effect.fn("benchmark.publishSeedSettlement")(function* (
  submissionId: SubmissionId,
  append: AppendRequest,
) {
  const storage = yield* RunStorage;
  const owned = yield* storage.claim(ClaimRequest.make({ threadId: append.threadId }));

  if (Option.isNone(owned) || owned.value.claim.submissionId !== submissionId)
    return yield* BenchmarkError.make({ message: "Ledger seed could not own its row" });
  const [first, ...rest] = append.batch.records;

  if (first === undefined)
    return yield* BenchmarkError.make({ message: "Ledger seed settlement has no canonical fact" });
  yield* owned.value.publishSettlement(
    CanonicalBatch.make({ ...append.batch, records: [first, ...rest] }),
  );
}, Effect.scoped);
