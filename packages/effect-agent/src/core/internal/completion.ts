import * as Schema from "effect/Schema";

/** The policy dimension reported by a budget-exhausted Run or Subagent. */
export const ExhaustedLimit = Schema.Literals(["tokens", "tool-calls", "turns"]);
export type ExhaustedLimit = typeof ExhaustedLimit.Type;

const CompletionMetadataFields = Schema.Struct({
  finishReason: Schema.Literals(["completed", "model-stop", "budget-exhausted"]),
  exhausted: Schema.optionalKey(ExhaustedLimit),
});

/** Reapply this check when composing the metadata fields into a larger Schema. */
export const completionMetadataCheck = Schema.makeFilter(
  ({ exhausted, finishReason }: typeof CompletionMetadataFields.Type) =>
    (finishReason === "budget-exhausted") === (exhausted !== undefined)
      ? undefined
      : '`exhausted` must be present exactly when `finishReason` is "budget-exhausted"',
);

/** Shared terminal metadata for Run results, events and interpreter commits. */
export const CompletionMetadata = CompletionMetadataFields.check(completionMetadataCheck);
export type CompletionMetadata = typeof CompletionMetadata.Type;

/** Output-bearing completions may declare an application disposition only without exhaustion. */
export const completionDispositionCheck = Schema.makeFilter(
  ({ finishReason, runDisposition }: CompletionMetadata & { readonly runDisposition?: unknown }) =>
    finishReason === "budget-exhausted" && runDisposition !== undefined
      ? {
          path: ["runDisposition"],
          issue: '`runDisposition` is not allowed when `finishReason` is "budget-exhausted"',
        }
      : undefined,
);
