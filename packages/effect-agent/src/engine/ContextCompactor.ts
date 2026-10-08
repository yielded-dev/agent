import type { AiError, LanguageModel, Model, Response } from "effect/ai";
import * as Prompt from "effect/ai/Prompt";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { type CompactionPolicy } from "../core/AgentPolicy.ts";
import { type RunId, type ThreadId } from "../core/Identifiers.ts";
import { isPersistedJson, PersistedJson } from "../core/internal/persisted-json.ts";
import { strictSchema } from "../core/internal/strict-schema.ts";
import { ModelCallUsage } from "../core/Usage.ts";
import { ContextHandoff, type ContextRolloverRequest } from "./ContextWindow.ts";
import {
  buildCompactedView,
  buildRolloverHandoff,
  choosePruneBound,
  chooseSummarizeCut,
  collectCoveredMessages,
  estimatePromptTokens,
  evaluateMessageTokenEstimates,
  renderForSummary,
  SUMMARY_MAX_LENGTH,
  SUMMARY_REQUEST_PREFIX,
  SUMMARY_REQUEST_SUFFIX,
  type ContextCompactionState,
} from "./internal/compaction.ts";

/** Maximum UTF-8 JSON size of one complete native replacement envelope. */
export const MAX_NATIVE_COMPACTION_BYTES = 262_144;

const NativeIdentity = Schema.NonEmptyString.check(Schema.isMaxLength(256));

/** The complete provider-owned replacement window, separate from ordinary Effect Prompt. */
export const NativeCompactionContext = Schema.Struct({
  format: NativeIdentity,
  data: PersistedJson,
  estimatedTokens: Schema.Natural,
}).pipe(strictSchema);

export type NativeCompactionContext = typeof NativeCompactionContext.Type;

const NativeCompactionFields = Schema.Struct({
  version: Schema.Literal(2),
  affinity: Schema.Struct({ provider: NativeIdentity, model: NativeIdentity }),
  context: NativeCompactionContext,
  usage: ModelCallUsage,
});

const encodeNativeCompaction = Schema.encodeSync(NativeCompactionFields);

/** Provider-issued replacement context with creator-owned, already-accounted usage. */
export const NativeCompaction = NativeCompactionFields.check(
  Schema.makeFilter(
    (native) =>
      isPersistedJson(encodeNativeCompaction(native), MAX_NATIVE_COMPACTION_BYTES) &&
      native.usage.purpose === "compaction" &&
      native.usage.usageStatus !== undefined &&
      native.usage.usageStatus !== "unknown" &&
      native.usage.pricingStatus !== undefined &&
      native.usage.webSearchCalls === 0 &&
      Number.isSafeInteger(native.usage.inputTokens.total + native.usage.outputTokens.total) &&
      native.usage.provider === native.affinity.provider &&
      native.usage.model === native.affinity.model,
    { title: "Bounded versioned native window with matching compaction accounting" },
  ),
).pipe(strictSchema);

export type NativeCompaction = typeof NativeCompaction.Type;

/** Provider output is accounted before the engine creates the authoritative native envelope. */
export interface NativeCompactionResult {
  readonly context: NativeCompactionContext;
  readonly provider: string;
  readonly model: string;
  readonly usage: Response.Usage;
  readonly responseId?: string;
}

/** A captured provider capability; replay affects only the supplied lazy model stream. */
export class NativeCompactionProvider extends Context.Service<
  NativeCompactionProvider,
  {
    readonly provider: string;
    readonly format: string;
    readonly compact: (request: {
      readonly model: string;
      readonly prompt: Prompt.Prompt;
      readonly previous: ReadonlyArray<NativeCompaction>;
    }) => Effect.Effect<NativeCompactionResult, AiError.AiError>;
    readonly validate: (window: NativeCompaction) => Effect.Effect<void, AiError.AiError>;
    readonly replay: <A, E, R>(
      stream: Stream.Stream<A, E, R>,
      request: { readonly model: string; readonly windows: ReadonlyArray<NativeCompaction> },
    ) => Stream.Stream<A, E | AiError.AiError, R>;
  }
>()("@effect-agent/engine/NativeCompactionProvider") {}

/**
 * A proposed view change. Source indices are exclusive prefix bounds, never record sequences.
 * Summaries contain at most 65,536 characters; the interpreter rejects oversized decisions
 * before committing or changing coverage.
 */
export const CompactionDecision = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("native"),
    through: Schema.Natural,
    native: NativeCompaction,
  }),
  Schema.Struct({
    kind: Schema.Literal("rollover"),
    through: Schema.Natural,
    handoff: Schema.optionalKey(ContextHandoff),
  }),
  Schema.Struct({ kind: Schema.Literal("clear-tool-results"), through: Schema.Natural }),
  Schema.Struct({
    kind: Schema.Literal("summarize"),
    through: Schema.Natural,
    summary: Schema.NonEmptyString.check(
      Schema.isMaxLength(SUMMARY_MAX_LENGTH),
      Schema.isPattern(/\S/),
    ),
  }),
]);

export type CompactionDecision = typeof CompactionDecision.Type;

/** Expected strategy or decision-validation failure. Causes stay in the live Effect only. */
export class CompactionError extends Schema.TaggedError<CompactionError>()("CompactionError", {
  message: Schema.String.check(Schema.isMaxLength(4_096)),
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

/** Upstream Model.captureRequirements returns this closed Layer. */
export type CompactionModelLayer = Layer.Layer<
  LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName
>;

/** A full message estimate, or undefined to use the native structural estimate. */
export type ContextMessageTokenEstimator = (message: Prompt.Message) => number | undefined;

/**
 * One bounded pass over an immutable source snapshot. A harness owns state, metering, and
 * application of decisions. The interpreter permits at most one prune followed by one replacement
 * (summary, native, or rollover), and one model call per Turn, shared across every trigger.
 * All model work must use summarize or compactNative so it is metered.
 * Callback failures and requirements pass through unchanged; strategy dependencies belong to
 * its construction Layer. Protected messages cannot be removed and Tool pairs cannot be split by a decision.
 */
export interface CompactionRequest<E, R> {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly turn: number;
  readonly source: Prompt.Prompt;
  readonly state: Readonly<ContextCompactionState>;
  readonly policy: CompactionPolicy;
  readonly targetTokens: number | undefined;
  readonly trigger: "pressure" | "overflow" | "requested";
  /** Budget admission may prohibit a separate model call while still allowing a rollover. */
  readonly modelCallAllowed: boolean;
  /** The current captured Model's message accounting, including native fallback. */
  readonly estimateMessageTokens?: ContextMessageTokenEstimator | undefined;
  /** Present for a successful, singleton context rollover Tool; through excludes later steering. */
  readonly requested?: (ContextRolloverRequest & { readonly through: number }) | undefined;
  /** Compact exactly the eligible prior-Run prefix through the engine-owned metering seam. */
  readonly compactNative: (through: number) => Effect.Effect<NativeCompaction, E, R>;
  readonly summarize: (
    prompt: Prompt.Prompt,
    model?: CompactionModelLayer,
  ) => Effect.Effect<string, E, R>;
}

export interface ContextCompaction {
  /** Decorators must retain this capability to consume native sidecars. */
  readonly native?: NativeCompactionProvider["Service"];
  /** Non-negative finite integer estimate, used for admission and derived prompt overhead too. */
  readonly estimate: (messages: ReadonlyArray<Prompt.Message>) => number;
  /** Emit decisions sequentially; the consumer commits each before pulling the next. */
  readonly compact: <E, R>(
    request: CompactionRequest<E, R>,
  ) => Stream.Stream<CompactionDecision, E | CompactionError, R>;
}

const evaluateEstimates = <A>(
  override: ContextMessageTokenEstimator | undefined,
  operation: (estimate: (message: Prompt.Message) => number) => A,
): Effect.Effect<A, CompactionError> =>
  Effect.suspend(() => {
    const result = evaluateMessageTokenEstimates(override, operation);

    return Option.isSome(result)
      ? Effect.succeed(result.value)
      : CompactionError.make({
          message: "Message token estimator returned an invalid token count",
        });
  });

const defaultCompactor = (model?: CompactionModelLayer): ContextCompaction => ({
  estimate: estimatePromptTokens,
  compact: <E, R>(request: CompactionRequest<E, R>) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const { source, policy, targetTokens, trigger, modelCallAllowed } = request;

        if (request.requested !== undefined) {
          return Stream.succeed({ kind: "rollover", ...request.requested });
        }
        if (request.state.nativeWindows.length > 0) {
          return yield* CompactionError.make({
            message: "Portable compaction requires an explicit rollover out of native context",
          });
        }
        const forceSummarize = trigger === "overflow";
        const state = { ...request.state };

        const keepRecentTokens =
          targetTokens === undefined
            ? policy.keepRecentTokens
            : Math.max(1, Math.min(policy.keepRecentTokens, targetTokens));

        const decisions: Array<CompactionDecision> = [];

        if (!forceSummarize && policy.mode !== "summarize") {
          const through = yield* evaluateEstimates(request.estimateMessageTokens, (estimate) =>
            choosePruneBound(source.content, state, keepRecentTokens, targetTokens, estimate),
          );

          if (through > state.clearedThrough) {
            state.clearedThrough = through;
            decisions.push({ kind: "clear-tool-results", through });
            if (
              targetTokens !== undefined &&
              (yield* evaluateEstimates(request.estimateMessageTokens, (estimate) =>
                estimatePromptTokens(buildCompactedView(source.content, state), estimate),
              )) <= targetTokens
            ) {
              return Stream.fromIterable(decisions);
            }
          }
        }
        const prune = Stream.fromIterable(decisions);

        if ((!modelCallAllowed || policy.mode === "prune") && !forceSummarize) return prune;

        return prune.pipe(
          Stream.concat(
            Stream.unwrap(
              Effect.gen(function* () {
                const through = yield* evaluateEstimates(
                  request.estimateMessageTokens,
                  (estimate) =>
                    chooseSummarizeCut(source.content, state, keepRecentTokens, estimate),
                );

                const covered = collectCoveredMessages(source.content, state, through);

                if (covered.length === 0) return Stream.empty;

                const transcript = renderForSummary(
                  covered,
                  state.replacement?.kind === "summarize"
                    ? state.replacement.summary
                    : state.replacement?.kind === "rollover"
                      ? state.replacement.handoff
                      : undefined,
                );

                if (transcript === undefined) {
                  return yield* CompactionError.make({
                    message: `Previous compaction summary exceeds ${SUMMARY_MAX_LENGTH} characters`,
                  });
                }

                const prompt = Prompt.fromMessages([
                  Prompt.userMessage({
                    content: [
                      Prompt.textPart({
                        text: `${SUMMARY_REQUEST_PREFIX}${transcript}${SUMMARY_REQUEST_SUFFIX}`,
                      }),
                    ],
                  }),
                ]);

                const text = yield* request.summarize(prompt, model);

                return Stream.succeed({
                  kind: "summarize",
                  through,
                  summary: text.trim(),
                } satisfies CompactionDecision);
              }),
            ),
          ),
        );
      }),
    ),
});

/**
 * The sole interpreter compaction port. Runs without an installed service provide layer at their
 * composition boundary. Decorators yield this service during construction and receive their
 * underlying implementation through Layer.provide. Direct harnesses use the same Layer contract.
 */
export class ContextCompactor extends Context.Service<ContextCompactor, ContextCompaction>()(
  "@effect-agent/engine/ContextCompactor",
) {
  static readonly layer = Layer.succeed(ContextCompactor, defaultCompactor());

  /**
   * Start fresh windows without a summarizer call. The engine retains instructions/input and
   * commits each boundary; a bounded emergency handoff preserves user inputs and unseen results.
   * Applications provide notes/history tools alongside this strategy.
   */
  static readonly layerRollover = Layer.succeed(ContextCompactor, {
    estimate: estimatePromptTokens,
    compact: <E, R>(request: CompactionRequest<E, R>) =>
      Stream.unwrap(
        Effect.gen(function* () {
          if (request.requested !== undefined) {
            return Stream.succeed({
              kind: "rollover",
              ...request.requested,
            } satisfies CompactionDecision);
          }

          // Trailing user steering may not be canonical until the next response. Keep it verbatim.
          const through =
            request.source.content.findLastIndex(
              (message) => message.role === "assistant" || message.role === "tool",
            ) + 1;

          if (collectCoveredMessages(request.source.content, request.state, through).length === 0) {
            return Stream.empty;
          }

          const handoff = yield* evaluateEstimates(request.estimateMessageTokens, (estimate) =>
            buildRolloverHandoff(
              request.source.content,
              request.state,
              request.targetTokens,
              through,
              estimate,
            ),
          );

          if (handoff === undefined)
            return Stream.fail(
              CompactionError.make({
                message: "Insufficient context capacity for an automatic rollover handoff",
              }),
            );

          return Stream.succeed({
            kind: "rollover",
            through,
            handoff,
          } satisfies CompactionDecision);
        }),
      ),
  });

  /**
   * Use the supplied native provider with the runtime-selected inference Model. Coverage is
   * limited to complete prior Runs; an explicit rollover or fresh Thread can change affinity.
   * Provider adapters validate their source content and saved format before consuming a window.
   */
  static readonly layerNative: Layer.Layer<ContextCompactor, never, NativeCompactionProvider> =
    Layer.effect(
      ContextCompactor,
      Effect.map(NativeCompactionProvider, (native) =>
        ContextCompactor.of({
          native,
          estimate: estimatePromptTokens,
          compact: <E, R>(request: CompactionRequest<E, R>) => {
            if (request.requested !== undefined)
              return Stream.succeed({
                kind: "rollover",
                ...request.requested,
              } satisfies CompactionDecision);
            if (request.trigger === "overflow" || !request.modelCallAllowed)
              return Stream.fail(
                CompactionError.make({
                  message:
                    "Native compaction cannot rescue overflow or exceed the model-work budget",
                }),
              );
            const through = request.state.protectedStart;

            if (through <= 0 || through <= (request.state.replacement?.through ?? 0))
              return Stream.fail(
                CompactionError.make({
                  message: "Native compaction requires additional complete prior-Run history",
                }),
              );

            return Stream.fromEffect(request.compactNative(through)).pipe(
              Stream.map((native): CompactionDecision => ({ kind: "native", through, native })),
            );
          },
        }),
      ),
    );

  /** Use the bounded default algorithm with a separate upstream Effect AI Model. */
  static readonly layerWithModel = <Provider, Requirements>(
    model: Model.Model<Provider, LanguageModel.LanguageModel, Requirements>,
  ): Layer.Layer<ContextCompactor, never, Requirements> =>
    Layer.effect(
      ContextCompactor,
      Effect.map(model.captureRequirements, (captured) => defaultCompactor(captured)),
    );
}
