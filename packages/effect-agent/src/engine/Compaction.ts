/**
 * Prompt-size estimates and context-window helpers used by the public compaction API.
 *
 * @since 0.1.0
 */

export {
  /**
   * Model-visible replacement for a tool result cleared by prune compaction.
   *
   * @category constants
   * @since 0.1.0
   */
  CLEARED_TOOL_RESULT,
  /**
   * Prefix of the user message that carries a compaction summary into the prompt.
   *
   * @category constants
   * @since 0.1.0
   */
  COMPACTION_SUMMARY_PREFIX,
  /**
   * Rollover text is recovery evidence; it never asserts that an external action succeeded.
   *
   * @category constants
   * @since 0.1.0
   */
  CONTEXT_ROLLOVER_PREFIX,
  /**
   * Stable across Attempts; the initial window uses turn zero.
   *
   * @category operations
   * @since 0.1.0
   */
  contextWindowId,
  /**
   * Shared by the live interpreter and canonical journal projection.
   *
   * @category operations
   * @since 0.1.0
   */
  contextWindowMessage,
  /**
   * Deterministic context estimate: UTF-8 bytes/4 for text and structural JSON,
   * plus image reservations based on dimensions when available. Image encodings
   * are transport, not text tokens. This never changes the model-visible message.
   *
   * @category operations
   * @since 0.1.0
   */
  estimateMessageTokens,
  /**
   * Sum of `estimateMessageTokens` over a message array.
   *
   * @category operations
   * @since 0.1.0
   */
  estimatePromptTokens,
  /**
   * Mutable per-Run compaction state held on the engine's RunContext. Indices
   * are positions in the SOURCE prompt (the official-history basis handed to
   * each Turn). Ordinary history grows append-only. Prepared history must retain
   * content-equivalent covered prefixes or the engine rejects the next Turn.
   *
   * @category models
   * @since 0.1.0
   */
  type ContextCompactionState,
} from "./internal/compaction.ts";
