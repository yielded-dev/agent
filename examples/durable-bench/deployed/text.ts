export const INSTRUCTIONS =
  "You are a benchmark agent. Call lookup as instructed, then answer briefly.";

// Match the ordinary-text guidance appended by Yielded for text(Schema.String).
export const TEXT_OUTPUT_INSTRUCTIONS = `${INSTRUCTIONS}\n\nFinal output contract: write the final reply as ordinary assistant text, without JSON wrapping. An empty reply is valid only when allowed by the output Schema and the task instructions.`;

/** Streaming-mode text is identical for every target and the transcript reference. */
export const PREAMBLE = "I will look up the requested records, then summarize what I find.";

export const responseText = (answer: string, streaming: boolean): string =>
  streaming
    ? `${answer}. The requested records have been checked in order. Each lookup returned the expected data, and this deterministic reply summarizes the completed work. All frameworks receive the same words and preserve the same conversation for the next turn.`
    : answer;
