/** Streaming-mode text is identical for every target and the transcript reference. */
export const PREAMBLE = "I will look up the requested records, then summarize what I find.";

export const responseText = (answer: string, streaming: boolean): string =>
  streaming
    ? `${answer}. The requested records have been checked in order. Each lookup returned the expected data, and this deterministic reply summarizes the completed work. All frameworks receive the same words and preserve the same conversation for the next turn.`
    : answer;
