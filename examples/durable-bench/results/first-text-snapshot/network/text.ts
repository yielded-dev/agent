import type { Step } from "../../../src/plan.ts";

/** 48 fixed fragments model 40 tokens/s; byte-identical for every framework. */
export const textFragments = (step: Step): readonly string[] => {
  const label = "call" in step ? `Lookup ${step.call}.` : "Complete.";
  const words =
    `${label} I am checking the requested records and keeping the answer grounded in the returned data. Each lookup contributes one piece of evidence to this deterministic response. The conversation remains ordered, and the next step follows only after this response has finished.`.split(
      " ",
    );
  return Array.from(
    { length: 48 },
    (_, index) => `${words[index % words.length]}${index === 47 ? "" : " "}`,
  );
};

export const responseText = (step: Step): string => textFragments(step).join("");
