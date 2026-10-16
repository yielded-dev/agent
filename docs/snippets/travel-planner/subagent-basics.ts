import { Agent, Subagent } from "@yielded/agent";
import * as Toolkit from "effect/ai/Toolkit";
import * as Schema from "effect/Schema";

export const Summarize = Subagent.make("summarize", {
  description: "Summarize a support case in three sentences.",
  target: Agent.make("summarizer", {
    input: Schema.String,
    output: Schema.String,
    instructions: "Summarize the case. Include the issue and next action.",
    toolkit: Toolkit.empty,
  }),
});

export const Support = Agent.make("support", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Use summarize to prepare a concise case summary, then answer the user.",
  toolkit: Toolkit.make(Summarize.tool),
});
