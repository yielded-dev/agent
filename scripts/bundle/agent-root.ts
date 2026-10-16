import { Agent } from "@yielded/agent";
import * as Toolkit from "effect/ai/Toolkit";
import * as Schema from "effect/Schema";

export const agent = Agent.make("bundle-probe", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Answer the question.",
  toolkit: Toolkit.empty,
});
