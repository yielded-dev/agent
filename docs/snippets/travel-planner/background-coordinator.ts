import { Subagent, Agent } from "@yielded/agent";
import { Schema } from "effect";
import { Toolkit } from "effect/ai";

import { CoordinatorInput } from "./background-input.ts";
import { Researcher } from "./researcher.ts";

const Research = Subagent.make("activity-researcher", { target: Researcher });

export const StartResearch = Subagent.startTool(Research);
export const FollowUpResearch = Subagent.followUpTool(Research);
export const ResearchReporting = Subagent.reporting(Research);

export const BackgroundCoordinator = Agent.make("background-trip-coordinator", {
  input: CoordinatorInput,
  output: Schema.String,
  toolkit: Toolkit.make(StartResearch.tool, FollowUpResearch.tool),
  instructions:
    "Help the user plan a trip. Start activity research in the background when needed. " +
    "Keep discussing their preferences while research runs. Send changed preferences " +
    "to the existing worker with follow_up. When WorkerCompletion arrives, explain " +
    "the findings and flag partial results. On failure or cancellation, help choose a next step. " +
    "Do not start another search just because a research report arrived.",
  policy: { maxTurns: 6, maxToolCalls: 4, maxDuration: "2 minutes", toolConcurrency: 2 },
});
