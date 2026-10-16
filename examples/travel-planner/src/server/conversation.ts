import * as Schema from "effect/Schema";

import { PlannerAnswer, Text, type PlannerSnapshot } from "../domain.ts";

export type Messages = Array<PlannerSnapshot["messages"][number]>;
export const completedAnswer = Schema.Union([Text, PlannerAnswer]);
