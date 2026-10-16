import * as NodeRuntime from "@effect/platform-node/NodeRuntime";

import { schedulingCrashWorker } from "./scheduling-worker.ts";

NodeRuntime.runMain(schedulingCrashWorker, { disableErrorReporting: true });
