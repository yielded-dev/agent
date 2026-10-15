import { fileURLToPath } from "node:url";

import { plannerStack } from "./deployment";

// Hosted compositions provide their own explicit names, routes and policy.
export default plannerStack({
  stackName: process.env.PLANNER_STACK_NAME ?? "travel-planner-local",
  workerName: process.env.PLANNER_WORKER_NAME ?? "travel-planner-local",
  rootDir: fileURLToPath(new URL(".", import.meta.url)),
  artifactsNamespace: process.env.PLANNER_ARTIFACTS_NAMESPACE ?? "travel-planner-local",
  appDomain: process.env.PLANNER_APP_DOMAIN ?? "localhost",
  ...(process.env.PLANNER_APP_ZONE_ID ? { appZoneId: process.env.PLANNER_APP_ZONE_ID } : {}),
  ...(process.env.PLANNER_DOMAIN ? { domain: process.env.PLANNER_DOMAIN } : {}),
  workersDev: true,
});
