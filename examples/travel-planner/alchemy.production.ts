import { fileURLToPath } from "node:url";

import { OAuthScope } from "@yielded/auth";

import { plannerStack } from "./deployment";

export default plannerStack({
  stackName: "effect-agent-travel-planner",
  workerName: "effect-agent-travel-planner",
  rootDir: fileURLToPath(new URL(".", import.meta.url)),
  artifactsNamespace: "effect-agent-travel-planner-auth-v1",
  appDomain: "effect-agent.com",
  appZoneId: "9662e63e42d87b741fbcae6b65506924",
  domain: "travel.effect-agent.com",
  routes: [
    { pattern: "agent.yielded.dev/travel*", zoneName: "yielded.dev" },
    { pattern: "*-trip.effect-agent.com/*", zoneId: "9662e63e42d87b741fbcae6b65506924" },
  ],
  workersDev: false,
  auth: {
    issuer: "https://auth.yielded.dev",
    resource: "https://agent.yielded.dev",
    jwksUrl: "https://auth.yielded.dev/oauth/yielded/jwks",
    requiredScope: OAuthScope.make("agent:planner"),
    hostedModelScope: OAuthScope.make("agent:hosted-model"),
  },
});
