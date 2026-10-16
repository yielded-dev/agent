import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import { NodeDurableHost } from "@yielded/agent-platform-node";
import * as Effect from "effect/Effect";

import { HostLive } from "./background-host.ts";

NodeRuntime.runMain(NodeDurableHost.run.pipe(Effect.provide(HostLive)));
