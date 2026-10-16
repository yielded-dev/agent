import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";

import { program } from "./delegation-live.ts";

NodeRuntime.runMain(program.pipe(Effect.tap(({ output }) => Console.log(output))));
