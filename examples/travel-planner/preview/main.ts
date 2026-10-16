import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Config from "effect/Config";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { localPreview } from "./runtime.ts";

Effect.gen(function* () {
  const port = yield* Config.schema(
    Schema.Int.check(Schema.isBetween({ minimum: 1024, maximum: 65535 })),
    "PREVIEW_PORT",
  ).pipe(Config.withDefault(4173));

  const { origin } = yield* localPreview(port);

  yield* Console.log(`Local travel preview: ${origin}/travel/
Accept the local HTTPS certificate. The preview gateway supplies a temporary test account.
Use sk-preview-local for the offline planner and send: complete travel cards fixture
This preview uses test data, makes no provider calls, and resets when stopped.`);

  return yield* Effect.never;
}).pipe(Effect.scoped, Effect.provide(NodeServices.layer), NodeRuntime.runMain);
