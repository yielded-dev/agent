import { ThreadId } from "@yielded/agent/identifiers";
import { Principal } from "@yielded/agent/submission-ledger";
import { WorkerError } from "@yielded/agent/worker";
import { WorkerHostAuthorizer } from "@yielded/agent/worker-host";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

// This local example permits one user to manage workers from one conversation.
export const principal = Schema.decodeSync(Principal)("travel-user");
export const threadId = Schema.decodeSync(ThreadId)("travel-chat");

export const WorkerAccessLive = Layer.succeed(WorkerHostAuthorizer)({
  authorize: (request) =>
    request.principal === principal && request.sourceThreadId === threadId
      ? Effect.succeed(principal)
      : WorkerError.make({ operation: request.operation, reason: "denied" }),
});
