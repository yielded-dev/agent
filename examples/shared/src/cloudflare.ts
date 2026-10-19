import { Deferred, Effect } from "effect";
import { DurableObjectState, Worker } from "effect-cf";

import type { AccountError } from "./contract";
import { Authentication, type AuthenticatedAccount } from "./server";

/** The platform retains bounded verification independently of the client's abort signal. */
export const authenticateRequest = Effect.fnUntraced(function* (request: Request) {
  const authentication = yield* Authentication;
  const context = yield* Worker.WorkerContext;
  const result = yield* Deferred.make<AuthenticatedAccount, AccountError>();

  yield* context.waitUntil(Deferred.into(authentication.authenticate(request), result));

  return yield* Deferred.await(result);
});

/** A Durable Object retains verification through its own lifetime's platform service. */
export const authenticateDurableObjectRequest = Effect.fnUntraced(function* (request: Request) {
  const authentication = yield* Authentication;
  const state = yield* DurableObjectState.DurableObjectState;
  const result = yield* Deferred.make<AuthenticatedAccount, AccountError>();

  yield* state.waitUntil(Deferred.into(authentication.authenticate(request), result));

  return yield* Deferred.await(result);
});
