import { Deferred, Effect } from "effect";

import type { AccountError } from "./contract";
import { Authentication, type AuthenticatedAccount } from "./server";

/** The platform retains bounded verification independently of the client's abort signal. */
export const authenticateRequest = Effect.fnUntraced(function* (
  request: Request,
  retain: (verification: Effect.Effect<boolean>) => Effect.Effect<void>,
) {
  const authentication = yield* Authentication;
  const result = yield* Deferred.make<AuthenticatedAccount, AccountError>();

  yield* retain(Deferred.into(authentication.authenticate(request), result));

  return yield* Deferred.await(result);
});
