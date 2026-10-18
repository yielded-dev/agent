import { Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/http";
import { AsyncResult, Atom, AtomHttpApi, AtomRegistry } from "effect/reactivity";

import { AccountError, AuthApi } from "./contract";

export const runtime = Atom.context();

export class AuthClient extends AtomHttpApi.Service<AuthClient>()("example-auth/Client", {
  api: AuthApi,
  runtime,
  httpClient: FetchHttpClient.layer,
}) {}

const changed = "yielded:session-changed";

const notify = () => {
  if (typeof window !== "undefined") window.dispatchEvent(new Event(changed));
};

/** Retry only an explicit pre-admission 401, once, after the host refreshes its HttpOnly cookie. */
export const fetchWithSession = Effect.fnUntraced(function* (
  input: RequestInfo | URL,
  init?: RequestInit,
) {
  const send = () => fetch(input, init);

  const response = yield* Effect.tryPromise({
    try: send,
    catch: () => AccountError.make({ code: "unavailable", message: "The request is unavailable." }),
  });

  if (response.status !== 401 || input instanceof Request || typeof location === "undefined")
    return response;
  const url = new URL(String(input), location.origin);

  if (url.origin !== location.origin || url.pathname.startsWith("/auth/")) return response;

  const refresh = yield* Effect.tryPromise({
    try: () =>
      fetch("/auth/refresh", { method: "POST", credentials: "same-origin", signal: init?.signal }),
    catch: () =>
      AccountError.make({ code: "unavailable", message: "Session refresh is unavailable." }),
  });

  notify();
  if (refresh.status === 429 || refresh.status >= 500)
    return yield* AccountError.make({
      code: "unavailable",
      message: "Session refresh is unavailable.",
    });
  if (refresh.status !== 200) return response;
  yield* Effect.promise(async () => {
    await response.body?.cancel();
    await refresh.body?.cancel();
  });

  return yield* Effect.tryPromise({
    try: send,
    catch: () => AccountError.make({ code: "unavailable", message: "The request is unavailable." }),
  });
});

export const authenticatedHttpClient = FetchHttpClient.layer.pipe(
  Layer.provide(
    Layer.succeed(FetchHttpClient.Fetch, (input, init) =>
      Effect.runPromise(fetchWithSession(input, init)),
    ),
  ),
);

export const session = AuthClient.query("auth", "session", { reactivityKeys: ["auth-session"] });

export const refreshSession = AuthClient.runtime.fn<void>()(
  Effect.fnUntraced(function* () {
    const client = yield* AuthClient;

    yield* client.auth.refresh({});
    notify();
  }),
);

export const signOut = AuthClient.runtime.fn<void>()(
  Effect.fnUntraced(function* (_, get) {
    const client = yield* AuthClient;

    yield* client.auth.logout({});
    get.refresh(session);
    notify();
  }),
);

export const auth = { session, signOut };

type AccountLifetime = {
  subject: string | null;
  generation: number;
  registry: AtomRegistry.AtomRegistry;
};

/** Dispose the previous account's queries and drafts before publishing the next identity. */
export const accountLifetime = Atom.make((get) => {
  let current: AccountLifetime = { subject: null, generation: 0, registry: AtomRegistry.make() };

  get.subscribe(
    session,
    (result) => {
      if (!AsyncResult.isSuccess(result)) return;
      const subject = result.value?.subjectId ?? null;

      if (subject === current.subject) return;
      current.registry.dispose();
      current = { subject, generation: current.generation + 1, registry: AtomRegistry.make() };
      get.setSelf(AsyncResult.success(current));
    },
    { immediate: true },
  );
  get.addFinalizer(() => current.registry.dispose());

  return AsyncResult.success(current);
});

/** Refresh near expiry, and recheck after another tab can have changed the host cookie. */
export const sessionObservation = Atom.make((get) => {
  if (typeof window === "undefined") return;
  get.mount(refreshSession);
  let timer: ReturnType<typeof setTimeout> | undefined;

  const recheck = () => {
    if (document.visibilityState === "visible") get.refresh(session);
  };

  get.subscribe(
    session,
    (result) => {
      clearTimeout(timer);
      if (AsyncResult.isSuccess(result) && result.value !== null)
        timer = setTimeout(
          () => get.set(refreshSession, undefined),
          Math.max(1000, result.value.expiresAtMillis - Date.now() - 30_000),
        );
    },
    { immediate: true },
  );
  window.addEventListener("focus", recheck);
  window.addEventListener(changed, recheck);
  document.addEventListener("visibilitychange", recheck);
  get.addFinalizer(() => {
    clearTimeout(timer);
    window.removeEventListener("focus", recheck);
    window.removeEventListener(changed, recheck);
    document.removeEventListener("visibilitychange", recheck);
  });
});
