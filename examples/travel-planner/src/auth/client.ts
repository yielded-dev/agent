import { auth, sessionObservation as observeSession } from "@yielded/agent-example-auth/client";
import { AsyncResult, Atom } from "effect/reactivity";

export { runtime, auth, accountLifetime } from "@yielded/agent-example-auth/client";

export const sessionObservation = Atom.make((get) => {
  get.mount(observeSession);
  if (typeof window === "undefined") return;
  let previous: string | undefined;

  get.subscribe(
    auth.session,
    (result) => {
      if (!AsyncResult.isSuccess(result)) return;
      const next = result.value?.subjectId;

      if (previous !== undefined && previous !== next) {
        const prefix = `travel-voice:v1:${previous}:`;

        try {
          for (let i = sessionStorage.length - 1; i >= 0; i--) {
            const key = sessionStorage.key(i);

            if (key?.startsWith(prefix)) sessionStorage.removeItem(key);
          }
        } catch {
          // Storage may be unavailable; the account registry still retires independently.
        }
      }
      previous = next;
    },
    { immediate: true },
  );
});
