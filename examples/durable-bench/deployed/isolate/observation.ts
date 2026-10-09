import type { Env, IsolateState } from "./protocol.ts";

export const BUILD_HEADER = "x-cold-bisect-build";
export const INGRESS_HEADER = "x-cold-bisect-ingress";

let id: string | undefined;
let statelessFetches = 0;
let durableObjectConstructors = 0;

/** Called only from a request or Object constructor; no random work at module evaluation. */
export const isolateObservation = (env: Env): IsolateState => ({
  id: (id ??= crypto.randomUUID()),
  build: env.BUILD,
  statelessFetches,
  durableObjectConstructors,
});

/** The routing snapshot excludes this request and includes earlier health/import/metrics calls. */
export const observeFetch = (env: Env): IsolateState => {
  const before = isolateObservation(env);

  statelessFetches++;

  return before;
};

/** Entry snapshots include the measured Object's own constructor. */
export const observeConstructor = (env: Env): void => {
  durableObjectConstructors++;
  isolateObservation(env);
};

/** Run before getByName or any RPC; only /health is allowed without an expected version. */
export const buildMismatch = (request: Request, env: Env): Response | undefined =>
  request.headers.get(BUILD_HEADER) === env.BUILD
    ? undefined
    : Response.json(
        {
          ok: false,
          error: `Worker build ${env.BUILD} does not match ${BUILD_HEADER}: ${request.headers.get(BUILD_HEADER) ?? "missing"}`,
          sample: new URL(request.url).searchParams.get("sample"),
        },
        { status: 409 },
      );
