import { NetworkPiDO } from "./pi.ts";
import {
  decodeIdentity,
  decodeSeed,
  errorText,
  readQuery,
  type Env,
  type Query,
} from "./protocol.ts";
import { NetworkActorDO, NetworkThreadDO, cloudflareThreadName, coordinate } from "./tardie.ts";
import { NetworkYieldedDO } from "./yielded.ts";

export { NetworkYieldedDO, NetworkPiDO, NetworkActorDO, NetworkThreadDO };

type Bindings = Env & {
  YIELDED?: DurableObjectNamespace<NetworkYieldedDO>;
  THREADS?: DurableObjectNamespace<NetworkThreadDO>;
  PI?: DurableObjectNamespace<NetworkPiDO>;
  ACTORS?: DurableObjectNamespace<NetworkActorDO>;
};

const abortObservation = async (abort: () => Promise<Response | void>) => {
  try {
    const response = await abort();
    const body = response === undefined ? "abort returned normally" : await response.text();

    return {
      kind: "returned",
      status: response?.status ?? null,
      body,
      expectedAbort: response?.status === 500 && body.includes("cf-latency explicit cold"),
    };
  } catch (cause) {
    const error = errorText(cause);

    return { kind: "rejected", error, expectedAbort: error.includes("cf-latency explicit cold") };
  }
};

async function cold(
  request: Request,
  query: Query,
  stub: { fetch(request: Request): Promise<Response> },
  actor?: DurableObjectStub<NetworkActorDO>,
) {
  const identityUrl = new URL(request.url);

  identityUrl.pathname = "/identity";
  const identityResponse = await stub.fetch(new Request(identityUrl, { headers: request.headers }));

  if (!identityResponse.ok)
    throw new Error(
      `Pre-cold identity failed: ${identityResponse.status} ${await identityResponse.text()}`,
    );
  const before = decodeIdentity(await identityResponse.json());
  const directoryBefore = actor === undefined ? undefined : await actor.identity();

  const directoryAbort =
    actor === undefined ? undefined : await abortObservation(() => actor.abortCold(query));

  const threadAbort = await abortObservation(() => stub.fetch(request));

  const ok =
    threadAbort.expectedAbort && (directoryAbort === undefined || directoryAbort.expectedAbort);

  const result = {
    ok,
    ...query,
    coldRequested: true,
    coldVerified: false,
    requiresFreshIncarnationAndEntryReceipt: true,
    before,
    directoryBefore,
    directoryAbort,
    threadAbort,
  };

  console.log({ cfLatency: "cold-receipt", ...result });

  return Response.json(result, { status: ok ? 200 : 502 });
}

export default {
  async fetch(request: Request, env: Bindings): Promise<Response> {
    if (!env.TOKEN || request.headers.get("authorization") !== `Bearer ${env.TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);

    try {
      const query = readQuery(url);

      if (url.pathname === "/cold" && request.method !== "POST")
        return new Response("POST required", { status: 405 });

      if (!query.object.startsWith("cf-latency-"))
        throw new Error("Object names must start with cf-latency-");
      if (query.target !== "yielded" && query.variant !== "baseline")
        throw new Error("defer-wakes is Yielded-only");
      if (query.target === "pi") {
        if (!env.PI) throw new Error("Missing PI binding");

        const stub = env.PI.getByName(query.object, { locationHint: "wnam" });

        return url.pathname === "/cold"
          ? await cold(request, query, stub)
          : await stub.fetch(request);
      }
      if (query.target === "yielded") {
        if (!env.YIELDED) throw new Error("Missing YIELDED binding");

        const stub = env.YIELDED.getByName(query.object, { locationHint: "wnam" });

        return url.pathname === "/cold"
          ? await cold(request, query, stub)
          : await stub.fetch(request);
      }
      if (!env.THREADS) throw new Error("Missing THREADS binding");
      if (!env.ACTORS) throw new Error("Missing ACTORS binding");
      const actor = env.ACTORS.getByName(query.object, { locationHint: "wnam" });

      const thread = env.THREADS.getByName(cloudflareThreadName(coordinate(query.object)), {
        locationHint: "wnam",
      });

      if (
        url.pathname === "/seed" &&
        env.PHASE === "seed" &&
        decodeSeed(await request.clone().json()).from === 0
      ) {
        const setup = await actor.fetch(
          new Request(`https://cf-latency/v1/actors/${encodeURIComponent(query.object)}/threads`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ name: "bench" }),
          }),
        );

        if (!setup.ok) throw new Error(`Tardie setup ${setup.status}: ${await setup.text()}`);
      }
      if (url.pathname === "/cold") return await cold(request, query, thread, actor);
      if (url.pathname !== "/run") return await thread.fetch(request);
      const directoryStart = await actor.begin(query);
      let response: Response;

      try {
        response = await thread.fetch(request);
      } catch (cause) {
        const directory = await actor.end();

        return Response.json(
          { ok: false, ...query, error: errorText(cause), directoryStart, directory },
          { status: 500 },
        );
      }
      const directory = await actor.end();

      // The Thread receipt carries per-provider-call counters. Actor counters
      // cover the whole turn and are kept separate to avoid nine measurement RPCs.
      return Response.json(
        { thread: await response.json(), directoryStart, directory, ...query },
        { status: response.status },
      );
    } catch (cause) {
      const error = errorText(cause);

      console.error({ cfLatency: "router-failure", sample: url.searchParams.get("sample"), error });

      return Response.json(
        { ok: false, error, sample: url.searchParams.get("sample") },
        { status: 500 },
      );
    }
  },
};
