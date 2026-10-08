import { decodeIdentity, errorText, readQuery, type Env } from "./protocol.ts";
import { NetworkYieldedDO } from "./yielded.ts";
import { opaqueId } from "./ids.ts";
export { NetworkYieldedDO };

type Bindings = Env & { YIELDED: DurableObjectNamespace<NetworkYieldedDO> };

export default {
  async fetch(request: Request, env: Bindings): Promise<Response> {
    if (!env.TOKEN || request.headers.get("authorization") !== `Bearer ${env.TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);
    if (url.pathname === "/health") return Response.json({ ok: true, version: opaqueId(env.VERSION.id), buildId: env.BUILD_ID, buildMode: env.BUILD_MODE, phase: env.PHASE });
    try {
      const query = readQuery(url);
      if (!query.object.startsWith("wake-defer-")) throw new Error("Object names must start with wake-defer-");
      if (query.variant === "candidate" && env.BUILD_MODE !== "ab") throw new Error("This is a baseline-only build");
      const stub = env.YIELDED.getByName(query.object, { locationHint: "wnam" });
      if (url.pathname !== "/cold") return await stub.fetch(request);
      if (request.method !== "POST") return new Response("POST required", { status: 405 });
      const identityUrl = new URL(url);
      identityUrl.pathname = "/identity";
      const response = await stub.fetch(new Request(identityUrl, { headers: request.headers }));
      if (!response.ok) throw new Error(`Pre-cold identity failed: ${response.status}`);
      const before = decodeIdentity(await response.json());
      let threadAbort;
      try {
        const aborted = await stub.fetch(request);
        const body = await aborted.text();
        threadAbort = { kind: "returned", status: aborted.status, body, expectedAbort: aborted.status === 500 && body.includes("wake-defer explicit cold") };
      } catch (cause) {
        const error = errorText(cause);
        threadAbort = { kind: "rejected", error, expectedAbort: error.includes("wake-defer explicit cold") };
      }
      const result = { ok: threadAbort.expectedAbort, ...query, coldRequested: true, coldVerified: false, before, threadAbort };
      console.log({ wakeDefer: "cold-receipt", ...result });
      return Response.json(result, { status: result.ok ? 200 : 502 });
    } catch (cause) {
      const error = errorText(cause);
      console.error({ wakeDefer: "router-failure", sample: url.searchParams.get("sample"), error });
      return Response.json({ ok: false, error }, { status: 500 });
    }
  },
};
