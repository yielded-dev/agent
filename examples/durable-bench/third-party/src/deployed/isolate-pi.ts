import {
  buildMismatch,
  INGRESS_HEADER,
  observeFetch,
} from "../../../deployed/isolate/observation.ts";
import { COLD_ABORT } from "../../../deployed/isolate/pi-host.ts";
import { parseIdentity, errorText, readQuery } from "../../../deployed/isolate/native-protocol.ts";
import type { Env } from "../../../deployed/isolate/protocol.ts";
import { PiDO } from "./pi.ts";

export { PiDO };

type Bindings = Env & { PI: DurableObjectNamespace<PiDO> };

const expectedAbort = async (abort: () => Promise<unknown>) => {
  try {
    const result = await abort();

    return (
      result instanceof Response &&
      result.status === 500 &&
      (await result.text()).includes(COLD_ABORT)
    );
  } catch (cause) {
    return errorText(cause).includes(COLD_ABORT);
  }
};

export default {
  async fetch(request: Request, env: Bindings): Promise<Response> {
    const ingress = observeFetch(env);

    if (!env.BENCH_TOKEN || request.headers.get("authorization") !== `Bearer ${env.BENCH_TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);

    try {
      if (url.pathname === "/health")
        return Response.json({ ok: true, build: env.BUILD, workerIsolate: ingress });
      const mismatch = buildMismatch(request, env);

      if (mismatch) return mismatch;
      const query = readQuery(url);

      if (query.target !== "pi") throw new Error("Target mismatch");
      if (
        ["/import", "/cold", "/submit", "/await", "/run", "/storage"].includes(url.pathname) &&
        request.method !== "POST"
      )
        return new Response("POST required", { status: 405 });
      if (!["/import", "/cold", "/submit", "/await", "/run", "/metrics", "/identity", "/storage"].includes(url.pathname))
        return new Response("not found", { status: 404 });
      if (url.pathname === "/submit" || url.pathname === "/await")
        throw new Error("Only Yielded uses native receipts");

      const stub = env.PI.getByName(query.object, { locationHint: "wnam" });

      if (url.pathname === "/cold") {
        const identityUrl = new URL(url);

        identityUrl.pathname = "/identity";
        const response = await stub.fetch(new Request(identityUrl, { headers: request.headers }));

        if (!response.ok) throw new Error("Pre-cold identity failed");
        const before = parseIdentity(await response.json());
        const threadAborted = await expectedAbort(() => stub.fetch(request));

        return Response.json(
          { ok: threadAborted, before, threadAborted },
          { status: threadAborted ? 200 : 502 },
        );
      }
      const headers = new Headers(request.headers);

      headers.set(INGRESS_HEADER, JSON.stringify(ingress));

      return await stub.fetch(new Request(request, { headers }));
    } catch (cause) {
      return Response.json(
        { ok: false, error: errorText(cause), sample: url.searchParams.get("sample") },
        { status: 500 },
      );
    }
  },
};
