import { opaqueId } from "./ids.ts";

const errorText = (cause: unknown) => cause instanceof Error ? cause.stack ?? cause.message : String(cause);

interface Env {
  TOKEN: string;
  TARGET_URL: string;
  BUILD_ID: string;
  VERSION: { id: string };
}

// A deployed Worker measures the whole target fetch with its own clock. The laptop
// separately retains its outer elapsed time and CF-Ray; the two are never pooled.
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (!env.TOKEN || request.headers.get("authorization") !== `Bearer ${env.TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);
    if (url.pathname === "/health") return Response.json({ ok: true, version: opaqueId(env.VERSION.id), buildId: env.BUILD_ID });
    if (!["/run", "/seed", "/identity", "/reset", "/cold", "/drain"].includes(url.pathname))
      return new Response("not found", { status: 404 });
    const destination = new URL(url.pathname + url.search, env.TARGET_URL);
    if (destination.protocol !== "https:") throw new Error("HTTPS target required");
    // Consume the inbound body before the target interval.
    const body = request.method === "GET" ? undefined : await request.arrayBuffer();
    const startedMs = Date.now();
    const start = performance.now();
    try {
      const response = await fetch(destination, {
        method: request.method,
        headers: { authorization: `Bearer ${env.TOKEN}`, "content-type": "application/json" },
        body, signal: AbortSignal.timeout(180_000),
      });
      const raw = await response.text();
      const endedMs = Date.now();
      const workerLatencyMs = performance.now() - start;
      const driver = { startedMs, endedMs, workerLatencyMs, version: opaqueId(env.VERSION.id), buildId: env.BUILD_ID,
        ingressColo: request.cf?.colo ?? null, incomingPlacement: request.headers.get("cf-placement"), targetCfRay: response.headers.get("cf-ray"),
        targetPlacement: response.headers.get("cf-placement"), receiptBytes: new TextEncoder().encode(raw).byteLength };
      let result;
      try { result = JSON.parse(raw); } catch { result = { ok: false, error: raw.slice(0, 2000) }; }
      console.log({ wakeDefer: "driver", path: url.pathname, sample: url.searchParams.get("sample"), object: url.searchParams.get("object"), status: response.status, ...driver });
      return Response.json({ ...result, driver }, { status: response.status, headers: { "cache-control": "no-store" } });
    } catch (cause) {
      const driver = { startedMs, endedMs: Date.now(), workerLatencyMs: performance.now() - start, version: opaqueId(env.VERSION.id), buildId: env.BUILD_ID, ingressColo: request.cf?.colo ?? null, incomingPlacement: request.headers.get("cf-placement") };
      const error = errorText(cause);
      console.error({ wakeDefer: "driver-failure", ...driver, error, path: url.pathname, sample: url.searchParams.get("sample") });
      return Response.json({ ok: false, error, driver }, { status: 502 });
    }
  },
};
