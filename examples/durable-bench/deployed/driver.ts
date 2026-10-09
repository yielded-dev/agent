import { Schema } from "effect";

import { MeasureRequest } from "./model.ts";
import { AwaitResult, RunResult, SubmitResult, Timeline } from "./worker/protocol.ts";

interface Env {
  BENCH_TOKEN: string;
  WORKERS_SUBDOMAIN: string;
  BUILD: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.headers.get("authorization") !== `Bearer ${env.BENCH_TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    if (new URL(request.url).pathname === "/health")
      return Response.json({ ok: true, build: env.BUILD });
    try {
      const input: unknown = await request.json();

      const { targetUrl, expectedBuild } = Schema.decodeUnknownSync(
        Schema.Struct({
          targetUrl: Schema.String,
          expectedBuild: Schema.optionalKey(Schema.NonEmptyString),
        }),
      )(input);

      const target = new URL(targetUrl);

      if (
        target.protocol !== "https:" ||
        target.username ||
        target.password ||
        !target.hostname.startsWith("cold-bisect-") ||
        !target.hostname.endsWith(`.${env.WORKERS_SUBDOMAIN}.workers.dev`)
      )
        return new Response("invalid target", { status: 400 });
      if (new URL(request.url).pathname === "/ready")
        return fetch(new URL("/health?probe=" + Date.now(), target), {
          headers: {
            authorization: `Bearer ${env.BENCH_TOKEN}`,
            "cache-control": "no-store",
            ...(expectedBuild === undefined ? {} : { "x-cold-bisect-build": expectedBuild }),
          },
        });
      const { query } = Schema.decodeUnknownSync(MeasureRequest)(input);

      const invoke = async (path: string, body?: unknown) => {
        const url = new URL(path, target);

        for (const [key, value] of Object.entries(query)) url.searchParams.set(key, String(value));

        const response = await fetch(url, {
          method: "POST",
          headers: {
            authorization: `Bearer ${env.BENCH_TOKEN}`,
            "content-type": "application/json",
            ...(expectedBuild === undefined ? {} : { "x-cold-bisect-build": expectedBuild }),
          },
          body: JSON.stringify(body ?? {}),
          signal: AbortSignal.timeout(180_000),
        });

        if (!response.ok) {
          const detail = (await response.text())
            .replaceAll(env.BENCH_TOKEN, "[redacted]")
            .slice(0, 1200);

          throw new Error(
            `Target ${path} returned HTTP ${response.status}; turn outcome may be unknown. ${detail}`,
          );
        }

        return response.json();
      };

      const started = Date.now();
      let admissionMs: number | undefined;
      let routing: typeof Timeline.Type | undefined;

      if (query.target === "yielded") {
        const raw = await invoke("/submit");
        const admission = Schema.decodeUnknownSync(SubmitResult)(raw);

        routing = Schema.decodeUnknownSync(
          Schema.Struct({ routing: Schema.optionalKey(Timeline) }),
        )(raw).routing;

        admissionMs = Date.now() - started;

        const settled = Schema.decodeUnknownSync(AwaitResult)(
          await invoke("/await", admission.receipt),
        );

        if (settled.settlement.outcome !== "completed")
          throw new Error("Yielded did not complete the turn.");
      } else if (query.target === "bare" || query.target === "pi") {
        const admission = Schema.decodeUnknownSync(
          Schema.Struct({ ok: Schema.Literal(true), receipt: Schema.Json }),
        )(await invoke("/submit"));

        admissionMs = Date.now() - started;
        Schema.decodeUnknownSync(RunResult)(await invoke("/await", admission.receipt));
      } else {
        Schema.decodeUnknownSync(RunResult)(await invoke("/run"));
      }

      const observedMs = Date.now();

      return Response.json({
        ok: true,
        driverMs: observedMs - started,
        observedMs,
        startedMs: started,
        admissionMs,
        routing,
        colo: typeof request.cf?.colo === "string" ? request.cf.colo : null,
      });
    } catch (cause) {
      return Response.json(
        {
          ok: false,
          error:
            cause instanceof Error
              ? cause.message
              : "Turn failed or response was lost; do not resubmit this input.",
        },
        { status: 502 },
      );
    }
  },
};
