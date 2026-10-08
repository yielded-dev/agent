import { BrowserCrypto } from "@effect/platform-browser";
import { CloudflareThreadClient } from "@yielded/agent-platform-cloudflare/cloudflare-thread-client";
import { digestDefinitions } from "@yielded/agent/digest";
import { ThreadId } from "@yielded/agent/identifiers";
import { IdempotencyKey, Principal, Receipt } from "@yielded/agent/receipt";
import { DateTime, Effect, Schema } from "effect";

import { turn, MEASURED_TOOLS } from "../../../src/plan.ts";
import { NetworkPiDO } from "./pi.ts";
import { decodeIdentity, errorText, readQuery, type Env, type Query } from "./protocol.ts";
import { agent, definitions, NetworkYieldedDO } from "./yielded.ts";

export { NetworkYieldedDO, NetworkPiDO };

type Bindings = Env & {
  YIELDED: DurableObjectNamespace<NetworkYieldedDO>;
  PI: DurableObjectNamespace<NetworkPiDO>;
};

const objectResult = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown));

async function cold(
  request: Request,
  query: Query,
  stub: { fetch(request: Request): Promise<Response> },
) {
  const url = new URL(request.url);

  url.pathname = "/identity";
  const response = await stub.fetch(new Request(url, { headers: request.headers }));

  if (!response.ok) throw new Error(`Pre-cold identity failed: ${response.status}`);
  const before = decodeIdentity(await response.json());
  let threadAbort;

  try {
    const response = await stub.fetch(request);
    const body = await response.text();

    threadAbort = {
      kind: "returned",
      status: response.status,
      body,
      expectedAbort: response.status === 500 && body.includes("prod-path explicit cold"),
    };
  } catch (cause) {
    const error = errorText(cause);

    threadAbort = {
      kind: "rejected",
      error,
      expectedAbort: error.includes("prod-path explicit cold"),
    };
  }

  const result = {
    ok: threadAbort.expectedAbort,
    ...query,
    coldRequested: true,
    coldVerified: false,
    requiresFreshIncarnationAndEntryReceipt: true,
    before,
    threadAbort,
  };

  console.log({ prodPath: "cold-receipt", ...result });

  return Response.json(result, { status: result.ok ? 200 : 502 });
}

async function probe(env: Env, sample: string) {
  const beforeMs = Date.now();
  const url = new URL("/echo", env.PROVIDER_URL);

  url.searchParams.set("sample", sample);
  const response = await fetch(url, { headers: { authorization: `Bearer ${env.TOKEN}` } });
  const provider = objectResult(await response.json());

  return { beforeMs, afterMs: Date.now(), provider, cfRay: response.headers.get("cf-ray") };
}

export default {
  async fetch(request: Request, env: Bindings): Promise<Response> {
    if (!env.TOKEN || request.headers.get("authorization") !== `Bearer ${env.TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);

    try {
      if (url.pathname === "/driver")
        return Response.json({
          ok: true,
          version: env.VERSION.id,
          generation: env.PHASE,
          ingressColo: request.cf?.colo,
          placementRegion: "aws:us-west-1",
        });
      const query = readQuery(url);

      if (!query.object.startsWith("prod-path-")) throw new Error("Object prefix mismatch");
      if (query.target === "pi" && query.variant !== "inline")
        throw new Error("Pi uses its native wait path");

      const stub =
        query.target === "pi"
          ? env.PI.getByName(query.object, { locationHint: "wnam" })
          : env.YIELDED.getByName(query.object, { locationHint: "wnam" });

      if (url.pathname === "/cold") {
        if (request.method !== "POST") return new Response("POST required", { status: 405 });

        return await cold(request, query, stub);
      }
      if (url.pathname !== "/run") return await stub.fetch(request);
      if (env.PHASE !== "measure") throw new Error("Measurement generation required");

      // Outside the timed turn: diagnose routing and clock offsets, without correction.
      const clockBefore = await probe(env, `${query.sample}-before`);
      let timing;

      if (query.target === "yielded" && query.variant === "production") {
        timing = await Effect.runPromise(
          Effect.gen(function* () {
            const client = yield* CloudflareThreadClient;
            const digests = yield* digestDefinitions(definitions);
            const input = turn(query.sample, MEASURED_TOOLS);
            const submitStartedMs = Date.now();

            const receipt = yield* client.submit(agent, input.text, {
              threadId: ThreadId.make(query.object),
              principal: Principal.make("bench"),
              idempotencyKey: IdempotencyKey.make(input.id),
              definitions: digests,
            });

            const receiptReceivedMs = Date.now();
            const settlement = yield* client.awaitSettlement(receipt);
            const settlementObservedMs = Date.now();

            if (settlement.outcome !== "completed")
              return yield* Effect.die(`Non-completed settlement: ${settlement.outcome}`);

            return {
              submitStartedMs,
              receiptReceivedMs,
              settlementObservedMs,
              turnMs: settlementObservedMs - submitStartedMs,
              admissionMs: receiptReceivedMs - submitStartedMs,
              receipt: Schema.encodeSync(Receipt)(receipt),
              settledAtMs: DateTime.toEpochMillis(settlement.settledAt),
              outcome: settlement.outcome,
            };
          }).pipe(
            Effect.provide([
              CloudflareThreadClient.layerFromBinding({ namespace: env.YIELDED }),
              BrowserCrypto.layer,
            ]),
          ),
        );
      } else {
        const submitStartedMs = Date.now();
        const response = await stub.fetch(request);
        const body = objectResult(await response.json());
        const settlementObservedMs = Date.now();

        if (!response.ok || body.ok !== true)
          throw new Error(`Inline turn failed: ${JSON.stringify(body)}`);
        timing = {
          submitStartedMs,
          settlementObservedMs,
          turnMs: settlementObservedMs - submitStartedMs,
          admissionMs: null,
          outcome: "completed",
        };
      }
      // Diagnostic payloads are read after the primary completion timestamp.
      const diagnosticStartedMs = Date.now();
      const metricsUrl = new URL(request.url);

      metricsUrl.pathname = "/metrics";
      const response = await stub.fetch(new Request(metricsUrl, { headers: request.headers }));
      const metrics = objectResult(await response.json());

      if (!response.ok || metrics.ok !== true)
        throw new Error(`Metrics failed: ${JSON.stringify(metrics)}`);
      const clockAfter = await probe(env, `${query.sample}-after`);

      const result = {
        ok: true,
        ...query,
        ...timing,
        metrics,
        clockBefore,
        clockAfter,
        diagnosticMs: Date.now() - diagnosticStartedMs,
        ingressColo: request.cf?.colo ?? null,
        driverVersion: env.VERSION.id,
        placementRegion: "aws:us-west-1",
      };

      console.log({ prodPath: "driver-turn", ...query, ...timing });

      return Response.json(result);
    } catch (cause) {
      const error = errorText(cause);

      console.error({ prodPath: "driver-failure", sample: url.searchParams.get("sample"), error });

      return Response.json(
        { ok: false, error, sample: url.searchParams.get("sample") },
        { status: 500 },
      );
    }
  },
};
