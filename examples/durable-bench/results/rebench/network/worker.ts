import { BrowserCrypto } from "@effect/platform-browser";
import { CloudflareThreadClient } from "@yielded/agent-platform-cloudflare/cloudflare-thread-client";
import { digestDefinitions } from "@yielded/agent/digest";
import { ThreadId } from "@yielded/agent/identifiers";
import { IdempotencyKey, Principal, Receipt } from "@yielded/agent/receipt";
import { DateTime, Effect, Schema } from "effect";

import { turn, MEASURED_TOOLS } from "../../../src/plan.ts";
import { NetworkPiDO } from "./pi.ts";
import { decodeIdentity, decodeSeed, errorText, readQuery, type Env, type Query } from "./protocol.ts";
import { NetworkActorDO, NetworkThreadDO, cloudflareThreadName, coordinate } from "./tardie.ts";
import { agent, definitions, NetworkYieldedDO } from "./yielded.ts";

export { NetworkYieldedDO, NetworkPiDO, NetworkActorDO, NetworkThreadDO };

type Bindings = Env & {
  YIELDED: DurableObjectNamespace<NetworkYieldedDO>;
  PI: DurableObjectNamespace<NetworkPiDO>;
  ACTORS: DurableObjectNamespace<NetworkActorDO>;
  THREADS: DurableObjectNamespace<NetworkThreadDO>;
};

const objectResult = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown));

async function cold(
  request: Request,
  query: Query,
  stub: { fetch(request: Request): Promise<Response> },
  actor?: DurableObjectStub<NetworkActorDO>,
) {
  const url = new URL(request.url);

  url.pathname = "/identity";
  const response = await stub.fetch(new Request(url, { headers: request.headers }));

  if (!response.ok) throw new Error(`Pre-cold identity failed: ${response.status}`);
  const before = decodeIdentity(await response.json());
  const directoryBefore = actor === undefined ? undefined : await actor.identity();
  let directoryAbort;
  if (actor !== undefined) {
    try {
      await actor.abortCold(query);
      directoryAbort = { kind: "returned", expectedAbort: false };
    } catch (cause) {
      const error = errorText(cause);
      directoryAbort = { kind: "rejected", error, expectedAbort: error.includes("rebench explicit cold") };
    }
  }
  let threadAbort;

  try {
    const response = await stub.fetch(request);
    const body = await response.text();

    threadAbort = {
      kind: "returned",
      status: response.status,
      body,
      expectedAbort: response.status === 500 && body.includes("rebench explicit cold"),
    };
  } catch (cause) {
    const error = errorText(cause);

    threadAbort = {
      kind: "rejected",
      error,
      expectedAbort: error.includes("rebench explicit cold"),
    };
  }

  const result = {
    ok: threadAbort.expectedAbort && (directoryAbort === undefined || directoryAbort.expectedAbort),
    ...query,
    coldRequested: true,
    coldVerified: false,
    requiresFreshIncarnationAndEntryReceipt: true,
    before,
    directoryBefore,
    directoryAbort,
    threadAbort,
  };

  console.log({ rebench: "cold-receipt", ...result });

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

      if (!query.object.startsWith("rebench-")) throw new Error("Object prefix mismatch");
      if (query.target !== "yielded" && query.variant !== "inline")
        throw new Error("Pi and tardie use their native wait paths");
      if (url.pathname === "/run" && query.target === "yielded" && query.variant !== "production")
        throw new Error("Only the production Yielded path is measured");
      const actor = query.target === "tardie"
        ? env.ACTORS.getByName(query.object, { locationHint: "wnam" })
        : undefined;

      const stub =
        query.target === "pi"
          ? env.PI.getByName(query.object, { locationHint: "wnam" })
          : query.target === "tardie"
            ? env.THREADS.getByName(cloudflareThreadName(coordinate(query.object)), { locationHint: "wnam" })
            : env.YIELDED.getByName(query.object, { locationHint: "wnam" });

      if (actor && url.pathname === "/seed" && env.PHASE === "seed" && decodeSeed(await request.clone().json()).from === 0) {
        const setup = await actor.fetch(new Request(`https://rebench/v1/actors/${encodeURIComponent(query.object)}/threads`, {
          method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ name: "bench" }),
        }));
        if (!setup.ok) throw new Error(`Tardie setup ${setup.status}: ${await setup.text()}`);
      }
      if (url.pathname === "/identity" && actor) {
        const response = await stub.fetch(request);
        return Response.json({ ...objectResult(await response.json()), directory: await actor.identity() }, { status: response.status });
      }
      if (url.pathname === "/cold") {
        if (request.method !== "POST") return new Response("POST required", { status: 405 });

        return await cold(request, query, stub, actor);
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
          throw new Error(`Native turn failed: ${JSON.stringify(body)}`);
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
      const directory = actor === undefined ? undefined : await actor.end();
      const directoryStart = directory;
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
        directoryStart,
        directory,
        clockBefore,
        clockAfter,
        diagnosticMs: Date.now() - diagnosticStartedMs,
        ingressColo: request.cf?.colo ?? null,
        driverVersion: env.VERSION.id,
        placementRegion: "aws:us-west-1",
      };

      console.log({ rebench: "driver-turn", ...query, ...timing });

      return Response.json(result);
    } catch (cause) {
      const error = errorText(cause);

      console.error({ rebench: "driver-failure", sample: url.searchParams.get("sample"), error });

      return Response.json(
        { ok: false, error, sample: url.searchParams.get("sample") },
        { status: 500 },
      );
    }
  },
};
