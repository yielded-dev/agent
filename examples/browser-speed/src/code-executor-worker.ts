import { dynamicWorkerCodeExecutorLayer } from "@yielded/agent-platform-cloudflare/cloudflare-code-mode";
import {
  CodeExecutionHost,
  CodeExecutionProtocolError,
  CodeExecutor,
  CodeHostCall,
  CodeHostCallResult,
} from "@yielded/agent/code-executor";
import { SandboxImplementation } from "@yielded/agent/sandbox";
import { Effect, Layer, Schema, Tracer } from "effect";
import { FetchHttpClient } from "effect/http";
import { OtlpSerialization, OtlpTracer } from "effect/observability";

import * as Wire from "./code-executor-wire.ts";

interface Env {
  readonly LOADER: WorkerLoader;
  readonly HOSTCALL: Fetcher;
}

class BrokerTransportError extends Schema.TaggedError<BrokerTransportError>()(
  "BrokerTransportError",
  {
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

const telemetry = OtlpTracer.layer({
  url: "http://127.0.0.1:4318/v1/traces",
  resource: { serviceName: "effect-agent-browser-code-executor" },
}).pipe(Layer.provide(OtlpSerialization.layerJson), Layer.provide(FetchHttpClient.layer));

/** The generated program runs in the production Dynamic Worker executor, never this host. */
export default {
  async fetch(request: globalThis.Request, env: Env): Promise<globalThis.Response> {
    const input = Schema.decodeSync(Wire.Request)(await request.text());

    const execute = Effect.gen(function* () {
      const executor = yield* CodeExecutor;
      const result = yield* executor.execute(input.request).pipe(Effect.result);

      return Schema.encodeSync(Wire.Outcome)(
        result._tag === "Success" ? { success: result.success } : { failure: result.failure },
      );
    }).pipe(
      Effect.provideService(CodeExecutionHost, {
        call: (call) =>
          Effect.acquireUseRelease(
            Effect.sync(() => new AbortController()),
            (controller) =>
              Effect.gen(function* () {
                const body = yield* Schema.encodeEffect(Schema.fromJsonString(CodeHostCall))(
                  call,
                ).pipe(
                  Effect.mapError((cause) =>
                    BrokerTransportError.make({ message: "Invalid broker call", cause }),
                  ),
                );

                const response = yield* Effect.tryPromise({
                  try: () =>
                    env.HOSTCALL.fetch(`https://broker.invalid/${input.id}`, {
                      method: "POST",
                      body,
                      signal: controller.signal,
                    }),
                  catch: (cause) =>
                    BrokerTransportError.make({ message: "Broker transport failed", cause }),
                });

                if (!response.ok)
                  return yield* BrokerTransportError.make({
                    message: `Acceptance broker transport refused the call (${response.status})`,
                  });

                const text = yield* Effect.tryPromise({
                  try: () => response.text(),
                  catch: (cause) =>
                    BrokerTransportError.make({ message: "Broker response failed", cause }),
                });

                return yield* Schema.decodeEffect(Schema.fromJsonString(CodeHostCallResult))(
                  text,
                ).pipe(
                  Effect.mapError((cause) =>
                    BrokerTransportError.make({ message: "Invalid broker response", cause }),
                  ),
                );
              }),
            (controller) => Effect.sync(() => controller.abort()),
          ).pipe(
            // CodeExecutionHost reserves typed tool failures for result data; transport faults
            // are defects at this host boundary, retaining their cause and interruption.
            Effect.orDie,
          ),
      }),
      Effect.provide(dynamicWorkerCodeExecutorLayer({ loader: env.LOADER })),
      Effect.withParentSpan(Tracer.externalSpan({ traceId: input.traceId, spanId: input.spanId })),
      Effect.provide(telemetry),
      Effect.scoped,
    );

    try {
      return new Response(await Effect.runPromise(execute, { signal: request.signal }));
    } catch {
      return new Response(
        Schema.encodeSync(Wire.Outcome)({
          failure: CodeExecutionProtocolError.make({
            implementation: SandboxImplementation.make({
              isolation: "isolated",
              identity: "cloudflare-dynamic-worker",
            }),
            message: "Acceptance worker transport failed.",
          }),
        }),
      );
    }
  },
};
