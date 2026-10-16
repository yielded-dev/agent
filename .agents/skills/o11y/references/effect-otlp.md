# Effect OTLP

Read the installed Effect `AGENTS.md` and its observability guide first. The snippets below use `effect/http` and `effect/observability`, checked against installed source. Match the repository's installed exports and signatures instead of upgrading to fit this example. Preserve an existing `@effect/opentelemetry` integration when it already owns the runtime.

## One shared exporter layer

This composition uses Effect's built-in OTLP exporters and JSON serialization. It does not require a Maple-specific SDK. Put it in the application's existing telemetry module and provide it at the composition root.

```ts
// telemetry.ts
import { Layer } from "effect";
import { FetchHttpClient } from "effect/http";
import { OtlpLogger, OtlpSerialization, OtlpTracer } from "effect/observability";

export const ObservabilityLive = Layer.mergeAll(
  OtlpTracer.layerFromConfig(),
  OtlpLogger.layerFromConfig({ mergeWithExisting: true }),
).pipe(
  Layer.provide(OtlpSerialization.layerJson),
  Layer.provide(FetchHttpClient.layer),
);
```

Use the environment settings in [local Maple](local-maple.md). In this Effect version, `layerFromConfig` requires both an endpoint and `OTEL_TRACES_EXPORTER=otlp` / `OTEL_LOGS_EXPORTER=otlp` for the respective signals; omitting the exporter selection disables that signal. `OTEL_SDK_DISABLED=true` disables both. The base endpoint gains the signal path; `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` are complete URL overrides. `OTEL_SERVICE_NAME` identifies the application. Existing config providers and deployment bindings must supply these values in the actual runtime.

For versions without `layerFromConfig`, compose `OtlpTracer.layer` and `OtlpLogger.layer` using the app's config service and explicit full URLs. Match installed option types. Configure headers through the existing secret mechanism only for sinks that need them.

## A minimal delivery check

Run this once through a Node development entrypoint after changing exporter wiring. It checks delivery and log correlation, not performance.

```ts
// telemetry-check.ts
import { NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";
import { ObservabilityLive } from "./telemetry.ts";

const program = Effect.gen(function* () {
  yield* Effect.annotateCurrentSpan("check.kind", "otlp");
  yield* Effect.logInfo("Trace exporter check");
}).pipe(Effect.withSpan("observability.check"));

NodeRuntime.runMain(program.pipe(Effect.provide(ObservabilityLive)));
```

For real application operations, use named `Effect.fn` functions or `Effect.withSpan` at meaningful boundaries. Keep span names stable, add bounded attributes, and preserve the original error and interruption behavior. Keep the same telemetry layer alive across the application's service lifetimes; include service acquisition when startup is the question.

Let scope shutdown flush completed spans and logs. Await disposal of an owned `ManagedRuntime`; do not add sleeps or call `process.exit` to finish export. The Node example is not a Worker bootstrap: [Cloudflare](cloudflare.md) owns request/DO delivery lifetimes. Reuse existing logging rather than installing duplicate sinks, and add metrics export only when the measurement needs it.
