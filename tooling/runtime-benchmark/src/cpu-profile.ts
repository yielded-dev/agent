import { Session } from "node:inspector/promises";

import { Effect, Exit } from "effect";

import { BenchmarkError, STEADY_STATE } from "./contracts.js";
import { writeEvidence } from "./evidence.js";

/** Inspector is a Node-only capability; its connection belongs to the worker's Scope. */
export const makeCpuProfiler = Effect.gen(function* () {
  const session = yield* Effect.acquireRelease(
    Effect.try({
      try: () => {
        const value = new Session();

        value.connect();

        return value;
      },
      catch: (cause) => BenchmarkError.make({ message: "Cannot connect CPU profiler", cause }),
    }),
    (value) => Effect.sync(() => value.disconnect()),
  );

  const request = <A>(name: string, run: () => Promise<A>) =>
    Effect.tryPromise({
      try: run,
      catch: (cause) => BenchmarkError.make({ message: `CPU profiler ${name} failed`, cause }),
    });

  yield* request("enable", () => session.post("Profiler.enable"));
  yield* request("sampling interval", () =>
    session.post("Profiler.setSamplingInterval", {
      interval: STEADY_STATE.samplingIntervalMicros,
    }),
  );

  return {
    capture: <A, E, R>(operation: Effect.Effect<A, E, R>, filename: string) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          yield* request("start", () => session.post("Profiler.start"));
          const result = yield* restore(operation).pipe(Effect.exit);
          // Stop before serialization, report I/O, and the enclosing application's finalizers.
          const { profile } = yield* request("stop", () => session.post("Profiler.stop"));

          yield* writeEvidence(filename, JSON.stringify(profile));
          if (Exit.isFailure(result)) return yield* Effect.failCause(result.cause);

          return {
            value: result.value,
            profileDurationMs: (profile.endTime - profile.startTime) / 1_000,
          };
        }),
      ),
  };
});
