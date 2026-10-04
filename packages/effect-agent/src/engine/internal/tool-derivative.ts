import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as ErrorReporter from "effect/ErrorReporter";
import * as Exit from "effect/Exit";

import type { RunToolFailureObserver, ToolFailureObservation } from "../RunOptions.ts";

/** @internal Only the explicitly installed trusted observer sees this live value. */
export const deliverToolFailure = (
  observer: RunToolFailureObserver,
  observation: ToolFailureObservation,
): Effect.Effect<void> =>
  isolateToolDerivative(Effect.suspend(() => observer.observe(observation)));

/**
 * Report failures of derivative work without consuming external interruption. Reporter defects
 * are isolated too. Neither observer delivery nor telemetry owns the authoritative Tool outcome.
 */
const reportDerivativeCause = <E>(cause: Cause.Cause<E>): Effect.Effect<void> => {
  const reportableReasons: Array<Cause.Reason<E>> = [];
  const interruptionReasons: Array<Cause.Interrupt> = [];

  for (const reason of cause.reasons) {
    if (Cause.isInterruptReason(reason)) interruptionReasons.push(reason);
    else reportableReasons.push(reason);
  }

  const reportExit =
    reportableReasons.length === 0
      ? Effect.succeed(Exit.succeed(undefined))
      : Effect.exit(ErrorReporter.report(Cause.fromReasons(reportableReasons)));

  return Effect.flatMap(reportExit, (exit) => {
    if (Exit.isFailure(exit)) {
      for (const reason of exit.cause.reasons) {
        if (Cause.isInterruptReason(reason)) interruptionReasons.push(reason);
      }
    }

    return interruptionReasons.length === 0
      ? Effect.void
      : Effect.failCause(Cause.fromReasons<never>(interruptionReasons));
  });
};

/** @internal Isolate reporting defects independently of Logger/Tracer configuration. */
export const isolateToolDerivative = <R>(
  effect: Effect.Effect<void, never, R>,
): Effect.Effect<void, never, R> => effect.pipe(Effect.catchCause(reportDerivativeCause));
