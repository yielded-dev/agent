import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as References from "effect/References";
import * as Scope from "effect/Scope";
import * as Tracer from "effect/Tracer";

/**
 * Live lifetime and diagnostics may cross the managed host's private storage boundary.
 *
 * @internal
 */
export const applicationInvocationContext = Context.pick(
  Scope.Scope,
  Clock.Clock,
  Tracer.ParentSpan,
  Tracer.Tracer,
  Tracer.MinimumTraceLevel,
  Tracer.CurrentTraceLevel,
  References.TracerEnabled,
  References.TracerTimingEnabled,
  References.TracerSpanAnnotations,
  References.TracerSpanLinks,
  References.CurrentLoggers,
  References.CurrentLogLevel,
  References.MinimumLogLevel,
  References.CurrentStackFrame,
  References.CurrentLogAnnotations,
  References.CurrentLogSpans,
);
