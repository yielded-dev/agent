import { Clock, Context, References, Scope, Tracer } from "effect";

/** Live lifetime and diagnostics may cross the managed host's private storage boundary. */
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
