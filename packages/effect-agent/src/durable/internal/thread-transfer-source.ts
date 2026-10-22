import * as Context from "effect/Context";

/**
 * Installed before opening the source pull, so producer fibers observe the same phase.
 * @internal
 */
export const ImportSourcePhase = Context.Reference<{ writing: boolean } | undefined>(
  "@effect-agent/thread/internal/ImportSourcePhase",
  { defaultValue: () => undefined },
);
