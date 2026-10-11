import { Context } from "effect";

/** Installed before opening the source pull, so producer fibers observe the same phase. */
export const ImportSourcePhase = Context.Reference<{ writing: boolean } | undefined>(
  "@effect-agent/thread/internal/ImportSourcePhase",
  { defaultValue: () => undefined },
);
