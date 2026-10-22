import type { CanonicalSequence, RunContinuation } from "@yielded/agent/records";
import type { ExecutionCheckpoint } from "@yielded/agent/thread-store";

/** @internal */
export interface ExecutionPredecessor {
  readonly progress: RunContinuation;
  readonly originalSequence: CanonicalSequence;
  readonly operational: ReadonlyArray<Pick<ExecutionCheckpoint["operational"][number], "recordId">>;
}

/** @internal */
export type ExecutionState =
  | { readonly checkpoint: ExecutionPredecessor; readonly bytes: number }
  | {
      readonly checkpoint: undefined;
      /** Exact canonical frontier at which both execution and initial Run facts were absent. */
      readonly throughSequence: CanonicalSequence;
      readonly bytes: number;
    };

// Private predecessors belong to the physical owner's mutation gate. Failed transactions
// and out-of-band maintenance discard them before readers can enter that gate again.
/** @internal */
export const executionStates = new WeakMap<object, Map<string, ExecutionState>>();

/** @internal */
export const executionStateKey = (namespace: string | undefined, threadId: string, runId: string) =>
  JSON.stringify([namespace, threadId, runId]);

/** @internal */
export const retainExecutionState = (
  states: Map<string, ExecutionState>,
  key: string,
  state: ExecutionState,
) => {
  states.delete(key);
  if (state.bytes > 4 * 1024 * 1024) return;
  states.set(key, state);
  let bytes = 0;

  for (const value of states.values()) bytes += value.bytes;
  while (states.size > 128 || bytes > 4 * 1024 * 1024) {
    const oldest = states.entries().next().value;

    if (oldest === undefined) break;
    bytes -= oldest[1].bytes;
    states.delete(oldest[0]);
  }
};
