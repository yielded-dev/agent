import type { RunId, TurnId } from "../../core/Identifiers.ts";
import type { CanonicalRecordEnvelope, ToolOperation } from "../Records.ts";

/** Derived from a validated canonical response; never a second persisted execution record. */
export interface ToolOperationState {
  readonly operation: ToolOperation;
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly turn: number;
  readonly dispatchBlocked: boolean;
  readonly settled: boolean;
  readonly resolved: boolean;
  readonly unknown: boolean;
}

const callKey = (runId: RunId, toolCallId: string): string => JSON.stringify([runId, toolCallId]);
const turnKey = (runId: RunId, turn: number): string => JSON.stringify([runId, turn]);

const toolOperationFacts = (records: ReadonlyArray<CanonicalRecordEnvelope>, runId?: RunId) => {
  const settled = new Set<string>();
  const resolved = new Set<string>();
  const unknown = new Set<string>();
  const unknownTurns = new Set<string>();
  const requested = new Map<string, { readonly key: string; readonly turn: number }>();
  const decisions = new Map<string, "approved" | "denied">();
  const blockedTurns = new Map<string, number>();

  for (const {
    record: { payload },
  } of records) {
    if (!("runId" in payload) || (runId !== undefined && payload.runId !== runId)) continue;
    switch (payload._tag) {
      case "ToolCallSettled":
        settled.add(callKey(payload.runId, payload.toolCallId));
        break;
      case "ToolCallResolved":
        if (
          payload.resolution === "completed-with-result" ||
          payload.resolution === "failed-with-error"
        )
          resolved.add(callKey(payload.runId, payload.toolCallId));
        break;
      case "ToolCallUnknown":
        unknown.add(callKey(payload.runId, payload.toolCallId));
        unknownTurns.add(turnKey(payload.runId, payload.turn));
        break;
      case "ToolApprovalRequested":
        if (payload.blocksInitialDispatch)
          requested.set(callKey(payload.runId, payload.toolCallId), {
            key: turnKey(payload.runId, payload.turn),
            turn: payload.turn,
          });
        break;
      case "ToolApprovalDecided":
        decisions.set(callKey(payload.runId, payload.toolCallId), payload.decision);
        break;
    }
  }
  for (const [call, turn] of requested)
    if (decisions.get(call) !== "approved") blockedTurns.set(turn.key, turn.turn);

  return { settled, resolved, unknown, unknownTurns, blockedTurns };
};

/**
 * Initial-dispatch proof can pass between approval suspensions while a canonical blocker
 * remains. It comes only from an undecided or denied request carrying that proof, never a
 * ledger intent, parameter rejection, or permission to retry an earlier execution.
 */
export const initialDispatchBlockedTurns = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  runId: RunId,
): ReadonlySet<number> => {
  const { blockedTurns, unknownTurns } = toolOperationFacts(records, runId);

  return new Set(
    [...blockedTurns].filter(([key]) => !unknownTurns.has(key)).map(([, turn]) => turn),
  );
};

/**
 * Whole-batch approval precedes every handler permit. A request carrying initial-dispatch
 * proof blocks the Turn until its decision is canonically approved. A resumed Attempt may
 * carry that proof forward to the next approval before dispatch. Parameter rejection blocks
 * only its own call and cannot certify the whole Turn.
 */
export const toolOperationStates = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  runId?: RunId,
): ReadonlyArray<ToolOperationState> => {
  const { settled, resolved, unknown, blockedTurns } = toolOperationFacts(records, runId);

  return records.flatMap(({ record: { payload } }) => {
    if (
      payload._tag !== "ModelResponseRecorded" ||
      (runId !== undefined && payload.runId !== runId)
    )
      return [];
    const rejected = new Set(payload.toolParameterRejections?.map((call) => call.toolCallId));

    return payload.toolOperations.map((operation) => {
      const key = callKey(payload.runId, operation.toolCallId);

      return {
        operation,
        runId: payload.runId,
        turnId: payload.turnId,
        turn: payload.turn,
        dispatchBlocked:
          rejected.has(operation.toolCallId) ||
          blockedTurns.has(turnKey(payload.runId, payload.turn)),
        settled: settled.has(key),
        resolved: resolved.has(key),
        unknown: unknown.has(key),
      };
    });
  });
};

/** Explicit Unknown evidence stays unresolved even if other facts claim dispatch was blocked. */
export const isUnresolvedToolOperation = (state: ToolOperationState): boolean =>
  !state.settled &&
  !state.resolved &&
  (state.unknown ||
    (!state.dispatchBlocked &&
      (state.operation.executionClass !== "readonly" ||
        state.operation.executionKind !== "ordinary")));

/** Unclosed effects derived from canonical declaration and closure facts. */
export const unresolvedToolOperations = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  runId?: RunId,
): ReadonlyArray<ToolOperationState> =>
  toolOperationStates(records, runId).filter(isUnresolvedToolOperation);
