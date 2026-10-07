import { Schema } from "effect";

import { type RunId, type ToolCallId } from "../../core/Identifiers.ts";
import {
  type CanonicalRecordEnvelope,
  type PromptRecordEnvelope,
  type CompactionCreated,
  type ToolCallSettled,
  RecordId,
} from "../Records.ts";

const decodeRecordId = Schema.decodeSync(RecordId);

/** Deterministic canonical record identity of one Turn's Tool result. */
export const toolCallSettledRecordId = (
  runId: RunId,
  turn: number,
  toolCallId: ToolCallId,
): RecordId => decodeRecordId(`tool-settled:${runId}:${turn}:${toolCallId}`);

export type JournalRecordEnvelope = CanonicalRecordEnvelope | PromptRecordEnvelope;

/** Metadata for one exact canonical prefix, including the exact selected Run evidence. */
export interface JournalMetadata {
  readonly ownerRunId: RunId | undefined;
  readonly firstSequenceByRun: ReadonlyMap<string, number>;
  readonly responseSequencesByRun: ReadonlyMap<string, ReadonlyArray<number>>;
  readonly terminalSequenceByRun: ReadonlyMap<string, number>;
  readonly settledSpans: ReadonlyArray<{
    readonly from: number;
    readonly to: number;
    readonly runId: RunId;
    readonly recordId: string;
  }>;
  readonly settledSequenceById: ReadonlyMap<string, number>;
  readonly settledToolCallRecordIds: ReadonlySet<string>;
  readonly settledById: ReadonlyMap<
    string,
    Pick<ToolCallSettled, "isFailure" | "budgetRejected" | "toolSelection">
  >;
  readonly compactions: ReadonlyArray<{
    readonly payload: CompactionCreated;
    readonly sequence: number;
  }>;
}

/**
 * Attempt-local metadata collector. Feed every validated record exactly once, in canonical order,
 * including records omitted from the control view. A snapshot is independent of later suffix
 * additions and may only accompany a replay stream bounded at that same captured tail.
 */
export const makeJournalMetadata = (ownerRunId: RunId | undefined) => {
  const firstSequenceByRun = new Map<string, number>();

  const responseSequencesByRun = new Map<string, Array<number>>();

  const declarationsByRun = new Map<
    RunId,
    Array<{
      readonly sequence: number;
      readonly turn: number;
      readonly callIds: ReadonlyArray<string>;
    }>
  >();

  const terminalSequenceByRun = new Map<string, number>();

  const settledSpans: Array<{
    readonly from: number;
    readonly to: number;
    readonly runId: RunId;
    readonly recordId: string;
  }> = [];

  const settledSequenceById = new Map<string, number>();

  const settledToolCallRecordIds = new Set<string>();

  const settledById = new Map<
    string,
    Pick<ToolCallSettled, "isFailure" | "budgetRejected" | "toolSelection">
  >();

  const compactions: Array<{ readonly payload: CompactionCreated; readonly sequence: number }> = [];

  return {
    add: (envelope: JournalRecordEnvelope): void => {
      const payload = envelope.record.payload;

      if (
        (payload._tag === "RunCompleted" ||
          payload._tag === "RunFailed" ||
          payload._tag === "SubmissionSettled") &&
        payload.runId !== undefined &&
        !terminalSequenceByRun.has(payload.runId)
      )
        terminalSequenceByRun.set(payload.runId, envelope.sequence);
      if (payload._tag === "CompactionCreated") {
        compactions.push({ payload, sequence: envelope.sequence });

        return;
      }
      if (
        "runId" in payload &&
        typeof payload.runId === "string" &&
        !firstSequenceByRun.has(payload.runId)
      )
        firstSequenceByRun.set(payload.runId, envelope.sequence);
      if (payload._tag === "ModelResponseRecorded") {
        const sequences = responseSequencesByRun.get(payload.runId) ?? [];

        sequences.push(envelope.sequence);
        responseSequencesByRun.set(payload.runId, sequences);
        const declarations = declarationsByRun.get(payload.runId) ?? [];

        // Retain references to bounded source identities, not an expanded settlement-ID
        // string and Map entry for every declared operation in the context.
        declarations.push({
          sequence: envelope.sequence,
          turn: payload.turn,
          callIds:
            "toolOperations" in payload
              ? payload.toolOperations.map((operation) => operation.toolCallId)
              : payload.messages.content.flatMap((message) =>
                  message.role === "assistant"
                    ? message.content.flatMap((part) =>
                        part.type === "tool-call" && !part.providerExecuted ? [part.id] : [],
                      )
                    : [],
                ),
        });
        declarationsByRun.set(payload.runId, declarations);
      } else if (payload._tag === "ToolCallSettled") {
        settledToolCallRecordIds.add(envelope.record.recordId);
        settledSequenceById.set(envelope.record.recordId, envelope.sequence);
        if (payload.runId === ownerRunId)
          settledById.set(envelope.record.recordId, {
            isFailure: payload.isFailure,
            ...(!("toolSelection" in payload) || payload.toolSelection === undefined
              ? {}
              : { toolSelection: payload.toolSelection }),
            ...(!("budgetRejected" in payload) || payload.budgetRejected === undefined
              ? {}
              : { budgetRejected: payload.budgetRejected }),
          });

        const from = declarationsByRun
          .get(payload.runId)
          ?.findLast(
            (declaration) =>
              declaration.callIds.includes(payload.toolCallId) &&
              toolCallSettledRecordId(payload.runId, declaration.turn, payload.toolCallId) ===
                envelope.record.recordId,
          )?.sequence;

        if (from !== undefined && from < envelope.sequence)
          settledSpans.push({
            from,
            to: envelope.sequence,
            runId: payload.runId,
            recordId: envelope.record.recordId,
          });
      }
    },
    snapshot: (): JournalMetadata => ({
      ownerRunId,
      firstSequenceByRun: new Map(firstSequenceByRun),
      responseSequencesByRun: new Map(
        [...responseSequencesByRun].map(([runId, sequences]) => [runId, [...sequences]]),
      ),
      terminalSequenceByRun: new Map(terminalSequenceByRun),
      settledSpans: [...settledSpans],
      settledSequenceById: new Map(settledSequenceById),
      settledToolCallRecordIds: new Set(settledToolCallRecordIds),
      settledById: new Map(settledById),
      compactions: [...compactions],
    }),
  };
};
