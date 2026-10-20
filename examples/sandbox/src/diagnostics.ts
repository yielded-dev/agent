import type { CanonicalRecordEnvelope } from "@yielded/agent/records";
import { DateTime, Schema } from "effect";
import { Prompt } from "effect/ai";

import { CodingInput, CommandOutput, type DiagnosticRow } from "./contract.ts";

const Failure = Schema.Struct({ errorTag: Schema.String, message: Schema.String });

/** Read-only projection of one bounded history page; no diagnostic state is persisted. */
export const diagnosticRows = (records: ReadonlyArray<CanonicalRecordEnvelope>) => {
  const rows: DiagnosticRow[] = [];
  const calls = new Map<string, number>();

  for (const { sequence, record } of records) {
    const p = record.payload;
    const at = DateTime.toEpochMillis(record.createdAt);
    const base = { id: String(sequence), at, detail: "", status: "Recorded", failed: false };

    switch (p._tag) {
      case "UserInputRecorded":
        if (Schema.is(CodingInput)(p.input))
          rows.push({
            ...base,
            label: "Message accepted",
            ...(p.runId === undefined ? {} : { runId: p.runId }),
            detail: p.input.settings
              ? `${p.input.settings.model} · ${p.input.settings.reasoningEffort} reasoning · ${p.input.settings.serviceTier}`
              : "Original model settings",
          });
        break;
      case "ModelResponseRecorded": {
        rows.push({
          ...base,
          label: `Model response · turn ${p.turn}`,
          runId: p.runId,
          detail: [...new Set(p.modelUsage?.map((usage) => usage.model) ?? [])].join(", "),
          ...(p.inputTokens === undefined ? {} : { inputTokens: p.inputTokens }),
          ...(p.outputTokens === undefined ? {} : { outputTokens: p.outputTokens }),
          responseIds:
            p.modelUsage?.flatMap((usage) =>
              usage.response?.id === undefined ? [] : [usage.response.id],
            ) ?? [],
        });
        const prompt = Schema.decodeSync(Prompt.Prompt)(p.messages);

        for (const message of prompt.content) {
          if (message.role !== "assistant") continue;
          for (const part of message.content) {
            if (part.type !== "tool-call" || part.providerExecuted) continue;
            calls.set(`${p.runId}:${part.id}`, rows.length);
            rows.push({
              ...base,
              id: `${sequence}:${part.id}`,
              label: part.name,
              detail: JSON.stringify(part.params).slice(0, 1024),
              runId: p.runId,
              toolCallId: part.id,
              status: "Requested",
            });
          }
        }
        break;
      }
      case "ToolCallSettled": {
        const index = calls.get(`${p.runId}:${p.toolCallId}`);
        const started = index === undefined ? undefined : rows[index];
        const output = Schema.is(CommandOutput)(p.result) ? p.result : undefined;
        const failure = p.isFailure && Schema.is(Failure)(p.result) ? p.result : undefined;

        const failed =
          p.isFailure || (output !== undefined && (output.timedOut || output.exitCode !== 0));

        const row: DiagnosticRow = {
          ...(started ?? base),
          label: p.toolName,
          runId: p.runId,
          toolCallId: p.toolCallId,
          detail: [
            started?.detail,
            failure === undefined
              ? undefined
              : `${failure.errorTag}: ${failure.message}`.slice(0, 4096),
            output?.stderr.slice(0, 2048),
            failed ? output?.stdout.slice(-2048) : undefined,
          ]
            .filter(Boolean)
            .join("\n\n"),
          failed,
          status: output?.timedOut
            ? "Timed out"
            : output
              ? `Exit ${output.exitCode}`
              : failed
                ? "Failed"
                : "Completed",
          ...(started === undefined ? {} : { durationMs: Math.max(0, at - started.at) }),
        };

        if (index === undefined) rows.push(row);
        else rows[index] = row;
        break;
      }
      case "SubmissionSettled":
        rows.push({
          ...base,
          label: "Run settled",
          ...(p.runId === undefined ? {} : { runId: p.runId }),
          status: p.outcome,
          failed: p.outcome === "failed",
          detail: Schema.is(Failure)(p.result) ? `${p.result.errorTag}: ${p.result.message}` : "",
        });
        break;
      case "ModelResponseInterrupted":
        rows.push({
          ...base,
          label: "Model response interrupted",
          runId: p.runId,
          status: "Not retained",
        });
        break;
      case "CompactionCreated":
        rows.push({ ...base, label: "Context compacted", runId: p.runId, detail: p.kind });
        break;
      case "ToolCallUnknown":
        rows.push({
          ...base,
          label: "Tool outcome unknown",
          runId: p.runId,
          toolCallId: p.toolCallId,
          status: "Needs review",
          failed: true,
        });
        break;
    }
  }

  return rows;
};
