import { BrowserCrypto } from "@effect/platform-browser";
import { ledgerLayer } from "@yielded/agent-storage-cloudflare/do-submission-ledger";
import { layer, invalidate } from "@yielded/agent-storage-cloudflare/do-thread-store";
import * as Agent from "@yielded/agent/agent";
import { canonicalJson } from "@yielded/agent/digest";
import { DurableAgentRuntime, DurableRuntimeConfig } from "@yielded/agent/durable-agent-runtime";
import { DurableRuntimeFailpoint } from "@yielded/agent/durable-failpoint";
import { RunToolAuthorization } from "@yielded/agent/run-options";
import { layer as runStorageLayer } from "@yielded/agent/run-storage";
import { IdempotencyKey } from "@yielded/agent/submission-ledger";
import { ThreadStore } from "@yielded/agent/thread-store";
import { ToolReconciler } from "@yielded/agent/tool-reconciler";
import { WakeScheduler } from "@yielded/agent/wake-scheduler";
import { Effect, Layer, Schema, Stream } from "effect";
import { LanguageModel, Model, Prompt, Toolkit } from "effect/ai";
import { expect, it } from "vite-plus/test";

import {
  id,
  thread,
  TEST_DEPLOYMENT,
  TEST_DIGESTS,
  TEST_PRINCIPAL,
  TEST_PRODUCER,
  withThreadStorage,
} from "./harness.ts";

// Requested cold-checkpoint seam: native persisted bytes and a real reopen distinguish
// a verified hit from a silently stale prompt or an implementation that always rescans.
it("reopens a projected prefix with an identical prompt and rebuilds damaged derivatives", async () => {
  const run = (
    name: string,
    turn: number,
    damage?: "missing" | "json" | "prompt" | "head" | "version",
  ) =>
    withThreadStorage(name, (storage) =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;

        if (damage !== undefined) {
          const rows = [
            ...storage.sql.exec<{ key: string; value: string }>(
              "SELECT key, value FROM effect_agent_meta WHERE key >= 'prompt-checkpoint/' AND key < 'prompt-checkpoint0' ORDER BY key",
            ),
          ];

          for (const row of rows) {
            if (damage === "missing")
              storage.sql.exec("DELETE FROM effect_agent_meta WHERE key = ?", row.key);
            else if (damage === "json")
              storage.sql.exec(
                "UPDATE effect_agent_meta SET value = ? WHERE key = ?",
                "{",
                row.key,
              );
            else if (damage === "prompt" && row.key.endsWith("00001"))
              storage.sql.exec(
                "UPDATE effect_agent_meta SET value = ? WHERE key = ?",
                row.value.replace("input 0", "wrong 0"),
                row.key,
              );
            else if (damage === "head" && row.key.endsWith("00000"))
              storage.sql.exec(
                "UPDATE effect_agent_meta SET value = ? WHERE key = ?",
                row.value.replace(
                  /"tailDigest":"[a-f0-9]+"/,
                  '"tailDigest":"' + "0".repeat(64) + '"',
                ),
                row.key,
              );
            else if (damage === "version" && row.key.endsWith("00000"))
              storage.sql.exec(
                "UPDATE effect_agent_meta SET value = ? WHERE key = ?",
                row.value.replace('"version":1', '"version":999'),
                row.key,
              );
          }
          yield* invalidate(storage);
        }
        let promptRecords = 0;
        let transcript = "";

        const counted = ThreadStore.of({
          ...store,
          readPrompt: (request) =>
            store.readPrompt(request).pipe(
              Stream.tap(() =>
                Effect.sync(() => {
                  promptRecords++;
                }),
              ),
            ),
        });

        const model = Model.make(
          "scripted",
          "cold-checkpoint",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: ({ prompt }) => {
                transcript = canonicalJson(
                  Schema.encodeSync(Schema.toCodecJson(Prompt.Prompt))(prompt),
                );

                return Stream.fromIterable([
                  { type: "text-start", id: "answer" },
                  { type: "text-delta", id: "answer", delta: '"done"' },
                  { type: "text-end", id: "answer" },
                  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
                ]);
              },
            }),
          ),
        );

        const agent = Agent.withModel(
          Agent.make("cold-checkpoint", {
            input: Schema.String,
            output: Schema.String,
            instructions: "Retain the conversation.",
            toolkit: Toolkit.empty,
            policy: { maxTurns: 1, maxDuration: "30 seconds" },
          }),
          model,
        );

        const runtime = yield* DurableAgentRuntime.pipe(
          Effect.provide(DurableAgentRuntime.layer.pipe(Layer.provide(runStorageLayer()))),
          Effect.provideService(ThreadStore, counted),
        );

        const receipt = yield* runtime.submit(agent, `input ${turn}`, {
          threadId: thread(name),
          principal: TEST_PRINCIPAL,
          idempotencyKey: id(IdempotencyKey, `input-${turn}`),
          definitions: TEST_DIGESTS,
        });

        const settlements = yield* runtime.processThread(agent, receipt.threadId);

        expect(settlements).toHaveLength(1);
        expect(settlements[0]?.outcome).toBe("completed");

        return { transcript, promptRecords };
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            layer({ storage }),
            ledgerLayer({ storage }),
            WakeScheduler.layerNoop,
            ToolReconciler.uncertain,
            DurableRuntimeFailpoint.layer,
            RunToolAuthorization.allowAll,
            DurableRuntimeConfig.layer({
              deploymentId: TEST_DEPLOYMENT,
              producerId: TEST_PRODUCER,
            }),
            BrowserCrypto.layer,
          ),
        ),
        Effect.scoped,
      ),
    );

  const reference = `prompt-reference-${crypto.randomUUID()}`;

  for (let turn = 0; turn < 8; turn++) await run(reference, turn);
  const rebuilt = await run(reference, 8, "missing");

  expect(rebuilt.promptRecords).toBeGreaterThan(4);

  for (const damage of [undefined, "json", "prompt", "head", "version"] as const) {
    const name = `prompt-${damage ?? "hit"}-${crypto.randomUUID()}`;

    for (let turn = 0; turn < 8; turn++) await run(name, turn);
    const observed = await run(name, 8, damage);

    expect(observed.transcript).toBe(rebuilt.transcript);
    if (damage === undefined) expect(observed.promptRecords).toBeLessThanOrEqual(4);
    else expect(observed.promptRecords).toBeGreaterThan(4);
  }
}, 60_000);
