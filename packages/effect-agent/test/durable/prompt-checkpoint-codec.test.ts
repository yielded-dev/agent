import { expect, it } from "@effect/vitest";
import * as Prompt from "effect/ai/Prompt";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { decodeCheckpointPrefix } from "../../src/durable/internal/prompt-history-cache.ts";

// Requested regression seam: hydration must track Effect's codec, including native
// value types that can differ while producing identical JSON on re-encoding.
it.effect("hydrates checkpoint messages exactly like Effect's JSON codec", () =>
  Effect.gen(function* () {
    const options = { provider: { cache: true, nested: [null, "value", 1] } };

    const files = [new URL("https://example.com/image"), new Uint8Array([1, 2, 3]), "AQID"].map(
      (data) => Prompt.filePart({ data, mediaType: "image/png", fileName: "image", options }),
    );

    const mixed = Prompt.fromMessages([
      Prompt.systemMessage({ content: "system", options }),
      Prompt.userMessage({
        content: [Prompt.textPart({ text: "user", options }), ...files],
        options,
      }),
      Prompt.assistantMessage({
        content: [
          Prompt.textPart({ text: "assistant", options }),
          Prompt.reasoningPart({ text: "reasoning", options }),
          ...files,
          Prompt.toolCallPart({
            id: "call",
            name: "tool",
            params: { x: [null, true] },
            providerExecuted: true,
            options,
          }),
          Prompt.toolResultPart({
            id: "call",
            name: "tool",
            result: { answer: 42 },
            isFailure: false,
            providerExecuted: true,
            options,
          }),
          Prompt.toolApprovalRequestPart({ approvalId: "approval", toolCallId: "call", options }),
        ],
        options,
      }),
      Prompt.toolMessage({
        content: [
          Prompt.toolResultPart({
            id: "call",
            name: "tool",
            result: "failed",
            isFailure: true,
            providerExecuted: false,
            options,
          }),
          Prompt.toolApprovalResponsePart({
            approvalId: "approval",
            approved: false,
            reason: "denied",
            options,
          }),
        ],
        options,
      }),
    ]);

    const codec = Schema.toCodecJson(Prompt.Prompt);
    const encoded = yield* Schema.encodeEffect(codec)(mixed);

    const minimal = {
      content: [
        { role: "system", content: "system" },
        { role: "user", content: "user" },
        { role: "assistant", content: "assistant" },
        {
          role: "assistant",
          content: [
            { type: "reasoning", text: "reason" },
            { type: "tool-call", id: "call", name: "tool", params: {} },
            { type: "tool-approval-request", approvalId: "approval", toolCallId: "call" },
          ],
        },
        {
          role: "tool",
          content: [
            { type: "tool-result", id: "call", name: "tool", result: null, isFailure: false },
            { type: "tool-approval-response", approvalId: "approval", approved: true },
          ],
        },
      ],
    };

    for (const input of [encoded, minimal]) {
      const json = JSON.stringify(input);

      // Both inputs qualify for hydration, so this also covers the fast path.
      yield* Schema.decodeEffect(
        Schema.fromJsonString(
          Schema.Struct({ content: Schema.Array(Schema.toEncoded(Prompt.Message)) }),
        ),
      )(json);
      const expected = yield* Schema.decodeEffect(Schema.fromJsonString(codec))(json);
      const actual = yield* decodeCheckpointPrefix(json, expected.content.length);

      expect(actual?.prompt).toStrictEqual(expected);
    }
    // The JSON codec's string-first union decodes URLs and binary data as strings.
    const fileCodec = Schema.toCodecJson(Prompt.FilePart);

    for (const [index, part] of files.entries()) {
      const json = yield* Schema.encodeEffect(fileCodec)(part);
      const expectedData = index === 0 ? "https://example.com/image" : "AQID";

      expect(json).toMatchObject({ data: expectedData });
      expect((yield* Schema.decodeEffect(fileCodec)(json)).data).toBe(expectedData);
    }
  }),
);
