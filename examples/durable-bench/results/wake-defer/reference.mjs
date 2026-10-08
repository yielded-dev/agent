import { createHash } from "node:crypto";
import { history, next, payload, turn } from "../../src/plan.ts";

// Reused reference projection from cf-latency/analyze.mjs. Actual native-provider
// receipts, including all prior measured inputs, must match every one of nine steps.
export const expectedSeed = { 50: "b017b487524e44a4", 250: "dcea9f30b0917245" };
const fingerprint = (messages) => createHash("sha256")
  .update(JSON.stringify(messages.map((message) => [message.role, message.text, message.calls ?? []])))
  .digest("hex").slice(0, 16);

export function reference() {
  const expected = {};
  for (const count of [50, 250]) {
    const messages = [];
    const append = (input) => {
      messages.push({ role: "user", text: input.text });
      const hashes = [];
      for (;;) {
        hashes.push(fingerprint(messages));
        const step = next(messages);
        if ("answer" in step) { messages.push({ role: "assistant", text: step.answer }); break; }
        messages.push({ role: "assistant", text: "", calls: [step.call] }, { role: "tool", text: payload(step.call) });
      }
      return hashes;
    };
    let seed;
    for (const input of history(0, count)) seed = append(input).at(-1);
    if (seed !== expectedSeed[count]) throw new Error(`Reference fixture drift at history ${count}`);
    expected[count] = { seed, turns: {} };
    for (let index = 0; index < 16; index++) expected[count].turns[`m${index}`] = append(turn(`m${index}`, 8));
  }
  return expected;
}
