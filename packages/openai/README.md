# @yielded/agent-openai

Native OpenAI compaction for Yielded Agent, using the unchanged
`OpenAiLanguageModel.model` from `@effect/ai-openai`.

`OpenAiCompaction.layer` provides Yielded's `ContextCompactor` and requires the configured
`OpenAiClient`:

```ts
import { OpenAiLanguageModel } from "@effect/ai-openai";
import { OpenAiCompaction } from "@yielded/agent-openai";
import { Layer } from "effect";

const ModelLive = OpenAiLanguageModel.model("gpt-5.4-mini", {
  store: false,
  reasoning: { effort: "none" },
});
const CompactorLive = OpenAiCompaction.layer;
const OpenAiLive = Layer.merge(ModelLive, CompactorLive);
```

Provide the same configured `OpenAiClient` Layer to `OpenAiLive`, then compose it with your
runtime or durable host. The compactor uses the runtime-selected model identity. The direct
entry is `@yielded/agent-openai/openai-compaction`.

The engine selects and accounts eligible completed history, commits its replacement,
and keeps the provider window beside the ordinary Prompt. The adapter validates the whole
`/responses/compact` result, then counts that exact window with JSON
`POST /responses/input_tokens` through the same configured client. Input-count endpoint
support is required; counting is part of the metered compact operation, not another inference.
The adapter prepends every retained native item before the stock model's newly encoded input;
it neither executes retained tools nor converts opaque state into Prompt parts.

## Configuration

Inference must use stateless full input: `store: false`, no conversation or previous
response ID, no item-reference mode, and no automatic truncation. ResponseIdTracker,
OpenAI WebSocket mode, and background responses are rejected. Retained state requires
its exact provider and effective request-model string; prefer pinned model snapshots.

Compact takes the model selected by the engine. It reads scoped
`OpenAiLanguageModel.Config` for instructions, prompt-cache key, and service tier.
Defaults passed only to a model constructor are private and cannot be inferred;
provide any required compact settings through the public scoped config. A conflicting
scoped model or stateful setting fails before dispatch. There is no parallel model-options API.

Existing client-construction and scoped OpenAiConfig transforms are preserved.
Keep scoped transforms outside the engine's replay boundary: replacing OpenAiConfig
inside the model stream bypasses the replay hook and is unsupported. Replay checks the
explicit model and any available provider identity, and verifies that its request hook
ran before forwarding output. Captured stock model streams need no ambient Model service. Arbitrary custom transports or postprocessors that bypass ordinary client
preprocessing are not supported. Stream consumers and unrelated streams do not inherit
the native prefix.

## Supported history and bounds

Covered history supports user/assistant text, plain reasoning with an OpenAI item ID,
and complete ordinary function-call/result pairs. Ordinary results must be in tool
messages; JSON results are encoded once, while string results remain strings. Missing
portable assistant item IDs stay absent. System instructions, files, hosted tools,
approval parts, and ordinary encrypted reasoning are rejected. Stock `OpenAiShell`,
`OpenAiLocalShell`, and `OpenAiApplyPatch` histories are also refused: local execution
does not make these provider-defined tools ordinary function calls.

Returned windows retain admitted text/refusal content, ordinary tool wire strings,
reasoning, and compaction items in order, including supported absent/null distinctions.
Unknown fields and unsupported shapes fail rather than being removed. The stored format
is `openai.responses.compaction@1` inside the core's versioned native envelope, with strict
provider data `{ output, inputTokens }`.

Combined input and returned windows are limited to 1,024 items. HTTP request/response
bodies are limited to 1 MiB; the complete accounted native envelope remains subject to
core's 256-KiB bound. The full-window estimate equals the stored provider input count,
including visible and opaque items. New-tail and tool overhead remain engine-owned.
Repeated compaction replaces this count; it does not accumulate old reserves or tokenize
ciphertext locally. Replay validates the stored count without calling the counter again.

Compact preserves independently readable billed token totals when later usage, output,
counting, or stored-state validation fails. Count failures use `AiError.InvalidOutputError`
with the compact call's usage, not counter-response usage. Missing cache/reasoning breakdowns
remain unknown. An unreadable, interrupted, or transport-size-limited compact response body
cannot supply known usage. Invalid responses use stock `AiError.InvalidOutputError`; replay request-hook
failures pass through stock OpenAI's `NetworkError` with `EncodeError`, not an invented
error variant. Deterministic guard failures should not be retried as transient network
failures. Native state is sensitive and is never included in adapter error descriptions.
