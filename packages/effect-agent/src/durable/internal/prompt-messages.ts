import * as Prompt from "effect/ai/Prompt";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

import type { PersistedJson } from "../Records.ts";

const decodePrompt = Schema.decodeUnknownEffect(Prompt.Prompt);
const decodedMessages = new WeakMap<object, Prompt.Prompt>();

/**
 * Decode immutable messages captured by the durable boundary. Callers must not pass
 * unowned input: the captured value owns the lifetime and validity of this identity cache.
 */
export const decodeOwnedPrompt = (
  messages: PersistedJson | Prompt.Prompt,
): Effect.Effect<Prompt.Prompt, Schema.SchemaError> =>
  Effect.suspend(() => {
    if (Prompt.isPrompt(messages)) return Effect.succeed(messages);
    if (!Predicate.isObject(messages)) return decodePrompt(messages);
    const cached = decodedMessages.get(messages);

    return cached === undefined
      ? decodePrompt(messages).pipe(
          Effect.tap((prompt) =>
            Effect.sync(() => {
              decodedMessages.set(messages, prompt);
            }),
          ),
        )
      : Effect.succeed(cached);
  });
