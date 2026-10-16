import type { PlatformError } from "effect";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import { Hex } from "effect/encoding";
import * as Schema from "effect/Schema";

import { canonicalJson as stringifyCanonicalJson } from "./internal/canonical-json.ts";
import type { DefinitionDigestInput } from "./Records.ts";
import { DefinitionDigests, Digest } from "./Records.ts";

/** Measure persisted text without allocating a UTF-8 copy. */
export { utf8ByteLength } from "../core/internal/utf8.ts";

export class DigestError extends Schema.TaggedError<DigestError>()("DigestError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

const utf8 = new TextEncoder();
const decodeDigest = Schema.decodeEffect(Digest);

/** Serialize schema-encoded JSON with the canonical digest's locale-independent key order. */
export const canonicalJson = (value: Schema.Json): string => stringifyCanonicalJson(value);

const digestText = Effect.fnUntraced(function* (
  value: string,
): Effect.fn.Return<Digest, DigestError, Crypto.Crypto> {
  const crypto = yield* Crypto.Crypto;

  const bytes = utf8.encode(value);

  const digest = yield* crypto
    .digest("SHA-256", bytes)
    .pipe(
      Effect.mapError((error: PlatformError.PlatformError) =>
        DigestError.make({ message: `SHA-256 failed: ${error.message}`, cause: error }),
      ),
    );

  return yield* decodeDigest(Hex.encode(digest)).pipe(
    Effect.mapError(() => DigestError.make({ message: "SHA-256 returned an invalid digest" })),
  );
});

/** Digest a JSON value using a stable, locale-independent object-key ordering (UTF-16 code units, RFC 8785 style). */
export const digestJson = (value: Schema.Json): Effect.Effect<Digest, DigestError, Crypto.Crypto> =>
  Effect.suspend(() => digestText(canonicalJson(value)));

/** Hash captured canonical JSON directly, sharing its encoding with accounting and storage. */
export const digestCanonicalJson = (json: string) => digestText(json);

/** Digest one schema-encoded Agent, Model, or Toolkit definition. */
export const digestDefinition = (definition: Schema.Json) => digestJson(definition);

/** Digest all replay-relevant definitions without hiding which authority changed. */
export const digestDefinitions = (
  definitions: DefinitionDigestInput,
): Effect.Effect<DefinitionDigests, DigestError, Crypto.Crypto> =>
  Effect.all({
    agent: digestDefinition(definitions.agent),
    model: digestDefinition(definitions.model),
    tools: digestDefinition(definitions.tools),
  }).pipe(Effect.map((digests) => DefinitionDigests.make(digests)));
