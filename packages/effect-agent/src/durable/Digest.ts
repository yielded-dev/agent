/**
 * Canonical JSON serialization, UTF-8 size accounting, and digests for persisted values and
 * definition identity.
 *
 * @since 0.1.0
 */

import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import type * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";

import { utf8Bytes } from "../core/internal/utf8.ts";
import { canonicalJson as stringifyCanonicalJson } from "./internal/canonical-json.ts";
import type { DefinitionDigestInput } from "./Records.ts";
import { DefinitionDigests, Digest } from "./Records.ts";

export {
  /**
   * Count encoded bytes without allocating an encoded copy of the text.
   *
   * @category operations
   * @since 0.1.0
   */
  utf8ByteLength,
} from "../core/internal/utf8.ts";

/**
 * Canonical serialization or digest computation failed for the supplied value.
 *
 * @category errors
 * @since 0.1.0
 */
export class DigestError extends Schema.TaggedError<DigestError>()("DigestError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

const decodeDigest = Schema.decodeEffect(Digest);

/**
 * Serialize schema-encoded JSON with the canonical digest's locale-independent key order.
 *
 * @category operations
 * @since 0.1.0
 */
export const canonicalJson = (value: Schema.Json): string => stringifyCanonicalJson(value);

const digestText = Effect.fnUntraced(function* (
  value: string,
): Effect.fn.Return<Digest, DigestError, Crypto.Crypto> {
  const crypto = yield* Crypto.Crypto;

  const bytes = utf8Bytes(value);

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

/**
 * Digest a JSON value using a stable, locale-independent object-key ordering (UTF-16 code units, RFC 8785 style).
 *
 * @category operations
 * @since 0.1.0
 */
export const digestJson = (value: Schema.Json): Effect.Effect<Digest, DigestError, Crypto.Crypto> =>
  Effect.suspend(() => digestText(canonicalJson(value)));

/**
 * Hash captured canonical JSON directly, sharing its encoding with accounting and storage.
 *
 * @category operations
 * @since 0.1.0
 */
export const digestCanonicalJson = (json: string) => digestText(json);

/**
 * Digest one schema-encoded Agent, Model, or Toolkit definition.
 *
 * @category operations
 * @since 0.1.0
 */
export const digestDefinition = (definition: Schema.Json) => digestJson(definition);

/**
 * Digest all replay-relevant definitions without hiding which authority changed.
 *
 * @category operations
 * @since 0.1.0
 */
export const digestDefinitions = (
  definitions: DefinitionDigestInput,
): Effect.Effect<DefinitionDigests, DigestError, Crypto.Crypto> =>
  Effect.all({
    agent: digestDefinition(definitions.agent),
    model: digestDefinition(definitions.model),
    tools: digestDefinition(definitions.tools),
  }).pipe(Effect.map((digests) => DefinitionDigests.make(digests)));
