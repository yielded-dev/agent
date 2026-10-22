/**
 * Bounded canonical Tool-result envelopes, including explicit truncation, serialization failure,
 * and parameter-rejection evidence.
 *
 * @since 0.1.0
 */

import * as AiError from "effect/ai/AiError";
import * as Schema from "effect/Schema";

import { ToolCallId } from "./Identifiers.ts";
import { codePointUtf8Length, utf8ByteLength } from "./internal/utf8.ts";
import { ToolResultId, ToolResultStorageError } from "./ToolResultStorage.ts";

/**
 * Engine-owned evidence that a fresh model call failed parameter validation.
 * Retain the exact JSON arguments beside the native error so recovery can distinguish
 * a rejected request from damaged executable parameters. This never authorizes execution.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ToolParameterRejection = Schema.Struct({
  toolCallId: ToolCallId,
  parameters: Schema.Json,
  error: Schema.toEncoded(
    Schema.Struct({
      ...AiError.AiError.fields,
      reason: AiError.ToolParameterValidationError,
    }),
  ),
});

/**
 * Decoded value of {@link ToolParameterRejection}.
 *
 * @category models
 * @since 0.1.0
 */
export type ToolParameterRejection = typeof ToolParameterRejection.Type;

/**
 * The minimal `TruncatedToolResult` envelope (empty head/tail, a 16-digit
 * `originalBytes`) plus a 64-character full-result ID fits within 256 bytes; every
 * accepted bound fits the envelope plus some real content, so the bound is a
 * hard resource guard rather than a best-effort one.
 */
const ENVELOPE_FLOOR_BYTES = 256;

/**
 * Byte bound applied to one encoded application Tool result before it enters history.
 *
 * @category models
 * @since 0.1.0
 */
export class ToolResultBounds extends Schema.Class<ToolResultBounds>("ToolResultBounds")({
  maxBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(ENVELOPE_FLOOR_BYTES)),
}) {}

/**
 * Canonical envelope replacing an encoded Tool result that exceeded its
 * policy bounds. `head` and `tail` are an exact prefix and suffix of the
 * original encoded JSON text. When fullResultId is present, read_tool_result can
 * retrieve the complete result from the host store. Otherwise the middle is lost.
 *
 * @category models
 * @since 0.1.0
 */
export class TruncatedToolResult extends Schema.Class<TruncatedToolResult>("TruncatedToolResult")({
  truncatedToolResult: Schema.Literal(true),
  originalBytes: Schema.Natural,
  head: Schema.String,
  tail: Schema.String,
  fullResultId: Schema.optionalKey(ToolResultId),
  fullResultError: Schema.optionalKey(ToolResultStorageError.fields.reason),
}) {}

/**
 * Canonical fail-closed sentinel replacing an encoded Tool result that could
 * not be JSON-serialized at all (cyclic value, BigInt, `undefined`, throwing
 * `toJSON`). The original value never enters history or durable records —
 * an unserializable value is unbounded by construction, so the bound seam
 * replaces it wholesale rather than passing it through (runtime spec §9).
 *
 * @category models
 * @since 0.1.0
 */
export class UnserializableToolResult extends Schema.Class<UnserializableToolResult>(
  "UnserializableToolResult",
)({
  unserializableToolResult: Schema.Literal(true),
  reason: Schema.String.check(Schema.isMaxLength(256)),
}) {}

/**
 * Build the encoded sentinel for a serialization failure. Total by
 * construction — the cause is untrusted (a hostile `toString` or an
 * accessor-backed `Error.message` can itself throw on this last-resort
 * path) — and byte-bound: the complete encoded sentinel stays within the
 * 256-byte `ToolResultBounds` schema floor, so it fits every accepted
 * policy without the caller re-applying bounds.
 *
 * @category operations
 * @since 0.1.0
 */
export const unserializableToolResult = (
  cause: unknown,
): typeof UnserializableToolResult.Encoded => {
  let reason: string;

  try {
    const message = cause instanceof Error ? cause.message : cause;

    reason = typeof message === "string" ? message : String(message);
  } catch {
    reason = "unserializable value";
  }

  const build = (clipped: string): typeof UnserializableToolResult.Encoded =>
    Schema.encodeSync(UnserializableToolResult)(
      UnserializableToolResult.make({
        unserializableToolResult: true,
        reason: clipped,
      }),
    );

  // JSON escaping inflates bytes, so shrink by whole code points until the
  // encoded sentinel fits the floor; the empty reason always fits.
  let clipped = takePrefixWithinBytes(reason, 128);
  let sentinel = build(clipped);

  while (clipped.length > 0 && utf8ByteLength(JSON.stringify(sentinel)) > 256) {
    const last = clipped.codePointAt(clipped.length - 1);

    clipped = clipped.slice(0, clipped.length - (last !== undefined && last > 0xffff ? 2 : 1));
    sentinel = build(clipped);
  }

  return sentinel;
};

const takePrefixWithinBytes = (value: string, maxBytes: number): string => {
  let bytes = 0;
  let index = 0;

  while (index < value.length) {
    const codePoint = value.codePointAt(index) ?? 0;
    const width = codePointUtf8Length(codePoint);

    if (bytes + width > maxBytes) break;
    bytes += width;
    index += codePoint > 0xffff ? 2 : 1;
  }

  return value.slice(0, index);
};

const takeSuffixWithinBytes = (value: string, maxBytes: number): string => {
  let bytes = 0;
  let index = value.length;

  while (index > 0) {
    const unit = value.charCodeAt(index - 1);
    const isLowSurrogate = unit >= 0xdc00 && unit <= 0xdfff;
    const pairStart = isLowSurrogate && index >= 2 ? index - 2 : index - 1;
    const codePoint = value.codePointAt(pairStart) ?? 0;
    const width = codePointUtf8Length(codePoint);

    if (bytes + width > maxBytes) break;
    bytes += width;
    index = pairStart;
  }

  return value.slice(index);
};

/**
 * Bound one encoded Tool result to `bounds.maxBytes` UTF-8 bytes.
 *
 * Within bounds the input is returned unchanged. Over bounds the result is
 * the JSON-encoded `TruncatedToolResult` envelope preserving an exact prefix
 * and suffix of the input, split ~50/50 across the byte budget left after
 * the envelope's own overhead and shrunk deterministically until the encoded
 * envelope itself fits. Slicing never splits a UTF-16 surrogate pair, so the
 * envelope always re-encodes as valid JSON.
 *
 * The Schema-level 256-byte floor on `ToolResultBounds.maxBytes` guarantees
 * every accepted bound fits the minimal envelope, including retention metadata, so the
 * returned value never exceeds `maxBytes` for a decodable bound. The
 * minimal-envelope fallback below survives purely as defense in depth.
 *
 * @category operations
 * @since 0.1.0
 */
export const applyToolResultBounds = (
  encodedJson: string,
  bounds: ToolResultBounds,
  retention: Pick<TruncatedToolResult, "fullResultId" | "fullResultError"> = {},
): string => {
  const originalBytes = utf8ByteLength(encodedJson);

  if (originalBytes <= bounds.maxBytes) return encodedJson;

  const render = (head: string, tail: string): string =>
    JSON.stringify(
      Schema.encodeSync(TruncatedToolResult)(
        TruncatedToolResult.make({
          truncatedToolResult: true,
          originalBytes,
          head,
          tail,
          ...retention,
        }),
      ),
    );

  const minimal = render("", "");
  const contentBudget = bounds.maxBytes - utf8ByteLength(minimal);

  if (contentBudget <= 0) return minimal;
  let headBudget = Math.floor(contentBudget / 2);
  let tailBudget = contentBudget - headBudget;

  for (;;) {
    const head = takePrefixWithinBytes(encodedJson, headBudget);
    const tail = takeSuffixWithinBytes(encodedJson, tailBudget);
    const output = render(head, tail);
    const outputBytes = utf8ByteLength(output);

    if (outputBytes <= bounds.maxBytes) return output;
    if (headBudget === 0 && tailBudget === 0) return minimal;
    const shrink = Math.max(1, Math.ceil((outputBytes - bounds.maxBytes) / 2));

    headBudget = Math.max(0, headBudget - shrink);
    tailBudget = Math.max(0, tailBudget - shrink);
  }
};
