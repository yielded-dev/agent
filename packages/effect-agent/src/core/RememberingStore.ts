/**
 * Source-order positions for host-managed remembering workflows. Positions are comparable only
 * within the same authority generation.
 *
 * @since 0.1.0
 */

import * as Schema from "effect/Schema";

const Identity = Schema.NonEmptyString.check(Schema.isMaxLength(256));

const Counter = Schema.Int.check(
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
);

/**
 * Host source-order lineage. Sequence is monotonic only within one authorityGeneration.
 * Opaque revision strings and different authority generations are never ordered.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SourcePosition = Schema.Struct({
  authorityGeneration: Identity,
  sequence: Counter,
});

/**
 * Decoded value of {@link SourcePosition}.
 *
 * @category models
 * @since 0.1.0
 */
export type SourcePosition = typeof SourcePosition.Type;

/**
 * Compare sequence positions within the same authority generation. Returns undefined when the
 * generations cannot be ordered.
 *
 * @category operations
 * @since 0.1.0
 */
export const comparePosition = (left: SourcePosition, right: SourcePosition): number | undefined =>
  left.authorityGeneration === right.authorityGeneration
    ? Math.sign(left.sequence - right.sequence)
    : undefined;
