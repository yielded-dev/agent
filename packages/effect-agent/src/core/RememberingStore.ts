import * as Schema from "effect/Schema";

const Identity = Schema.NonEmptyString.check(Schema.isMaxLength(256));

const Counter = Schema.Int.check(
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
);

/** Host source-order lineage. Sequence is monotonic only within one authorityGeneration.
 * Opaque revision strings and different authority generations are never ordered.
 */
export const SourcePosition = Schema.Struct({
  authorityGeneration: Identity,
  sequence: Counter,
});

export type SourcePosition = typeof SourcePosition.Type;

export const comparePosition = (left: SourcePosition, right: SourcePosition): number | undefined =>
  left.authorityGeneration === right.authorityGeneration
    ? Math.sign(left.sequence - right.sequence)
    : undefined;
