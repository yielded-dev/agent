/**
 * Schema-owned final-output formats for Agent definitions.
 *
 * @since 0.1.0
 */

export {
  /**
   * Declare ordinary assistant text as an Agent's final-output wire format. Apply this to the
   * complete output Schema after composing transformations. Its encoded type must be string;
   * decoding, checks, transformations, and service requirements remain owned by that Schema.
   * Text is preserved verbatim, including whitespace, quotes, and an empty reply when allowed.
   * Required completion Tools still take precedence. Unmarked Schemas use JSON final output.
   *
   * @example
   * ```ts
   * output: Output.text(Schema.String.check(Schema.isMaxLength(20_000)))
   * ```
   *
   * @category operations
   * @since 0.1.0
   */
  textOutput as text,
} from "./internal/output-contract.ts";
