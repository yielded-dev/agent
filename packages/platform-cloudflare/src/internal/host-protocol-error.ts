import * as Schema from "effect/Schema";

/** Ceiling for host protocol diagnostic strings. */
const MAX_HOST_DIAGNOSTIC_LENGTH = 4_096;

const BoundedDiagnostic = Schema.String.check(Schema.isMaxLength(MAX_HOST_DIAGNOSTIC_LENGTH));

/**
 * Truncate a diagnostic string to the host protocol's bounded length.
 *
 * @category models
 * @since 0.1.0
 */
export const boundHostDiagnostic = (value: string): string =>
  value.length > MAX_HOST_DIAGNOSTIC_LENGTH
    ? `${value.slice(0, MAX_HOST_DIAGNOSTIC_LENGTH - 3)}...`
    : value;

/**
 * The envelope itself could not be honored: the Object could not decode the request, or a
 * response could not be encoded/decoded. Never carries operation semantics.
 *
 * @category errors
 * @since 0.1.0
 */
export class HostProtocolError extends Schema.TaggedError<HostProtocolError>()(
  "HostProtocolError",
  {
    message: BoundedDiagnostic,
  },
) {}
