import * as FailureDiagnostic from "@yielded/agent/failure-diagnostic";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import type * as Effect from "effect/Effect";
import * as ErrorReporter from "effect/ErrorReporter";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

/** Browser stage and classification. Original causes stay private and out of public JSON. */
export class BrowserRunFailure extends Data.TaggedError("BrowserRunFailure")<{
  readonly operation: string;
  readonly reason:
    | "provider"
    | "timeout"
    | "malformed"
    | "configuration"
    | "authorization"
    | "rate-limited"
    | "pending";
  readonly status?: number;
  readonly cause?: BrowserRunFailure | { readonly name: string };
}> {
  readonly [ErrorReporter.severity] = "Error" as const;
}

/** A public projection of an already reported private failure must not produce another issue. */
export const reportedBrowserError = <A extends object>(error: A): A =>
  Object.assign(error, { [ErrorReporter.ignore]: true });

export const inheritBrowserReport = <A extends object>(original: unknown, error: A): A =>
  ErrorReporter.isIgnored(original) ? reportedBrowserError(error) : error;

const reasonSchema = Schema.Literals([
  "provider",
  "timeout",
  "malformed",
  "configuration",
  "authorization",
  "rate-limited",
  "pending",
]);

const statusSchema = Schema.Int.check(Schema.isBetween({ minimum: 100, maximum: 599 }));

const failureNames = new Set([
  "TypeError",
  "RangeError",
  "SyntaxError",
  "TimeoutError",
  "SchemaError",
  "ProtocolError",
  "TargetCloseError",
  "ConnectionClosedError",
]);

/** Construct a reporter-safe copy, never the error carrying the private original cause. */
const projectBrowserFailure = (operation: string, cause?: unknown): BrowserRunFailure => {
  const source = cause instanceof BrowserRunFailure ? cause.cause : cause;

  return new BrowserRunFailure({
    operation: cause instanceof BrowserRunFailure ? cause.operation : operation,
    reason:
      cause instanceof BrowserRunFailure
        ? cause.reason
        : Predicate.isObject(cause) && "reason" in cause && Schema.is(reasonSchema)(cause.reason)
          ? cause.reason
          : "provider",
    ...(Predicate.isObject(cause) && "status" in cause && Schema.is(statusSchema)(cause.status)
      ? { status: cause.status }
      : {}),
    ...(source instanceof BrowserRunFailure
      ? { cause: projectBrowserFailure(operation, source) }
      : Predicate.isObject(source) &&
          "name" in source &&
          typeof source.name === "string" &&
          failureNames.has(source.name)
        ? { cause: { name: source.name } }
        : {}),
  });
};

export const browserFailure = (operation: string, cause?: unknown): BrowserRunFailure => {
  if (cause instanceof BrowserRunFailure) return cause;

  const failure = projectBrowserFailure(operation, cause);

  return cause === undefined ? failure : FailureDiagnostic.withPrivateCause(failure, cause);
};

/** Keep mixed defects and interruptions distinct while discarding all foreign diagnostic content. */
export const reportBrowserCause = <E>(
  operation: string,
  cause: Cause.Cause<E>,
): Effect.Effect<void> =>
  ErrorReporter.report(
    Cause.fromReasons(
      cause.reasons
        .filter((reason) => !Cause.isFailReason(reason) || !ErrorReporter.isIgnored(reason.error))
        .map((reason) => {
          switch (reason._tag) {
            case "Fail":
              return Cause.makeFailReason(projectBrowserFailure(operation, reason.error));
            case "Die":
              return Cause.makeDieReason(projectBrowserFailure(operation, reason.defect));
            case "Interrupt":
              return reason;
          }
        }),
    ),
  );
