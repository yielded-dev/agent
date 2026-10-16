import * as Schema from "effect/Schema";

export const RunId = Schema.String.check(Schema.isPattern(/^[a-z0-9-]{1,24}$/));

/** Every response leaving the deployed checkout Worker identifies its deployment. */
export const workerRunHeader = "x-checkout-run-id";

/** Source-authored diagnostics only; never publish provider text or session capabilities. */
export const WorkerFailure = Schema.Literals([
  "scrape:PageCaptureNavigationError",
  "scrape:PageCaptureProtocolError",
  "scrape:PageCaptureRateLimitedError",
  "scrape:PageCaptureOutputLimitError",
  "scrape:PageCaptureUnsupportedError",
  "scrape:TimeoutError",
  "scrape:assertion",
  "login-readiness",
  "authority",
  "cleanup",
  "BrowserSessionError",
  "CodeModeFailure",
  "CodeModeDescriptionError",
  "CodeExecutorStartError",
  "CodeExecutorTerminatedError",
  "CodeExecutionTimeoutError",
  "CodeExecutionProtocolError",
  "CodeProgramFailedError",
  "CodeSourceError",
  "CodeOutputLimitError",
  "CodeHostCallLimitError",
  "BrowserUseError",
  "BrowserUseError:invalid",
  "BrowserUseError:browser",
  "SchemaError",
  "AiError",
  "AgentPolicyError",
  "AgentOutputError",
  "ModelProtocolError",
  "TypeError",
  "ReferenceError",
  "RangeError",
  "SyntaxError",
  "worker-failure",
]);

export const CheckoutPhase = Schema.Literals([
  "idle",
  "scrape",
  "acquire",
  "connect",
  "login-readiness",
  "agent",
  "observe",
  "act",
  "credential-email",
  "credential-password",
  "credential-submit",
  "submit",
  "receipt",
  "close",
  "complete",
]);

export const Receipt = Schema.Struct({
  buyer: Schema.String,
  product: Schema.String,
  color: Schema.String,
  size: Schema.String,
  quantity: Schema.Natural,
  address: Schema.String,
  shipping: Schema.String,
  subtotal: Schema.Natural,
  shippingCents: Schema.Natural,
  tax: Schema.Natural,
  total: Schema.Natural,
  currency: Schema.String,
  paid: Schema.Boolean,
});

export const BrowserErrorName = Schema.Literals([
  "TypeError",
  "RangeError",
  "SyntaxError",
  "TimeoutError",
  "SchemaError",
  "ProtocolError",
  "TargetCloseError",
  "ConnectionClosedError",
]);

/** Only authored browser stages and finite classifications may enter public proof evidence. */
export const BrowserDiagnostic = Schema.Struct({
  operation: Schema.Literals([
    "session.acquire",
    "session.connect",
    "session.page",
    "session.target",
    "session.command",
    "session.close",
    "session.disconnect",
    "browser-use.frame.target-info",
    "browser-use.frame.attach",
    "browser-use.frame.send",
  ]),
  reason: Schema.Literals([
    "provider",
    "timeout",
    "malformed",
    "configuration",
    "authorization",
    "rate-limited",
    "pending",
  ]),
  status: Schema.NullOr(Schema.Int.check(Schema.isBetween({ minimum: 100, maximum: 599 }))),
  errorName: Schema.optionalKey(BrowserErrorName),
});

export const BrowserDiagnostics = Schema.Array(BrowserDiagnostic).check(Schema.isMaxLength(16));

export const Evidence = Schema.Struct({
  phase: CheckoutPhase,
  started: Schema.Boolean,
  attempts: Schema.Natural,
  receipt: Schema.NullOr(Receipt),
  closed: Schema.Boolean,
  scrapeAttempts: Schema.Natural,
  loginRequests: Schema.Natural,
  failure: Schema.NullOr(Schema.String),
  browserDiagnostics: BrowserDiagnostics,
});

export class CheckoutError extends Schema.TaggedError<CheckoutError>()("CheckoutError", {
  stage: Schema.String,
  message: Schema.String,
}) {}
