import { Schema } from "effect";

export const RunId = Schema.String.check(Schema.isPattern(/^[a-z0-9-]{1,24}$/));

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
  "credential",
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
});

export const BrowserDiagnostics = Schema.Array(BrowserDiagnostic).check(Schema.isMaxLength(16));

export const LoginCall = Schema.Struct({
  call: Schema.Literals(["email-type", "password-type", "wait-navigation", "click-login"]),
  state: Schema.Literals(["started", "succeeded", "failed"]),
  elapsedMillis: Schema.Natural,
  errorName: Schema.NullOr(
    Schema.Literals([
      "Error",
      "ProtocolError",
      "TargetCloseError",
      "TimeoutError",
      "ConnectionClosedError",
      "TypeError",
      "unknown",
    ]),
  ),
});

export const LoginCalls = Schema.Array(LoginCall).check(Schema.isMaxLength(8));

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
  loginCalls: LoginCalls,
  loginSessionFailure: Schema.NullOr(
    Schema.Struct({ reason: Schema.String, dispatch: Schema.String, cleanup: Schema.String }),
  ),
});

export class CheckoutError extends Schema.TaggedError<CheckoutError>()("CheckoutError", {
  stage: Schema.String,
  message: Schema.String,
}) {}
