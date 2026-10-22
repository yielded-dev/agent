/**
 * Stateless capture of one bounded PNG image. Returned bytes are caller-owned and carry no
 * persistence or later handoff guarantee.
 *
 * @since 0.1.0
 */

import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  PageCaptureEngine,
  PageCaptureNavigationError,
  PageCaptureProtocolError,
  PageCaptureRateLimitedError,
  PageCaptureTarget,
  PageCaptureUnsupportedError,
  PageNavigationOptions,
  PageResourcePolicy,
  PageViewport,
} from "./PageCapture.ts";
import { SandboxImplementation } from "./Sandbox.ts";

const MAX_SCREENSHOT_BYTES = 8 * 1024 * 1024;
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));

/**
 * The byte limit and full-page choice are fixed before Browser Run starts.
 *
 * @category models
 * @since 0.1.0
 */
export class PageScreenshotLimits extends Schema.Class<PageScreenshotLimits>(
  "PageScreenshotLimits",
)({
  maxOutputBytes: PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_SCREENSHOT_BYTES)),
}) {}

/**
 * Schema-first request for exactly one PNG screenshot.
 *
 * @category models
 * @since 0.1.0
 */
export class PageScreenshotRequest extends Schema.Class<PageScreenshotRequest>(
  "PageScreenshotRequest",
)({
  target: PageCaptureTarget,
  engine: PageCaptureEngine,
  limits: PageScreenshotLimits,
  fullPage: Schema.Boolean,
  navigation: Schema.optionalKey(PageNavigationOptions),
  viewport: Schema.optionalKey(PageViewport),
  resourcePolicy: Schema.optionalKey(PageResourcePolicy),
}) {}

/**
 * The only successful output: bytes are caller-owned and never durable framework data.
 *
 * @category models
 * @since 0.1.0
 */
export class PageScreenshotResult extends Schema.Class<PageScreenshotResult>(
  "PageScreenshotResult",
)({
  implementation: SandboxImplementation,
  mediaType: Schema.Literal("image/png"),
  bytes: Schema.Uint8Array.check(Schema.isMaxLength(MAX_SCREENSHOT_BYTES)),
}) {}

/**
 * The response crossed the request's PNG byte limit.
 *
 * @category errors
 * @since 0.1.0
 */
export class PageScreenshotOutputLimitError extends Schema.TaggedError<PageScreenshotOutputLimitError>()(
  "PageScreenshotOutputLimitError",
  {
    implementation: SandboxImplementation,
    limit: PositiveInt,
    observed: Schema.Natural,
  },
) {}

/**
 * Expected screenshot failures. The shared capture errors keep identical semantics.
 *
 * @category schemas
 * @since 0.1.0
 */
export const PageScreenshotError = Schema.Union([
  PageCaptureRateLimitedError,
  PageCaptureNavigationError,
  PageCaptureUnsupportedError,
  PageCaptureProtocolError,
  PageScreenshotOutputLimitError,
]);

/**
 * Decoded value of {@link PageScreenshotError}.
 *
 * @category models
 * @since 0.1.0
 */
export type PageScreenshotError = typeof PageScreenshotError.Type;

/**
 * Stateless, one-output PNG capture port. It owns neither persistence nor later byte handoff.
 *
 * @category services
 * @since 0.1.0
 */
export class PageScreenshot extends Context.Service<
  PageScreenshot,
  {
    readonly capture: (
      request: PageScreenshotRequest,
    ) => Effect.Effect<PageScreenshotResult, PageScreenshotError>;
  }
>()("@effect-agent/sandbox/PageScreenshot") {}

/**
 * Adapter function for one bounded PNG capture. Returned bytes belong to the caller.
 *
 * @category models
 * @since 0.1.0
 */
export type PageScreenshotCapture = (
  request: PageScreenshotRequest,
) => Effect.Effect<PageScreenshotResult, PageScreenshotError>;
