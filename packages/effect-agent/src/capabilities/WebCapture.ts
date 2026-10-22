import * as Schema from "effect/Schema";

import { PageScrapeCaptured } from "../sandbox/PageCapture.ts";

const maxResponseBytesLimit = 5 * 1024 * 1024;
const maxFailureTextLength = 4 * 1024;
const BoundedFailureText = Schema.String.check(Schema.isMaxLength(maxFailureTextLength));
const BoundedErrorTag = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const BoundedUrl = Schema.NonEmptyString.check(Schema.isMaxLength(8 * 1024));
const BoundedContent = Schema.String.check(Schema.isMaxLength(maxResponseBytesLimit));
const BoundedSelector = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));

/** Model-selectable actions for a page-reading Tool. */
export const WebCaptureAction = Schema.Literals(["markdown", "content", "links"]);
export type WebCaptureAction = typeof WebCaptureAction.Type;

/** Model-decoded parameters: one absolute https URL plus one capture action. */
export const WebCaptureParameters = Schema.Struct({
  url: BoundedUrl,
  action: WebCaptureAction,
});

/** Model-decoded scrape parameters: one allowed URL and 1..64 CSS selectors. */
export const WebCaptureScrapeParameters = Schema.Struct({
  url: BoundedUrl,
  selectors: Schema.Array(BoundedSelector).check(Schema.isMinLength(1), Schema.isMaxLength(64)),
});

/**
 * The bounded model-visible capture success. Exactly the field matching the
 * requested action is present.
 */
export class WebCaptureSuccess extends Schema.Class<WebCaptureSuccess>(
  "@effect-agent/capabilities/WebCaptureSuccess",
)({
  url: BoundedUrl,
  action: WebCaptureAction,
  markdown: Schema.optionalKey(BoundedContent),
  html: Schema.optionalKey(BoundedContent),
  links: Schema.optionalKey(Schema.Array(BoundedUrl).check(Schema.isMaxLength(4_096))),
}) {}

/**
 * The bounded model-visible failure envelope. `failureMode: "return"` turns
 * it into a failed Tool result, so a model can pick a different target or
 * action without a blind retry. `retryAfterMillis` carries the platform's
 * backoff hint when the capture was rate-limited.
 */
export class WebCaptureFailure extends Schema.TaggedError<WebCaptureFailure>()(
  "WebCaptureFailure",
  {
    errorTag: BoundedErrorTag,
    message: BoundedFailureText,
    retryAfterMillis: Schema.optionalKey(Schema.Natural),
  },
) {}

/** The bounded model-visible selector scrape success. */
export class WebCaptureScrapeSuccess extends Schema.Class<WebCaptureScrapeSuccess>(
  "@effect-agent/capabilities/WebCaptureScrapeSuccess",
)({
  url: BoundedUrl,
  groups: PageScrapeCaptured.fields.groups,
}) {}
